#!/usr/bin/env python3
"""Dreame LoxBerry Gateway — bridges Dreame/MOVA cloud to LoxBerry MQTT."""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import secrets
import signal
import ssl
import sys
import time
from pathlib import Path

import aiohttp
import aiomqtt
import paho.mqtt.client as paho_mqtt
import threading
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

# ── Brand configuration ───────────────────────────────────────────────────────
BRAND_CONFIG = {
    "dreame": {
        "domain": "eu.iot.dreame.tech:13267",
        "tenant_id": "000000",
        "authorization": "Basic ZHJlYW1lX2FwcHYxOkFQXmR2QHpAU1FZVnhOODg=",
        "meta": "cv=i_829",
        "rlc_key": "EETjszu*XI5znHsI",
        "mqtt_fallback": "app.mt.eu.iot.dreame.tech:19973",
        "iot_com_prefix": "10000",
    },
    "mova": {
        "domain": "eu.iot.mova-tech.com:13267",
        "tenant_id": "000002",
        "authorization": "Basic bW92YV9hcHA6VjdLb0NoTFc4dkhBQ3FHYg==",
        "meta": "cv=i_829",
        "rlc_key": "gigxlmqwZ]7oWZUF",
        "mqtt_fallback": "app.mt.eu.iot.mova-tech.com:19974",
        "iot_com_prefix": "20000",
    },
}


# ── Krypto-Hilfsfunktionen ────────────────────────────────────────────────────
def _pad_pkcs7(data: bytes, block_size: int = 16) -> bytes:
    pad_len = block_size - (len(data) % block_size)
    return data + bytes([pad_len] * pad_len)


def _compute_rlc(rlc_key: str) -> str:
    """AES-128-ECB(key=rlc_key, plaintext='eu|en|DE') → hex string."""
    key = rlc_key.encode("utf-8")
    plaintext = _pad_pkcs7(b"eu|en|DE")
    cipher = Cipher(algorithms.AES(key), modes.ECB(), backend=default_backend())
    enc = cipher.encryptor()
    return (enc.update(plaintext) + enc.finalize()).hex()


def _md5_password(password: str) -> str:
    """MD5(password + salt) → hex string. Protocol-mandated by Dreame cloud API."""
    return hashlib.md5((password + "RAylYC%fmSKp7%Tq").encode()).hexdigest()


# ── Gerätetyp-Erkennung ───────────────────────────────────────────────────────
def _get_device_type(model: str) -> str:
    return "mower" if "mower" in model.lower() else "vacuum"


# ── Binär-Parser (Mähroboter siid=1 piid=1) ──────────────────────────────────
def parse_binary_state_1(value) -> dict:
    if isinstance(value, str):
        return {}
    buf = bytes(value)
    if len(buf) < 19:
        return {}
    error_code    = int.from_bytes(buf[1:5], "little")
    battery       = buf[11] & 0x7F
    charging      = (buf[11] & 0x80) >> 7
    robot_state   = buf[14]
    docking_state = (robot_state & 0x1C) >> 2
    wifi_rssi     = buf[17] - 256 if buf[17] > 127 else buf[17]
    return {
        "error_code":    error_code,
        "battery":       battery,
        "charging":      charging,
        "robot_state":   robot_state,
        "docking_state": docking_state,
        "wifi_rssi":     wifi_rssi,
    }


# ── AutoSwitch-Normalisierung ─────────────────────────────────────────────────
def _normalize_autoswitch(value) -> dict:
    if isinstance(value, dict) and "k" in value and "v" in value:
        return {value["k"]: value["v"]}
    if isinstance(value, list):
        return {
            item["k"]: item["v"]
            for item in value
            if isinstance(item, dict) and "k" in item and "v" in item
        }
    return {}


# ── State-JSON-Builder ────────────────────────────────────────────────────────
_MOWER_STATUS_STR = {
    0: "Idle", 1: "Working", 2: "Paused", 3: "Returning",
    4: "Charging", 5: "Error", 6: "Docking",
}
_VACUUM_STATUS_STR = {
    0: "Idle", 1: "Cleaning", 2: "Returning", 3: "Charging",
    4: "Error", 5: "Paused", 6: "Sleeping",
}


def build_state_json(device: dict, props: dict) -> dict:
    """Map siid/piid property dict → LoxBerry state JSON for this device."""
    dt = device.get("device_type", "vacuum")
    state: dict = {
        "device_type": dt,
        "name":        device.get("name", ""),
        "model":       device.get("model", ""),
        "did":         device.get("did", ""),
        "online":      device.get("online", False),
    }
    if dt == "mower":
        status = props.get((3, 1), 0)
        state.update({
            "status":          status,
            "status_str":      _MOWER_STATUS_STR.get(status, "Unknown"),
            "battery":         props.get((3, 2), 0),
            "charging":        props.get((3, 3), 0),
            "error":           props.get((3, 5), 0),
            "mowing_time_min": props.get((12, 1), 0),
            "mowing_area_m2":  props.get((12, 2), 0),
            "task_status":     props.get((2, 2), 0),
            "warn_status":     props.get((2, 3), 0),
        })
    else:
        status = props.get((4, 1), 0)
        state.update({
            "status":              status,
            "status_str":          _VACUUM_STATUS_STR.get(status, "Unknown"),
            "battery":             props.get((3, 1), 0),
            "charging":            props.get((3, 2), 0),
            "error":               props.get((4, 3), 0),
            "cleaning_time_min":   props.get((4, 13), 0),
            "cleaned_area_m2":     props.get((4, 14), 0),
            "suction_level":       props.get((4, 4), 0),
            "water_volume":        props.get((4, 5), 0),
            "cleaning_mode":       props.get((4, 23), 0),
            "task_status":         props.get((4, 2), 0),
            "warn_status":         props.get((4, 22), 0),
            "dnd_enabled":         props.get((5, 1), 0),
            "child_lock":          props.get((4, 27), 0),
        })
    return state


def build_station_json(props: dict) -> dict:
    """Map siid 25 properties → LoxBerry state_station JSON (vacuum only)."""
    cw = props.get((25, 1), 0)
    dw = props.get((25, 2), 0)
    db = props.get((25, 3), 0)
    cw_str = {0: "Installed", 1: "Not installed", 2: "Low water"}.get(cw, "Unknown")
    dw_str = {0: "Installed", 1: "Not installed/Full"}.get(dw, "Unknown")
    db_str = {0: "Installed", 1: "Not installed", 2: "Check"}.get(db, "Unknown")
    return {
        "clean_water_tank":     cw,
        "clean_water_tank_str": cw_str,
        "dirty_water_tank":     dw,
        "dirty_water_tank_str": dw_str,
        "dust_bag":             db,
        "dust_bag_str":         db_str,
        "detergent":            props.get((25, 4), 0),
        "hot_water":            props.get((25, 5), 0),
    }


# ── CLI-Args ──────────────────────────────────────────────────────────────────
_ap = argparse.ArgumentParser(add_help=False)
_ap.add_argument("--logfile",   default="")
_ap.add_argument("--logdbkey",  default="")
_ap.add_argument("--configdir", default="")
_ap.add_argument("--lbsconfig", default="/opt/loxberry/config/system")
_ap.add_argument("--loglevel",  type=int, default=6)
_args, _ = _ap.parse_known_args()

# ── Pfade ─────────────────────────────────────────────────────────────────────
LBHOMEDIR    = os.environ.get("LBHOMEDIR", "/opt/loxberry")
LBSCONFIG    = Path(_args.lbsconfig)
CONFIGDIR    = Path(_args.configdir) if _args.configdir else Path(LBHOMEDIR) / "config/plugins/dreame"
GENERAL_JSON = LBSCONFIG / "general.json"
PLUGIN_CFG   = CONFIGDIR / "pluginconfig.json"
PID_FILE     = Path("/dev/shm/dreame_gateway.pid")

# ── Logging ───────────────────────────────────────────────────────────────────
_loglevel = _args.loglevel
_logfile  = _args.logfile
_logger = logging.getLogger("dreame_gateway")
_logger.propagate = False
_logger.setLevel(logging.DEBUG)
_handler = (
    logging.FileHandler(_logfile, mode="a", encoding="utf-8")
    if _logfile else logging.StreamHandler(sys.stdout)
)
_handler.setFormatter(logging.Formatter(
    "%(asctime)s.%(msecs)03d <%(levelname)s> %(message)s",
    datefmt="%H:%M:%S"
))
_logger.addHandler(_handler)


def _log(level: int, levelname: str, msg: str) -> None:
    if level <= _loglevel:
        record = logging.LogRecord(
            name=_logger.name, level=logging.DEBUG,
            pathname="", lineno=0, msg=msg, args=(), exc_info=None,
        )
        record.levelname = levelname
        _handler.emit(record)


def LOGSTART(msg: str) -> None: _log(5, "OK",    msg)
def LOGERR(msg: str)   -> None: _log(3, "ERR",   msg)
def LOGWARN(msg: str)  -> None: _log(4, "WARN",  msg)
def LOGOK(msg: str)    -> None: _log(5, "OK",    msg)
def LOGINF(msg: str)   -> None: _log(6, "INFO",  msg)
def LOGDEB(msg: str)   -> None: _log(7, "DEBUG", msg)


def _logend() -> None:
    dbkey = _args.logdbkey
    if not dbkey:
        return
    if not re.match(r'^[\w]+$', dbkey):
        return
    os.system(
        f'perl -e \'use LoxBerry::Log; '
        f'my $l = LoxBerry::Log->new(dbkey => "{dbkey}", append => 1); '
        f'LOGEND "Gateway stopped."; exit;\''
    )


# ── Config ────────────────────────────────────────────────────────────────────
def _load_json(path: Path) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        LOGERR(f"Cannot read {path}: {e}")
        return {}


def _save_json_atomic(path: Path, data: dict) -> None:
    tmp = path.with_suffix(".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        tmp.replace(path)
    except Exception as e:
        tmp.unlink(missing_ok=True)
        LOGERR(f"Cannot write {path}: {e}")


def load_plugin_config() -> dict:
    cfg = _load_json(PLUGIN_CFG)
    cfg.setdefault("cloud_service",        "dreame")
    cfg.setdefault("username",             "")
    cfg.setdefault("password_hash",        "")
    cfg.setdefault("access_token",         "")
    cfg.setdefault("refresh_token",        "")
    cfg.setdefault("expires_at",           0)
    cfg.setdefault("uid",                  "")
    cfg.setdefault("base_topic",           "dreame")
    cfg.setdefault("polling_interval_min", 30)
    cfg.setdefault("devices",             [])
    return cfg


def save_plugin_config(cfg: dict) -> None:
    _save_json_atomic(PLUGIN_CFG, cfg)


def _is_enabled(val) -> bool:
    return str(val).strip().lower() in ("true", "1", "yes", "on")


def _str_or_none(val) -> "str | None":
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def get_mqtt_broker_config(general: dict) -> dict:
    mqtt = general.get("Mqtt", {})
    host     = mqtt.get("Brokerhost", "localhost")
    port     = int(mqtt.get("Brokerport", 1883))
    username = _str_or_none(mqtt.get("Brokeruser"))
    password = _str_or_none(mqtt.get("Brokerpass"))
    use_local = _is_enabled(mqtt.get("Uselocalbroker", "true"))
    tls = False
    tls_verify = False
    tls_cafile = None
    if use_local and _is_enabled(mqtt.get("Tlsenabled", "false")):
        tls        = True
        tls_verify = False
        tls_cafile = "/etc/mosquitto/tls/ca.crt"
        port       = int(mqtt.get("Tlsport", 8883))
    elif not use_local and _is_enabled(mqtt.get("TlsExternalEnabled", "false")):
        tls        = True
        tls_verify = _is_enabled(mqtt.get("TlsExternalValidatecert", "false"))
        tls_cafile = None
        if mqtt.get("TlsExternalPort"):
            port = int(mqtt.get("TlsExternalPort"))
    return {
        "host": host, "port": port,
        "username": username, "password": password,
        "tls": tls, "tls_verify": tls_verify, "tls_cafile": tls_cafile,
    }


def _build_mqtt_kwargs(broker: dict) -> dict:
    kwargs: dict = {"hostname": broker["host"], "port": broker["port"]}
    if broker.get("username"):
        kwargs["username"] = broker["username"]
    if broker.get("password"):
        kwargs["password"] = broker["password"]
    if broker.get("tls"):
        ctx = ssl.create_default_context()
        if not broker.get("tls_verify"):
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        elif broker.get("tls_cafile") and os.path.isfile(broker["tls_cafile"]):
            ctx.load_verify_locations(broker["tls_cafile"])
        kwargs["tls_context"] = ctx
    return kwargs


# ── PID + Shutdown ────────────────────────────────────────────────────────────
def write_pid() -> None:
    try:
        PID_FILE.write_text(str(os.getpid()), encoding="utf-8")
    except Exception as e:
        LOGERR(f"Cannot write PID: {e}")


def remove_pid() -> None:
    PID_FILE.unlink(missing_ok=True)


_shutdown_event: asyncio.Event = asyncio.Event()


def _handle_sigterm(*_) -> None:
    LOGINF("SIGTERM received — shutting down")
    _shutdown_event.set()


# ── Dreame Auth ───────────────────────────────────────────────────────────────
def _build_dreame_headers(brand: dict, access_token: "str | None" = None) -> dict:
    """Build required HTTP headers for Dreame cloud API calls."""
    headers = {
        "user-agent":    "Dart/3.2 (dart:io)",
        "dreame-meta":   brand["meta"],
        "dreame-rlc":    _compute_rlc(brand["rlc_key"]),
        "tenant-id":     brand["tenant_id"],
        "authorization": brand["authorization"],
    }
    if access_token:
        headers["dreame-auth"] = f"bearer {access_token}"
    return headers


async def dreame_login(
    session: aiohttp.ClientSession,
    brand: dict,
    username: str,
    password: str,
) -> dict:
    """POST /dreame-auth/oauth/token → {access_token, refresh_token, expires_in, uid}."""
    headers = _build_dreame_headers(brand)
    headers["content-type"] = "application/x-www-form-urlencoded"
    data = {
        "grant_type": "password",
        "scope":      "all",
        "platform":   "IOS",
        "type":       "account",
        "username":   username,
        "password":   _md5_password(password),
        "country":    "DE",
        "lang":       "de",
    }
    url = f"https://{brand['domain']}/dreame-auth/oauth/token"
    async with session.post(url, headers=headers, data=data, ssl=False) as resp:
        resp.raise_for_status()
        body = await resp.json(content_type=None)
    if "access_token" not in body:
        raise RuntimeError(f"Login failed: {body}")
    return {
        "access_token":  body["access_token"],
        "refresh_token": body.get("refresh_token", ""),
        "expires_in":    int(body.get("expires_in", 3600)),
        "uid":           str(body.get("uid", "")),
    }


async def dreame_refresh_token(
    session: aiohttp.ClientSession,
    brand: dict,
    refresh_token: str,
) -> dict:
    """POST /dreame-auth/oauth/token with grant_type=refresh_token."""
    headers = _build_dreame_headers(brand)
    headers["content-type"] = "application/x-www-form-urlencoded"
    data = {
        "grant_type":    "refresh_token",
        "refresh_token": refresh_token,
    }
    url = f"https://{brand['domain']}/dreame-auth/oauth/token"
    async with session.post(url, headers=headers, data=data, ssl=False) as resp:
        resp.raise_for_status()
        body = await resp.json(content_type=None)
    if "access_token" not in body:
        raise RuntimeError(f"Token refresh failed: {body}")
    return {
        "access_token":  body["access_token"],
        "refresh_token": body.get("refresh_token", refresh_token),
        "expires_in":    int(body.get("expires_in", 3600)),
        "uid":           str(body.get("uid", "")),
    }


# ── Dreame REST API ───────────────────────────────────────────────────────────
_request_id = 1


def _next_request_id() -> int:
    global _request_id
    _request_id = (_request_id % 99999) + 1
    return _request_id


def _parse_device_list(records: list) -> list:
    devices = []
    for r in records:
        model = r.get("model", "")
        devices.append({
            "did":         r.get("did", ""),
            "model":       model,
            "name":        r.get("customName") or model,
            "device_type": _get_device_type(model),
            "bind_domain": r.get("bindDomain", ""),
            "online":      bool(r.get("online", False)),
        })
    return devices


async def get_device_list(
    session: aiohttp.ClientSession,
    brand: dict,
    access_token: str,
) -> list:
    """POST /dreame-user-iot/iotuserbind/device/listV2 → list of device dicts."""
    url = f"https://{brand['domain']}/dreame-user-iot/iotuserbind/device/listV2"
    headers = _build_dreame_headers(brand, access_token)
    headers["content-type"] = "application/json"
    body = {
        "sharedStatus": 1, "current": 1, "size": 100,
        "lang": "de", "timestamp": int(time.time() * 1000),
    }
    async with session.post(url, headers=headers, json=body, ssl=False) as resp:
        resp.raise_for_status()
        data = await resp.json(content_type=None)
    records = data.get("result", {}).get("records", [])
    return _parse_device_list(records)


async def send_command(
    session: aiohttp.ClientSession,
    brand: dict,
    access_token: str,
    did: str,
    method: str,
    params,
) -> dict:
    """POST sendCommand to Dreame cloud → result dict."""
    req_id = _next_request_id()
    prefix = brand["iot_com_prefix"]
    url = f"https://{brand['domain']}/dreame-iot-com-{prefix}/device/sendCommand"
    headers = _build_dreame_headers(brand, access_token)
    headers["content-type"] = "application/json"
    body = {
        "did": did, "id": req_id,
        "data": {
            "did": did, "id": req_id,
            "method": method, "params": params, "from": "XXXXXX",
        },
    }
    async with session.post(url, headers=headers, json=body, ssl=False) as resp:
        resp.raise_for_status()
        return await resp.json(content_type=None)


async def send_mower_command(
    session: aiohttp.ClientSession,
    brand: dict,
    access_token: str,
    did: str,
    payload: dict,
) -> dict:
    """Mower special channel: action siid=2 aiid=50 with arbitrary payload."""
    params = {"did": did, "siid": 2, "aiid": 50, "in": [payload]}
    return await send_command(session, brand, access_token, did, "action", params)


async def get_properties(
    session: aiohttp.ClientSession,
    brand: dict,
    access_token: str,
    did: str,
    siid_piid_list: list,
) -> dict:
    """get_properties for a list of (siid, piid) pairs → {(siid, piid): value}."""
    params = [{"did": did, "siid": s, "piid": p} for s, p in siid_piid_list]
    result = await send_command(session, brand, access_token, did, "get_properties", params)
    out = {}
    for item in result.get("result", {}).get("data", {}).get("result", []):
        siid = item.get("siid")
        piid = item.get("piid")
        if siid is not None and piid is not None and "value" in item:
            out[(siid, piid)] = item["value"]
    return out


async def load_mower_settings(
    session: aiohttp.ClientSession,
    brand: dict,
    access_token: str,
    did: str,
) -> dict:
    """Load mower CFG via getCFG action (siid=2 aiid=50) → settings dict."""
    resp = await send_mower_command(session, brand, access_token, did, {"m": "g", "t": "CFG"})
    raw = resp.get("result", {}).get("data", {}).get("result", {})
    out: dict = {}

    def _cfg_int(key: str) -> "int | None":
        val = raw.get(key)
        if val is None:
            return None
        return val.get("value", 0) if isinstance(val, dict) else int(val)

    for cfg_key, out_key in [
        ("WRP", "rain_protection"), ("FDP", "frost_protection"),
        ("VOL", "volume"), ("CLS", "child_lock"),
        ("STUN", "anti_theft"), ("AOP", "ai_obstacle"),
        ("PROT", "grass_protection"), ("PATH", "path_display"),
    ]:
        v = _cfg_int(cfg_key)
        if v is not None:
            out[out_key] = v

    # WRP also has time + sen fields
    wrp = raw.get("WRP")
    if isinstance(wrp, dict):
        out["rain_delay_min"] = wrp.get("time", 0)

    # PRE: cutting preferences array [zone, mow_mode, cutting_height_mm, ...]
    pre = raw.get("PRE")
    arr = pre.get("value", []) if isinstance(pre, dict) else (pre if isinstance(pre, list) else [])
    out["_pre_array"] = arr
    if len(arr) > 2:
        out["cutting_height_mm"] = arr[2]
    if len(arr) > 1:
        out["mow_mode"] = arr[1]
    if len(arr) > 9:
        out["edge_mowing"] = arr[9]
    if len(arr) > 8:
        out["edge_detection"] = arr[8]
    if len(arr) > 5:
        out["direction_change"] = arr[5]

    # CMS: consumables [blade_hours_raw, brush_hours_raw, robot_hours_raw]
    cms = raw.get("CMS")
    cms_val = cms.get("value", [0, 0, 0]) if isinstance(cms, dict) else (cms if isinstance(cms, list) else [0, 0, 0])
    out["_cms_array"] = cms_val

    return out


async def load_mower_history(
    session: aiohttp.ClientSession,
    brand: dict,
    access_token: str,
    did: str,
    uid: str,
) -> list:
    """Last 20 mowing sessions via /dreame-user-iot/mower/history/listV2."""
    url = f"https://{brand['domain']}/dreame-user-iot/mower/history/listV2"
    headers = _build_dreame_headers(brand, access_token)
    headers["content-type"] = "application/json"
    now = int(time.time())
    body = {
        "did": did, "uid": uid,
        "time_start": now - 86400 * 90,
        "time_end": now,
        "limit": 20,
        "from": 0,
        "region": "eu",  # plugin targets EU cloud only
    }
    async with session.post(url, headers=headers, json=body, ssl=False) as resp:
        resp.raise_for_status()
        data = await resp.json(content_type=None)
    return data.get("result", {}).get("records", [])


async def load_statistic(
    session: aiohttp.ClientSession,
    brand: dict,
    access_token: str,
    did: str,
    device_type: str,
) -> dict:
    """Load consumable/statistic properties per device type."""
    if device_type == "mower":
        props = await get_properties(session, brand, access_token, did, [
            (12, 2), (12, 3), (12, 4),
        ])
        return {
            "total_mow_time_min": props.get((12, 2), 0),
            "total_mow_count":    props.get((12, 3), 0),
            "total_mow_area_m2":  props.get((12, 4), 0),
        }
    else:
        props = await get_properties(session, brand, access_token, did, [
            (12, 1), (12, 2), (12, 3), (12, 4),
            (9, 1), (9, 2), (10, 1), (10, 2),
            (11, 1), (11, 2), (16, 1), (30, 1),
        ])
        return {
            "first_cleaning_date":     props.get((12, 1), 0),
            "total_cleaning_time_min": props.get((12, 2), 0),
            "cleaning_count":          props.get((12, 3), 0),
            "total_cleaned_area_m2":   props.get((12, 4), 0),
            "main_brush_left_pct":     props.get((9,  1), 0),
            "main_brush_time_left_h":  props.get((9,  2), 0),
            "side_brush_left_pct":     props.get((10, 1), 0),
            "side_brush_time_left_h":  props.get((10, 2), 0),
            "filter_left_pct":         props.get((11, 1), 0),
            "filter_time_left_h":      props.get((11, 2), 0),
            "sensor_dirty_left_pct":   props.get((16, 1), 0),
            "wheel_dirty_left_pct":    props.get((30, 1), 0),
        }


# ── Dreame Cloud MQTT ─────────────────────────────────────────────────────────

class DreameMqttClient:
    """paho-MQTT client for Dreame Cloud MQTTS, runs in its own thread.
    State updates are forwarded via asyncio.Queue to the asyncio event loop."""

    def __init__(
        self,
        bind_domain: str,
        did: str,
        uid: str,
        access_token: str,
        queue: asyncio.Queue,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        host_port   = bind_domain.split(":")
        self._host  = host_port[0]
        self._port  = int(host_port[1]) if len(host_port) > 1 else 8883
        self._did   = did
        self._uid   = uid
        self._token = access_token
        self._queue = queue
        self._loop  = loop

        client_id    = "p_" + secrets.token_hex(8)
        self._client = paho_mqtt.Client(
            callback_api_version=paho_mqtt.CallbackAPIVersion.VERSION1,
            client_id=client_id,
            reconnect_on_failure=False,
        )
        self._client.username_pw_set(uid, access_token)

        # TLS — Dreame cloud uses self-signed certificates
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode    = ssl.CERT_NONE
        self._client.tls_set_context(ctx)

        self._client.on_connect    = self._on_connect
        self._client.on_message    = self._on_message
        self._client.on_disconnect = self._on_disconnect

        self._thread: "threading.Thread | None" = None
        self._stop_flag = threading.Event()

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, daemon=True, name=f"dreame-mqtt-{self._did}"
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_flag.set()
        self._client.disconnect()
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop_flag.is_set():
            try:
                LOGINF(f"[{self._did}] Connecting Dreame Cloud MQTT {self._host}:{self._port}")
                self._client.connect(self._host, self._port, keepalive=60)
                self._client.loop_forever()
            except Exception as e:
                LOGERR(f"[{self._did}] Dreame MQTT error: {e}")
                if not self._stop_flag.is_set():
                    time.sleep(10)

    def _on_connect(self, client, userdata, flags, rc) -> None:
        if rc == 0:
            topic = f"/status/{self._did}/{self._uid}/#"
            client.subscribe(topic, qos=0)
            LOGOK(f"[{self._did}] Dreame MQTT connected, subscribed: {topic}")
        else:
            LOGERR(f"[{self._did}] Dreame MQTT connection failed: rc={rc}")

    def _on_message(self, client, userdata, msg) -> None:
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            asyncio.run_coroutine_threadsafe(
                self._queue.put({"did": self._did, "payload": payload}),
                self._loop,
            )
        except Exception as e:
            LOGWARN(f"[{self._did}] Dreame MQTT message parse error: {e}")

    def _on_disconnect(self, client, userdata, rc) -> None:
        if rc != 0 and not self._stop_flag.is_set():
            LOGWARN(f"[{self._did}] Dreame MQTT disconnected (rc={rc}), will reconnect...")


# ── StateMapper ───────────────────────────────────────────────────────────────
def map_properties_changed(
    device: dict,
    params: list,
    current_props: dict,
    mower_settings: dict,
) -> "dict | None":
    """Process properties_changed params, update current_props.
    Returns updated state-JSON dict, {"_trigger_cfg_reload": True}, or None."""
    updated = False
    trigger_cfg_reload = False

    for item in params:
        siid  = item.get("siid")
        piid  = item.get("piid")
        value = item.get("value")
        if siid is None or piid is None:
            continue

        # Mower binary state (siid=1 piid=1)
        if device["device_type"] == "mower" and siid == 1 and piid == 1:
            parsed = parse_binary_state_1(value)
            if parsed:
                current_props[(3, 2)] = parsed.get("battery",  current_props.get((3, 2), 0))
                current_props[(3, 3)] = parsed.get("charging", current_props.get((3, 3), 0))
                if parsed.get("error_code", 0):
                    current_props[(3, 5)] = parsed["error_code"]
                updated = True
            continue

        # Mower position data (siid=1 piid=4): skip, too technical for Loxone
        if device["device_type"] == "mower" and siid == 1 and piid == 4:
            continue

        # CFG reload trigger (siid=2 piid=51)
        if device["device_type"] == "mower" and siid == 2 and piid == 51:
            trigger_cfg_reload = True
            continue

        # AutoSwitch (siid=4 piid=50)
        if siid == 4 and piid == 50:
            try:
                as_data = json.loads(value) if isinstance(value, str) else value
                switches = _normalize_autoswitch(as_data)
                for k, v in switches.items():
                    current_props[("autoswitch", k)] = v
            except Exception:
                pass
            updated = True
            continue

        current_props[(siid, piid)] = value
        updated = True

    if trigger_cfg_reload:
        return {"_trigger_cfg_reload": True}
    if not updated:
        return None
    return build_state_json(device, current_props)


# ── CommandHandler ────────────────────────────────────────────────────────────
MOWER_ACTIONS: dict = {
    "start":         {"siid": 2, "aiid": 1,  "in": []},
    "stop":          {"siid": 2, "aiid": 2,  "in": []},
    "pause":         {"siid": 2, "aiid": 4,  "in": []},
    "dock":          {"siid": 5, "aiid": 3,  "in": []},
    "clear_warning": {"siid": 4, "aiid": 3,  "in": []},
}
MOWER_SPECIAL: dict = {
    "find": {"m": "a", "p": 0, "o": 9},
    "lock": {"m": "a", "p": 0, "o": 12},
}
VACUUM_ACTIONS: dict = {
    "start":         {"siid": 2, "aiid": 1,  "in": []},
    "pause":         {"siid": 2, "aiid": 2,  "in": []},
    "stop":          {"siid": 4, "aiid": 2,  "in": []},
    "dock":          {"siid": 3, "aiid": 1,  "in": []},
    "locate":        {"siid": 7, "aiid": 1,  "in": []},
    "auto_empty":    {"siid": 15,"aiid": 1,  "in": []},
    "start_washing": {"siid": 4, "aiid": 4,  "in": []},
    "clear_warning": {"siid": 4, "aiid": 3,  "in": []},
}
VACUUM_SETTINGS: dict = {
    "suction_level":        (4, 4),
    "water_volume":         (4, 5),
    "cleaning_mode":        (4, 23),
    "volume":               (7, 1),
    "child_lock":           (4, 27),
    "carpet_boost":         (4, 12),
    "carpet_cleaning":      (4, 36),
    "carpet_sensitivity":   (4, 28),
    "carpet_recognition":   (4, 33),
    "drying_time":          (4, 40),
    "auto_water_refilling": (4, 51),
    "auto_add_detergent":   (4, 37),
    "mop_wash_level":       (4, 46),
    "dnd_enable":           (5, 1),
    "dnd_start":            (5, 2),
    "dnd_end":              (5, 3),
    "auto_dust_collecting": (15, 1),
    "auto_empty_frequency": (15, 2),
    "water_temperature":    (28, 8),
    "wetness_level":        (28, 1),
    "silent_drying":        (28, 27),
    "hair_compression":     (28, 28),
}
VACUUM_AUTOSWITCH: dict = {
    "auto_drying":           "AutoDry",
    "smart_charging":        "SmartCharge",
    "stain_avoidance":       "StainIdentify",
    "collision_avoidance":   "LessColl",
    "max_suction":           "SuctionMax",
    "hot_washing":           "HotWash",
    "uv_sterilization":      "UVLight",
    "ultra_clean_mode":      "SuperWash",
    "mop_extend":            "MopExtrSwitch",
    "self_clean_frequency":  "BackWashType",
}
MOWER_AUTOSWITCH: dict = {
    "collision_avoidance":   "LessColl",
    "auto_charging":         "SmartCharge",
    "clean_genius":          "SmartHost",
    "cleaning_route":        "CleanRoute",
}
MOWER_PRE_SETTINGS: dict = {
    "mow_mode":         1,
    "cutting_height":   2,
    "direction_change": 5,
    "edge_detection":   8,
    "edge_mowing":      9,
}
MOWER_CFG_SETTINGS: dict = {
    "rain_protection":  lambda v: {"m": "s", "t": "WRP",  "d": v if isinstance(v, dict) else {"value": int(v)}},
    "frost_protection": lambda v: {"m": "s", "t": "FDP",  "d": {"value": int(v)}},
    "volume":           lambda v: {"m": "s", "t": "VOL",  "d": {"value": int(v)}},
    "child_lock":       lambda v: {"m": "s", "t": "CLS",  "d": {"value": int(v)}},
    "anti_theft":       lambda v: {"m": "s", "t": "STUN", "d": {"value": int(v)}},
    "ai_obstacle":      lambda v: {"m": "s", "t": "AOP",  "d": {"value": int(v)}},
    "grass_protection": lambda v: {"m": "s", "t": "PROT", "d": {"value": int(v)}},
    "path_display":     lambda v: {"m": "s", "t": "PATH", "d": {"value": int(v)}},
    "low_speed":        lambda v: {"m": "s", "t": "LOW",  "d": v},
    "headlight":        lambda v: {"m": "s", "t": "LIT",  "d": v},
}
MOWER_PROP_SETTINGS: dict = {
    "dnd_enable":         (5, 1),
    "obstacle_avoidance": (4, 21),
    "schedule":           (8, 2),
}


async def handle_set_command(
    session: aiohttp.ClientSession,
    brand: dict,
    access_token: str,
    device: dict,
    command: str,
) -> "tuple[str, int]":
    """Execute a set command. Returns (result_str, result_num)."""
    did = device["did"]
    dt  = device["device_type"]
    try:
        if dt == "mower":
            if command in MOWER_SPECIAL:
                await send_mower_command(session, brand, access_token, did, MOWER_SPECIAL[command])
            elif command in MOWER_ACTIONS:
                action = MOWER_ACTIONS[command]
                params = {**action, "did": did}
                await send_command(session, brand, access_token, did, "action", params)
            else:
                return "error", 1
        else:
            if command in VACUUM_ACTIONS:
                action = VACUUM_ACTIONS[command]
                params = {**action, "did": did}
                await send_command(session, brand, access_token, did, "action", params)
            else:
                return "error", 1
        return "ok", 0
    except Exception as e:
        LOGERR(f"[{did}] set '{command}' error: {e}")
        return "error", 1


async def handle_settings_command(
    session: aiohttp.ClientSession,
    brand: dict,
    access_token: str,
    device: dict,
    key: str,
    value,
    current_pre_array: list,
) -> "tuple[str, int]":
    """Execute a settings/{key} command. Returns (result_str, result_num)."""
    did = device["did"]
    dt  = device["device_type"]
    try:
        if dt == "vacuum":
            if key in VACUUM_SETTINGS:
                siid, piid = VACUUM_SETTINGS[key]
                params = [{"did": did, "siid": siid, "piid": piid, "value": value}]
                await send_command(session, brand, access_token, did, "set_properties", params)
            elif key in VACUUM_AUTOSWITCH:
                as_key = VACUUM_AUTOSWITCH[key]
                as_val = json.dumps({"k": as_key, "v": int(value)})
                params = [{"did": did, "siid": 4, "piid": 50, "value": as_val}]
                await send_command(session, brand, access_token, did, "set_properties", params)
            elif key == "schedule":
                params = [{"did": did, "siid": 8, "piid": 2, "value": value}]
                await send_command(session, brand, access_token, did, "set_properties", params)
            else:
                return "error", 1
        else:  # mower
            if key in MOWER_AUTOSWITCH:
                as_key = MOWER_AUTOSWITCH[key]
                as_val = json.dumps({"k": as_key, "v": int(value)})
                params = [{"did": did, "siid": 4, "piid": 50, "value": as_val}]
                await send_command(session, brand, access_token, did, "set_properties", params)
            elif key in MOWER_CFG_SETTINGS:
                payload = MOWER_CFG_SETTINGS[key](value)
                await send_mower_command(session, brand, access_token, did, payload)
            elif key in MOWER_PROP_SETTINGS:
                siid, piid = MOWER_PROP_SETTINGS[key]
                params = [{"did": did, "siid": siid, "piid": piid, "value": value}]
                await send_command(session, brand, access_token, did, "set_properties", params)
            elif key in MOWER_PRE_SETTINGS:
                # PRE Read-Modify-Write
                idx = MOWER_PRE_SETTINGS[key]
                pre = list(current_pre_array)
                while len(pre) <= idx:
                    pre.append(0)
                pre[idx] = int(value)
                payload = {"m": "s", "t": "PRE", "d": {"value": pre}}
                await send_mower_command(session, brand, access_token, did, payload)
            else:
                return "error", 1
        return "ok", 0
    except Exception as e:
        LOGERR(f"[{did}] settings/{key} error: {e}")
        return "error", 1


# ── Async Tasks ───────────────────────────────────────────────────────────────


async def task_dreame_to_lbmqtt(
    device: dict,
    queue: asyncio.Queue,
    broker: dict,
    base_topic: str,
    session: aiohttp.ClientSession,
    brand: dict,
    cfg: dict,
    mower_settings: "dict | None",
) -> None:
    """Receive state updates from Dreame Cloud queue, publish to LoxBerry MQTT."""
    did = device["did"]
    dt  = device["device_type"]
    current_props: dict = {}
    current_pre  : list = mower_settings.get("_pre_array", []) if mower_settings else []

    mqtt_kwargs = _build_mqtt_kwargs(broker)
    while not _shutdown_event.is_set():
        try:
            async with aiomqtt.Client(**mqtt_kwargs) as lbmqtt:
                LOGOK(f"[{did}] LoxBerry MQTT publisher connected")
                while not _shutdown_event.is_set():
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    payload = item.get("payload", {})
                    data    = payload.get("data", {})
                    method  = data.get("method", "")
                    if method != "properties_changed":
                        continue
                    result = map_properties_changed(
                        device, data.get("params", []), current_props,
                        mower_settings or {}
                    )
                    if result is None:
                        continue
                    if result.get("_trigger_cfg_reload"):
                        LOGINF(f"[{did}] CFG reload triggered")
                        try:
                            mower_settings = await load_mower_settings(
                                session, brand, cfg["access_token"], did
                            )
                            current_pre = mower_settings.get("_pre_array", [])
                            state = build_state_json(device, current_props)
                            state.update({k: v for k, v in mower_settings.items() if not k.startswith("_")})
                            await lbmqtt.publish(f"{base_topic}/{did}/state", json.dumps(state), retain=True)
                        except Exception as e:
                            LOGERR(f"[{did}] CFG reload error: {e}")
                        continue
                    await lbmqtt.publish(f"{base_topic}/{did}/state", json.dumps(result), retain=True)
                    if dt == "vacuum":
                        station = build_station_json(current_props)
                        await lbmqtt.publish(
                            f"{base_topic}/{did}/state_station",
                            json.dumps(station),
                            retain=True,
                        )
        except aiomqtt.MqttError as e:
            if not _shutdown_event.is_set():
                LOGWARN(f"[{did}] LoxBerry MQTT publisher disconnected: {e} — reconnect in 5s")
                await asyncio.sleep(5)
        except Exception as e:
            LOGERR(f"[{did}] task_dreame_to_lbmqtt error: {e}")
            await asyncio.sleep(5)


async def task_lbmqtt_to_dreame(
    device: dict,
    broker: dict,
    base_topic: str,
    session: aiohttp.ClientSession,
    brand: dict,
    cfg: dict,
    pre_array_ref: list,
) -> None:
    """Subscribe to LoxBerry MQTT set/settings, dispatch commands to Dreame cloud."""
    did            = device["did"]
    set_topic      = f"{base_topic}/{did}/set"
    settings_topic = f"{base_topic}/{did}/settings/#"
    mqtt_kwargs    = _build_mqtt_kwargs(broker)

    while not _shutdown_event.is_set():
        try:
            async with aiomqtt.Client(**mqtt_kwargs) as lbmqtt:
                await lbmqtt.subscribe(set_topic)
                await lbmqtt.subscribe(settings_topic)
                LOGINF(f"[{did}] Subscribed: {set_topic}, {settings_topic}")
                async with lbmqtt.messages() as messages:
                    async for msg in messages:
                        if _shutdown_event.is_set():
                            break
                        topic_str   = str(msg.topic)
                        payload_str = msg.payload.decode("utf-8").strip()
                        result_str, result_num = "error", 1
                        command_name = ""
                        try:
                            if topic_str == set_topic:
                                command_name = payload_str
                                result_str, result_num = await handle_set_command(
                                    session, brand, cfg["access_token"], device, payload_str
                                )
                            elif "/settings/" in topic_str:
                                key = topic_str.split("/settings/")[-1]
                                command_name = f"settings/{key}"
                                try:
                                    value = json.loads(payload_str)
                                except json.JSONDecodeError:
                                    value = payload_str
                                result_str, result_num = await handle_settings_command(
                                    session, brand, cfg["access_token"], device,
                                    key, value, pre_array_ref
                                )
                        except Exception as e:
                            LOGERR(f"[{did}] Command '{command_name}' error: {e}")
                            continue
                        result_payload = json.dumps({
                            "command":    command_name,
                            "result":     result_str,
                            "result_num": result_num,
                            "reason":     "none" if result_str == "ok" else str(result_num),
                        })
                        await lbmqtt.publish(f"{base_topic}/{did}/command_result", result_payload)
        except aiomqtt.MqttError as e:
            if not _shutdown_event.is_set():
                LOGWARN(f"[{did}] LoxBerry MQTT subscriber disconnected: {e} — reconnect in 5s")
                await asyncio.sleep(5)
        except Exception as e:
            LOGERR(f"[{did}] task_lbmqtt_to_dreame error: {e}")
            await asyncio.sleep(5)


async def task_token_refresh(
    session: aiohttp.ClientSession,
    brand: dict,
    cfg: dict,
    cfg_lock: asyncio.Lock,
) -> None:
    """Refresh access token 5 minutes before expiry; fall back to re-login on error."""
    while not _shutdown_event.is_set():
        await asyncio.sleep(30)
        now        = time.time()
        expires_at = cfg.get("expires_at", 0)
        if expires_at == 0 or now < expires_at - 300:
            continue
        LOGINF("Token expiring — refreshing")
        async with cfg_lock:
            try:
                tokens = await dreame_refresh_token(session, brand, cfg["refresh_token"])
                cfg["access_token"]  = tokens["access_token"]
                cfg["refresh_token"] = tokens["refresh_token"]
                cfg["expires_at"]    = time.time() + tokens["expires_in"]
                save_plugin_config(cfg)
                LOGOK("Token refreshed successfully")
            except Exception as e:
                LOGERR(f"Token refresh failed ({e}) — attempting re-login")
                try:
                    tokens = await dreame_login(
                        session, brand, cfg["username"],
                        cfg.get("_password_plain", "")
                    )
                    cfg["access_token"]  = tokens["access_token"]
                    cfg["refresh_token"] = tokens["refresh_token"]
                    cfg["uid"]           = tokens["uid"]
                    cfg["expires_at"]    = time.time() + tokens["expires_in"]
                    save_plugin_config(cfg)
                    LOGOK("Re-login successful")
                except Exception as e2:
                    LOGERR(f"Re-login failed: {e2}")


async def task_statistic_poll(
    devices: list,
    session: aiohttp.ClientSession,
    brand: dict,
    cfg: dict,
    broker: dict,
    base_topic: str,
) -> None:
    """Poll statistics/consumables periodically (default 30 min)."""
    interval = int(cfg.get("polling_interval_min", 30)) * 60
    while not _shutdown_event.is_set():
        await asyncio.sleep(interval)
        for device in devices:
            if _shutdown_event.is_set():
                break
            did = device["did"]
            try:
                stat = await load_statistic(
                    session, brand, cfg["access_token"], did, device["device_type"]
                )
                if device["device_type"] == "mower":
                    history = await load_mower_history(
                        session, brand, cfg["access_token"], did, cfg["uid"]
                    )
                    if history:
                        last = history[0]
                        stat["last_mow_date"]         = last.get("time", 0)
                        stat["last_mow_duration_min"] = last.get("duration", 0)
                        stat["last_mow_area_m2"]      = last.get("area", 0)
                        stat["last_mow_completed"]    = last.get("completed", False)
                mqtt_kwargs = _build_mqtt_kwargs(broker)
                async with aiomqtt.Client(**mqtt_kwargs) as lbmqtt:
                    await lbmqtt.publish(
                        f"{base_topic}/{did}/statistic",
                        json.dumps(stat),
                        retain=True,
                    )
                LOGDEB(f"[{did}] Statistic published")
            except Exception as e:
                LOGERR(f"[{did}] Statistic poll error: {e}")


# ── main ──────────────────────────────────────────────────────────────────────
async def _async_main() -> None:
    LOGSTART("Dreame Gateway started")
    write_pid()
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT,  _handle_sigterm)

    cfg     = load_plugin_config()
    general = _load_json(GENERAL_JSON)
    broker  = get_mqtt_broker_config(general)
    brand   = BRAND_CONFIG.get(cfg.get("cloud_service", "dreame"), BRAND_CONFIG["dreame"])

    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Token check / login
        if not cfg.get("access_token") or time.time() >= cfg.get("expires_at", 0) - 60:
            LOGINF("No valid token — logging in")
            password_plain = cfg.get("_password_plain", "")
            if not password_plain:
                LOGERR("No password in config — please log in via WebUI")
                return
            tokens = await dreame_login(session, brand, cfg["username"], password_plain)
            cfg["access_token"]  = tokens["access_token"]
            cfg["refresh_token"] = tokens["refresh_token"]
            cfg["uid"]           = tokens["uid"]
            cfg["expires_at"]    = time.time() + tokens["expires_in"]
            save_plugin_config(cfg)

        # Load device list (fall back to cached)
        devices = cfg.get("devices", [])
        try:
            devices = await get_device_list(session, brand, cfg["access_token"])
            cfg["devices"] = devices
            save_plugin_config(cfg)
            LOGOK(f"{len(devices)} device(s) found")
        except Exception as e:
            LOGWARN(f"Could not load device list ({e}), using cache")

        if not devices:
            LOGERR("No devices found — gateway exiting")
            return

        cfg_lock   = asyncio.Lock()
        loop       = asyncio.get_running_loop()
        base_topic = cfg.get("base_topic", "dreame")

        tasks_coro  = []
        dreame_clients = []

        for device in devices:
            did  = device["did"]
            bind = device.get("bind_domain", "")
            queue: asyncio.Queue = asyncio.Queue(maxsize=100)

            # Mower: load settings at startup
            mower_settings = None
            pre_array_ref  = []
            if device["device_type"] == "mower":
                try:
                    mower_settings = await load_mower_settings(
                        session, brand, cfg["access_token"], did
                    )
                    pre_array_ref = mower_settings.get("_pre_array", [])
                    LOGOK(f"[{did}] Mower settings loaded")
                except Exception as e:
                    LOGWARN(f"[{did}] Mower settings error: {e}")

            # Vacuum: load station properties at startup
            if device["device_type"] == "vacuum" and bind:
                try:
                    station_props = await get_properties(
                        session, brand, cfg["access_token"], did,
                        [(25, 1), (25, 2), (25, 3), (25, 4), (25, 5)]
                    )
                    mqtt_kwargs = _build_mqtt_kwargs(broker)
                    async with aiomqtt.Client(**mqtt_kwargs) as lbmqtt:
                        station = build_station_json(station_props)
                        await lbmqtt.publish(
                            f"{base_topic}/{did}/state_station",
                            json.dumps(station),
                            retain=True,
                        )
                except Exception as e:
                    LOGWARN(f"[{did}] Station props error: {e}")

            # Start Dreame cloud MQTT (only if device has a bind_domain)
            if bind:
                dc = DreameMqttClient(bind, did, cfg["uid"], cfg["access_token"], queue, loop)
                dc.start()
                dreame_clients.append(dc)
                tasks_coro.append(task_dreame_to_lbmqtt(
                    device, queue, broker, base_topic, session, brand, cfg, mower_settings
                ))
            tasks_coro.append(task_lbmqtt_to_dreame(
                device, broker, base_topic, session, brand, cfg, pre_array_ref
            ))

        tasks_coro.append(task_token_refresh(session, brand, cfg, cfg_lock))
        tasks_coro.append(task_statistic_poll(devices, session, brand, cfg, broker, base_topic))

        all_tasks = [asyncio.create_task(c) for c in tasks_coro]
        await _shutdown_event.wait()

        for dc in dreame_clients:
            dc.stop()
        for t in all_tasks:
            t.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)

    remove_pid()
    _logend()
    LOGINF("Dreame Gateway stopped")


def main() -> None:
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
