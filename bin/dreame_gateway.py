#!/usr/bin/env python3
"""Dreame LoxBerry Gateway — bridges Dreame/MOVA cloud to LoxBerry MQTT."""

import argparse
import asyncio
import base64
import hashlib
import json
import logging
import logging.handlers
import os
import re
import secrets
import signal
import ssl
import sys
import time
import zlib
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
# Status enums. Both device types report their user-facing status on (2,1) — the
# value the manufacturer app displays. Cross-checked against Tasshack/dreame-vacuum
# and TA2k/ioBroker.dreame, and verified against a live Mova Z60 protocol.
_MOWER_STATUS_STR = {
    1: "Working", 2: "Standby", 3: "Working", 4: "Paused",
    5: "Returning to charge", 6: "Charging", 7: "Error",
    8: "Raining pause", 9: "Initializing", 10: "Leaving station",
    11: "Mapping", 12: "Border mowing", 13: "Charging completed",
    14: "Upgrading", 15: "Relocating", 16: "Task navigating",
}
_VACUUM_STATUS_STR = {
    1: "Cleaning", 2: "Standby", 3: "Paused", 4: "Paused",
    5: "Returning to charge", 6: "Charging", 7: "Mopping",
    8: "Mop drying", 9: "Mop washing", 10: "Returning to wash",
    11: "Mapping", 12: "Cleaning", 13: "Charging completed",
    14: "Upgrading", 15: "Summon to clean", 16: "Self-repairing",
    17: "Returning to install mop pad", 18: "Returning to remove mop pad",
    19: "Water system self-test", 20: "Cleaning mop pad and adding water",
    21: "Cleaning paused", 22: "Auto-emptying",
    23: "Remote controlled cleaning", 24: "Smart charging",
    25: "Second cleaning", 26: "Following", 27: "Spot cleaning",
    28: "Returning for dust collection", 29: "Waiting for tasks",
    30: "Cleaning washboard base", 31: "Returning to drain",
    32: "Draining", 33: "Water system emptying", 34: "Emptying",
    35: "Dust bag drying", 36: "Dust bag drying paused",
    37: "Heading to extra cleaning", 38: "Extra cleaning",
    95: "Finding pet paused", 96: "Finding pet", 97: "Shortcut running",
    98: "Camera monitoring", 99: "Camera monitoring paused",
    101: "Initial deep cleaning", 102: "Initial deep cleaning paused",
    103: "Sanitizing", 104: "Sanitizing with dry",
    105: "Changing mop", 106: "Changing mop paused",
    107: "Floor maintaining", 108: "Floor maintaining paused",
}
# (4,1) — internal cleaning task status, published alongside as task/task_str.
_VACUUM_TASK_STR = {
    0: "Idle", 1: "Paused", 2: "Cleaning", 3: "Back home",
    4: "Part cleaning", 5: "Follow wall", 6: "Charging", 7: "OTA",
    8: "FCT", 9: "WiFi set", 10: "Power off", 11: "Factory",
    12: "Error", 13: "Remote control", 14: "Sleeping",
    15: "Self test", 16: "Factory function test", 17: "Standby",
    18: "Segment cleaning", 19: "Zone cleaning", 20: "Spot cleaning",
    21: "Fast mapping", 22: "Monitor cruise", 23: "Monitor spot",
    24: "Summon clean",
}
# Charging status on (3,2) — the two device types use different enums.
_VACUUM_CHARGING_STR = {
    1: "Charging", 2: "Not charging", 3: "Charging completed",
    5: "Returning to charge",
}
_MOWER_CHARGING_STR = {0: "Not charging", 1: "Charging"}
# Cleaning mode, see _unpack_cleaning_mode(). Devices that lift their mop pad use
# the inverted coding; plain devices use the straight one.
_VACUUM_MODE_LIFTING_STR = {
    0: "Sweeping and mopping", 1: "Mopping", 2: "Sweeping",
}
_VACUUM_MODE_PLAIN_STR = {
    0: "Sweeping", 1: "Mopping", 2: "Sweeping and mopping",
    3: "Mopping after sweeping",
}


_MOWER_NAMED: set = {(2,1),(2,2),(3,1),(3,2),(4,2),(4,3),(4,7),(4,35)}
_VACUUM_NAMED: set = {(2,1),(2,2),(3,1),(3,2),(4,1),(4,2),(4,3),(4,4),(4,5),
                      (4,7),(4,14),(4,22),(4,23),(4,27),(4,35),(5,1)}
_STATION_NAMED: set = {
    (25,1),(25,2),(25,3),(25,4),(25,5),           # SIID 25 – older models
    (27,1),(27,2),(27,3),(27,4),(27,5),(27,15),    # SIID 27 – newer models
}

# ── Curated property names (A) ────────────────────────────────────────────────
# High-confidence (siid, piid) → name map for props that are NOT already published
# with a descriptive key (i.e. not in *_NAMED / _STATION_NAMED). Sourced from
# python-miio, Tasshack/dreame-vacuum and the plugin's own VACUUM_SETTINGS /
# load_statistic mappings. Anything not covered here falls through to the MIoT
# spec lookup (B) and finally to p_<siid>_<piid> so no value is ever lost.
_VACUUM_PROP_NAMES: dict = {
    (4, 6):  "mop_attached",            (4, 12): "carpet_boost",
    (4, 28): "carpet_sensitivity",      (4, 33): "carpet_recognition",
    (4, 36): "carpet_cleaning",         (4, 37): "auto_add_detergent",
    (4, 40): "drying_time",             (4, 46): "mop_wash_level",
    (4, 51): "auto_water_refilling",
    (5, 2):  "dnd_start",               (5, 3):  "dnd_end",
    (5, 4):  "dnd_schedule",
    (7, 1):  "volume",
    # SIID 9/10/30 carry hours on piid 1 and percent on piid 2; SIID 11/16/18/26
    # are the other way round. The asymmetry is in the device spec, not a typo —
    # confirmed by both references and by a live Mova Z60 protocol.
    (9, 1):  "main_brush_time_left_h",  (9, 2):  "main_brush_left_pct",
    (10, 1): "side_brush_time_left_h",  (10, 2): "side_brush_left_pct",
    (11, 1): "filter_left_pct",         (11, 2): "filter_time_left_h",
    (12, 1): "first_cleaning_date",     (12, 2): "total_cleaning_time_min",
    (12, 3): "cleaning_count",          (12, 4): "total_cleaned_area_m2",
    (15, 1): "auto_dust_collecting",    (15, 2): "auto_empty_frequency",
    (15, 3): "dust_collection",         (15, 5): "auto_empty_status",
    (16, 1): "sensor_dirty_left_pct",   (16, 2): "sensor_dirty_time_left_h",
    (17, 1): "secondary_filter_left_pct",
    (17, 2): "secondary_filter_time_left_h",
    (18, 1): "mop_pad_left_pct",        (18, 2): "mop_pad_time_left_h",
    (26, 1): "dirty_water_tank_left_pct",
    (26, 2): "dirty_water_tank_time_left_h",
    (28, 1): "wetness_level",           (28, 8): "water_temperature",
    (28, 27): "silent_drying",          (28, 28): "hair_compression",
    (30, 1): "wheel_dirty_time_left_h", (30, 2): "wheel_dirty_left_pct",
}
_MOWER_PROP_NAMES: dict = {
    (2, 50): "task_info",               (2, 52): "mowing_preference",
    (2, 55): "ai_obstacles",            (2, 56): "zone_status",
    (2, 58): "self_check",              (2, 65): "task_type",
    (4, 14): "serial_number",           (4, 18): "faults",
    (4, 21): "obstacle_avoidance",      (4, 27): "child_lock",
    (4, 42): "map_index",               (4, 43): "map_name",
    (5, 100): "rtk_status",             (5, 106): "gps_satellites",
    (5, 107): "positioning_mode",
    (12, 1): "first_mow_date",          (12, 2): "total_mow_time_min",
    (12, 3): "total_mow_count",         (12, 4): "total_mow_area_m2",
}

# ── MIoT spec lookup cache (B) ────────────────────────────────────────────────
# Filled at startup by ensure_prop_names() per model: {model: {(siid,piid): name}}.
# Used only for props that A did not already cover.
_SPEC_PROP_NAMES: dict = {}


def _prop_name_map(dt: str) -> dict:
    return _MOWER_PROP_NAMES if dt == "mower" else _VACUUM_PROP_NAMES


def _has_mop_pad_lifting(props: dict, model: str) -> bool:
    """Whether the device lifts its mop pad. Decides how (4,23) is encoded.
    Derived exactly as Tasshack/dreame-vacuum does: a self-wash base (4,25) plus an
    auto-empty base (15,3), with the r2216 series as a hardcoded exception."""
    self_wash  = (4, 25) in props
    auto_empty = (15, 3) in props
    return (self_wash and auto_empty) or "r2216" in (model or "")


def _unpack_cleaning_mode(value, props: dict, model: str):
    """Decode (4,23) into a plain cleaning mode and its label.
    Returns (mode, label). Devices that lift their mop pad pack the mode into the
    low two bits and invert its meaning — 0 is sweeping *and* mopping there, while
    on a plain device 0 is sweeping only. Publishing the raw value is therefore
    misleading on both. Source: Tasshack/dreame-vacuum cleaning_mode().

    The upper bytes are deliberately not decoded into named fields: on a live Gen2
    device byte 1 read 25 while the explicit wetness property (28,1) read 16, so
    they are not the same quantity. They stay available in cleaning_mode_raw."""
    if not isinstance(value, int) or isinstance(value, bool):
        return None, "Unknown"
    lifting = _has_mop_pad_lifting(props, model)
    mode    = (value & 3) if lifting else (value & 1 if value > 0xFF else value)
    if lifting:
        label = _VACUUM_MODE_LIFTING_STR.get(mode, "Unknown")
    elif (4, 25) in props:
        # Self-wash base without pad lifting: only "mopping" has its own code.
        label = "Mopping" if mode == 1 else "Sweeping and mopping"
    else:
        label = _VACUUM_MODE_PLAIN_STR.get(mode, "Unknown")
    return mode, label


def build_state_json(device: dict, props: dict) -> dict:
    """Map siid/piid property dict → LoxBerry state JSON for this device.
    Named fields are published with descriptive keys; all other numeric (siid,piid)
    props are appended as p_<siid>_<piid> so no data is lost."""
    dt = device.get("device_type", "vacuum")
    named = _MOWER_NAMED if dt == "mower" else _VACUUM_NAMED
    state: dict = {
        "device_type": dt,
        "name":        device.get("name", ""),
        "model":       device.get("model", ""),
        "did":         device.get("did", ""),
        "online":      device.get("online", False),
    }
    # The user-facing status lives on (2,1) for both device types, the error on (2,2).
    status   = props.get((2, 1), 0)
    charging = props.get((3, 2), 0)
    if dt == "mower":
        state.update({
            "status":          status,
            "status_str":      _MOWER_STATUS_STR.get(status, "Unknown"),
            "battery":         props.get((3, 1), 0),
            "charging":        charging,
            "charging_str":    _MOWER_CHARGING_STR.get(charging, "Unknown"),
            "error":           props.get((2, 2), 0),
            "mowing_time_min": props.get((4, 2), 0),
            "mowing_area_m2":  props.get((4, 3), 0),
            "task_status":     props.get((4, 7), 0),
            "warn_status":     props.get((4, 35), 0),
        })
    else:
        task = props.get((4, 1), 0)
        state.update({
            "status":              status,
            "status_str":          _VACUUM_STATUS_STR.get(status, "Unknown"),
            "battery":             props.get((3, 1), 0),
            "charging":            charging,
            "charging_str":        _VACUUM_CHARGING_STR.get(charging, "Unknown"),
            "error":               props.get((2, 2), 0),
            "task":                task,
            "task_str":            _VACUUM_TASK_STR.get(task, "Unknown"),
            "cleaning_time_min":   props.get((4, 2), 0),
            "cleaned_area_m2":     props.get((4, 3), 0),
            "suction_level":       props.get((4, 4), 0),
            "water_volume":        props.get((4, 5), 0),
            "task_status":         props.get((4, 7), 0),
            "ai_detection":        props.get((4, 22), 0),
            "warn_status":         props.get((4, 35), 0),
            "dnd_enabled":         props.get((5, 1), 0),
            "child_lock":          props.get((4, 27), 0),
        })
        # (4,14) is the serial number — a string on every device seen so far.
        if (4, 14) in props:
            state["serial_number"] = props[(4, 14)]
        # (4,23) is a packed value on Gen2 devices; keep the raw one alongside.
        if (4, 23) in props:
            raw = props[(4, 23)]
            mode, mode_str = _unpack_cleaning_mode(raw, props, device.get("model", ""))
            state["cleaning_mode_raw"] = raw
            state["cleaning_mode"]     = mode if mode is not None else raw
            state["cleaning_mode_str"] = mode_str
    # Append remaining props: name them via the curated map (A) or the resolved
    # MIoT spec (B); anything still unknown — or a name collision — stays as
    # p_<siid>_<piid> so no value is lost.
    name_map = _prop_name_map(dt)
    spec_map = _SPEC_PROP_NAMES.get(device.get("model", ""), {})
    for key, val in props.items():
        if not (isinstance(key, tuple) and key not in named and key not in _STATION_NAMED):
            continue
        nm = name_map.get(key) or spec_map.get(key)
        if nm and nm not in state:
            state[nm] = val
        else:
            state[f"p_{key[0]}_{key[1]}"] = val
    return state


def build_station_json(props: dict) -> dict:
    """Map station properties → LoxBerry state_station JSON (vacuum only).
    SIID 25 (older models) takes priority; falls back to SIID 27 (newer models)."""
    if (25, 1) in props:
        cw  = props.get((25, 1), 0)
        dw  = props.get((25, 2), 0)
        db  = props.get((25, 3), 0)
        det = props.get((25, 4), 0)
        hw  = props.get((25, 5), 0)
    else:
        cw  = props.get((27, 1), 0)
        dw  = props.get((27, 2), 0)
        db  = props.get((27, 3), 0)
        det = props.get((27, 4), 0)
        hw  = props.get((27, 15), 0)
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
        "detergent":            det,
        "hot_water":            hw,
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

# System language (LoxBerry Base.Lang from general.json); set once in _async_main.
# Controls the language of derived room names. "de" → German, anything else → English.
SYSTEM_LANG  = "en"

# ── Logging ───────────────────────────────────────────────────────────────────
_loglevel = _args.loglevel
_logfile  = _args.logfile
_logger = logging.getLogger("dreame_gateway")
_logger.propagate = False
_logger.setLevel(logging.DEBUG)
# WatchedFileHandler (statt FileHandler): LoxBerry kann die Logdatei unter dem
# laufenden Prozess weglöschen (z.B. wenn der Gateway-Start ins Plugin-Install-/
# Log-Wartungs-Fenster fällt und die Log-Session verwaist). WatchedFileHandler
# erkennt das gelöschte/ersetzte Inode und legt die Datei neu an, statt ins
# verwaiste Inode „ins Nichts" weiterzuschreiben.
_handler = (
    logging.handlers.WatchedFileHandler(_logfile, mode="a", encoding="utf-8")
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


# These fields live in memory only and are NEVER written to the SD card.
# They are re-obtained at startup via token refresh / login, so persisting them
# would only cause needless write wear (token refresh happens ~hourly).
_EPHEMERAL_FIELDS = frozenset(("access_token", "expires_at", "uid", "uid_num"))


def load_plugin_config() -> dict:
    cfg = _load_json(PLUGIN_CFG)
    # One-time migration of configs written by older plugin versions:
    #   - rename _password_plain → password_plain
    #   - drop the now-unused password_hash
    #   - strip ephemeral fields that used to be persisted
    dirty = False
    if "_password_plain" in cfg:
        cfg.setdefault("password_plain", cfg["_password_plain"])
        del cfg["_password_plain"]
        dirty = True
    if "password_hash" in cfg:
        del cfg["password_hash"]
        dirty = True
    if "polling_interval_min" in cfg:
        cfg.setdefault("statistic_poll_interval_sec",
                       int(cfg["polling_interval_min"]) * 60)
        del cfg["polling_interval_min"]
        dirty = True
    if any(k in cfg for k in _EPHEMERAL_FIELDS):
        for k in _EPHEMERAL_FIELDS:
            cfg.pop(k, None)
        dirty = True
    if dirty:
        _save_json_atomic(PLUGIN_CFG, cfg)
    cfg.setdefault("cloud_service",           "dreame")
    cfg.setdefault("username",                "")
    cfg.setdefault("password_plain",          "")
    cfg.setdefault("refresh_token",           "")
    cfg.setdefault("base_topic",                  "dreame")
    cfg.setdefault("statistic_poll_interval_sec", 300)
    cfg.setdefault("state_poll_interval_sec",     60)
    cfg.setdefault("devices",                 [])
    # Ephemeral — memory only, populated by the startup refresh/login below.
    cfg["access_token"] = ""
    cfg["expires_at"]   = 0
    cfg["uid"]          = ""
    cfg["uid_num"]      = ""
    return cfg


def save_plugin_config(cfg: dict) -> None:
    on_disk = {k: v for k, v in cfg.items() if k not in _EPHEMERAL_FIELDS}
    _save_json_atomic(PLUGIN_CFG, on_disk)


async def publish_gateway_status(broker: dict, base_topic: str, cfg: dict) -> None:
    """Publish auth status retained to {base_topic}/gateway.
    Read by the WebUI (ajax.cgi) via mqtt_get — keeps the token status visible
    even though access_token/expires_at are no longer stored on the SD card."""
    token   = cfg.get("access_token", "")
    payload = {
        "state":         "running",
        "authenticated": bool(token and cfg.get("expires_at", 0) > time.time()),
        "expires_at":    cfg.get("expires_at", 0),
    }
    try:
        async with aiomqtt.Client(**_build_mqtt_kwargs(broker)) as lbmqtt:
            await lbmqtt.publish(f"{base_topic}/gateway", json.dumps(payload), retain=True)
    except Exception as e:
        LOGWARN(f"Cannot publish gateway status: {e}")


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


# Created lazily inside the running loop in _async_main(). Must NOT be built at
# import time: on Python 3.9 (Raspbian Bullseye) asyncio.Event() eagerly binds to
# the loop returned by get_event_loop() at construction, which differs from the loop
# asyncio.run() creates later — the first .wait() then raises
# "got Future attached to a different loop". Py3.10+ binds lazily and is unaffected.
_shutdown_event: "asyncio.Event | None" = None


def _handle_sigterm(*_) -> None:
    LOGINF("SIGTERM received — shutting down")
    if _shutdown_event is not None:
        _shutdown_event.set()


# ── Dreame Auth ───────────────────────────────────────────────────────────────
def _jwt_payload(token: str) -> dict:
    """Decode the JWT payload (second segment) without verifying the signature."""
    try:
        parts = token.split(".")
        payload = parts[1]
        payload += "=" * (4 - len(payload) % 4)
        return json.loads(base64.b64decode(payload))
    except Exception:
        return {}


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
    jwt = _jwt_payload(body["access_token"])
    return {
        "access_token":  body["access_token"],
        "refresh_token": body.get("refresh_token", ""),
        "expires_in":    int(body.get("expires_in", 3600)),
        "uid":           str(body.get("uid", "")),
        "uid_num":       str(jwt.get("u", "")),
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
    jwt = _jwt_payload(body["access_token"])
    return {
        "access_token":  body["access_token"],
        "refresh_token": body.get("refresh_token", refresh_token),
        "expires_in":    int(body.get("expires_in", 3600)),
        "uid":           str(body.get("uid", "")),
        "uid_num":       str(jwt.get("u", "")),
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
            "did":              r.get("did", ""),
            "model":            model,
            "name":             r.get("customName") or model,
            "device_type":      _get_device_type(model),
            "bind_domain":      r.get("bindDomain", ""),
            "online":           bool(r.get("online", False)),
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
    records = (
        data.get("data", {}).get("page", {}).get("records")
        or data.get("result", {}).get("records", [])
    )
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


_GET_PROPS_CHUNK = 50

async def _get_props_batch(
    session: aiohttp.ClientSession,
    brand: dict,
    access_token: str,
    did: str,
    chunk: list,
) -> "dict | None":
    """One get_properties request for a chunk → {(siid, piid): value}, or None if the
    cloud gave no usable envelope. Transport errors propagate (handled by the caller)."""
    params = [{"did": did, "siid": s, "piid": p} for s, p in chunk]
    result = await send_command(session, brand, access_token, did, "get_properties", params)
    if result is None:
        return None
    out = {}
    for item in (result.get("data") or {}).get("result", []):
        siid = item.get("siid")
        piid = item.get("piid")
        if siid is not None and piid is not None and "value" in item:
            out[(siid, piid)] = item["value"]
    return out


async def _get_props_resilient(
    session: aiohttp.ClientSession,
    brand: dict,
    access_token: str,
    did: str,
    chunk: list,
) -> dict:
    """Fetch a chunk, isolating 'poison' properties. Some props (e.g. the AutoSwitch
    composite (4,50) on Gen2 models) make the Dreame cloud return a 200-OK response
    with an EMPTY result list for the WHOLE batch — voiding every other property in it.
    When a ≥2-prop request comes back empty, split it and retry each half so a single
    bad property only loses itself. A 1-prop empty result is just an unsupported prop."""
    values = await _get_props_batch(session, brand, access_token, did, chunk)
    if values is None:                     # no usable envelope → nothing to split
        return {}
    if values or len(chunk) <= 1:          # got data, or can't split further
        return values
    mid = len(chunk) // 2
    left  = await _get_props_resilient(session, brand, access_token, did, chunk[:mid])
    right = await _get_props_resilient(session, brand, access_token, did, chunk[mid:])
    left.update(right)
    return left


async def get_properties(
    session: aiohttp.ClientSession,
    brand: dict,
    access_token: str,
    did: str,
    siid_piid_list: list,
) -> dict:
    """get_properties for a list of (siid, piid) pairs → {(siid, piid): value}.
    Batches in chunks of _GET_PROPS_CHUNK to stay within cloud API limits; a chunk
    that comes back 200-but-empty is recursively split (_get_props_resilient) so one
    unsupported/poison property can't void its batch-mates."""
    out = {}
    for i in range(0, len(siid_piid_list), _GET_PROPS_CHUNK):
        chunk = siid_piid_list[i:i + _GET_PROPS_CHUNK]
        out.update(await _get_props_resilient(session, brand, access_token, did, chunk))
    return out


# ── MIoT spec resolution (B) ──────────────────────────────────────────────────
# Public Xiaomi MIoT spec registry. Newer Dreame "Gen 2" devices (e.g.
# dreame.vacuum.r2469a) are NOT published there — the lookup then yields nothing
# and the affected props simply stay as p_<siid>_<piid>.
_MIOT_INSTANCES_URL = "https://miot-spec.org/miot-spec-v2/instances?status=all"
_MIOT_INSTANCE_URL  = "https://miot-spec.org/miot-spec-v2/instance?type={urn}"
_MIOT_TIMEOUT       = aiohttp.ClientTimeout(total=30)


def _slugify(text: str) -> str:
    """'Main Brush Left Time' → 'main_brush_left_time'."""
    s = re.sub(r"[^a-z0-9]+", "_", text.strip().lower())
    return s.strip("_")


def _spec_cache_file(model: str) -> Path:
    return CONFIGDIR / "specs" / f"{re.sub(r'[^A-Za-z0-9._-]', '_', model)}.json"


def _load_spec_cache(model: str) -> "dict | None":
    """Return cached {(siid,piid): name} for the model, or None if never resolved.
    An (intentionally) empty dict means 'resolved but nothing found' → no refetch."""
    f = _spec_cache_file(model)
    if not f.is_file():
        return None
    raw = _load_json(f)
    out: dict = {}
    for k, v in raw.items():
        try:
            s, p = k.split("_")
            out[(int(s), int(p))] = v
        except Exception:
            continue
    return out


def _save_spec_cache(model: str, names: dict) -> None:
    f = _spec_cache_file(model)
    try:
        f.parent.mkdir(parents=True, exist_ok=True)
        serial = {f"{s}_{p}": n for (s, p), n in names.items()}
        _save_json_atomic(f, serial)
    except Exception as e:
        LOGWARN(f"Cannot write spec cache {f}: {e}")


async def _resolve_miot_urn(session: aiohttp.ClientSession, model: str) -> "str | None":
    """Find the newest MIoT spec URN for a model from the public instances list."""
    async with session.get(_MIOT_INSTANCES_URL, timeout=_MIOT_TIMEOUT) as resp:
        resp.raise_for_status()
        data = await resp.json(content_type=None)
    best_urn, best_ver = None, -1
    for inst in data.get("instances", []):
        if inst.get("model") != model:
            continue
        urn = inst.get("type", "")
        try:
            ver = int(urn.rsplit(":", 1)[-1])
        except Exception:
            ver = 0
        if ver > best_ver:
            best_urn, best_ver = urn, ver
    return best_urn


async def _fetch_miot_spec_names(session: aiohttp.ClientSession, urn: str) -> dict:
    """Fetch a MIoT instance spec → {(siid,piid): slugified-property-name}."""
    url = _MIOT_INSTANCE_URL.format(urn=urn)
    async with session.get(url, timeout=_MIOT_TIMEOUT) as resp:
        resp.raise_for_status()
        spec = await resp.json(content_type=None)
    names: dict = {}
    for svc in spec.get("services", []):
        siid = svc.get("iid")
        for prop in svc.get("properties", []):
            piid = prop.get("iid")
            desc = prop.get("description") or prop.get("name") or ""
            slug = _slugify(desc)
            if siid is not None and piid is not None and slug:
                names[(siid, piid)] = slug
    return names


async def ensure_prop_names(
    session: aiohttp.ClientSession,
    device: dict,
    props: dict,
) -> None:
    """Resolve names for a device's properties. D+A are static; B (MIoT spec) is
    only queried when A leaves unknown props, and its result is cached per model.
    On any failure the unknown props remain p_<siid>_<piid> (never lost)."""
    model = device.get("model", "")
    if not model or model in _SPEC_PROP_NAMES:
        return

    cached = _load_spec_cache(model)
    if cached is not None:
        _SPEC_PROP_NAMES[model] = cached
        return

    dt       = device.get("device_type", "vacuum")
    named    = _MOWER_NAMED if dt == "mower" else _VACUUM_NAMED
    curated  = _prop_name_map(dt)
    unknown  = [
        k for k in props
        if isinstance(k, tuple) and isinstance(k[0], int)
        and k not in named and k not in _STATION_NAMED and k not in curated
    ]
    if not unknown:
        return  # D+A already cover everything — no spec lookup needed

    names: dict = {}
    try:
        urn = await _resolve_miot_urn(session, model)
        if urn:
            names = await _fetch_miot_spec_names(session, urn)
            LOGOK(f"[{device.get('did','')}] MIoT spec resolved for {model}: "
                  f"{len(names)} names ({len(unknown)} props were unknown)")
        else:
            LOGINF(f"[{device.get('did','')}] {model} not in MIoT registry — "
                   f"{len(unknown)} props stay as p_x_y")
    except Exception as e:
        LOGWARN(f"[{device.get('did','')}] MIoT spec lookup failed ({e}) — "
                f"{len(unknown)} props stay as p_x_y")

    _SPEC_PROP_NAMES[model] = names
    _save_spec_cache(model, names)


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


# ── Room (segment) list via map decode (B) ───────────────────────────────────
# Ported from TA2k/ioBroker.dreame (decodeMultiMapData up to seg_inf). The map
# string is base64url, optionally AES-CBC encrypted, then zlib-deflated. The
# room/segment info lives in a JSON blob (`expands.seg_inf`) appended after the
# bitmap. Whole pipeline is best-effort — any failure yields [] and is logged.
def _map_aes_key(seed: str) -> bytes:
    """crypto-js getAesKey: first 32 hex chars of SHA256(seed), used as UTF-8 bytes (AES-256)."""
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:32].encode("utf-8")


def _pkcs7_unpad(data: bytes) -> bytes:
    if not data:
        return data
    pad = data[-1]
    if 1 <= pad <= 16 and len(data) >= pad:
        return data[:-pad]
    return data


def _b64_fix(s: str) -> str:
    s = s.replace("-", "+").replace("_", "/")
    return s + "=" * (-len(s) % 4)


def _decode_map_payload(map_str: str) -> bytes:
    """Decode a Dreame map frame string → raw inflated map bytes."""
    if not map_str:
        return b""
    if "," in map_str:
        src, seed = map_str.split(",", 1)
        ct = base64.b64decode(_b64_fix(src))
        cipher = Cipher(algorithms.AES(_map_aes_key(seed)), modes.CBC(b"\x00" * 16),
                        backend=default_backend())
        dec = cipher.decryptor()
        buf = _pkcs7_unpad(dec.update(ct) + dec.finalize())
    else:
        buf = base64.b64decode(_b64_fix(map_str))
    try:
        return zlib.decompress(buf)
    except zlib.error:
        return zlib.decompress(buf, -zlib.MAX_WBITS)


def _extract_seg_inf(raw: bytes) -> dict:
    """Pull expands.seg_inf out of inflated map bytes (blob starts at 27 + iw*ih)."""
    if len(raw) < 27:
        return {}
    iwidth  = int.from_bytes(raw[19:21], "little")
    iheight = int.from_bytes(raw[21:23], "little")
    start = 27 + iwidth * iheight
    if len(raw) <= start:
        return {}
    try:
        expands = json.loads(raw[start:].decode("utf-8", "ignore"))
    except Exception:
        return {}
    return expands.get("seg_inf", {}) or {}


# Dreame stores a *custom* room name (base64) in seg_inf only for type 0. For
# predefined room categories it stores just the `type` code and the app renders
# the localized name from it. We mirror that with this table (codes per
# Tasshack/dreame-vacuum). Codes 0–8 verified against a live German app; 9–15
# are best-effort. Type 0 = custom → its name comes from the base64 field.
_SEGMENT_TYPE_NAMES = {
    "de": {
        1: "Wohnzimmer", 2: "Schlafzimmer", 3: "Arbeitszimmer", 4: "Küche",
        5: "Esszimmer", 6: "Bad", 7: "Balkon", 8: "Flur", 9: "Hauswirtschaftsraum",
        10: "Ankleide", 11: "Besprechungsraum", 12: "Büro", 13: "Fitnessraum",
        14: "Freizeitraum", 15: "Gästezimmer",
    },
    "en": {
        1: "Living Room", 2: "Primary Bedroom", 3: "Study", 4: "Kitchen",
        5: "Dining Hall", 6: "Bathroom", 7: "Balcony", 8: "Corridor", 9: "Utility Room",
        10: "Closet", 11: "Meeting Room", 12: "Office", 13: "Fitness Area",
        14: "Recreation Area", 15: "Secondary Bedroom",
    },
}


def _rooms_from_seg_inf(seg_inf: dict, lang: "str | None" = None) -> list:
    """seg_inf dict → sorted [{id, name, type}].

    Name resolution: custom base64 `name` (type 0) → localized name from the
    segment `type` code → generic "Raum N"/"Room N" fallback. `type` is always
    included (None when the map carries no type for that segment)."""
    lang = (lang or SYSTEM_LANG).lower()
    type_names = _SEGMENT_TYPE_NAMES["de"] if lang.startswith("de") else _SEGMENT_TYPE_NAMES["en"]
    generic = "Raum" if lang.startswith("de") else "Room"
    rooms = []
    for area_id, item in seg_inf.items():
        try:
            rid = int(area_id)
        except Exception:
            continue
        rtype = item.get("type") if isinstance(item, dict) else None
        name = ""
        if isinstance(item, dict) and item.get("name"):
            try:
                name = base64.b64decode(item["name"]).decode("utf-8", "ignore")
            except Exception:
                name = ""
        if not name:
            name = type_names.get(rtype, f"{generic} {rid}")
        rooms.append({"id": rid, "name": name, "type": rtype})
    rooms.sort(key=lambda r: r["id"])
    return rooms


def _pick_map_frame(mapstr) -> str:
    """From the downloaded 'mapstr' pick the encoded frame carrying seg_inf."""
    if isinstance(mapstr, str):
        return mapstr
    frames = mapstr if isinstance(mapstr, list) else ([mapstr] if isinstance(mapstr, dict) else [])
    chosen = None
    for fr in frames:
        if isinstance(fr, dict) and (fr.get("first") == 0 or fr.get("id") == 0):
            chosen = fr
            break
    if chosen is None and frames and isinstance(frames[0], dict):
        chosen = frames[0]
    if isinstance(chosen, dict):
        return chosen.get("map") or chosen.get("thb") or ""
    return ""


async def _get_map_download_url(
    session: aiohttp.ClientSession, brand: dict, access_token: str,
    did: str, model: str, filename: str,
) -> "str | None":
    """POST /dreame-user-iot/iotfile/getDownloadUrl → signed map download URL."""
    url = f"https://{brand['domain']}/dreame-user-iot/iotfile/getDownloadUrl"
    headers = _build_dreame_headers(brand, access_token)
    headers["content-type"] = "application/json"
    body = {"did": did, "model": model, "filename": filename, "region": "eu"}
    async with session.post(url, headers=headers, json=body, ssl=False) as resp:
        resp.raise_for_status()
        data = await resp.json(content_type=None)
    return data.get("data")


async def load_rooms(
    session: aiohttp.ClientSession, brand: dict, access_token: str,
    did: str, model: str,
) -> list:
    """Fetch + decode the device map → list of {id, name} rooms (vacuum only).
    Best-effort: returns [] on any failure. Pipeline ported from ioBroker.dreame."""
    # 1) current map object name (siid 6, piid 8)
    props = await get_properties(session, brand, access_token, did, [(6, 8)])
    raw_val = props.get((6, 8))
    object_name = ""
    if isinstance(raw_val, dict):
        object_name = raw_val.get("object_name", "")
    elif isinstance(raw_val, str):
        try:
            object_name = json.loads(raw_val).get("object_name", "")
        except Exception:
            object_name = ""
    if not object_name:
        return []
    # 2) signed download URL
    file_url = await _get_map_download_url(session, brand, access_token, did, model, object_name)
    if not file_url:
        return []
    # 3) download map JSON
    async with session.get(file_url, ssl=False) as resp:
        resp.raise_for_status()
        content = await resp.json(content_type=None)
    mapstr = (content or {}).get("mapstr")
    if mapstr is None:
        return []
    # 4) decode → seg_inf → rooms
    raw = _decode_map_payload(_pick_map_frame(mapstr))
    return _rooms_from_seg_inf(_extract_seg_inf(raw))


# ── Dreame Cloud MQTT ─────────────────────────────────────────────────────────

class DreameMqttClient:
    """paho-MQTT client for Dreame Cloud MQTTS, runs in its own thread.
    State updates are forwarded via asyncio.Queue to the asyncio event loop."""

    def __init__(
        self,
        bind_domain: str,
        did: str,
        uid: str,
        uid_num: str,
        master_uid: str,
        master_uid_uuid: str,
        access_token: str,
        queue: asyncio.Queue,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        host_port        = bind_domain.split(":")
        self._host       = host_port[0]
        self._port       = int(host_port[1]) if len(host_port) > 1 else 8883
        self._did        = did
        self._uid        = uid
        self._uid_num    = uid_num
        self._master_uid = master_uid
        self._master_uid_uuid = master_uid_uuid
        self._token      = access_token
        self._queue      = queue
        self._loop       = loop
        # Map mid → topic list for subscription debugging
        self._sub_mid_topics: dict = {}

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
        self._client.on_subscribe  = self._on_subscribe

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
            raw = msg.payload.decode("utf-8")
            LOGINF(f"[{self._did}] Dreame MQTT raw topic={msg.topic} payload={raw[:500]}")
            payload = json.loads(raw)
            asyncio.run_coroutine_threadsafe(
                self._queue.put({"did": self._did, "payload": payload}),
                self._loop,
            )
        except Exception as e:
            LOGWARN(f"[{self._did}] Dreame MQTT message parse error: {e} raw={msg.payload[:200]}")

    def _on_subscribe(self, client, userdata, mid, granted_qos) -> None:
        if any(q >= 128 for q in granted_qos):
            LOGWARN(f"[{self._did}] Dreame MQTT subscription refused (qos={granted_qos})")

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
                current_props[(3, 1)] = parsed.get("battery",  current_props.get((3, 1), 0))
                current_props[(3, 2)] = parsed.get("charging", current_props.get((3, 2), 0))
                # robot_state is the faster push path for the mower status; 0 carries
                # no information, so never let it overwrite a polled (2,1).
                if parsed.get("robot_state", 0):
                    current_props[(2, 1)] = parsed["robot_state"]
                if parsed.get("error_code", 0):
                    current_props[(2, 2)] = parsed["error_code"]
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
    # cleaning_mode is (4,23) but goes through _set_cleaning_mode() instead — the
    # value is packed on Gen2 devices and needs a read-modify-write.
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


# ── Room (segment) cleaning command (A + B) ──────────────────────────────────
# Triggered via the set topic with payload 'clean_rooms:<arg>'. Two forms:
#   • CSV  : 'clean_rooms:3,5,2'                          → rooms with live defaults
#   • JSON : 'clean_rooms:{"rooms":[{"id":3,"suction":3}]}' → per-room overrides
#            'clean_rooms:[[3,1,2,2,1]]'                  → raw selects passthrough
# Cloud call is action siid=4 aiid=1 in=[{piid:1,value:18},{piid:10,value:selects}].
_CLEAN_ROOMS_DEFAULT_REPEATS = 1   # per-command, not a device property
_CLEAN_ROOMS_FALLBACK_SUCTION = 1  # Standard — only if the live read fails
_CLEAN_ROOMS_FALLBACK_WATER   = 2  # Medium   — only if the live read fails


def _parse_clean_rooms_arg(arg: str) -> list:
    """Parse the '<arg>' of 'clean_rooms:<arg>' into [{id, repeats?, suction?, water?}, …]."""
    arg = arg.strip()
    if arg[:1] in ("{", "["):
        data = json.loads(arg)
        if isinstance(data, list):                      # raw selects array
            return [{"_raw": item} for item in data]
        rooms = data.get("rooms", []) if isinstance(data, dict) else []
        out = []
        for r in rooms:
            if isinstance(r, dict):
                out.append(r)
            elif isinstance(r, (int, str)):
                out.append({"id": int(r)})
        return out
    return [{"id": int(tok)} for tok in arg.split(",") if tok.strip()]


def _build_selects_string(rooms: list, def_suction: int, def_water: int) -> str:
    """Build the '{"selects":[[id,repeats,suction,water,order]]}' JSON string."""
    selects, order = [], 1
    for r in rooms:
        if isinstance(r.get("_raw"), list):
            selects.append(r["_raw"])
        else:
            selects.append([
                int(r["id"]),
                int(r.get("repeats", _CLEAN_ROOMS_DEFAULT_REPEATS)),
                int(r.get("suction", def_suction)),
                int(r.get("water",   def_water)),
                order,
            ])
        order += 1
    return json.dumps({"selects": selects}, separators=(",", ":"))


async def _handle_clean_rooms(
    session: aiohttp.ClientSession,
    brand: dict,
    access_token: str,
    device: dict,
    command: str,
) -> "tuple[str, int]":
    """Segment cleaning (vacuum only). Returns (result_str, result_num)."""
    did = device["did"]
    arg = command.split(":", 1)[1] if ":" in command else ""
    rooms = _parse_clean_rooms_arg(arg)
    if not rooms:
        LOGWARN(f"[{did}] clean_rooms: no rooms parsed from '{arg}'")
        return "error", 1
    # Defaults for unset per-room values = the device's *current* suction/water,
    # read live from the cloud (the poll tasks' props are not shared with this task).
    def_suction, def_water = _CLEAN_ROOMS_FALLBACK_SUCTION, _CLEAN_ROOMS_FALLBACK_WATER
    try:
        live = await get_properties(session, brand, access_token, did, [(4, 4), (4, 5)])
        if isinstance(live.get((4, 4)), int):
            def_suction = live[(4, 4)]
        if isinstance(live.get((4, 5)), int):
            def_water = live[(4, 5)]
    except Exception as e:
        LOGWARN(f"[{did}] clean_rooms: live suction/water read failed ({e}) — using fallbacks")
    selects = _build_selects_string(rooms, def_suction, def_water)
    LOGINF(f"[{did}] clean_rooms → {selects}")
    params = {
        "did": did, "siid": 4, "aiid": 1,
        "in": [{"piid": 1, "value": 18}, {"piid": 10, "value": selects}],
    }
    await send_command(session, brand, access_token, did, "action", params)
    return "ok", 0


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
            if command == "clean_rooms" or command.startswith("clean_rooms:"):
                return await _handle_clean_rooms(session, brand, access_token, device, command)
            elif command in VACUUM_ACTIONS:
                action = VACUUM_ACTIONS[command]
                params = {**action, "did": did}
                await send_command(session, brand, access_token, did, "action", params)
            else:
                return "error", 1
        return "ok", 0
    except Exception as e:
        LOGERR(f"[{did}] set '{command}' error: {e}")
        return "error", 1


async def _set_cleaning_mode(
    session: aiohttp.ClientSession,
    brand: dict,
    access_token: str,
    did: str,
    value,
    model: str,
) -> bool:
    """Write the cleaning mode to (4,23), preserving the packed upper bytes.
    Reads the current value plus the two capability properties first: a bare write
    would zero the settings packed alongside the mode, and how wide the mode field
    is depends on whether the device lifts its mop pad. The value is the same plain
    mode that build_state_json publishes as cleaning_mode, so reading and writing
    stay symmetric. Returns True on success."""
    try:
        requested = int(value)
    except (TypeError, ValueError):
        LOGERR(f"[{did}] cleaning_mode: '{value}' is not a number")
        return False
    mask, raw = 1, None
    try:
        current = await get_properties(
            session, brand, access_token, did, [(4, 23), (4, 25), (15, 3)]
        )
        mask = 3 if _has_mop_pad_lifting(current, model) else 1
        raw  = current.get((4, 23))
    except Exception as e:
        # Fall back to the bare mode — better than refusing the command outright.
        LOGWARN(f"[{did}] cleaning_mode: could not read current value ({e}), "
                f"writing plain mode {requested}")
    mode      = requested & mask
    new_value = mode
    if isinstance(raw, int) and not isinstance(raw, bool) and raw > 0xFF:
        new_value = (raw & ~mask) | mode
    params = [{"did": did, "siid": 4, "piid": 23, "value": new_value}]
    await send_command(session, brand, access_token, did, "set_properties", params)
    return True


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
            if key == "cleaning_mode":
                # (4,23) is packed on Gen2 devices — writing a bare mode there would
                # wipe the upper bytes, so read the current value first and replace
                # only the mode bits.
                if not await _set_cleaning_mode(session, brand, access_token, did,
                                                value, device.get("model", "")):
                    return "error", 1
            elif key in VACUUM_SETTINGS:
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
    initial_props: "dict | None" = None,
) -> None:
    """Receive state updates from Dreame Cloud queue, publish to LoxBerry MQTT."""
    did = device["did"]
    dt  = device["device_type"]
    current_props: dict = initial_props.copy() if initial_props else {}
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


_VACUUM_POLL_PROPS = [
    # SIID 2 – Robot Cleaner (base state, used by some models)
    (2, 1), (2, 2),
    # SIID 3 – Battery
    (3, 1), (3, 2), (3, 3),
    # SIID 4 – Vacuum Extend (core status + settings)
    (4, 1),  (4, 2),  (4, 3),  (4, 4),  (4, 5),  (4, 6),  (4, 7),
    (4, 11), (4, 12), (4, 13), (4, 14),
    (4, 16), (4, 17), (4, 18), (4, 19), (4, 20),
    (4, 21), (4, 22), (4, 23), (4, 24), (4, 25), (4, 26), (4, 27), (4, 28), (4, 29),
    (4, 33), (4, 34), (4, 35), (4, 36), (4, 37),
    (4, 40), (4, 41), (4, 45), (4, 46), (4, 47), (4, 48), (4, 49),
    # (4,50) AutoSwitch composite is intentionally NOT bulk-polled: on Gen2 models
    # it voids its whole get_properties batch (battery/status would read 0). It is
    # delivered via the realtime push path (map_properties_changed); get_properties'
    # split-retry is the safety net should it ever sneak back into a batch.
    (4, 51), (4, 52), (4, 53), (4, 58), (4, 60), (4, 63), (4, 64), (4, 83),
    # SIID 5 – DND
    (5, 1), (5, 2), (5, 3), (5, 4),
    # SIID 7 – Volume
    (7, 1),
    # SIID 9 – Main Brush
    (9, 1), (9, 2),
    # SIID 10 – Side Brush
    (10, 1), (10, 2),
    # SIID 11 – Filter
    (11, 1), (11, 2),
    # SIID 12 – Statistics
    (12, 1), (12, 2), (12, 3), (12, 4), (12, 5), (12, 6),
    # SIID 15 – Auto Empty
    (15, 1), (15, 2), (15, 3), (15, 5),
    # SIID 16 – Sensor
    (16, 1), (16, 2),
    # SIID 17 – Secondary filter
    (17, 1), (17, 2),
    # SIID 18 – Mop pad
    (18, 1), (18, 2),
    # SIID 26 – Dirty water tank
    (26, 1), (26, 2),
    # SIID 25 – Station status (older models)
    (25, 1), (25, 2), (25, 3), (25, 4), (25, 5),
    # SIID 27 – Station status (newer models)
    (27, 1), (27, 2), (27, 3), (27, 4), (27, 5), (27, 15),
    # SIID 28 – Extended Settings
    (28, 1),  (28, 2),  (28, 3),  (28, 4),  (28, 5),  (28, 8),
    (28, 14), (28, 15), (28, 16), (28, 18), (28, 22),
    (28, 27), (28, 28), (28, 29), (28, 52),
    # SIID 30 – Wheel
    (30, 1), (30, 2),
]
_MOWER_POLL_PROPS = [
    # SIID 2 – Mower Service: status (2,1) is the value the app displays
    (2, 1), (2, 2), (2, 50), (2, 52), (2, 55), (2, 56), (2, 58), (2, 65),
    # SIID 3 – Battery
    (3, 1), (3, 2),
    # SIID 4 – Mower Extend: runtime, area, task and warning status
    (4, 2), (4, 3), (4, 7), (4, 14), (4, 18), (4, 21), (4, 27), (4, 35),
    (4, 42), (4, 43),
    # SIID 5 – Positioning
    (5, 100), (5, 106), (5, 107),
    # SIID 12 – Statistics
    (12, 1), (12, 2), (12, 3), (12, 4),
]


async def task_state_poll(
    device: dict,
    broker: dict,
    base_topic: str,
    session: aiohttp.ClientSession,
    brand: dict,
    cfg: dict,
    mower_settings: "dict | None",
    initial_props: "dict | None" = None,
) -> None:
    """Poll Dreame cloud for state changes and publish to LoxBerry MQTT."""
    did = device["did"]
    dt  = device["device_type"]
    current_props: dict = initial_props.copy() if initial_props else {}
    poll_props = _VACUUM_POLL_PROPS if dt == "vacuum" else _MOWER_POLL_PROPS
    mqtt_kwargs = _build_mqtt_kwargs(broker)

    while not _shutdown_event.is_set():
        interval = int(cfg.get("state_poll_interval_sec", 60))
        await asyncio.sleep(interval)
        if _shutdown_event.is_set():
            break
        try:
            new_props = await get_properties(session, brand, cfg["access_token"], did, poll_props)
            if not new_props:
                continue
            changed = {k: v for k, v in new_props.items() if current_props.get(k) != v}
            if not changed:
                continue
            current_props.update(new_props)
            state = build_state_json(device, current_props)
            if dt == "mower" and mower_settings:
                state.update({k: v for k, v in mower_settings.items() if not k.startswith("_")})
            async with aiomqtt.Client(**mqtt_kwargs) as lbmqtt:
                await lbmqtt.publish(f"{base_topic}/{did}/state", json.dumps(state), retain=True)
                if dt == "vacuum":
                    station = build_station_json(current_props)
                    await lbmqtt.publish(
                        f"{base_topic}/{did}/state_station", json.dumps(station), retain=True
                    )
            LOGINF(f"[{did}] State polled: {len(changed)} props changed")
        except Exception as e:
            LOGERR(f"[{did}] State poll error: {e}")


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
                async for msg in lbmqtt.messages:
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
    broker: dict,
    base_topic: str,
) -> None:
    """Refresh access token 5 minutes before expiry; fall back to re-login on error.
    Only writes the config to the SD card when the refresh_token actually changes —
    access_token/expires_at/uid are ephemeral (memory only)."""
    while not _shutdown_event.is_set():
        await asyncio.sleep(30)
        now        = time.time()
        expires_at = cfg.get("expires_at", 0)
        if expires_at == 0 or now < expires_at - 300:
            continue
        LOGINF("Token expiring — refreshing")
        async with cfg_lock:
            try:
                old_refresh          = cfg.get("refresh_token", "")
                tokens               = await dreame_refresh_token(session, brand, old_refresh)
                cfg["access_token"]  = tokens["access_token"]
                cfg["refresh_token"] = tokens["refresh_token"]
                cfg["uid_num"]       = tokens.get("uid_num", "")
                cfg["expires_at"]    = time.time() + tokens["expires_in"]
                if cfg["refresh_token"] != old_refresh:
                    save_plugin_config(cfg)
                    LOGOK("Token refreshed — new refresh_token persisted")
                else:
                    LOGOK("Token refreshed (memory only)")
            except Exception as e:
                LOGERR(f"Token refresh failed ({e}) — attempting re-login")
                try:
                    tokens = await dreame_login(
                        session, brand, cfg["username"],
                        cfg.get("password_plain", "")
                    )
                    cfg["access_token"]  = tokens["access_token"]
                    cfg["refresh_token"] = tokens["refresh_token"]
                    cfg["uid"]           = tokens["uid"]
                    cfg["uid_num"]       = tokens.get("uid_num", "")
                    cfg["expires_at"]    = time.time() + tokens["expires_in"]
                    save_plugin_config(cfg)
                    LOGOK("Re-login successful")
                except Exception as e2:
                    LOGERR(f"Re-login failed: {e2}")
        await publish_gateway_status(broker, base_topic, cfg)


async def task_statistic_poll(
    devices: list,
    session: aiohttp.ClientSession,
    brand: dict,
    cfg: dict,
    broker: dict,
    base_topic: str,
) -> None:
    """Poll statistics/consumables (and the vacuum room list) periodically (default 300 s)."""
    interval = int(cfg.get("statistic_poll_interval_sec", 300))
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

            # Room (segment) list — vacuum only, same interval, best-effort.
            if device["device_type"] == "vacuum":
                try:
                    rooms = await load_rooms(
                        session, brand, cfg["access_token"], did, device.get("model", "")
                    )
                    mqtt_kwargs = _build_mqtt_kwargs(broker)
                    async with aiomqtt.Client(**mqtt_kwargs) as lbmqtt:
                        await lbmqtt.publish(
                            f"{base_topic}/{did}/rooms",
                            json.dumps({"rooms": rooms}),
                            retain=True,
                        )
                    LOGDEB(f"[{did}] Rooms published: {len(rooms)}")
                except Exception as e:
                    LOGWARN(f"[{did}] Rooms fetch error: {e}")


# ── main ──────────────────────────────────────────────────────────────────────
async def _async_main() -> None:
    # Build the shutdown event inside the running loop (see note at its declaration).
    global _shutdown_event
    _shutdown_event = asyncio.Event()
    LOGSTART("Dreame Gateway started")
    write_pid()
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT,  _handle_sigterm)

    cfg     = load_plugin_config()
    general = _load_json(GENERAL_JSON)
    broker  = get_mqtt_broker_config(general)

    # System language (Base.Lang) drives derived room names: "de" → German, else English.
    global SYSTEM_LANG
    SYSTEM_LANG = str((general.get("Base") or {}).get("Lang", "en")) or "en"
    LOGINF(f"System language: {SYSTEM_LANG}")
    brand   = BRAND_CONFIG.get(cfg.get("cloud_service", "dreame"), BRAND_CONFIG["dreame"])

    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(
        connector=connector,
        headers={"Accept-Encoding": "gzip, deflate"},
    ) as session:
        # Obtain a fresh access token. access_token/expires_at/uid are ephemeral
        # (always empty after load), so authenticate on every start. Prefer the
        # refresh_token; only fall back to a full password login if that fails.
        authed = False
        if cfg.get("refresh_token"):
            try:
                old_refresh          = cfg["refresh_token"]
                tokens               = await dreame_refresh_token(session, brand, old_refresh)
                cfg["access_token"]  = tokens["access_token"]
                cfg["refresh_token"] = tokens["refresh_token"]
                cfg["uid"]           = tokens.get("uid", "")
                cfg["uid_num"]       = tokens.get("uid_num", "")
                cfg["expires_at"]    = time.time() + tokens["expires_in"]
                if cfg["refresh_token"] != old_refresh:
                    save_plugin_config(cfg)
                authed = True
                LOGOK("Authenticated via refresh_token")
            except Exception as e:
                LOGWARN(f"Startup token refresh failed ({e}) — trying password login")
        if not authed:
            password_plain = cfg.get("password_plain", "")
            if not password_plain:
                LOGERR("No password in config — please log in via WebUI")
                return
            LOGINF("Logging in with username/password")
            tokens = await dreame_login(session, brand, cfg["username"], password_plain)
            cfg["access_token"]  = tokens["access_token"]
            cfg["refresh_token"] = tokens["refresh_token"]
            cfg["uid"]           = tokens["uid"]
            cfg["uid_num"]       = tokens.get("uid_num", "")
            cfg["expires_at"]    = time.time() + tokens["expires_in"]
            save_plugin_config(cfg)
            LOGOK("Login successful")

        # Publish initial auth status for the WebUI
        await publish_gateway_status(broker, cfg.get("base_topic", "dreame"), cfg)

        # Load device list (fall back to cached); only persist if it changed
        devices = cfg.get("devices", [])
        try:
            new_devices = await get_device_list(session, brand, cfg["access_token"])
            if new_devices != devices:
                cfg["devices"] = new_devices
                save_plugin_config(cfg)
            devices = new_devices
            LOGOK(f"{len(devices)} device(s) found")
        except Exception as e:
            LOGWARN(f"Could not load device list ({e}), using cache")

        if not devices:
            LOGERR("No devices found — gateway exiting")
            return

        cfg_lock   = asyncio.Lock()
        base_topic = cfg.get("base_topic", "dreame")

        tasks_coro = []

        for device in devices:
            did = device["did"]

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

            # Fetch all initial properties and publish state + state_station
            initial_props: "dict | None" = None
            try:
                poll_props = _VACUUM_POLL_PROPS if device["device_type"] == "vacuum" else _MOWER_POLL_PROPS
                initial_props = await get_properties(
                    session, brand, cfg["access_token"], did, poll_props
                )
                LOGOK(f"[{did}] Initial props loaded: {len(initial_props)} values")
                # Resolve property names (D+A static, B = MIoT spec only if needed)
                # before the first publish so state keys are descriptive from the start.
                await ensure_prop_names(session, device, initial_props)
                mqtt_kwargs = _build_mqtt_kwargs(broker)
                async with aiomqtt.Client(**mqtt_kwargs) as lbmqtt:
                    state = build_state_json(device, initial_props)
                    if mower_settings:
                        state.update({k: v for k, v in mower_settings.items() if not k.startswith("_")})
                    await lbmqtt.publish(f"{base_topic}/{did}/state", json.dumps(state), retain=True)
                    if device["device_type"] == "vacuum":
                        station = build_station_json(initial_props)
                        await lbmqtt.publish(
                            f"{base_topic}/{did}/state_station", json.dumps(station), retain=True
                        )
            except Exception as e:
                LOGWARN(f"[{did}] Initial props error: {e}")

            tasks_coro.append(task_state_poll(
                device, broker, base_topic, session, brand, cfg,
                mower_settings, initial_props
            ))
            tasks_coro.append(task_lbmqtt_to_dreame(
                device, broker, base_topic, session, brand, cfg, pre_array_ref
            ))

        tasks_coro.append(task_token_refresh(session, brand, cfg, cfg_lock, broker, base_topic))
        tasks_coro.append(task_statistic_poll(devices, session, brand, cfg, broker, base_topic))

        all_tasks = [asyncio.create_task(c) for c in tasks_coro]
        await _shutdown_event.wait()

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
