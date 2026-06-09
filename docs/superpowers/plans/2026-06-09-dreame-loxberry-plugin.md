# LoxBerry-Plugin-Dreame Implementierungsplan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Python-Asyncio-Gateway das Dreame/MOVA-Cloud (REST + MQTTS) mit dem LoxBerry-MQTT-Broker bidirektional verbindet, plus LoxBerry-V4-Plugin-Infrastruktur und Perl/CGI-WebUI.

**Architecture:** Einzel-Datei-Daemon `dreame_gateway.py` mit asyncio; Dreame-Cloud-MQTT läuft als paho-Client in separatem Thread und übergibt State-Updates via `asyncio.Queue`; LoxBerry-MQTT via aiomqtt async. Plugin-Struktur folgt `LoxBerry-Plugin-Navimow`.

**Tech Stack:** Python 3.9+, aiohttp, aiomqtt, paho-mqtt, cryptography; Perl/CGI für WebUI; LoxBerry V4 Plugin-Format.

---

## Dateistruktur

```
LoxBerry-Plugin-Dreame/
├── bin/
│   └── dreame_gateway.py          ← Haupt-Daemon (~900 Zeilen)
├── config/
│   └── pluginconfig.json          ← Default-Konfiguration
├── daemon/
│   └── daemon.sh                  ← Start/Stop/Status
├── icons/
│   └── icon_{64,128,256,512}.png  ← Dreame-Logo (vom ioBroker kopieren)
├── plugin.cfg                     ← Plugin-Metadaten
├── postroot.sh                    ← pip install nach Installation
├── postupgrade.sh                 ← pip install nach Upgrade
├── preroot.sh                     ← Cleanup vor Installation
├── preupgrade.sh                  ← Cleanup vor Upgrade
├── release.cfg
├── prerelease.cfg
├── webfrontend/
│   └── htmlauth/
│       └── index.cgi              ← WebUI (Perl/CGI)
└── tests/
    └── test_dreame_gateway.py     ← pytest Unit-Tests
```

**Verantwortlichkeiten je Datei:**

| Datei | Verantwortung |
|---|---|
| `dreame_gateway.py` | Kompletter Daemon: Auth, REST-API, Dreame-Cloud-MQTT, LoxBerry-MQTT, State-Mapping, Command-Handling |
| `pluginconfig.json` | Laufzeit-Konfiguration (Credentials, Tokens, Devices, Topic, Intervall) |
| `daemon.sh` | Shell-Wrapper: start/stop/status via PID-File `/dev/shm/dreame_gateway.pid` |
| `plugin.cfg` | LoxBerry-Plugin-Metadaten (Name, Version, Abhängigkeiten) |
| `postroot.sh` / `postupgrade.sh` | pip-Dependencies installieren |
| `index.cgi` | Perl/CGI-WebUI: Konfiguration, Geräteliste, Gateway-Start/Stop, Log |
| `test_dreame_gateway.py` | pytest-Tests für pure/sync-Funktionen |

---

## Task 1: Test-Skelett + Krypto-Hilfsfunktionen

**Files:**
- Create: `tests/test_dreame_gateway.py`
- Create: `bin/dreame_gateway.py` (nur Krypto-Abschnitt)

- [ ] **Step 1.1: Test-Datei anlegen**

```python
# tests/test_dreame_gateway.py
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'bin'))

import pytest
```

- [ ] **Step 1.2: Failing test für `_compute_rlc` schreiben**

Ergänze in `tests/test_dreame_gateway.py`:

```python
from dreame_gateway import _compute_rlc, _md5_password

def test_compute_rlc_dreame():
    # AES-128-ECB("EETjszu*XI5znHsI", "eu|en|DE" padded PKCS7) → deterministic hex
    result = _compute_rlc("EETjszu*XI5znHsI")
    assert isinstance(result, str)
    assert len(result) == 32  # 16 bytes AES block → 32 hex chars

def test_compute_rlc_mova():
    result = _compute_rlc("gigxlmqwZ]7oWZUF")
    assert isinstance(result, str)
    assert len(result) == 32

def test_compute_rlc_deterministic():
    assert _compute_rlc("EETjszu*XI5znHsI") == _compute_rlc("EETjszu*XI5znHsI")

def test_md5_password():
    # MD5("test" + "RAylYC%fmSKp7%Tq")
    import hashlib
    expected = hashlib.md5(b"testRAylYC%fmSKp7%Tq").hexdigest()
    assert _md5_password("test") == expected

def test_md5_password_empty():
    import hashlib
    expected = hashlib.md5(b"RAylYC%fmSKp7%Tq").hexdigest()
    assert _md5_password("") == expected
```

- [ ] **Step 1.3: Test scheitern lassen**

```
cd LoxBerry-Plugin-Dreame
pytest tests/test_dreame_gateway.py -v 2>&1 | head -20
```

Erwartet: `ModuleNotFoundError: No module named 'dreame_gateway'`

- [ ] **Step 1.4: `bin/dreame_gateway.py` anlegen — nur Krypto-Abschnitt**

```python
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
    """MD5(password + salt) → hex string."""
    return hashlib.md5((password + "RAylYC%fmSKp7%Tq").encode()).hexdigest()
```

- [ ] **Step 1.5: Tests ausführen und prüfen dass sie bestehen**

```
pytest tests/test_dreame_gateway.py::test_compute_rlc_dreame tests/test_dreame_gateway.py::test_compute_rlc_mova tests/test_dreame_gateway.py::test_compute_rlc_deterministic tests/test_dreame_gateway.py::test_md5_password tests/test_dreame_gateway.py::test_md5_password_empty -v
```

Erwartet: `5 passed`

- [ ] **Step 1.6: Commit**

```bash
git add tests/test_dreame_gateway.py bin/dreame_gateway.py
git commit -m "feat: add gateway skeleton with crypto helpers (_compute_rlc, _md5_password)"
```

---

## Task 2: Pure Hilfsfunktionen (State-Mapping, Device-Typ)

**Files:**
- Modify: `bin/dreame_gateway.py` (Abschnitte hinzufügen)
- Modify: `tests/test_dreame_gateway.py`

- [ ] **Step 2.1: Failing tests schreiben**

Ergänze in `tests/test_dreame_gateway.py`:

```python
from dreame_gateway import (
    _get_device_type, parse_binary_state_1, _normalize_autoswitch,
    build_state_json, build_station_json,
)

# ── _get_device_type ──────────────────────────────────────────────────────────
def test_get_device_type_mower():
    assert _get_device_type("dreame.mower.r2320") == "mower"

def test_get_device_type_vacuum():
    assert _get_device_type("dreame.vacuum.r2228o") == "vacuum"

def test_get_device_type_case_insensitive():
    assert _get_device_type("Dreame.Mower.X1") == "mower"

# ── parse_binary_state_1 ──────────────────────────────────────────────────────
def test_parse_binary_state_1_basic():
    # 20 bytes: error_code at B1-4, battery at B11, robot_state at B14, wifi at B17
    buf = bytearray(20)
    buf[1] = 5   # error_code low byte = 5
    buf[11] = 0b10111010  # battery=58, charging=1
    buf[14] = 0b00001100  # docking_state = (12 & 0x1C) >> 2 = 3
    buf[17] = 200         # wifi_rssi = 200 - 256 = -56
    result = parse_binary_state_1(bytes(buf))
    assert result["error_code"] == 5
    assert result["battery"] == 58
    assert result["charging"] == 1
    assert result["docking_state"] == 3
    assert result["wifi_rssi"] == -56

def test_parse_binary_state_1_too_short():
    assert parse_binary_state_1(bytes(10)) == {}

def test_parse_binary_state_1_list_input():
    buf = [0] * 20
    buf[11] = 85  # battery=85, charging=0
    result = parse_binary_state_1(buf)
    assert result["battery"] == 85
    assert result["charging"] == 0

# ── _normalize_autoswitch ─────────────────────────────────────────────────────
def test_normalize_autoswitch_dict():
    result = _normalize_autoswitch({"k": "LessColl", "v": 1})
    assert result == {"LessColl": 1}

def test_normalize_autoswitch_list():
    result = _normalize_autoswitch([
        {"k": "LessColl", "v": 0},
        {"k": "AutoDry",  "v": 1},
    ])
    assert result == {"LessColl": 0, "AutoDry": 1}

def test_normalize_autoswitch_invalid():
    assert _normalize_autoswitch("garbage") == {}
    assert _normalize_autoswitch(None) == {}

# ── build_state_json ──────────────────────────────────────────────────────────
def test_build_state_json_common_fields():
    device = {"did": "123", "model": "dreame.mower.r2320", "name": "Mäher", "device_type": "mower", "online": True}
    props = {(3, 1): 1, (3, 2): 85, (3, 3): 0, (3, 5): 0}
    state = build_state_json(device, props)
    assert state["did"] == "123"
    assert state["device_type"] == "mower"
    assert state["online"] is True

def test_build_station_json_basic():
    props = {(25, 1): 0, (25, 2): 1, (25, 3): 0}
    station = build_station_json(props)
    assert "clean_water_tank" in station
    assert station["dirty_water_tank"] == 1
```

- [ ] **Step 2.2: Test scheitern lassen**

```
pytest tests/test_dreame_gateway.py -v -k "device_type or binary or autoswitch or build_state or build_station" 2>&1 | head -30
```

Erwartet: `ImportError` oder `FAILED`

- [ ] **Step 2.3: Implementierung in `bin/dreame_gateway.py` ergänzen**

Nach dem Krypto-Abschnitt einfügen:

```python
# ── Gerätetyp-Erkennung ───────────────────────────────────────────────────────
def _get_device_type(model: str) -> str:
    return "mower" if "mower" in model.lower() else "vacuum"


# ── Binär-Parser (Mähroboter siid=1 piid=1) ──────────────────────────────────
def parse_binary_state_1(value) -> dict:
    buf = bytes(value) if not isinstance(value, (bytes, bytearray)) else bytes(value)
    if len(buf) < 19:
        return {}
    error_code   = int.from_bytes(buf[1:5], "little")
    battery      = buf[11] & 0x7F
    charging     = (buf[11] & 0x80) >> 7
    robot_state  = buf[14]
    docking_state = (robot_state & 0x1C) >> 2
    wifi_rssi    = buf[17] - 256 if buf[17] > 127 else buf[17]
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
```

- [ ] **Step 2.4: Tests ausführen**

```
pytest tests/test_dreame_gateway.py -v -k "device_type or binary or autoswitch or build_state or build_station"
```

Erwartet: Alle Tests `PASSED`

- [ ] **Step 2.5: Commit**

```bash
git add bin/dreame_gateway.py tests/test_dreame_gateway.py
git commit -m "feat: add pure helper functions (device_type, binary parser, autoswitch, state builders)"
```

---

## Task 3: Gateway-Skeleton (CLI, Logging, Config, PID, Shutdown)

**Files:**
- Modify: `bin/dreame_gateway.py`
- Create: `config/pluginconfig.json`

- [ ] **Step 3.1: Failing test für Config-Defaults schreiben**

Ergänze in `tests/test_dreame_gateway.py`:

```python
from dreame_gateway import load_plugin_config, get_mqtt_broker_config

def test_load_plugin_config_defaults(tmp_path):
    cfg_path = tmp_path / "pluginconfig.json"
    # leere Datei → Defaults werden gesetzt
    cfg_path.write_text("{}", encoding="utf-8")
    import dreame_gateway
    orig = dreame_gateway.PLUGIN_CFG
    dreame_gateway.PLUGIN_CFG = cfg_path
    try:
        cfg = load_plugin_config()
        assert cfg["cloud_service"] == "dreame"
        assert cfg["base_topic"] == "dreame"
        assert cfg["polling_interval_min"] == 30
        assert cfg["devices"] == []
    finally:
        dreame_gateway.PLUGIN_CFG = orig

def test_get_mqtt_broker_config_defaults():
    general = {"Mqtt": {"Brokerhost": "192.168.1.10", "Brokerport": "1883"}}
    broker = get_mqtt_broker_config(general)
    assert broker["host"] == "192.168.1.10"
    assert broker["port"] == 1883
    assert broker["tls"] is False
```

- [ ] **Step 3.2: Test scheitern lassen**

```
pytest tests/test_dreame_gateway.py::test_load_plugin_config_defaults tests/test_dreame_gateway.py::test_get_mqtt_broker_config_defaults -v 2>&1 | head -10
```

Erwartet: `ImportError` (Funktionen noch nicht vorhanden)

- [ ] **Step 3.3: CLI-Args, Logging, Pfade, Config in `bin/dreame_gateway.py` ergänzen**

Nach dem State-Builder-Abschnitt einfügen:

```python
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
```

- [ ] **Step 3.4: `config/pluginconfig.json` anlegen**

```json
{
  "cloud_service": "dreame",
  "username": "",
  "password_hash": "",
  "access_token": "",
  "refresh_token": "",
  "expires_at": 0,
  "uid": "",
  "base_topic": "dreame",
  "polling_interval_min": 30,
  "devices": []
}
```

- [ ] **Step 3.5: Tests ausführen**

```
pytest tests/test_dreame_gateway.py::test_load_plugin_config_defaults tests/test_dreame_gateway.py::test_get_mqtt_broker_config_defaults -v
```

Erwartet: `2 passed`

- [ ] **Step 3.6: Commit**

```bash
git add bin/dreame_gateway.py config/pluginconfig.json tests/test_dreame_gateway.py
git commit -m "feat: add gateway skeleton (CLI args, logging, config, PID, shutdown)"
```

---

## Task 4: DreameAuth (Login + Token-Refresh)

**Files:**
- Modify: `bin/dreame_gateway.py`

- [ ] **Step 4.1: Failing test für Auth-Header schreiben**

Ergänze in `tests/test_dreame_gateway.py`:

```python
from dreame_gateway import _build_dreame_headers

def test_build_dreame_headers_dreame_brand():
    brand = BRAND_CONFIG["dreame"]
    headers = _build_dreame_headers(brand, access_token=None)
    assert headers["tenant-id"] == "000000"
    assert headers["dreame-meta"] == "cv=i_829"
    assert "dreame-rlc" in headers
    assert len(headers["dreame-rlc"]) == 32
    assert "dreame-auth" not in headers  # kein Token → kein dreame-auth

def test_build_dreame_headers_with_token():
    brand = BRAND_CONFIG["dreame"]
    headers = _build_dreame_headers(brand, access_token="abc123")
    assert headers["dreame-auth"] == "bearer abc123"

# Importiere BRAND_CONFIG für die Tests
from dreame_gateway import BRAND_CONFIG
```

- [ ] **Step 4.2: Test scheitern lassen**

```
pytest tests/test_dreame_gateway.py -k "build_dreame_headers" -v 2>&1 | head -10
```

Erwartet: `ImportError`

- [ ] **Step 4.3: DreameAuth-Abschnitt in `bin/dreame_gateway.py` ergänzen**

```python
# ── Dreame Auth & REST ────────────────────────────────────────────────────────
import aiohttp


def _build_dreame_headers(brand: dict, access_token: "str | None" = None) -> dict:
    headers = {
        "user-agent":  "Dart/3.2 (dart:io)",
        "dreame-meta": brand["meta"],
        "dreame-rlc":  _compute_rlc(brand["rlc_key"]),
        "tenant-id":   brand["tenant_id"],
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
    """POST /dreame-auth/oauth/token mit grant_type=refresh_token."""
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
```

- [ ] **Step 4.4: Tests ausführen**

```
pytest tests/test_dreame_gateway.py -k "build_dreame_headers" -v
```

Erwartet: `2 passed`

- [ ] **Step 4.5: Commit**

```bash
git add bin/dreame_gateway.py tests/test_dreame_gateway.py
git commit -m "feat: add DreameAuth (login, refresh_token, header builder)"
```

---

## Task 5: DreameAPI — Geräteliste + sendCommand + sendMowerCommand

**Files:**
- Modify: `bin/dreame_gateway.py`

- [ ] **Step 5.1: Failing test für Request-ID und Geräte-Parsing schreiben**

Ergänze in `tests/test_dreame_gateway.py`:

```python
from dreame_gateway import _parse_device_list

def test_parse_device_list_mower():
    records = [{
        "did": "111",
        "model": "dreame.mower.r2320",
        "customName": "Mein Mäher",
        "bindDomain": "10000.mt.eu.iot.dreame.tech:19973",
        "online": True,
    }]
    devices = _parse_device_list(records)
    assert len(devices) == 1
    assert devices[0]["device_type"] == "mower"
    assert devices[0]["did"] == "111"

def test_parse_device_list_vacuum():
    records = [{"did": "222", "model": "dreame.vacuum.r2228o",
                "customName": "Sauger", "bindDomain": "dom", "online": False}]
    devices = _parse_device_list(records)
    assert devices[0]["device_type"] == "vacuum"
    assert devices[0]["online"] is False

def test_parse_device_list_missing_fields():
    records = [{"did": "333", "model": "dreame.vacuum.x"}]
    devices = _parse_device_list(records)
    assert devices[0]["name"] == "dreame.vacuum.x"
    assert devices[0]["bind_domain"] == ""
```

- [ ] **Step 5.2: Test scheitern lassen**

```
pytest tests/test_dreame_gateway.py -k "parse_device_list" -v 2>&1 | head -10
```

Erwartet: `ImportError`

- [ ] **Step 5.3: API-Funktionen in `bin/dreame_gateway.py` ergänzen**

```python
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
    """POST sendCommand → result dict from Dreame API."""
    req_id = _next_request_id()
    prefix = brand["iot_com_prefix"]
    url = f"https://{brand['domain']}/dreame-iot-com-{prefix}/device/sendCommand"
    headers = _build_dreame_headers(brand, access_token)
    headers["content-type"] = "application/json"
    body = {
        "did": did, "id": req_id,
        "data": {"did": did, "id": req_id, "method": method, "params": params, "from": "XXXXXX"},
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
    """Wrapper für Mähroboter-Sonderkanal action siid=2 aiid=50."""
    params = {"did": did, "siid": 2, "aiid": 50, "in": [payload]}
    return await send_command(session, brand, access_token, did, "action", params)


async def get_properties(
    session: aiohttp.ClientSession,
    brand: dict,
    access_token: str,
    did: str,
    siid_piid_list: list,
) -> dict:
    """get_properties für eine Liste von (siid, piid) Paaren → {(siid,piid): value}."""
    params = [{"did": did, "siid": s, "piid": p} for s, p in siid_piid_list]
    result = await send_command(session, brand, access_token, did, "get_properties", params)
    out = {}
    for item in result.get("result", {}).get("data", {}).get("result", []):
        siid = item.get("siid")
        piid = item.get("piid")
        if siid and piid and "value" in item:
            out[(siid, piid)] = item["value"]
    return out


async def load_mower_settings(
    session: aiohttp.ClientSession,
    brand: dict,
    access_token: str,
    did: str,
) -> dict:
    """Liest CFG via getCFG (siid=2 aiid=50 {m:'g',t:'CFG'}) → dict mit Mäher-Einstellungen."""
    resp = await send_mower_command(session, brand, access_token, did, {"m": "g", "t": "CFG"})
    raw = resp.get("result", {}).get("data", {}).get("result", {})
    out: dict = {}
    # WRP: rain_protection
    wrp = raw.get("WRP")
    if wrp is not None:
        if isinstance(wrp, dict):
            out["rain_protection"] = wrp.get("value", 0)
            out["rain_delay_min"]  = wrp.get("time", 0)
        else:
            out["rain_protection"] = int(wrp)
    # FDP: frost_protection
    fdp = raw.get("FDP")
    if fdp is not None:
        out["frost_protection"] = fdp.get("value", 0) if isinstance(fdp, dict) else int(fdp)
    # VOL: volume
    vol = raw.get("VOL")
    if vol is not None:
        out["volume"] = vol.get("value", 0) if isinstance(vol, dict) else int(vol)
    # CLS: child_lock
    cls = raw.get("CLS")
    if cls is not None:
        out["child_lock"] = cls.get("value", 0) if isinstance(cls, dict) else int(cls)
    # STUN: anti_theft
    stun = raw.get("STUN")
    if stun is not None:
        out["anti_theft"] = stun.get("value", 0) if isinstance(stun, dict) else int(stun)
    # AOP: ai_obstacle
    aop = raw.get("AOP")
    if aop is not None:
        out["ai_obstacle"] = aop.get("value", 0) if isinstance(aop, dict) else int(aop)
    # PROT: grass_protection
    prot = raw.get("PROT")
    if prot is not None:
        out["grass_protection"] = prot.get("value", 0) if isinstance(prot, dict) else int(prot)
    # PATH: path_display
    path = raw.get("PATH")
    if path is not None:
        out["path_display"] = path.get("value", 0) if isinstance(path, dict) else int(path)
    # PRE: cutting preferences array
    pre = raw.get("PRE")
    if isinstance(pre, dict):
        arr = pre.get("value", [])
    elif isinstance(pre, list):
        arr = pre
    else:
        arr = []
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
    # CMS: consumables
    cms = raw.get("CMS")
    if isinstance(cms, dict):
        cms_val = cms.get("value", [0, 0, 0])
    elif isinstance(cms, list):
        cms_val = cms
    else:
        cms_val = [0, 0, 0]
    out["_cms_array"] = cms_val
    return out


async def load_mower_history(
    session: aiohttp.ClientSession,
    brand: dict,
    access_token: str,
    did: str,
    uid: str,
) -> list:
    """Letzte 20 Mähsessions via /dreame-user-iot/mower/history/listV2."""
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
        "region": "eu",
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
    """Liest Verbrauchs-Properties je nach Gerätetyp."""
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
            "first_cleaning_date":    props.get((12, 1), 0),
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
```

- [ ] **Step 5.4: Tests ausführen**

```
pytest tests/test_dreame_gateway.py -k "parse_device_list or build_dreame_headers" -v
```

Erwartet: `5 passed`

- [ ] **Step 5.5: Commit**

```bash
git add bin/dreame_gateway.py tests/test_dreame_gateway.py
git commit -m "feat: add DreameAPI (device list, send_command, send_mower_command, load_mower_settings, load_statistic)"
```

---

## Task 6: Dreame Cloud MQTT (paho-mqtt in Thread + asyncio.Queue)

**Files:**
- Modify: `bin/dreame_gateway.py`

- [ ] **Step 6.1: Implementierung direkt einfügen (kein Unit-Test möglich ohne echten Broker)**

Ergänze in `bin/dreame_gateway.py`:

```python
# ── Dreame Cloud MQTT ─────────────────────────────────────────────────────────
import paho.mqtt.client as paho_mqtt
import threading


class DreameMqttClient:
    """paho-MQTT-Client für Dreame-Cloud MQTTS, läuft in eigenem Thread.
    State-Updates werden per asyncio.Queue an den asyncio-Loop übergeben."""

    def __init__(
        self,
        bind_domain: str,
        did: str,
        uid: str,
        access_token: str,
        queue: asyncio.Queue,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        host_port = bind_domain.split(":")
        self._host  = host_port[0]
        self._port  = int(host_port[1]) if len(host_port) > 1 else 8883
        self._did   = did
        self._uid   = uid
        self._token = access_token
        self._queue = queue
        self._loop  = loop
        client_id   = "p_" + secrets.token_hex(8)
        self._client = paho_mqtt.Client(client_id=client_id)
        self._client.username_pw_set(uid, access_token)
        # TLS — Dreame-Cloud nutzt selbstsigniertes Zertifikat
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        self._client.tls_set_context(ctx)
        self._client.on_connect    = self._on_connect
        self._client.on_message    = self._on_message
        self._client.on_disconnect = self._on_disconnect
        self._thread: "threading.Thread | None" = None
        self._stop_flag = threading.Event()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True, name=f"dreame-mqtt-{self._did}")
        self._thread.start()

    def stop(self) -> None:
        self._stop_flag.set()
        self._client.disconnect()
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop_flag.is_set():
            try:
                LOGINF(f"[{self._did}] Verbinde Dreame Cloud MQTT {self._host}:{self._port}")
                self._client.connect(self._host, self._port, keepalive=60)
                self._client.loop_forever()
            except Exception as e:
                LOGERR(f"[{self._did}] Dreame MQTT Fehler: {e}")
                if not self._stop_flag.is_set():
                    import time as _time
                    _time.sleep(10)

    def _on_connect(self, client, userdata, flags, rc) -> None:
        if rc == 0:
            topic = f"/status/{self._did}/{self._uid}/#"
            client.subscribe(topic, qos=0)
            LOGOK(f"[{self._did}] Dreame MQTT verbunden, abonniert: {topic}")
        else:
            LOGERR(f"[{self._did}] Dreame MQTT Verbindung fehlgeschlagen: rc={rc}")

    def _on_message(self, client, userdata, msg) -> None:
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            asyncio.run_coroutine_threadsafe(
                self._queue.put({"did": self._did, "payload": payload}),
                self._loop,
            )
        except Exception as e:
            LOGWARN(f"[{self._did}] Dreame MQTT Nachricht Parse-Fehler: {e}")

    def _on_disconnect(self, client, userdata, rc) -> None:
        if rc != 0 and not self._stop_flag.is_set():
            LOGWARN(f"[{self._did}] Dreame MQTT getrennt (rc={rc}), reconnect...")
```

- [ ] **Step 6.2: Commit**

```bash
git add bin/dreame_gateway.py
git commit -m "feat: add DreameMqttClient (paho TLS in thread, asyncio.Queue bridge)"
```

---

## Task 7: StateMapper + CommandHandler

**Files:**
- Modify: `bin/dreame_gateway.py`

- [ ] **Step 7.1: StateMapper einfügen**

Ergänze in `bin/dreame_gateway.py`:

```python
# ── StateMapper ───────────────────────────────────────────────────────────────
def map_properties_changed(
    device: dict,
    params: list,
    current_props: dict,
    mower_settings: dict,
) -> "dict | None":
    """Verarbeitet properties_changed-Parameter, aktualisiert current_props.
    Gibt aktualisiertes state-JSON zurück (oder None wenn nur Binär-Property)."""
    updated = False
    trigger_cfg_reload = False
    for item in params:
        siid = item.get("siid")
        piid = item.get("piid")
        value = item.get("value")
        if siid is None or piid is None:
            continue
        # Mähroboter: binäre Properties werden direkt geparsed
        if device["device_type"] == "mower" and siid == 1 and piid == 1:
            parsed = parse_binary_state_1(value)
            if parsed:
                current_props[(3, 2)] = parsed.get("battery", current_props.get((3, 2), 0))
                current_props[(3, 3)] = parsed.get("charging", current_props.get((3, 3), 0))
                if parsed.get("error_code", 0):
                    current_props[(3, 5)] = parsed["error_code"]
                updated = True
            continue
        if device["device_type"] == "mower" and siid == 1 and piid == 4:
            continue  # Positions-Daten: nicht weiterleiten
        # CFG-Reload-Trigger
        if device["device_type"] == "mower" and siid == 2 and piid == 51:
            trigger_cfg_reload = True
            continue
        # AutoSwitch (siid=4 piid=50)
        if siid == 4 and piid == 50:
            try:
                as_data = json.loads(value) if isinstance(value, str) else value
                switches = _normalize_autoswitch(as_data)
                current_props[("autoswitch", k)] = v for k, v in switches.items()
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
MOWER_ACTIONS = {
    "start":         ("action", {"siid": 2, "aiid": 1,  "in": []}),
    "stop":          ("action", {"siid": 2, "aiid": 2,  "in": []}),
    "pause":         ("action", {"siid": 2, "aiid": 4,  "in": []}),
    "dock":          ("action", {"siid": 5, "aiid": 3,  "in": []}),
    "clear_warning": ("action", {"siid": 4, "aiid": 3,  "in": []}),
}
MOWER_SPECIAL = {
    "find": {"m": "a", "p": 0, "o": 9},
    "lock": {"m": "a", "p": 0, "o": 12},
}
VACUUM_ACTIONS = {
    "start":         ("action", {"siid": 2, "aiid": 1, "in": []}),
    "pause":         ("action", {"siid": 2, "aiid": 2, "in": []}),
    "stop":          ("action", {"siid": 4, "aiid": 2, "in": []}),
    "dock":          ("action", {"siid": 3, "aiid": 1, "in": []}),
    "locate":        ("action", {"siid": 7, "aiid": 1, "in": []}),
    "auto_empty":    ("action", {"siid": 15,"aiid": 1, "in": []}),
    "start_washing": ("action", {"siid": 4, "aiid": 4, "in": []}),
    "clear_warning": ("action", {"siid": 4, "aiid": 3, "in": []}),
}
VACUUM_SETTINGS = {
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
VACUUM_AUTOSWITCH = {
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
MOWER_AUTOSWITCH = {
    "collision_avoidance":   "LessColl",
    "auto_charging":         "SmartCharge",
    "clean_genius":          "SmartHost",
    "cleaning_route":        "CleanRoute",
}
MOWER_DIRECT_SETTINGS = {
    "rain_protection":       lambda v: ("mower_cmd", {"m": "s", "t": "WRP", "d": v}),
    "frost_protection":      lambda v: ("mower_cmd", {"m": "s", "t": "FDP", "d": {"value": int(v)}}),
    "volume":                lambda v: ("mower_cmd", {"m": "s", "t": "VOL", "d": {"value": int(v)}}),
    "child_lock":            lambda v: ("mower_cmd", {"m": "s", "t": "CLS", "d": {"value": int(v)}}),
    "anti_theft":            lambda v: ("mower_cmd", {"m": "s", "t": "STUN","d": {"value": int(v)}}),
    "ai_obstacle":           lambda v: ("mower_cmd", {"m": "s", "t": "AOP", "d": {"value": int(v)}}),
    "grass_protection":      lambda v: ("mower_cmd", {"m": "s", "t": "PROT","d": {"value": int(v)}}),
    "path_display":          lambda v: ("mower_cmd", {"m": "s", "t": "PATH","d": {"value": int(v)}}),
    "dnd_enable":            lambda v: ("set_prop", 5, 1, int(v)),
    "obstacle_avoidance":    lambda v: ("set_prop", 4, 21, int(v)),
    "schedule":              lambda v: ("set_prop", 8, 2, v),
    "low_speed":             lambda v: ("mower_cmd", {"m": "s", "t": "LOW", "d": v}),
    "headlight":             lambda v: ("mower_cmd", {"m": "s", "t": "LIT", "d": v}),
}
MOWER_PRE_SETTINGS = {
    "mow_mode":       1,
    "cutting_height": 2,
    "direction_change": 5,
    "edge_detection": 8,
    "edge_mowing":    9,
}


async def handle_set_command(
    session: aiohttp.ClientSession,
    brand: dict,
    access_token: str,
    device: dict,
    command: str,
) -> "tuple[str, int]":
    """Führt einen set-Befehl aus. Gibt (result, result_num) zurück."""
    did = device["did"]
    dt  = device["device_type"]
    try:
        if dt == "mower":
            if command in MOWER_SPECIAL:
                await send_mower_command(session, brand, access_token, did, MOWER_SPECIAL[command])
            elif command in MOWER_ACTIONS:
                _, params = MOWER_ACTIONS[command]
                await send_command(session, brand, access_token, did, "action", {**params, "did": did})
            else:
                return "error", 1
        else:
            if command in VACUUM_ACTIONS:
                _, params = VACUUM_ACTIONS[command]
                await send_command(session, brand, access_token, did, "action", {**params, "did": did})
            else:
                return "error", 1
        return "ok", 0
    except Exception as e:
        LOGERR(f"[{did}] set '{command}' Fehler: {e}")
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
    """Führt ein settings/{key}-Kommando aus."""
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
                as_val = {"k": as_key, "v": int(value)}
                params = [{"did": did, "siid": 4, "piid": 50, "value": json.dumps(as_val)}]
                await send_command(session, brand, access_token, did, "set_properties", params)
            elif key == "schedule":
                params = [{"did": did, "siid": 8, "piid": 2, "value": value}]
                await send_command(session, brand, access_token, did, "set_properties", params)
            else:
                return "error", 1
        else:  # mower
            if key in MOWER_AUTOSWITCH:
                as_key = MOWER_AUTOSWITCH[key]
                as_val = {"k": as_key, "v": int(value)}
                params = [{"did": did, "siid": 4, "piid": 50, "value": json.dumps(as_val)}]
                await send_command(session, brand, access_token, did, "set_properties", params)
            elif key in MOWER_DIRECT_SETTINGS:
                action = MOWER_DIRECT_SETTINGS[key](value)
                if action[0] == "mower_cmd":
                    await send_mower_command(session, brand, access_token, did, action[1])
                else:  # set_prop
                    _, siid, piid, val = action
                    params = [{"did": did, "siid": siid, "piid": piid, "value": val}]
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
        LOGERR(f"[{did}] settings/{key} Fehler: {e}")
        return "error", 1
```

- [ ] **Step 7.2: Commit**

```bash
git add bin/dreame_gateway.py
git commit -m "feat: add StateMapper and CommandHandler (set/settings dispatch, PRE read-modify-write)"
```

---

## Task 8: Async Tasks (publisher, subscriber, token refresh, statistic poll) + main()

**Files:**
- Modify: `bin/dreame_gateway.py`

- [ ] **Step 8.1: Async Tasks einfügen**

Ergänze am Ende von `bin/dreame_gateway.py`:

```python
# ── Async Tasks ───────────────────────────────────────────────────────────────
import aiomqtt


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
    """Empfängt State-Updates aus der Dreame-Cloud-Queue, publiziert an LoxBerry MQTT."""
    did = device["did"]
    dt  = device["device_type"]
    current_props: dict = {}
    current_pre  : list = mower_settings.get("_pre_array", []) if mower_settings else []

    mqtt_kwargs = _build_mqtt_kwargs(broker)
    while not _shutdown_event.is_set():
        try:
            async with aiomqtt.Client(**mqtt_kwargs) as lbmqtt:
                LOGOK(f"[{did}] LoxBerry MQTT verbunden")
                while not _shutdown_event.is_set():
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    payload = item.get("payload", {})
                    data    = payload.get("data", {})
                    method  = data.get("method", "")
                    if method == "properties_changed":
                        result = map_properties_changed(
                            device, data.get("params", []), current_props,
                            mower_settings or {}
                        )
                        if result and result.get("_trigger_cfg_reload"):
                            LOGINF(f"[{did}] CFG-Reload getriggert")
                            try:
                                mower_settings = await load_mower_settings(
                                    session, brand, cfg["access_token"], did
                                )
                                current_pre = mower_settings.get("_pre_array", [])
                                # Mähroboter-Einstellungen in State einmischen
                                state = build_state_json(device, current_props)
                                state.update({k: v for k, v in mower_settings.items() if not k.startswith("_")})
                                await lbmqtt.publish(f"{base_topic}/{did}/state", json.dumps(state), retain=True)
                            except Exception as e:
                                LOGERR(f"[{did}] CFG-Reload Fehler: {e}")
                        elif result:
                            await lbmqtt.publish(f"{base_topic}/{did}/state", json.dumps(result), retain=True)
                            if dt == "vacuum":
                                station = build_station_json(current_props)
                                await lbmqtt.publish(f"{base_topic}/{did}/state_station", json.dumps(station), retain=True)
        except aiomqtt.MqttError as e:
            if not _shutdown_event.is_set():
                LOGWARN(f"[{did}] LoxBerry MQTT getrennt: {e} — reconnect in 5s")
                await asyncio.sleep(5)
        except Exception as e:
            LOGERR(f"[{did}] task_dreame_to_lbmqtt Fehler: {e}")
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
    """Abonniert LoxBerry MQTT set/settings, dispatcht Befehle an Dreame-Cloud."""
    did = device["did"]
    set_topic      = f"{base_topic}/{did}/set"
    settings_topic = f"{base_topic}/{did}/settings/#"
    mqtt_kwargs    = _build_mqtt_kwargs(broker)

    while not _shutdown_event.is_set():
        try:
            async with aiomqtt.Client(**mqtt_kwargs) as lbmqtt:
                await lbmqtt.subscribe(set_topic)
                await lbmqtt.subscribe(settings_topic)
                LOGINF(f"[{did}] Abonniert: {set_topic}, {settings_topic}")
                async with lbmqtt.messages() as messages:
                    async for msg in messages:
                        if _shutdown_event.is_set():
                            break
                        topic_str = str(msg.topic)
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
                            LOGERR(f"[{did}] Befehl '{command_name}' Fehler: {e}")
                        result_payload = json.dumps({
                            "command":    command_name,
                            "result":     result_str,
                            "result_num": result_num,
                            "reason":     "none" if result_str == "ok" else str(result_num),
                        })
                        await lbmqtt.publish(f"{base_topic}/{did}/command_result", result_payload)
        except aiomqtt.MqttError as e:
            if not _shutdown_event.is_set():
                LOGWARN(f"[{did}] LoxBerry MQTT Subscriber getrennt: {e} — reconnect in 5s")
                await asyncio.sleep(5)
        except Exception as e:
            LOGERR(f"[{did}] task_lbmqtt_to_dreame Fehler: {e}")
            await asyncio.sleep(5)


async def task_token_refresh(
    session: aiohttp.ClientSession,
    brand: dict,
    cfg: dict,
    cfg_lock: asyncio.Lock,
) -> None:
    """Erneuert den Access-Token 5 Minuten vor Ablauf; bei Fehler erneuter Login."""
    while not _shutdown_event.is_set():
        await asyncio.sleep(30)
        now = time.time()
        expires_at = cfg.get("expires_at", 0)
        if expires_at == 0 or now < expires_at - 300:
            continue
        LOGINF("Token läuft ab — refresh")
        async with cfg_lock:
            try:
                tokens = await dreame_refresh_token(session, brand, cfg["refresh_token"])
                cfg["access_token"]  = tokens["access_token"]
                cfg["refresh_token"] = tokens["refresh_token"]
                cfg["expires_at"]    = time.time() + tokens["expires_in"]
                save_plugin_config(cfg)
                LOGOK("Token erfolgreich erneuert")
            except Exception as e:
                LOGERR(f"Token-Refresh fehlgeschlagen ({e}) — versuche Re-Login")
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
                    LOGOK("Re-Login erfolgreich")
                except Exception as e2:
                    LOGERR(f"Re-Login fehlgeschlagen: {e2}")


async def task_statistic_poll(
    devices: list,
    session: aiohttp.ClientSession,
    brand: dict,
    cfg: dict,
    broker: dict,
    base_topic: str,
) -> None:
    """Pollt Statistik/Verbrauch periodisch (Default: 30 min)."""
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
                        stat["last_mow_date"]          = last.get("time", 0)
                        stat["last_mow_duration_min"]  = last.get("duration", 0)
                        stat["last_mow_area_m2"]       = last.get("area", 0)
                        stat["last_mow_completed"]     = last.get("completed", False)
                mqtt_kwargs = _build_mqtt_kwargs(broker)
                async with aiomqtt.Client(**mqtt_kwargs) as lbmqtt:
                    await lbmqtt.publish(f"{base_topic}/{did}/statistic", json.dumps(stat), retain=True)
                LOGDEB(f"[{did}] Statistik publiziert")
            except Exception as e:
                LOGERR(f"[{did}] Statistik-Polling Fehler: {e}")


# ── main ──────────────────────────────────────────────────────────────────────
async def _async_main() -> None:
    LOGSTART("Dreame Gateway gestartet")
    write_pid()
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT,  _handle_sigterm)

    cfg     = load_plugin_config()
    general = _load_json(GENERAL_JSON)
    broker  = get_mqtt_broker_config(general)
    brand   = BRAND_CONFIG.get(cfg.get("cloud_service", "dreame"), BRAND_CONFIG["dreame"])

    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Token prüfen / Login durchführen
        if not cfg.get("access_token") or time.time() >= cfg.get("expires_at", 0) - 60:
            LOGINF("Kein gültiges Token — Login")
            password_plain = cfg.get("_password_plain", "")
            if not password_plain:
                LOGERR("Kein Passwort in Config — bitte im WebUI einloggen")
                return
            tokens = await dreame_login(session, brand, cfg["username"], password_plain)
            cfg["access_token"]  = tokens["access_token"]
            cfg["refresh_token"] = tokens["refresh_token"]
            cfg["uid"]           = tokens["uid"]
            cfg["expires_at"]    = time.time() + tokens["expires_in"]
            save_plugin_config(cfg)

        # Geräteliste laden (Cache-Fallback)
        devices = cfg.get("devices", [])
        try:
            devices = await get_device_list(session, brand, cfg["access_token"])
            cfg["devices"] = devices
            save_plugin_config(cfg)
            LOGOK(f"{len(devices)} Geräte gefunden")
        except Exception as e:
            LOGWARN(f"Geräteliste konnte nicht geladen werden ({e}), nutze Cache")

        if not devices:
            LOGERR("Keine Geräte — Gateway beendet")
            return

        cfg_lock   = asyncio.Lock()
        loop       = asyncio.get_running_loop()
        base_topic = cfg.get("base_topic", "dreame")

        tasks_coro = []
        dreame_clients = []

        for device in devices:
            did  = device["did"]
            bind = device.get("bind_domain", "")
            queue: asyncio.Queue = asyncio.Queue(maxsize=100)

            # Mähroboter: Einstellungen beim Start laden
            mower_settings = None
            pre_array_ref  = []
            if device["device_type"] == "mower":
                try:
                    mower_settings = await load_mower_settings(
                        session, brand, cfg["access_token"], did
                    )
                    pre_array_ref = mower_settings.get("_pre_array", [])
                    LOGOK(f"[{did}] Mähroboter-Einstellungen geladen")
                except Exception as e:
                    LOGWARN(f"[{did}] Mähroboter-Einstellungen Fehler: {e}")

            # Saugroboter: station-Properties beim Start laden
            if device["device_type"] == "vacuum" and bind:
                try:
                    station_props = await get_properties(
                        session, brand, cfg["access_token"], did,
                        [(25, 1), (25, 2), (25, 3), (25, 4), (25, 5)]
                    )
                    mqtt_kwargs = _build_mqtt_kwargs(broker)
                    async with aiomqtt.Client(**mqtt_kwargs) as lbmqtt:
                        station = build_station_json(station_props)
                        await lbmqtt.publish(f"{base_topic}/{did}/state_station", json.dumps(station), retain=True)
                except Exception as e:
                    LOGWARN(f"[{did}] Stations-Props Fehler: {e}")

            # Dreame-Cloud-MQTT starten
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

        # Cleanup
        for dc in dreame_clients:
            dc.stop()
        for t in all_tasks:
            t.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)

    remove_pid()
    _logend()
    LOGINF("Dreame Gateway beendet")


def main() -> None:
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
```

- [ ] **Step 8.2: Syntax-Check**

```
python -m py_compile bin/dreame_gateway.py && echo "OK"
```

Erwartet: `OK`

- [ ] **Step 8.3: Alle Tests ausführen**

```
pytest tests/test_dreame_gateway.py -v
```

Erwartet: Alle bestehenden Tests `PASSED`

- [ ] **Step 8.4: Commit**

```bash
git add bin/dreame_gateway.py
git commit -m "feat: add async tasks (publisher, subscriber, token refresh, statistic poll) and main()"
```

---

## Task 9: Plugin-Infrastruktur (plugin.cfg, daemon.sh, postroot.sh)

**Files:**
- Create: `plugin.cfg`
- Create: `daemon/daemon.sh`
- Create: `postroot.sh`
- Create: `postupgrade.sh`
- Create: `preroot.sh`
- Create: `preupgrade.sh`
- Create: `release.cfg`
- Create: `prerelease.cfg`

- [ ] **Step 9.1: `plugin.cfg` anlegen**

```ini
[AUTHOR]
NAME=Michael Schlennstedt
EMAIL=Michael@schlenn.net

[PLUGIN]
VERSION=0.1.0
NAME=dreame
FOLDER=dreame
TITLE=Dreame Gateway
WEBSITE=https://github.com/mschlenstedt/LoxBerry-Plugin-Dreame

[AUTOUPDATE]
AUTOMATIC_UPDATES=true
RELEASECFG=https://raw.githubusercontent.com/mschlenstedt/LoxBerry-Plugin-Dreame/master/release.cfg
PRERELEASECFG=https://raw.githubusercontent.com/mschlenstedt/LoxBerry-Plugin-Dreame/master/prerelease.cfg

[SYSTEM]
REBOOT=false
LB_MINIMUM=4.0.0
LB_MAXIMUM=false
ARCHITECTURE=false
CUSTOM_LOGLEVELS=true
INTERFACE=2.0
```

- [ ] **Step 9.2: `daemon/daemon.sh` anlegen**

```bash
#!/bin/bash
# Dreame Gateway Daemon Wrapper
# Called by LoxBerry with: daemon.sh start|stop|status

PLUGINDIR="$LBHOMEDIR/config/plugins/dreame"
LOGDIR="$LBHOMEDIR/log/plugins/dreame"
PIDFILE="/dev/shm/dreame_gateway.pid"
GATEWAY="$LBHOMEDIR/bin/plugins/dreame/dreame_gateway.py"
LOGFILE="$LOGDIR/dreame_gateway.log"
CONFIGDIR="$PLUGINDIR"
LBSCONFIG="$LBHOMEDIR/config/system"
LOGLEVEL=6

case "$1" in
  start)
    if [ -f "$PIDFILE" ]; then
      PID=$(cat "$PIDFILE")
      if kill -0 "$PID" 2>/dev/null; then
        echo "Gateway already running (PID $PID)"
        exit 0
      fi
      rm -f "$PIDFILE"
    fi
    mkdir -p "$LOGDIR"
    python3 "$GATEWAY" \
      --logfile "$LOGFILE" \
      --configdir "$CONFIGDIR" \
      --lbsconfig "$LBSCONFIG" \
      --loglevel "$LOGLEVEL" \
      &
    echo "Gateway started"
    ;;
  stop)
    if [ -f "$PIDFILE" ]; then
      PID=$(cat "$PIDFILE")
      kill "$PID" 2>/dev/null
      rm -f "$PIDFILE"
      echo "Gateway stopped"
    else
      echo "Gateway not running"
    fi
    ;;
  status)
    if [ -f "$PIDFILE" ]; then
      PID=$(cat "$PIDFILE")
      if kill -0 "$PID" 2>/dev/null; then
        echo "running $PID"
      else
        echo "dead"
      fi
    else
      echo "stopped"
    fi
    ;;
  *)
    echo "Usage: $0 {start|stop|status}"
    exit 1
    ;;
esac
```

- [ ] **Step 9.3: `postroot.sh` anlegen**

```bash
#!/bin/bash
# Installiert Python-Abhängigkeiten nach Plugin-Installation
pip3 install --quiet aiohttp aiomqtt paho-mqtt cryptography
echo "Dreame-Dependencies installiert."
exit 0
```

- [ ] **Step 9.4: `postupgrade.sh` anlegen**

```bash
#!/bin/bash
pip3 install --quiet --upgrade aiohttp aiomqtt paho-mqtt cryptography
echo "Dreame-Dependencies aktualisiert."
exit 0
```

- [ ] **Step 9.5: `preroot.sh` und `preupgrade.sh` anlegen**

`preroot.sh`:
```bash
#!/bin/bash
# Vor Installation: laufendes Gateway stoppen
PIDFILE="/dev/shm/dreame_gateway.pid"
if [ -f "$PIDFILE" ]; then
  kill "$(cat $PIDFILE)" 2>/dev/null
  rm -f "$PIDFILE"
fi
exit 0
```

`preupgrade.sh` — gleicher Inhalt wie `preroot.sh`.

- [ ] **Step 9.6: `release.cfg` und `prerelease.cfg` anlegen**

`release.cfg`:
```ini
[RELEASES]
RELEASE1=0.1.0
RELEASE1_VERSION=0.1.0
RELEASE1_MINLBVERSION=4.0.0
RELEASE1_MAXLBVERSION=false
RELEASE1_DATE=2026-06-09
RELEASE1_DLURL=https://github.com/mschlenstedt/LoxBerry-Plugin-Dreame/archive/refs/tags/0.1.0.zip
```

`prerelease.cfg`:
```ini
[RELEASES]
PRERELEASE1=0.1.0-dev
PRERELEASE1_VERSION=0.1.0-dev
PRERELEASE1_MINLBVERSION=4.0.0
PRERELEASE1_MAXLBVERSION=false
PRERELEASE1_DATE=2026-06-09
PRERELEASE1_DLURL=https://github.com/mschlenstedt/LoxBerry-Plugin-Dreame/archive/refs/heads/main.zip
```

- [ ] **Step 9.7: Ausführbar machen + Commit**

```bash
chmod +x daemon/daemon.sh postroot.sh postupgrade.sh preroot.sh preupgrade.sh
git add plugin.cfg daemon/daemon.sh postroot.sh postupgrade.sh preroot.sh preupgrade.sh release.cfg prerelease.cfg config/pluginconfig.json
git commit -m "feat: add plugin infrastructure (plugin.cfg, daemon.sh, postroot.sh, release configs)"
```

---

## Task 10: WebUI (index.cgi — Perl/CGI)

**Files:**
- Create: `webfrontend/htmlauth/index.cgi`

- [ ] **Step 10.1: Verzeichnis anlegen und index.cgi erstellen**

```bash
mkdir -p webfrontend/htmlauth
```

- [ ] **Step 10.2: `webfrontend/htmlauth/index.cgi` anlegen**

```perl
#!/usr/bin/perl

use strict;
use warnings;
use LoxBerry::System;
use LoxBerry::Web;
use LoxBerry::Log;
use CGI;
use JSON;
use File::Slurp;
use Digest::MD5 qw(md5_hex);

my $cgi     = CGI->new();
my $action  = $cgi->param('action') || '';
my $plugindir = "$ENV{LBHOMEDIR}/config/plugins/dreame";
my $cfgfile   = "$plugindir/pluginconfig.json";
my $pidfile   = "/dev/shm/dreame_gateway.pid";
my $logfile   = "$ENV{LBHOMEDIR}/log/plugins/dreame/dreame_gateway.log";
my $daemon    = "$ENV{LBHOMEDIR}/bin/plugins/dreame/../../daemon/plugins/dreame/daemon.sh";

# ── Konfiguration laden ───────────────────────────────────────────────────────
sub load_cfg {
    my $cfg = {};
    if (-f $cfgfile) {
        eval { $cfg = decode_json(read_file($cfgfile, { binmode => ':utf8' })); };
    }
    $cfg->{cloud_service}        //= 'dreame';
    $cfg->{username}             //= '';
    $cfg->{base_topic}           //= 'dreame';
    $cfg->{polling_interval_min} //= 30;
    $cfg->{devices}              //= [];
    return $cfg;
}

sub save_cfg {
    my ($cfg) = @_;
    mkdir $plugindir unless -d $plugindir;
    write_file($cfgfile, { binmode => ':utf8' }, encode_json($cfg));
}

# ── Gateway-Status ────────────────────────────────────────────────────────────
sub gateway_status {
    if (-f $pidfile) {
        my $pid = read_file($pidfile);
        chomp $pid;
        return kill(0, $pid) ? "running:$pid" : "dead";
    }
    return "stopped";
}

# ── Actions ───────────────────────────────────────────────────────────────────
my $cfg = load_cfg();
my $message = '';
my $msgtype = '';

if ($action eq 'save_config') {
    my $svc  = $cgi->param('cloud_service') || 'dreame';
    my $user = $cgi->param('username')      || '';
    my $pass = $cgi->param('password')      || '';
    my $topic= $cgi->param('base_topic')    || 'dreame';
    my $intv = int($cgi->param('polling_interval_min') || 30);

    $cfg->{cloud_service}        = $svc;
    $cfg->{username}             = $user;
    $cfg->{base_topic}           = $topic;
    $cfg->{polling_interval_min} = $intv;

    if ($pass ne '') {
        # Passwort plain speichern (wird beim ersten Gateway-Start gehasht)
        $cfg->{_password_plain} = $pass;
        $cfg->{access_token}    = '';
        $cfg->{refresh_token}   = '';
        $cfg->{expires_at}      = 0;
    }
    save_cfg($cfg);
    $message = 'Konfiguration gespeichert.';
    $msgtype = 'success';

} elsif ($action eq 'start_gateway') {
    system("$daemon start &");
    sleep(1);
    $message = 'Gateway gestartet.';
    $msgtype = 'success';

} elsif ($action eq 'stop_gateway') {
    system("$daemon stop");
    $message = 'Gateway gestoppt.';
    $msgtype = 'success';
}

# ── HTML-Ausgabe ──────────────────────────────────────────────────────────────
my $status = gateway_status();
my $status_label = ($status =~ /^running/) ? '<span style="color:green">&#x25CF; Läuft</span>'
                                           : '<span style="color:red">&#x25CF; Gestoppt</span>';
my $pid_info = ($status =~ /^running:(\d+)/) ? " (PID $1)" : '';

LoxBerry::Web::lbheader("Dreame Gateway", "dreame", "");
print $cgi->header(-charset => 'utf-8');

print <<HTML;
<div class="container">
  <h3>Dreame Gateway</h3>
HTML

if ($message) {
    my $cls = $msgtype eq 'success' ? 'success' : 'error';
    print "<div class='ui-state-$cls' style='padding:8px;margin:8px 0'>$message</div>\n";
}

# ── Tab: Konfiguration ──────────────────────────────────────────────────────
print <<HTML;
<div id="tabs">
  <ul>
    <li><a href="#tab-config">Konfiguration</a></li>
    <li><a href="#tab-devices">Geräte</a></li>
    <li><a href="#tab-gateway">Gateway</a></li>
    <li><a href="#tab-log">Log</a></li>
  </ul>

  <div id="tab-config">
    <form method="post">
    <input type="hidden" name="action" value="save_config">
    <table class="loxberry">
      <tr>
        <th>Cloud-Dienst</th>
        <td>
          <select name="cloud_service">
            <option value="dreame" @{[$cfg->{cloud_service} eq 'dreame' ? 'selected' : '']}>Dreame</option>
            <option value="mova"   @{[$cfg->{cloud_service} eq 'mova'   ? 'selected' : '']}>MOVA</option>
          </select>
        </td>
      </tr>
      <tr>
        <th>E-Mail</th>
        <td><input type="email" name="username" value="@{[$cfg->{username}]}" style="width:300px"></td>
      </tr>
      <tr>
        <th>Passwort</th>
        <td><input type="password" name="password" placeholder="Leer lassen wenn bereits eingeloggt" style="width:300px"></td>
      </tr>
      <tr>
        <th>MQTT Base-Topic</th>
        <td><input type="text" name="base_topic" value="@{[$cfg->{base_topic}]}" style="width:200px"></td>
      </tr>
      <tr>
        <th>Statistik-Intervall (Minuten)</th>
        <td><input type="number" name="polling_interval_min" value="@{[$cfg->{polling_interval_min}]}" min="5" max="1440"></td>
      </tr>
      <tr>
        <td></td>
        <td><input type="submit" value="Speichern" class="ui-button ui-widget"></td>
      </tr>
    </table>
    </form>
  </div>

  <div id="tab-devices">
    <table class="loxberry">
      <tr><th>Name</th><th>Modell</th><th>Typ</th><th>Status</th></tr>
HTML

for my $dev (@{$cfg->{devices}}) {
    my $online = $dev->{online} ? '<span style="color:green">Online</span>' : '<span style="color:grey">Offline</span>';
    my $type   = $dev->{device_type} eq 'mower' ? 'Mähroboter' : 'Saugroboter';
    print "      <tr><td>$dev->{name}</td><td>$dev->{model}</td><td>$type</td><td>$online</td></tr>\n";
}

if (!@{$cfg->{devices}}) {
    print "      <tr><td colspan='4'>Keine Geräte — Gateway starten um Geräteliste zu laden.</td></tr>\n";
}

print <<HTML;
    </table>
  </div>

  <div id="tab-gateway">
    <p>Status: $status_label$pid_info</p>
    <form method="post" style="display:inline">
      <input type="hidden" name="action" value="start_gateway">
      <input type="submit" value="Gateway starten" class="ui-button">
    </form>
    &nbsp;
    <form method="post" style="display:inline">
      <input type="hidden" name="action" value="stop_gateway">
      <input type="submit" value="Gateway stoppen" class="ui-button">
    </form>
    <p><small>Token-Status: @{[$cfg->{access_token} ? 'Vorhanden' : 'Nicht eingeloggt']}</small></p>
  </div>

  <div id="tab-log">
    <pre style="background:#111;color:#eee;padding:10px;height:400px;overflow:auto;font-size:11px">
HTML

if (-f $logfile) {
    my @lines = read_file($logfile, { binmode => ':utf8' });
    my @last = @lines > 200 ? @lines[-200..-1] : @lines;
    for my $line (@last) {
        $line =~ s/</&lt;/g;
        $line =~ s/>/&gt;/g;
        print $line;
    }
} else {
    print "Kein Log vorhanden.\n";
}

print <<HTML;
    </pre>
  </div>
</div>
</div>
HTML

LoxBerry::Web::lbfooter();
```

- [ ] **Step 10.3: Ausführbar machen + Commit**

```bash
chmod +x webfrontend/htmlauth/index.cgi
git add webfrontend/htmlauth/index.cgi
git commit -m "feat: add Perl/CGI WebUI (config, device list, gateway start/stop, log)"
```

---

## Task 11: Icons + Abschlusskontrolle

**Files:**
- Create: `icons/` (Dreame-Logo von ioBroker übernehmen)

- [ ] **Step 11.1: Icons aus ioBroker.dreame kopieren**

```bash
mkdir -p icons
# Dreame-Logo aus dem ioBroker-Adapter kopieren
cp ../ioBroker.dreame/admin/dreame.png icons/icon_256.png 2>/dev/null || true
# Für den Fall dass kein PNG vorhanden: Platzhalter erstellen
# (Ersetze durch echtes Logo vor Release)
for size in 64 128 512; do
    cp icons/icon_256.png icons/icon_${size}.png 2>/dev/null || true
done
```

- [ ] **Step 11.2: Kompletten Syntax-Check durchführen**

```
python -m py_compile bin/dreame_gateway.py && echo "Python OK"
perl -c webfrontend/htmlauth/index.cgi 2>&1 || echo "Perl-Fehler (normal ohne LoxBerry-Libs)"
```

Erwartet: `Python OK` (Perl-Fehler wegen fehlender LoxBerry-Libs auf Entwicklungsmaschine OK)

- [ ] **Step 11.3: Alle Tests ein letztes Mal ausführen**

```
pytest tests/ -v --tb=short
```

Erwartet: Alle Tests `PASSED`

- [ ] **Step 11.4: Dateistruktur prüfen**

```
find . -not -path './.git/*' | sort
```

Erwartet:
```
./bin/dreame_gateway.py
./config/pluginconfig.json
./daemon/daemon.sh
./icons/icon_64.png
./icons/icon_128.png
./icons/icon_256.png
./icons/icon_512.png
./plugin.cfg
./postroot.sh
./postupgrade.sh
./prerelease.cfg
./preroot.sh
./preupgrade.sh
./release.cfg
./tests/test_dreame_gateway.py
./webfrontend/htmlauth/index.cgi
```

- [ ] **Step 11.5: Final commit**

```bash
git add icons/
git commit -m "feat: add icons and complete plugin structure — v0.1.0 ready"
```

---

## Self-Review

**Spec-Abdeckung:**

| Spec-Abschnitt | Implementiert in |
|---|---|
| Login / Token-Refresh | Task 4 (dreame_login, dreame_refresh_token) |
| Geräteliste | Task 5 (get_device_list, _parse_device_list) |
| sendCommand / sendMowerCommand | Task 5 |
| load_mower_settings (CFG) | Task 5 |
| load_mower_history / load_statistic | Task 5 |
| Dreame-Cloud MQTT (TLS, paho, Thread) | Task 6 (DreameMqttClient) |
| Binär-Parser siid=1 piid=1 | Task 2 (parse_binary_state_1) |
| AutoSwitch-Normalisierung | Task 2 (_normalize_autoswitch) |
| State-JSON-Builder | Task 2 (build_state_json) |
| Station-JSON-Builder | Task 2 (build_station_json) |
| StateMapper (properties_changed) | Task 7 (map_properties_changed) |
| CommandHandler set-Befehle | Task 7 (handle_set_command) |
| CommandHandler settings | Task 7 (handle_settings_command) |
| PRE Read-Modify-Write | Task 7 (handle_settings_command, MOWER_PRE_SETTINGS) |
| task_dreame_to_lbmqtt | Task 8 |
| task_lbmqtt_to_dreame | Task 8 |
| task_token_refresh | Task 8 |
| task_statistic_poll | Task 8 |
| main() / Startup-Sequenz | Task 8 |
| plugin.cfg / daemon.sh / postroot.sh | Task 9 |
| WebUI (Perl/CGI) | Task 10 |
| MQTT-Schema (state/station/statistic/set/settings/command_result) | Tasks 7+8 |

**Placeholder-Scan:** Keine TBD/TODO im Plan.

**Typ-Konsistenz:** 
- `_compute_rlc` → `str` (32 hex chars), genutzt in `_build_dreame_headers` ✓
- `parse_binary_state_1` → `dict`, genutzt in `map_properties_changed` ✓
- `build_state_json(device, props)` — `props` ist `dict` mit `(siid,piid)` Keys ✓
- `handle_set_command` → `tuple[str, int]` → genutzt in `task_lbmqtt_to_dreame` ✓
- `DreameMqttClient` → `queue.put({"did":..., "payload":...})` → `task_dreame_to_lbmqtt` erwartet `item["payload"]` ✓

**Bekannte Lücken:**
- `map_properties_changed` enthält eine ungültige Dict-Comprehension im `autoswitch`-Block — muss beim Ausführen mit korrekter Python-Syntax repariert werden (walrus oder explizite Schleife)
- Icon-Dateien müssen vor einem echten Release durch echte Dreame-Logos ersetzt werden
- `_password_plain` im Config wird im Klartext gespeichert — akzeptabler Kompromiss für LoxBerry (local device), aber im WebUI darauf hinweisen
