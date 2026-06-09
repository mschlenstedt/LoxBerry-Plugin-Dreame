# tests/test_dreame_gateway.py
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'bin'))

import pytest

from dreame_gateway import _compute_rlc, _md5_password

def test_compute_rlc_dreame():
    # AES-128-ECB("EETjszu*XI5znHsI", "eu|en|DE" padded PKCS7) → deterministic hex
    result = _compute_rlc("EETjszu*XI5znHsI")
    assert result == "7787607c258cdd79141ec1866eb5476c"  # known-vector
    assert len(result) == 32  # 16 bytes AES block → 32 hex chars

def test_compute_rlc_mova():
    result = _compute_rlc("gigxlmqwZ]7oWZUF")
    assert result == "e5828c1d3144dc8d6815f24fa67a5e3f"  # known-vector
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

def test_parse_binary_state_1_string_input():
    assert parse_binary_state_1("not binary") == {}

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
    assert state["status"] == 1
    assert state["battery"] == 85
    assert state["status_str"] == "Working"  # status=1 for mower

def test_build_state_json_vacuum():
    device = {"did": "456", "model": "dreame.vacuum.r2228o", "name": "Sauger", "device_type": "vacuum", "online": False}
    props = {(4, 1): 0, (3, 1): 75, (3, 2): 1}
    state = build_state_json(device, props)
    assert state["device_type"] == "vacuum"
    assert state["battery"] == 75
    assert state["charging"] == 1

def test_build_station_json_basic():
    props = {(25, 1): 0, (25, 2): 1, (25, 3): 0}
    station = build_station_json(props)
    assert station["clean_water_tank"] == 0
    assert station["clean_water_tank_str"] == "Installed"
    assert station["dirty_water_tank"] == 1
    assert station["dirty_water_tank_str"] == "Not installed/Full"
    assert station["dust_bag"] == 0

from dreame_gateway import load_plugin_config, get_mqtt_broker_config
import dreame_gateway

def test_load_plugin_config_defaults(tmp_path):
    cfg_path = tmp_path / "pluginconfig.json"
    cfg_path.write_text("{}", encoding="utf-8")
    orig = dreame_gateway.PLUGIN_CFG
    dreame_gateway.PLUGIN_CFG = cfg_path
    try:
        cfg = load_plugin_config()
        assert cfg["cloud_service"] == "dreame"
        assert cfg["base_topic"] == "dreame"
        assert cfg["polling_interval_min"] == 30
        assert cfg["devices"] == []
        assert cfg["access_token"] == ""
        assert cfg["refresh_token"] == ""
        assert cfg["expires_at"] == 0
        assert cfg["uid"] == ""
        assert cfg["username"] == ""
        assert cfg["password_hash"] == ""
    finally:
        dreame_gateway.PLUGIN_CFG = orig

def test_load_plugin_config_no_file(tmp_path):
    # Non-existent file should return defaults without error
    orig = dreame_gateway.PLUGIN_CFG
    dreame_gateway.PLUGIN_CFG = tmp_path / "nonexistent.json"
    try:
        cfg = load_plugin_config()
        assert cfg["cloud_service"] == "dreame"
        assert cfg["devices"] == []
    finally:
        dreame_gateway.PLUGIN_CFG = orig

def test_get_mqtt_broker_config_defaults():
    general = {"Mqtt": {"Brokerhost": "192.168.1.10", "Brokerport": "1883"}}
    broker = get_mqtt_broker_config(general)
    assert broker["host"] == "192.168.1.10"
    assert broker["port"] == 1883
    assert broker["tls"] is False
    assert broker["username"] is None

def test_get_mqtt_broker_config_tls():
    general = {"Mqtt": {
        "Brokerhost": "lb.local",
        "Brokerport": "1883",
        "Uselocalbroker": "true",
        "Tlsenabled": "true",
        "Tlsport": "8883",
    }}
    broker = get_mqtt_broker_config(general)
    assert broker["tls"] is True
    assert broker["port"] == 8883
    assert broker["tls_verify"] is False

def test_get_mqtt_broker_config_external_tls():
    general = {"Mqtt": {
        "Brokerhost": "external.broker.com",
        "Brokerport": "1883",
        "Uselocalbroker": "false",
        "TlsExternalEnabled": "true",
        "TlsExternalPort": "8884",
        "TlsExternalValidatecert": "true",
    }}
    broker = get_mqtt_broker_config(general)
    assert broker["tls"] is True
    assert broker["port"] == 8884
    assert broker["tls_verify"] is True

from dreame_gateway import _build_dreame_headers, BRAND_CONFIG

def test_build_dreame_headers_no_token():
    brand = BRAND_CONFIG["dreame"]
    headers = _build_dreame_headers(brand, access_token=None)
    assert headers["tenant-id"] == "000000"
    assert headers["dreame-meta"] == "cv=i_829"
    assert "dreame-rlc" in headers
    assert len(headers["dreame-rlc"]) == 32
    assert "dreame-auth" not in headers

def test_build_dreame_headers_with_token():
    brand = BRAND_CONFIG["dreame"]
    headers = _build_dreame_headers(brand, access_token="abc123")
    assert headers["dreame-auth"] == "bearer abc123"
    assert headers["tenant-id"] == "000000"

def test_build_dreame_headers_mova():
    brand = BRAND_CONFIG["mova"]
    headers = _build_dreame_headers(brand, access_token=None)
    assert headers["tenant-id"] == "000002"
    assert len(headers["dreame-rlc"]) == 32

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
    assert devices[0]["name"] == "Mein Mäher"
    assert devices[0]["bind_domain"] == "10000.mt.eu.iot.dreame.tech:19973"
    assert devices[0]["online"] is True

def test_parse_device_list_vacuum():
    records = [{"did": "222", "model": "dreame.vacuum.r2228o",
                "customName": "Sauger", "bindDomain": "dom", "online": False}]
    devices = _parse_device_list(records)
    assert devices[0]["device_type"] == "vacuum"
    assert devices[0]["online"] is False

def test_parse_device_list_missing_fields():
    records = [{"did": "333", "model": "dreame.vacuum.x"}]
    devices = _parse_device_list(records)
    assert devices[0]["name"] == "dreame.vacuum.x"  # falls back to model
    assert devices[0]["bind_domain"] == ""

def test_parse_device_list_empty():
    assert _parse_device_list([]) == []
