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
