# tests/test_dreame_gateway.py
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'bin'))

import pytest

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
