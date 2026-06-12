<script>
const _GW_STYLE = 'flex:1;min-height:3rem;padding:0.5rem 1rem;border-radius:4px;font-weight:500;display:flex;align-items:center;';
let _currentGwPid = null;

function updateGatewayStatus() {
    fetch('ajax.cgi?action=getpid')
        .then(r => r.json())
        .then(data => {
            _currentGwPid = data.pid || null;
            const el = document.getElementById('gw_status_text');
            if (!el) return;
            if (data.pid) {
                el.textContent = 'Gateway läuft (PID ' + data.pid + ')';
                el.style.cssText = _GW_STYLE + 'background:#6dac20;color:black;';
            } else {
                el.textContent = 'Gateway nicht aktiv';
                el.style.cssText = _GW_STYLE + 'background:#d0021b;color:white;';
            }
        })
        .catch(() => {
            _currentGwPid = null;
            const el = document.getElementById('gw_status_text');
            if (el) {
                el.textContent = 'Gateway nicht aktiv';
                el.style.cssText = _GW_STYLE + 'background:#d0021b;color:white;';
            }
        });
}

// After a restart the new gateway needs a few seconds to authenticate and
// publish its auth status — poll the token box so it updates without a reload.
function _refreshTokenAfterRestart() {
    let attempts = 0;
    updateTokenStatus();
    const poll = setInterval(() => {
        attempts++;
        updateTokenStatus();
        if (attempts >= 8) clearInterval(poll);   // ~24s, poll every 3s
    }, 3000);
}

// Rebuild the "Geräte" list from a devices array (built with text nodes so
// cloud-supplied names can't inject markup).
function _renderDevices(devices) {
    const box = document.getElementById('devices_list');
    if (!box) return;
    box.textContent = '';
    if (!devices || !devices.length) {
        const p = document.createElement('p');
        p.innerHTML = 'Keine Geräte &mdash; Gateway starten um Geräteliste zu laden.';
        box.appendChild(p);
        return;
    }
    const ul = document.createElement('ul');
    ul.className = 'lb-list';
    devices.forEach(d => {
        const li = document.createElement('li');
        li.appendChild(document.createTextNode((d.name || '') + ' '));
        const sm1 = document.createElement('small');
        sm1.textContent = '(' + (d.model || '') + ')';
        li.appendChild(sm1);
        li.appendChild(document.createTextNode(' — '));
        const sm2 = document.createElement('small');
        sm2.textContent = d.type || '';
        li.appendChild(sm2);
        ul.appendChild(li);
    });
    box.appendChild(ul);
}

function updateDevices() {
    fetch('ajax.cgi?action=getdevices')
        .then(r => r.json())
        .then(data => _renderDevices(data.devices))
        .catch(() => {});
}

// After a restart the gateway re-fetches the device list (incl. renamed
// devices) from the cloud and rewrites the config a few seconds later — poll
// so the names update without a reload, mirroring _refreshTokenAfterRestart().
function _refreshDevicesAfterRestart() {
    let attempts = 0;
    const poll = setInterval(() => {
        attempts++;
        updateDevices();
        if (attempts >= 8) clearInterval(poll);   // ~24s, poll every 3s
    }, 3000);
}

function _pollNewPid(oldPid) {
    let attempts = 0;
    const poll = setInterval(() => {
        fetch('ajax.cgi?action=getpid')
            .then(r => r.json())
            .then(data => {
                attempts++;
                if ((data.pid && data.pid !== oldPid) || attempts >= 15) {
                    clearInterval(poll);
                    updateGatewayStatus();
                }
            })
            .catch(() => { if (++attempts >= 15) { clearInterval(poll); updateGatewayStatus(); } });
    }, 1000);
}

function updateTokenStatus() {
    fetch('ajax.cgi?action=gettokenstatus')
        .then(r => r.json())
        .then(data => {
            const badge   = document.getElementById('token_badge');
            const expires = document.getElementById('token_expires');
            if (!badge) return;
            if (data.ok) {
                badge.textContent = 'Eingeloggt';
                badge.className   = 'lb-badge lb-badge-success';
                if (expires) {
                    const h = Math.floor(data.expires_in / 3600);
                    const m = Math.floor((data.expires_in % 3600) / 60);
                    expires.textContent = h + 'h ' + m + 'm';
                }
            } else if (data.has_refresh) {
                badge.textContent = 'Token abgelaufen';
                badge.className   = 'lb-badge lb-badge-warning';
                if (expires) expires.textContent = '--';
            } else {
                badge.textContent = 'Nicht eingeloggt';
                badge.className   = 'lb-badge lb-badge-danger';
                if (expires) expires.textContent = '--';
            }
        })
        .catch(() => {});
}

const btnRestart = document.getElementById('btn_restart');
if (btnRestart) {
    btnRestart.addEventListener('click', function(e) {
        e.preventDefault();
        const btn = this;
        const oldPid = _currentGwPid;

        // Gray "restarting" banner immediately
        const el = document.getElementById('gw_status_text');
        if (el) {
            el.textContent = 'Gateway wird neu gestartet …';
            el.style.cssText = _GW_STYLE + 'background:#9e9e9e;color:white;';
        }
        btn.classList.add('lb-btn-loading');

        fetch('ajax.cgi?action=restart')
            .then(r => r.json())
            .then(data => {
                btn.classList.remove('lb-btn-loading');
                if (data && !data.ok && data.error) {
                    if (el) {
                        el.textContent = 'Fehler: ' + data.error;
                        el.style.cssText = _GW_STYLE + 'background:#f5a623;color:black;';
                    }
                    return;
                }
                if (data.pid && data.pid !== oldPid) {
                    updateGatewayStatus();
                } else {
                    _pollNewPid(oldPid);
                }
                _refreshTokenAfterRestart();
                _refreshDevicesAfterRestart();
            })
            .catch(() => { btn.classList.remove('lb-btn-loading'); updateGatewayStatus(); });
    });
}

const btnStop = document.getElementById('btn_stop');
if (btnStop) {
    btnStop.addEventListener('click', function(e) {
        e.preventDefault();
        fetch('ajax.cgi?action=stop')
            .then(() => updateGatewayStatus())
            .catch(() => {});
    });
}

// Config tab: show/hide password toggle (eye button next to the field)
const btnShowPass = document.getElementById('showpass');
if (btnShowPass) {
    btnShowPass.addEventListener('click', function() {
        const inp = document.getElementById('password');
        if (!inp) return;
        if (inp.type === 'password') {
            inp.type = 'text';
            this.innerHTML = '<i class="pi pi-eye-slash"></i>';
        } else {
            inp.type = 'password';
            this.innerHTML = '<i class="pi pi-eye"></i>';
        }
    });
}

updateGatewayStatus();
updateTokenStatus();
setInterval(updateGatewayStatus, 5000);
setInterval(updateTokenStatus,   30000);
</script>
