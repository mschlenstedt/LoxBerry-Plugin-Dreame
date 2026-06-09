<script>
function updateGatewayStatus() {
    fetch('ajax.cgi?action=getpid')
        .then(r => r.json())
        .then(data => {
            const el = document.getElementById('gw_status_text');
            if (!el) return;
            if (data.pid) {
                el.textContent = 'Gateway läuft (PID ' + data.pid + ')';
                el.style.cssText = 'flex:1;min-height:3rem;padding:0.5rem 1rem;border-radius:4px;background:#6dac20;color:black;font-weight:500;display:flex;align-items:center;';
            } else {
                el.textContent = 'Gateway nicht aktiv';
                el.style.cssText = 'flex:1;min-height:3rem;padding:0.5rem 1rem;border-radius:4px;background:#d0021b;color:white;font-weight:500;display:flex;align-items:center;';
            }
        })
        .catch(() => {});
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

const btnStart = document.getElementById('btn_start');
if (btnStart) {
    btnStart.addEventListener('click', function(e) {
        e.preventDefault();
        this.classList.add('lb-btn-loading');
        fetch('ajax.cgi?action=start')
            .then(r => r.json())
            .then(() => {
                btnStart.classList.remove('lb-btn-loading');
                setTimeout(updateGatewayStatus, 1500);
            })
            .catch(() => btnStart.classList.remove('lb-btn-loading'));
    });
}

const btnStop = document.getElementById('btn_stop');
if (btnStop) {
    btnStop.addEventListener('click', function(e) {
        e.preventDefault();
        fetch('ajax.cgi?action=stop')
            .then(r => r.json())
            .then(() => setTimeout(updateGatewayStatus, 500))
            .catch(() => {});
    });
}

updateGatewayStatus();
updateTokenStatus();
setInterval(updateGatewayStatus, 5000);
setInterval(updateTokenStatus,   30000);
</script>
