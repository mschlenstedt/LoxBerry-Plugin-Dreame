#!/bin/bash
# LoxBerry calls this at boot (as root, with NO argument).
# LBHOMEDIR and LBSCONFIG are set in /etc/environment.
#
# The gateway runs as its own systemd service <folder>-gateway.service
# (User=loxberry, Restart=on-failure). Before, it was started as a plain
# background process — when started from the WebUI (ajax.cgi) it lived in
# apache2.service's cgroup and was SIGKILLed by systemd on every Apache
# restart, silently, and nothing ever started it again.
#
# The unit file is (re)written on every root call, so it also survives a
# re-created LoxBerry container (/etc is not on the persistent volume).
# It is deliberately NOT enabled: this script starts it at boot unless the
# WebUI's persistent stop flag (gateway_stopped) is set.
#
#   start   (default) write unit as root, start unless stopped by the user
#   stop    set stop flag, stop the service
#   restart clear stop flag, restart the service
#   status  running <pid> | stopped
#   install write/refresh the unit only (root)

PLUGIN_FOLDER=$(basename "$0")
ACTION="${1:-start}"

LBHOMEDIR="${LBHOMEDIR:-/opt/loxberry}"
LBPBINDIR="${LBHOMEDIR}/bin/plugins/${PLUGIN_FOLDER}"
LBPCONFIGDIR="${LBHOMEDIR}/config/plugins/${PLUGIN_FOLDER}"
STARTER="${LBPBINDIR}/start_gateway.sh"
PIDFILE="/dev/shm/dreame_gateway.pid"
STOPPED_FLAG="${LBPCONFIGDIR}/gateway_stopped"
UNIT="${PLUGIN_FOLDER}-gateway.service"
UNITFILE="/etc/systemd/system/${UNIT}"

# root runs systemctl directly; loxberry may run /bin/systemctl via sudo
# (LoxBerry's default sudoers).
if [ "$(id -u)" = "0" ]; then SYSTEMCTL="/bin/systemctl"; else SYSTEMCTL="sudo -n /bin/systemctl"; fi

write_unit() {
    [ "$(id -u)" = "0" ] || return 0
    chmod 755 "$STARTER" 2>/dev/null
    local tmp="${UNITFILE}.tmp"
    cat > "$tmp" <<EOF
[Unit]
Description=LoxBerry Plugin Dreame Gateway (${PLUGIN_FOLDER})
After=network-online.target mosquitto.service
Wants=network-online.target

[Service]
Type=simple
User=loxberry
Group=loxberry
EnvironmentFile=/etc/environment
ExecStart=${STARTER}
Restart=on-failure
RestartSec=60
TimeoutStopSec=30
EOF
    if ! cmp -s "$tmp" "$UNITFILE"; then
        mv "$tmp" "$UNITFILE"
        /bin/systemctl daemon-reload
    else
        rm -f "$tmp"
    fi
}

# A gateway started the old way (setsid/double-fork, not under systemd) would
# run in parallel to the service — both using and rotating the same refresh
# token. Stop it before starting the service.
kill_unmanaged() {
    [ -f "$PIDFILE" ] || return 0
    local pid; pid=$(cat "$PIDFILE" 2>/dev/null)
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null || return 0
    if [ "$($SYSTEMCTL show -p MainPID --value "$UNIT" 2>/dev/null)" = "$pid" ]; then
        return 0
    fi
    kill "$pid" 2>/dev/null
    for _ in 1 2 3 4 5 6 7 8 9 10; do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
    kill -9 "$pid" 2>/dev/null
    rm -f "$PIDFILE"
}

case "$ACTION" in
  install)
    write_unit
    ;;
  start)
    write_unit
    if [ -f "$STOPPED_FLAG" ]; then
        logger "Dreame: gateway_stopped flag set — not starting"
        exit 0
    fi
    if [ ! -x "$STARTER" ]; then
        logger "Dreame: starter not found at $STARTER"
        exit 1
    fi
    kill_unmanaged
    $SYSTEMCTL start "$UNIT"
    logger "Dreame: $UNIT started"
    ;;
  restart)
    write_unit
    rm -f "$STOPPED_FLAG"
    kill_unmanaged
    $SYSTEMCTL restart "$UNIT"
    logger "Dreame: $UNIT restarted"
    ;;
  stop)
    # Persist the stop so the gateway stays down across reboots
    touch "$STOPPED_FLAG"
    $SYSTEMCTL stop "$UNIT" 2>/dev/null
    kill_unmanaged
    rm -f "$PIDFILE"
    logger "Dreame: gateway stopped"
    ;;
  status)
    PID=$($SYSTEMCTL show -p MainPID --value "$UNIT" 2>/dev/null)
    if [ -n "$PID" ] && [ "$PID" != "0" ]; then
        echo "running $PID"
    elif [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
        echo "running $(cat "$PIDFILE") (not under systemd)"
    else
        echo "stopped"
    fi
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|install}"
    exit 1
    ;;
esac
exit 0
