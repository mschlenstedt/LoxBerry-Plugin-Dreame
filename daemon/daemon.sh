#!/bin/bash
# LoxBerry calls this at boot with PLUGINNAME set to the NAME from plugin.cfg.
# LBHOMEDIR and LBSCONFIG are set in /etc/environment.

PLUGIN_FOLDER=$(basename "$0")

# LoxBerry runs plugin daemons at boot with NO argument (loxberryinit.sh:
# "$PLUGINDAEMONS > /dev/null &"). Default the action to 'start' so the boot
# call reaches the start case — otherwise an empty $1 falls through to *) ->
# "Usage" -> exit 1 and the gateway never starts after a reboot.
ACTION="${1:-start}"

LBPBINDIR="${LBHOMEDIR}/bin/plugins/${PLUGIN_FOLDER}"
LBPCONFIGDIR="${LBHOMEDIR}/config/plugins/${PLUGIN_FOLDER}"
LBPLOGDIR="${LBHOMEDIR}/log/plugins/${PLUGIN_FOLDER}"
GATEWAY="${LBPBINDIR}/dreame_gateway.py"
PIDFILE="/dev/shm/dreame_gateway.pid"
STOPPED_FLAG="${LBPCONFIGDIR}/gateway_stopped"

case "$ACTION" in
  start)
    # The gateway must run as the unprivileged 'loxberry' user, never as root.
    # LoxBerry calls this daemon as root at boot, so re-exec the start logic as
    # loxberry. The whole script then re-runs from the top under loxberry, so all
    # paths recompute correctly and the log files end up loxberry-owned. We pass
    # LBHOMEDIR/LBSCONFIG explicitly because the login shell (su -) resets the env.
    if [ "$(id -un)" != "loxberry" ]; then
        exec su - loxberry -c "LBHOMEDIR='$LBHOMEDIR' LBSCONFIG='$LBSCONFIG' bash '$0' start"
    fi

    # Do not start if the gateway was manually stopped via the WebUI
    if [ -f "$STOPPED_FLAG" ]; then
        logger "Dreame: gateway_stopped flag set — not starting"
        exit 0
    fi

    if [ ! -f "$GATEWAY" ]; then
        logger "Dreame: gateway not found at $GATEWAY"
        exit 1
    fi

    mkdir -p "$LBPLOGDIR"

    # Register log entry in LoxBerry log database and get filename + dbkey
    read LOGFILE LOGDBKEY < <(perl -e "
        use LoxBerry::Log;
        my \$log = LoxBerry::Log->new(
            name    => 'dreame_gateway',
            package => '$PLUGIN_FOLDER',
            logdir  => '$LBPLOGDIR',
            addtime => 1,
        );
        \$log->LOGSTART('Dreame Gateway started');
        print \$log->{filename} . ' ' . (\$log->{dbkey} // 0) . \"\n\";
    ")

    if [ -z "$LOGFILE" ]; then
        LOGFILE="${LBPLOGDIR}/dreame_gateway.log"
        LOGDBKEY="0"
    fi

    python3 "$GATEWAY" \
        --logfile    "$LOGFILE" \
        --logdbkey   "$LOGDBKEY" \
        --configdir  "$LBPCONFIGDIR" \
        --lbsconfig  "$LBSCONFIG" \
        &

    logger "Dreame: gateway started (PID $!)"
    ;;
  stop)
    # Persist the stop so the gateway stays down across reboots
    touch "$STOPPED_FLAG"
    if [ -f "$PIDFILE" ]; then
        PID=$(cat "$PIDFILE")
        kill "$PID" 2>/dev/null
        rm -f "$PIDFILE"
        logger "Dreame: gateway stopped"
    else
        logger "Dreame: gateway not running"
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
