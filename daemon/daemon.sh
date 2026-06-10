#!/bin/bash
# Dreame Gateway Daemon Wrapper
# Called by LoxBerry with: daemon.sh start|stop|status

LBHOMEDIR="${LBHOMEDIR:-/opt/loxberry}"
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
