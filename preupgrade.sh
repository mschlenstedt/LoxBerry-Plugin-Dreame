#!/bin/bash
# Vor Upgrade: laufendes Gateway stoppen
PIDFILE="/dev/shm/dreame_gateway.pid"
if [ -f "$PIDFILE" ]; then
  kill "$(cat $PIDFILE)" 2>/dev/null
  rm -f "$PIDFILE"
fi
exit 0
