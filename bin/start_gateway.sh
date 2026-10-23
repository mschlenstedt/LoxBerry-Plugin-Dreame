#!/bin/bash
# ExecStart of <folder>-gateway.service (User=loxberry, written by the daemon
# script). Registers a new LoxBerry log session for this run, then execs the
# gateway in the foreground so systemd supervises the python process itself.

LBHOMEDIR="${LBHOMEDIR:-/opt/loxberry}"
LBSCONFIG="${LBSCONFIG:-$LBHOMEDIR/config/system}"
LBPBINDIR="$(dirname "$(readlink -f "$0")")"
PLUGIN_FOLDER="$(basename "$LBPBINDIR")"
LBPCONFIGDIR="$LBHOMEDIR/config/plugins/$PLUGIN_FOLDER"
LBPLOGDIR="$LBHOMEDIR/log/plugins/$PLUGIN_FOLDER"
GATEWAY="$LBPBINDIR/dreame_gateway.py"

mkdir -p "$LBPLOGDIR"

# Register the log in the LoxBerry log database and get filename + dbkey
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
    LOGFILE="$LBPLOGDIR/dreame_gateway.log"
    LOGDBKEY="0"
fi

# Uncaught Python errors (tracebacks) end up in the same LoxBerry log.
exec >>"$LOGFILE" 2>&1
exec python3 "$GATEWAY" \
    --logfile    "$LOGFILE" \
    --logdbkey   "$LOGDBKEY" \
    --configdir  "$LBPCONFIGDIR" \
    --lbsconfig  "$LBSCONFIG"
