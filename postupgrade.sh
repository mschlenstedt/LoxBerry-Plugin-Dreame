#!/bin/bash

ARGV0=$0 # Zero argument is shell command
ARGV1=$1 # First argument is temp folder during install
ARGV2=$2 # Second argument is Plugin-Name for scripts etc.
ARGV3=$3 # Third argument is Plugin installation folder
ARGV4=$4 # Forth argument is Plugin version
ARGV5=$5 # Fifth argument is Base folder of LoxBerry

echo "<INFO> Restoring saved config (mirror — only backed-up files survive)"
# Mirror the config dir back from the backup instead of merge-copying it.
# --delete removes files the package shipped but the backup did NOT contain
# (e.g. the bundled config/gateway_stopped flag of a fresh install). If the
# flag WAS in the backup (user deliberately stopped the gateway), it is
# restored and survives the upgrade. Trailing slash = copy contents, not dir.
rsync -a --delete /tmp/$ARGV1\_upgrade/config/$ARGV3/ $ARGV5/config/plugins/$ARGV3/

echo "<INFO> Copy back existing log files"
cp -p -v -r /tmp/$ARGV1\_upgrade/log/$ARGV3/* $ARGV5/log/plugins/$ARGV3/

echo "<INFO> Copy back existing data files"
cp -p -v -r /tmp/$ARGV1\_upgrade/data/$ARGV3/* $ARGV5/data/plugins/$ARGV3/

# ---- Gateway nach dem Upgrade wieder starten (laeuft hier als User 'loxberry') ----
# preupgrade.sh stoppt den Gateway, setzt aber KEIN gateway_stopped-Flag.
# Daher nur starten, wenn der Nutzer ihn nicht absichtlich gestoppt hat.
# Muss NACH dem Config-Copy-Back stehen, damit ein zurueckgespieltes
# gateway_stopped-Flag respektiert wird.
PLUGIN_FOLDER="$ARGV3"
LBPCONFIGDIR="$ARGV5/config/plugins/$PLUGIN_FOLDER"
LBPLOGDIR="$ARGV5/log/plugins/$PLUGIN_FOLDER"
GATEWAY="$ARGV5/bin/plugins/$PLUGIN_FOLDER/dreame_gateway.py"
STOPPED_FLAG="$LBPCONFIGDIR/gateway_stopped"
LBSCONFIG="$ARGV5/config/system"

if [ -f "$STOPPED_FLAG" ]; then
    echo "<INFO> gateway_stopped-Flag gesetzt - Gateway wird nicht neu gestartet"
elif [ ! -f "$GATEWAY" ]; then
    echo "<WARNING> Gateway nicht gefunden unter $GATEWAY - kein Neustart"
else
    mkdir -p "$LBPLOGDIR"
    # Log-Session in der LoxBerry-Log-DB registrieren (Dateiname + dbkey holen)
    read LOGFILE LOGDBKEY < <(perl -e "
        use LoxBerry::Log;
        my \$log = LoxBerry::Log->new(
            name    => 'dreame_gateway',
            package => '$PLUGIN_FOLDER',
            logdir  => '$LBPLOGDIR',
            addtime => 1,
        );
        \$log->LOGSTART('Dreame Gateway nach Upgrade gestartet');
        print \$log->{filename} . ' ' . (\$log->{dbkey} // 0) . \"\n\";
    ")
    [ -z "$LOGFILE" ] && { LOGFILE="$LBPLOGDIR/dreame_gateway.log"; LOGDBKEY="0"; }

    # Vom Installer-Prozess loslösen (setsid), Streams umlenken
    setsid python3 "$GATEWAY" \
        --logfile    "$LOGFILE" \
        --logdbkey   "$LOGDBKEY" \
        --configdir  "$LBPCONFIGDIR" \
        --lbsconfig  "$LBSCONFIG" \
        </dev/null >>"$LOGFILE" 2>&1 &
    echo "<OK> Gateway nach Upgrade neu gestartet (PID $!)"
fi

echo "<INFO> Remove temporary folders"
rm -r /tmp/$ARGV1\_upgrade

# Exit with Status 0
exit 0
