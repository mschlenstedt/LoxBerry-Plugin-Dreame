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

# ---- Gateway-Neustart nach dem Upgrade ----
# Erledigt postroot.sh (läuft als root nach diesem Skript): es schreibt die
# systemd-Unit neu und startet den Dienst, sofern kein gateway_stopped-Flag
# zurückgespielt wurde. Ein Start hier (setsid aus dem Installer heraus) würde
# außerhalb von systemd laufen und mit dem Dienst um den Refresh-Token konkurrieren.

echo "<INFO> Remove temporary folders"
rm -r /tmp/$ARGV1\_upgrade

# Exit with Status 0
exit 0
