#!/bin/bash
# Installiert Python-Abhängigkeiten nach Plugin-Installation
pip3 install --quiet aiohttp aiomqtt paho-mqtt cryptography
echo "Dreame-Dependencies installiert."

# Gateway als systemd-Dienst einrichten und (neu) starten — außer der Nutzer hat
# ihn gestoppt (gateway_stopped; eine frische Installation liefert das Flag mit,
# startet also nicht automatisch). Läuft als root, nach postupgrade.sh, d.h.
# nach dem Zurückspielen der Konfiguration.
DAEMON="$5/system/daemons/plugins/$3"
if [ -x "$DAEMON" ]; then
    "$DAEMON" install
    if [ -f "$5/config/plugins/$3/gateway_stopped" ]; then
        echo "<INFO> gateway_stopped-Flag gesetzt - Gateway wird nicht gestartet"
    else
        "$DAEMON" restart
        echo "<OK> Gateway-Dienst $3-gateway.service gestartet"
    fi
fi
exit 0
