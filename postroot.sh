#!/bin/bash
# Installiert Python-Abhängigkeiten nach Plugin-Installation
pip3 install --quiet aiohttp aiomqtt paho-mqtt cryptography
echo "Dreame-Dependencies installiert."
exit 0
