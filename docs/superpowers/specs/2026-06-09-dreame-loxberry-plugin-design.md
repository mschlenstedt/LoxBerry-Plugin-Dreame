# Design: LoxBerry-Plugin-Dreame

**Datum:** 2026-06-09  
**Status:** Genehmigt  
**Ziel-Repo:** https://github.com/mschlenstedt/LoxBerry-Plugin-Dreame

---

## 1. Ziel

Ein LoxBerry-Plugin, das Dreame- und MOVA-Roboter (Mähroboter und Saugroboter) mit dem LoxBerry-MQTT-Broker verbindet. Das Plugin folgt strukturell dem LoxBerry-Plugin-Navimow und besteht aus:

- einem Python-Asyncio-Daemon (`dreame_gateway.py`), der mit der Dreame-Cloud kommuniziert
- einer PHP/CGI-WebUI zur Konfiguration (Login, Geräteübersicht, Start/Stop, Log)
- dem Standard-LoxBerry-Plugin-Aufbau (V4, wie SamplePlugin-V4)

Das Gateway leitet Statusdaten aus der Dreame-Cloud an den LoxBerry-MQTT-Broker weiter und nimmt Steuerbefehle vom LoxBerry-MQTT-Broker entgegen und leitet sie zur Dreame-Cloud weiter.

**Referenz-Implementierungen:**
- `LoxBerry-Plugin-Navimow` – strukturelle Vorlage (Python-Daemon, WebUI, Plugin-Aufbau)
- `ioBroker.dreame` – vollständige Protokoll-Referenz (Auth, MQTT, REST, Befehlssatz)

---

## 2. Gerätetypen und Marken

| Marke | Cloud-Domain | tenantId | iotComPrefix | MQTT-Fallback |
|---|---|---|---|---|
| Dreame | `eu.iot.dreame.tech:13267` | `000000` | `10000` | `app.mt.eu.iot.dreame.tech:19973` |
| MOVA | `eu.iot.mova-tech.com:13267` | `000002` | `20000` | `app.mt.eu.iot.mova-tech.com:19974` |

Gerätetyp-Erkennung: `device.model` enthält `"mower"` → Mähroboter, sonst Saugroboter.

---

## 3. Dreame-Cloud-Protokoll

### 3.1 Login (POST)

```
POST https://{domain}/dreame-auth/oauth/token
Content-Type: application/x-www-form-urlencoded
```

**Pflicht-Header:**
```
user-agent:    Dart/3.2 (dart:io)
dreame-meta:   cv=i_829
dreame-rlc:    AES-128-ECB(key=brand.rlcKey, plaintext="eu|en|DE") → hex
tenant-id:     brand.tenantId
host:          brand.domain
authorization: Basic <brand.appCredentials_base64>
dreame-auth:   bearer
```

**rlcKey je Marke:**
- Dreame: `EETjszu*XI5znHsI`
- MOVA: `gigxlmqwZ]7oWZUF`

**Body (form-urlencoded):**
```
grant_type = password
scope      = all
platform   = IOS
type       = account
username   = <E-Mail>
password   = MD5(<passwort> + "RAylYC%fmSKp7%Tq")
country    = DE
lang       = de
```

**Antwort:**
```json
{
  "access_token": "...",
  "refresh_token": "...",
  "expires_in": 3600,
  "uid": "12345"
}
```

Token und uid werden in `pluginconfig.json` gespeichert.

### 3.2 Token-Refresh (POST)

```
POST https://{domain}/dreame-auth/oauth/token
grant_type    = refresh_token
refresh_token = <refresh_token>
```

Wird 5 Minuten vor Ablauf automatisch ausgeführt. Bei Fehler: erneuter Login.

### 3.3 Geräteliste (POST)

```
POST https://{domain}/dreame-user-iot/iotuserbind/device/listV2
dreame-auth: bearer <access_token>

Body JSON: {"sharedStatus":1,"current":1,"size":100,"lang":"de","timestamp":<ms>}
```

Liefert `did`, `model`, `customName`, `bindDomain` (für MQTT-URL), `online`, `battery`.  
Wird beim Start geladen und in `pluginconfig.json` gecacht.

### 3.4 REST-Befehle (sendCommand, POST)

```
POST https://{domain}/dreame-iot-com-{iotComPrefix}/device/sendCommand
dreame-auth: bearer <access_token>
```

Body:
```json
{
  "did": "<did>",
  "id": <requestId>,
  "data": {
    "did": "<did>",
    "id": <requestId>,
    "method": "get_properties" | "set_properties" | "action",
    "params": { ... },
    "from": "XXXXXX"
  }
}
```

**get_properties** – Property-Werte lesen:
```json
"params": [{"did":"<did>","siid":3,"piid":1}]
```

**set_properties** – Property-Werte schreiben:
```json
"params": [{"did":"<did>","siid":4,"piid":4,"value":2}]
```

**action** – Aktion auslösen:
```json
"params": {"did":"<did>","siid":2,"aiid":1,"in":[]}
```

### 3.5 Mähroboter-Sonderkanal (sendMowerCommand)

Mähroboter-spezifische Befehle laufen über `action siid=2 aiid=50`:

```json
"params": {"did":"<did>","siid":2,"aiid":50,"in":[<payload>]}
```

| Payload | Bedeutung |
|---|---|
| `{"m":"g","t":"CFG"}` | Konfiguration lesen (getCFG) |
| `{"m":"s","t":"WRP","d":{"value":1,"time":8,"sen":0}}` | Regenschutz setzen |
| `{"m":"s","t":"FDP","d":{"value":1}}` | Frostschutz setzen |
| `{"m":"s","t":"VOL","d":{"value":80}}` | Lautstärke setzen |
| `{"m":"s","t":"PRE","d":{"value":[...]}}` | Mähpräferenzen setzen |
| `{"m":"s","t":"CMS","d":{"value":[0,x,x]}}` | Verschleißteile zurücksetzen |
| `{"m":"a","p":0,"o":9}` | Roboter suchen (Ton) |
| `{"m":"a","p":0,"o":12}` | Roboter sperren |

PRE-Array-Indizes: `[0]=zone, [1]=mow_mode, [2]=cutting_height_mm, [3]=obstacle_dist_mm, [5]=direction_change, [8]=edge_detection, [9]=edge_mowing`

### 3.6 AutoSwitch (set_properties siid:4 piid:50)

AutoSwitch-Einstellungen werden als einzelnes JSON-Objekt geschrieben:
```json
{"k":"LessColl","v":1}
```

AutoSwitch-Keys: `LessColl`, `FillinLight`, `SmartHost`, `CleanRoute`, `SmartCharge`, `MeticulousTwist`, `MaterialDirectionClean`, `PetPartClean` (Mäher)  
Zusätzlich Saugroboter: `AutoDry`, `StainIdentify`, `CleanType`, `SuctionMax`, `SmartDrying`, `HotWash`, `UVLight`, `SuperWash`, `MopExtrSwitch`, `SmartAutoMop`, `SmartAutoWash`, `BackWashType`, `CarpetFineClean`, `LacuneMopScalable`, `MopScalable2`

### 3.7 Dreame-Cloud-MQTT (eingehend)

**Verbindungsparameter:**
```
URL:       mqtts://{device.bindDomain}   (z.B. 10000.mt.eu.iot.dreame.tech:19973)
clientId:  p_{8 zufällige hex Bytes}
username:  session.uid
password:  session.access_token
TLS:       rejectUnauthorized=False (selbstsigniertes Zertifikat)
```

**Subscribe-Topic:** `/status/{did}/{uid}/{model}/eu/`

**Nachrichtenformat:**
```json
{
  "id": 92,
  "did": "123456",
  "data": {
    "method": "properties_changed",
    "params": [
      {"did":"123456","siid":2,"piid":1,"value":1},
      {"did":"123456","siid":3,"piid":1,"value":85}
    ]
  }
}
```

**Sonder-Properties Mähroboter (binär):**

- `siid=1, piid=1` (20+ Bytes): Fehlercode (B1-4), Akku (B11 & 0x7F), Ladebit (B11 >> 7), RobotState (B14), WiFi-RSSI (B17), LTE-RSSI (B18), BLE-RSSI (B16)
- `siid=1, piid=4` (12-bit gepackt): X/Y-Position + Winkel des Roboters

Diese Binär-Properties werden geparst, aber **nicht** in MQTT weitergeleitet (zu technisch für Loxone). Nur abgeleitete Felder (battery, charging) fließen in `state`.

- `siid=2, piid=51`: Trigger → getCFG erneut laden
- `siid=4, piid=50`: AutoSwitch JSON → in `state` als Einzel-Felder

### 3.8 Statistik-Quellen

**Mähroboter:**
- siid 12: total_mow_time (piid 2), total_mow_count (piid 3), total_mow_area (piid 4)
- History-API (letzten 20 Sessions): `POST /dreame-user-iot/mower/history/listV2`
- getCFG CMS: blade/brush/robot Betriebsstunden

**Saugroboter:**
- siid 9 (Hauptbürste), siid 10 (Seitenbürste), siid 11 (Filter), siid 12 (Statistik), siid 16 (Sensor), siid 30 (Rad)

---

## 4. MQTT-Schema (LoxBerry-seitig)

### 4.1 Topics (Übersicht)

```
dreame/{did}/state           ← Gerätestatus-JSON         (retain=true)
dreame/{did}/state_station   ← Stationsstatus-JSON        (retain=true, nur Saugroboter)
dreame/{did}/statistic       ← Statistik + Verbrauch JSON (retain=true)
dreame/{did}/set             ← Aktionsbefehle (Text)      (kein retain)
dreame/{did}/settings/{key}  ← Einstellungswerte          (kein retain)
dreame/{did}/command_result  ← Befehlsergebnis JSON       (kein retain)
```

Base-Topic ist konfigurierbar (Default: `dreame`).

### 4.2 `state`-JSON

**Gemeinsame Felder (Mäher + Sauger):**
```json
{
  "device_type": "mower",
  "name": "Dreame A2 1200",
  "model": "dreame.mower.r2320",
  "did": "123456",
  "status": 1,
  "status_str": "Working",
  "battery": 85,
  "charging": 0,
  "error": 0,
  "error_str": "none",
  "online": true
}
```

**Zusatzfelder Mähroboter:**
```json
{
  "mowing_time_min": 45,
  "mowing_area_m2": 120,
  "task_status": 1,
  "warn_status": 0,
  "rain_protection": 1,
  "frost_protection": 0,
  "cutting_height_mm": 35,
  "volume": 80,
  "grass_protection": 1,
  "child_lock": 0,
  "anti_theft": 0
}
```

**Zusatzfelder Saugroboter:**
```json
{
  "cleaning_time_min": 23,
  "cleaned_area_m2": 45,
  "suction_level": 2,
  "water_volume": 2,
  "cleaning_mode": 0,
  "task_status": 2,
  "warn_status": 0,
  "dnd_enabled": 0,
  "child_lock": 0
}
```

### 4.3 `state_station`-JSON (nur Saugroboter)

```json
{
  "clean_water_tank": 0,
  "clean_water_tank_str": "Installed",
  "dirty_water_tank": 0,
  "dirty_water_tank_str": "Installed",
  "dust_bag": 0,
  "dust_bag_str": "Installed",
  "detergent": 0,
  "hot_water": 0
}
```

`clean_water_tank`: 0=Installed, 1=Not installed, 2=Low water  
`dirty_water_tank`: 0=Installed, 1=Not installed/Full  
`dust_bag`: 0=Installed, 1=Not installed, 2=Check

Quelle: siid 25 via `properties_changed`. Wird bei Verbindungsaufbau per `get_properties` geladen.

### 4.4 `statistic`-JSON

**Mähroboter:**
```json
{
  "total_mow_time_min": 1234,
  "total_mow_count": 42,
  "total_mow_area_m2": 5600,
  "last_mow_date": 1748000000,
  "last_mow_duration_min": 95,
  "last_mow_area_m2": 320,
  "last_mow_completed": true,
  "blade_hours": 12.5,
  "blade_health_pct": 88,
  "brush_hours": 3.2,
  "brush_health_pct": 99,
  "robot_maintenance_hours": 1.1,
  "robot_maintenance_health_pct": 98
}
```

**Saugroboter:**
```json
{
  "first_cleaning_date": 1700000000,
  "total_cleaning_time_min": 3600,
  "cleaning_count": 145,
  "total_cleaned_area_m2": 12500,
  "main_brush_left_pct": 85,
  "main_brush_time_left_h": 178,
  "side_brush_left_pct": 92,
  "side_brush_time_left_h": 195,
  "filter_left_pct": 78,
  "filter_time_left_h": 156,
  "sensor_dirty_left_pct": 95,
  "wheel_dirty_left_pct": 88
}
```

Update: Beim Start, nach abgeschlossener Mähsession (Statuswechsel aktiv → idle), zyklisch (Default: 30 min).

### 4.5 `set`-Befehle (Aktionen)

**Mähroboter** (`dreame/{did}/set`):

| Befehl | Dreame-API |
|---|---|
| `start` | action siid:2 aiid:1 |
| `stop` | action siid:2 aiid:2 |
| `pause` | action siid:2 aiid:4 |
| `dock` | action siid:5 aiid:3 |
| `find` | sendMowerCommand {m:'a',p:0,o:9} |
| `lock` | sendMowerCommand {m:'a',p:0,o:12} |
| `clear_warning` | action siid:4 aiid:3 |

**Saugroboter** (`dreame/{did}/set`):

| Befehl | Dreame-API |
|---|---|
| `start` | action siid:2 aiid:1 |
| `pause` | action siid:2 aiid:2 |
| `stop` | action siid:4 aiid:2 |
| `dock` | action siid:3 aiid:1 |
| `locate` | action siid:7 aiid:1 |
| `auto_empty` | action siid:15 aiid:1 |
| `start_washing` | action siid:4 aiid:4 |
| `clear_warning` | action siid:4 aiid:3 |

### 4.6 `settings/{key}`-Einstellungen

**Mähroboter** (`dreame/{did}/settings/{key}`):

| Key | Wert | Mechanismus |
|---|---|---|
| `cutting_height` | 30–60 (mm) | CFG PRE[2] read-modify-write |
| `mow_mode` | 0=Standard, 1=Efficient | CFG PRE[1] |
| `edge_mowing` | 0\|1 | CFG PRE[9] |
| `edge_detection` | 0\|1 | CFG PRE[8] |
| `direction_change` | 0=auto, 1=off | CFG PRE[5] |
| `rain_protection` | `{"value":1,"time":8,"sen":0}` oder `{"value":0}` | CFG WRP |
| `frost_protection` | 0\|1 | CFG FDP |
| `low_speed` | `{"value":1,"time":[1200,480]}` oder `{"value":0}` | CFG LOW |
| `grass_protection` | 0\|1 | CFG PROT |
| `volume` | 0–100 | CFG VOL |
| `child_lock` | 0\|1 | CFG CLS |
| `anti_theft` | 0\|1 | CFG STUN |
| `ai_obstacle` | 0\|1 | CFG AOP |
| `headlight` | `{"value":1,"time":[480,1200],"light":[1,1,1,1]}` | CFG LIT |
| `path_display` | 0\|1 | CFG PATH |
| `collision_avoidance` | 0\|1 | AutoSwitch LessColl |
| `auto_charging` | 0\|1 | AutoSwitch SmartCharge |
| `clean_genius` | 0–2 | AutoSwitch SmartHost |
| `cleaning_route` | 1–4 | AutoSwitch CleanRoute |
| `schedule` | JSON | set_properties siid:8 piid:2 |
| `dnd_enable` | 0\|1 | set_properties siid:5 piid:1 |
| `obstacle_avoidance` | 0\|1 | set_properties siid:4 piid:21 |

**Saugroboter** (`dreame/{did}/settings/{key}`):

| Key | Wert | Mechanismus |
|---|---|---|
| `suction_level` | 0–3 | set_properties siid:4 piid:4 |
| `water_volume` | 1–3 | set_properties siid:4 piid:5 |
| `cleaning_mode` | 0–3 | set_properties siid:4 piid:23 |
| `volume` | 0–100 | set_properties siid:7 piid:1 |
| `child_lock` | 0\|1 | set_properties siid:4 piid:27 |
| `carpet_boost` | 0\|1 | set_properties siid:4 piid:12 |
| `carpet_cleaning` | 0–2 | set_properties siid:4 piid:36 |
| `carpet_sensitivity` | 1–3 | set_properties siid:4 piid:28 |
| `carpet_recognition` | 0\|1 | set_properties siid:4 piid:33 |
| `drying_time` | 2–4 | set_properties siid:4 piid:40 |
| `auto_water_refilling` | 0\|1 | set_properties siid:4 piid:51 |
| `auto_add_detergent` | 0\|1 | set_properties siid:4 piid:37 |
| `mop_wash_level` | Zahl | set_properties siid:4 piid:46 |
| `dnd_enable` | 0\|1 | set_properties siid:5 piid:1 |
| `dnd_start` | "HH:MM" | set_properties siid:5 piid:2 |
| `dnd_end` | "HH:MM" | set_properties siid:5 piid:3 |
| `auto_dust_collecting` | 0\|1 | set_properties siid:15 piid:1 |
| `auto_empty_frequency` | Zahl | set_properties siid:15 piid:2 |
| `water_temperature` | 0–3 | set_properties siid:28 piid:8 |
| `wetness_level` | Zahl | set_properties siid:28 piid:1 |
| `silent_drying` | 0\|1 | set_properties siid:28 piid:27 |
| `hair_compression` | 0\|1 | set_properties siid:28 piid:28 |
| `auto_drying` | 0\|1 | AutoSwitch AutoDry |
| `smart_charging` | 0\|1 | AutoSwitch SmartCharge |
| `stain_avoidance` | 0\|1 | AutoSwitch StainIdentify |
| `collision_avoidance` | 0\|1 | AutoSwitch LessColl |
| `max_suction` | 0\|1 | AutoSwitch SuctionMax |
| `hot_washing` | 0\|1 | AutoSwitch HotWash |
| `uv_sterilization` | 0\|1 | AutoSwitch UVLight |
| `ultra_clean_mode` | 0\|1 | AutoSwitch SuperWash |
| `mop_extend` | 0\|1 | AutoSwitch MopExtrSwitch |
| `self_clean_frequency` | 0–2 | AutoSwitch BackWashType |
| `schedule` | JSON | set_properties siid:8 piid:2 |

### 4.7 `command_result`-JSON

```json
{
  "command": "start",
  "result": "ok",
  "result_num": 0,
  "reason": "none"
}
```

`result`: `"ok"` oder `"error"`. Bei Fehler enthält `reason` den Dreame-Fehlercode (z.B. `"80001"` = Timeout).

---

## 5. Python-Gateway-Architektur

### 5.1 Datei: `bin/dreame_gateway.py`

Einzel-Datei-Daemon analog zu `navimow_gateway.py`. Gegliedert in logische Abschnitte:

```
── CLI-Args & Logging     (wie Navimow)
── Konfiguration          load/save pluginconfig.json
── DreameAuth             login(), refresh_token()
── DreameAPI              get_device_list(), get_properties(), send_command(),
                          send_mower_command(), load_mower_settings(),
                          load_statistic(), load_mower_history()
── StateMapper            Dreame-MQTT-Message → LoxBerry-State-JSONs
                          (state, state_station, statistic)
── CommandHandler         LoxBerry-set/settings → Dreame-API-Calls
── task_dreame_to_lbmqtt  Dreame-Cloud-MQTT → LoxBerry-MQTT Publisher
── task_lbmqtt_to_dreame  LoxBerry-MQTT → Dreame-API Command-Dispatcher
── task_token_refresh     Token-Watchdog (5 min vor Ablauf)
── task_statistic_poll    Statistik-Polling (konfigurierbar, Default 30 min)
── main()                 asyncio.run()
```

### 5.2 Dreame-Cloud-MQTT-Verbindung

Da die Dreame-Cloud MQTTS (TLS) verwendet und `paho-mqtt` für `mqtts://` mit Python-asyncio umständlich ist, wird eine synchrone `paho.mqtt.client`-Instanz in einem separaten Thread betrieben. State-Updates werden per `asyncio.Queue` an den asyncio-Loop übergeben (gleiche Technik wie Navimow mit aiomqtt, hier aber mit paho wegen fehlendem WebSocket-Bedarf).

**Alternative:** `aiomqtt` mit `ssl_context` (bevorzugt, falls aiomqtt auf dem LoxBerry verfügbar ist).

### 5.3 Binär-Parsing (Mähroboter)

`siid=1 piid=1` (Status-Byte-Array):
```python
buf = bytes(value)
error_code   = int.from_bytes(buf[1:5], 'little')
battery      = buf[11] & 0x7F
charging     = (buf[11] & 0x80) >> 7
robot_state  = buf[14]
docking_state = (robot_state & 0x1C) >> 2
wifi_rssi    = buf[17] - 256 if buf[17] > 127 else buf[17]
```

`siid=1 piid=4` (Positions-Byte-Array): wird empfangen aber nicht weiter an LoxBerry publiziert.

### 5.4 PRE Read-Modify-Write

Mähroboter-Einstellungen im PRE-Array erfordern:
1. `sendMowerCommand({m:'g', t:'CFG'})` → aktuelles PRE-Array lesen
2. Ziel-Index ändern
3. `sendMowerCommand({m:'s', t:'PRE', d:{value: pre_array}})` → schreiben

Das Gateway führt diesen 3-Schritt-Prozess sequenziell in `CommandHandler` durch.

---

## 6. Plugin-Aufbau (LoxBerry V4)

Struktur analog zu `LoxBerry-Plugin-Navimow`:

```
bin/
  dreame_gateway.py          Haupt-Daemon
  dreame_sdk_update.sh       Pip-Dependencies installieren (falls nötig)
config/
  pluginconfig.json          Laufzeit-Konfiguration
  gateway_stopped            Marker-Datei: Gateway soll gestoppt bleiben
daemon/
  daemon.sh                  Start/Stop/Status des Gateways
icons/
  icon_64.png  icon_128.png  icon_256.png  icon_512.png   (Dreame-Logo)
plugin.cfg                   Plugin-Metadaten
postroot.sh / postupgrade.sh  pip-Dependencies bei Install/Upgrade
prerelease.cfg / release.cfg
webfrontend/
  htmlauth/
    index.cgi                WebUI (Perl/CGI, wie Navimow)
```

### 6.1 `pluginconfig.json` (Konfigurationsfelder)

```json
{
  "cloud_service": "dreame",
  "username": "user@example.com",
  "password_hash": "",
  "access_token": "",
  "refresh_token": "",
  "expires_at": 0,
  "uid": "",
  "base_topic": "dreame",
  "polling_interval_min": 30,
  "devices": []
}
```

Das Passwort wird **nicht** im Klartext gespeichert. Nach dem ersten Login wird nur `access_token` + `refresh_token` persistiert. Das Passwort wird nur für den initialen Login verwendet.

`devices`-Eintrag:
```json
{
  "did": "123456",
  "model": "dreame.mower.r2320",
  "name": "Dreame A2 1200",
  "device_type": "mower",
  "bind_domain": "10000.mt.eu.iot.dreame.tech:19973",
  "online": true
}
```

### 6.2 Python-Dependencies

```
aiohttp        HTTP-Client (Dreame REST-API)
aiomqtt        LoxBerry-MQTT (async)
paho-mqtt      Dreame-Cloud-MQTT (sync, TLS)
cryptography   AES-128-ECB für dreame-rlc Header
```

Installiert per `pip install` in `postroot.sh` / `postupgrade.sh`.

---

## 7. WebUI (Perl/CGI)

Analog zu `LoxBerry-Plugin-Navimow/webfrontend/htmlauth/index.cgi`.

**Seiten:**
1. **Konfiguration** – Cloud-Marke (Dreame/MOVA), E-Mail, Passwort, Base-Topic, Polling-Intervall, Login-Button
2. **Geräte** – Liste der gefundenen Geräte (Name, Modell, Typ, online/offline, Akku)
3. **Gateway** – Start/Stop-Button, aktueller Status (läuft/gestoppt), PID
4. **Log** – Letzte Logzeilen des Gateways

Nach dem Login wird das Token gespeichert und das Passwort-Feld geleert. Ein Re-Login ist nur nötig wenn der Refresh-Token abläuft (typisch: mehrere Wochen).

---

## 8. Fehlerbehandlung

| Szenario | Verhalten |
|---|---|
| Login fehlgeschlagen | LOGERR, Gateway wartet, WebUI zeigt Fehler |
| Token abgelaufen | automatischer Refresh (5 min vor Ablauf); bei Fehlschlag: erneuter Login mit gespeicherten Credentials |
| Dreame-MQTT unterbrochen | paho-mqtt reconnect automatisch (reconnect_delay=10s) |
| LoxBerry-MQTT unterbrochen | aiomqtt reconnect-Loop (wie Navimow) |
| Dreame REST Timeout (Code 80001) | LOGDEB (bekannter Timeout), kein Fehler |
| Unbekannte Dreame REST Fehler | LOGWARN |
| Binäre MQTT-Nachricht nicht parsebar | LOGDEB, überspringen |
| JSON parse error (MQTT truncated) | LOGINF (bekannte Server-Limitierung auf 4096 Bytes) |
| settings/{key} unbekannt | LOGWARN + `command_result` mit `result=error, reason=unknown_key` |

---

## 9. Nicht im Scope (Phase 1)

- Kartenrendering (PNG-Ausgabe) — erfordert `canvas`/`Pillow`, zu CPU-intensiv
- 3D-LIDAR-Karten-URL
- Raum-spezifisches Reinigen (custom-clean mit Room-IDs)
- Shortcut-Verwaltung
- WiFi-Karten
- MOVA-spezifische Besonderheiten (falls vorhanden)

Diese Features können in späteren Versionen ergänzt werden.

---

## 10. Startup-Sequenz

```
1. Lade pluginconfig.json
2. Prüfe ob gateway_stopped existiert → wenn ja, beende
3. Schreibe PID-Datei
4. Lade LoxBerry general.json → MQTT-Broker-Config
5. Wenn kein access_token: LOGWARN, warte auf WebUI-Login
6. Wenn token expired/expiring: refresh_token()
7. Lade Geräteliste (REST)
8. Für jedes Gerät: get_properties (Statistik, Station-Status)
9. Verbinde Dreame-Cloud-MQTT (paho, Thread)
10. Verbinde LoxBerry-MQTT (aiomqtt, async)
11. Publiziere initialen state/statistic/state_station für alle Geräte
12. Starte Tasks:
    - task_dreame_to_lbmqtt
    - task_lbmqtt_to_dreame
    - task_token_refresh
    - task_statistic_poll
13. await shutdown_event
```
