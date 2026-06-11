# LoxBerry Plugin Dreame — Development Notes

## Logging-Architektur (Python-Gateway + LoxBerry)

### Prinzip

LoxBerry verwaltet Log-Sessions in einer SQLite-Datenbank (`/opt/loxberry/log/system_tmpfs/logs_sqlite.dat`). Damit Logs im Webinterface (`loglist_html()`) erscheinen, muss die Session **in Perl** registriert werden — Python kann das nicht direkt.

### Ablauf

1. **Perl (ajax.cgi) erzeugt die Log-Session:**
   ```perl
   my $log = LoxBerry::Log->new(
       name    => 'dreame_gateway',
       package => $lbpplugindir,   # z.B. 'dreame'
       addtime => 1,
   );
   $log->LOGSTART("Dreame Gateway started");
   my $logfile  = $log->{filename};
   my $logdbkey = $log->{dbkey} // 0;
   ```

2. **Perl übergibt `--logfile` und `--logdbkey` an Python:**
   ```perl
   exec('python3', $gateway,
       '--logfile',   $logfile,
       '--logdbkey',  $logdbkey,
       '--configdir', $lbpconfigdir,
       '--lbsconfig', $lbsconf,
   );
   ```

3. **Python schreibt direkt in die Logdatei** (kein Perl-Aufruf für normales Logging):
   ```python
   _handler = logging.FileHandler(_logfile, mode="a", encoding="utf-8")
   ```

4. **Python ruft LOGEND beim Beenden auf** (via `os.system` + Perl):
   ```python
   os.system(
       f'perl -e \'use LoxBerry::Log; '
       f'my $l = LoxBerry::Log->new(dbkey => "{dbkey}", append => 1); '
       f'LOGEND "Gateway stopped."; exit;\''
   )
   ```

### Wichtige Details

- `LoxBerry::Log->new()` **ohne** `logdir` erkennt den Pfad automatisch, wenn es aus einem CGI-Skript aufgerufen wird (`$lbplogdir` auto-detect via `$0`)
- `package` muss exakt `$lbpplugindir` sein (nur Ordnername, z.B. `dreame`), damit `loglist_html()` mit `WHERE PACKAGE = 'dreame'` findet
- Das `logfile` enthält Timestamp im Namen, z.B. `20260611_062432_594_dreame_gateway.log`
- `dbkey` ist ein eindeutiger String-Schlüssel in der SQLite-DB

### Fehler-Fallstricke

- **Python subprocess für LOGSTART**: Falsch! Nicht von Python aus Perl LOGSTART aufrufen — immer in Perl registrieren
- **`result.get("data", {})` bei null-Daten**: Die Cloud-API kann `{"data": null}` zurückgeben. `result.get("data", {})` gibt dann `None` zurück (weil Key existiert). Fix: `(result.get("data") or {}).get("result", [])`
- **daemon.sh** braucht `PLUGIN_FOLDER=$(basename "$0")` — die alte Methode via `plugindatabase.dat` funktioniert nicht (Datei ist leer)
- **Config beim Upgrade sichern** — siehe Abschnitt „Upgrade: Config/Log/Data erhalten". Ohne `pre-/postupgrade.sh` überschreibt ein Upgrade die `pluginconfig.json`.
- **Templates MÜSSEN im Top-Level `templates/` liegen** — NICHT unter `webfrontend/templates/`. LoxBerry kopiert beim Install nur das Top-Level-`templates/` nach `$lbptemplatedir` (`/opt/loxberry/templates/plugins/dreame/`). Liegen sie woanders, liefert `read_file("$lbptemplatedir/...")` in `index.cgi` `undef` → **alle WebUI-Tabs sind leer** (nur LoxBerry-Header/Footer rendern, kein Plugin-Inhalt). Symptom tritt erst beim sauberen Git-Install auf, nicht im Dev-Setup mit manuell platzierten Dateien.

## Gateway-Handling (Start / Restart / Stop)

Nach dem Vorbild des Navimow-Plugins umgesetzt. **Source of Truth** für „soll der Gateway laufen?" ist ein persistentes Flag-File `config/gateway_stopped`.

### Zustandsmaschine

| Aktion | Verhalten |
|---|---|
| **Boot** (`daemon/daemon.sh start`) | Wenn `gateway_stopped` existiert → **nicht starten** (`exit 0`). Sonst Gateway im Hintergrund starten. → Ein manueller Stop überlebt den Reboot. |
| **Restart** (`ajax.cgi?action=restart`) | Laufende Instanz killen (TERM → nach 10s KILL) → PID-File löschen → **`gateway_stopped` löschen** → Double-Fork + `exec python3` → bis ~5s auf neue PID pollen. |
| **Stop** (`ajax.cgi?action=stop`) | TERM → KILL → PID-File löschen → **`gateway_stopped` anlegen** (auch wenn nichts lief). |

### Komponenten

- **PID-File**: `/dev/shm/dreame_gateway.pid` — vom Gateway selbst geschrieben (`write_pid()`), SIGTERM-Handler räumt sauber auf, entfernt PID beim Exit. Keine Änderung am Gateway nötig.
- **`ajax.cgi`** Aktionen: `getpid`, `restart`, `stop`, `gettokenstatus`. Start des Prozesses via Double-Fork + `setsid` + `exec`, damit er vom CGI-Prozess losgelöst ist.
- **WebUI** (`gateway_tab.html` + `javascript.js`): Status-Div `gw_status_text` mit **3 Farben** + 2 Buttons (Neustart `pi-refresh` / Stoppen `pi-times`):
  - 🟢 `#6dac20` läuft (PID x) · 🔴 `#d0021b` nicht aktiv · ⚪ `#9e9e9e` wird neu gestartet (transient) · 🟠 `#f5a623` Fehler
  - Restart-Handler zeigt **sofort** das graue Banner, ruft `restart`, und wenn die neue PID noch nicht da ist → `_pollNewPid(oldPid)` pollt bis 15s. Poll-Intervall 5s.

### Fallstricke

- **Kein separater „Start"-Button**: Navimow-Modell kennt nur Restart (= Flag löschen + starten) und Stop (= Flag setzen + killen). Restart ist auch der Weg, einen gestoppten Gateway wieder hochzufahren.
- Wer den Boot-Start ändert, muss den Flag-Check im `start`-Case von `daemon.sh` beibehalten — sonst startet ein gestoppter Gateway beim Reboot wieder.

## Zeilenenden (LF) — `.gitattributes`

Das Plugin läuft auf LoxBerry (Linux). Alle Textdateien **müssen LF** behalten — CRLF bricht Shell-Scripts (`daemon.sh`), Perl-CGIs (`ajax.cgi`, `index.cgi`) und Python (Shebang-Zeile). Beim Editieren unter Windows wandelt Git sonst auf CRLF.

- `.gitattributes` erzwingt `* text=auto eol=lf` → im Repo **und** im Checkout immer LF, unabhängig vom Editier-OS.
- Die Warnung `LF will be replaced by CRLF the next time Git touches it` ist damit erledigt.
- Nach Änderungen an `.gitattributes` einmal `git add --renormalize .` ausführen, damit bereits getrackte Dateien angeglichen werden.

## Upgrade: Config/Log/Data erhalten (`preupgrade.sh` / `postupgrade.sh`)

Bei einem Plugin-Upgrade installiert LoxBerry den `config/`-Ordner neu und **überschreibt damit `pluginconfig.json`** (inkl. Tokens, gespeichertem Login, `gateway_stopped`-Flag). Lösung wie beim Navimow-Plugin: sichern vor dem Upgrade, zurückspielen danach.

### Ablauf

LoxBerry ruft beide Skripte mit positionalen Argumenten auf: `$1` = Temp-Ordnername, `$3` = Plugin-Ordner (`dreame`), `$5` = LoxBerry-Basis (`/opt/loxberry`).

- **`preupgrade.sh`** (vor dem Upgrade): stoppt das laufende Gateway, legt `/tmp/<temp>_upgrade/{config,log,data}` an und kopiert `$5/{config,log,data}/plugins/dreame/` dorthin.
- **`postupgrade.sh`** (nach dem Upgrade): kopiert die gesicherten Dateien aus `/tmp/<temp>_upgrade/.../dreame/*` zurück nach `$5/{config,log,data}/plugins/dreame/` und löscht den Temp-Ordner.

### Wichtige Details

- **Kein pip-Install in `postupgrade.sh`** — die Python-Deps werden bei jedem Install *und* Upgrade von `postroot.sh` erledigt. Nicht duplizieren.
- Greift **nur beim Upgrade**, nicht beim Erst-Install — dort laufen `preroot.sh`/`postroot.sh`.
- Neue Config-Keys künftiger Versionen gehen nicht verloren: `index.cgi` setzt fehlende Keys per `//=` auf Defaults.
- Das `gateway_stopped`-Flag liegt in `config/` und wird mitgesichert → ein gestoppter Gateway bleibt auch über ein Upgrade hinweg gestoppt.
