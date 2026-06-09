#!/usr/bin/perl

use strict;
use warnings;
use LoxBerry::System;
use LoxBerry::Web;
use LoxBerry::Log;
use CGI;
use JSON;
use File::Slurp;
use Digest::MD5 qw(md5_hex);

my $cgi     = CGI->new();
my $action  = $cgi->param('action') || '';
my $plugindir = "$ENV{LBHOMEDIR}/config/plugins/dreame";
my $cfgfile   = "$plugindir/pluginconfig.json";
my $pidfile   = "/dev/shm/dreame_gateway.pid";
my $logfile   = "$ENV{LBHOMEDIR}/log/plugins/dreame/dreame_gateway.log";
my $daemon    = "$ENV{LBHOMEDIR}/bin/plugins/dreame/../../daemon/plugins/dreame/daemon.sh";

# ── Konfiguration laden ───────────────────────────────────────────────────────
sub load_cfg {
    my $cfg = {};
    if (-f $cfgfile) {
        eval { $cfg = decode_json(read_file($cfgfile, { binmode => ':utf8' })); };
    }
    $cfg->{cloud_service}        //= 'dreame';
    $cfg->{username}             //= '';
    $cfg->{base_topic}           //= 'dreame';
    $cfg->{polling_interval_min} //= 30;
    $cfg->{devices}              //= [];
    return $cfg;
}

sub save_cfg {
    my ($cfg) = @_;
    mkdir $plugindir unless -d $plugindir;
    write_file($cfgfile, { binmode => ':utf8' }, encode_json($cfg));
}

# ── Gateway-Status ────────────────────────────────────────────────────────────
sub gateway_status {
    if (-f $pidfile) {
        my $pid = read_file($pidfile);
        chomp $pid;
        return kill(0, $pid) ? "running:$pid" : "dead";
    }
    return "stopped";
}

# ── Actions ───────────────────────────────────────────────────────────────────
my $cfg = load_cfg();
my $message = '';
my $msgtype = '';

if ($action eq 'save_config') {
    my $svc  = $cgi->param('cloud_service') || 'dreame';
    my $user = $cgi->param('username')      || '';
    my $pass = $cgi->param('password')      || '';
    my $topic= $cgi->param('base_topic')    || 'dreame';
    my $intv = int($cgi->param('polling_interval_min') || 30);

    $cfg->{cloud_service}        = $svc;
    $cfg->{username}             = $user;
    $cfg->{base_topic}           = $topic;
    $cfg->{polling_interval_min} = $intv;

    if ($pass ne '') {
        # Passwort plain speichern (wird beim ersten Gateway-Start gehasht)
        $cfg->{_password_plain} = $pass;
        $cfg->{access_token}    = '';
        $cfg->{refresh_token}   = '';
        $cfg->{expires_at}      = 0;
    }
    save_cfg($cfg);
    $message = 'Konfiguration gespeichert.';
    $msgtype = 'success';

} elsif ($action eq 'start_gateway') {
    system("$daemon start &");
    sleep(1);
    $message = 'Gateway gestartet.';
    $msgtype = 'success';

} elsif ($action eq 'stop_gateway') {
    system("$daemon stop");
    $message = 'Gateway gestoppt.';
    $msgtype = 'success';
}

# ── HTML-Ausgabe ──────────────────────────────────────────────────────────────
my $status = gateway_status();
my $status_label = ($status =~ /^running/) ? '<span style="color:green">&#x25CF; Läuft</span>'
                                           : '<span style="color:red">&#x25CF; Gestoppt</span>';
my $pid_info = ($status =~ /^running:(\d+)/) ? " (PID $1)" : '';

LoxBerry::Web::lbheader("Dreame Gateway", "dreame", "");
print $cgi->header(-charset => 'utf-8');

print <<HTML;
<div class="container">
  <h3>Dreame Gateway</h3>
HTML

if ($message) {
    my $cls = $msgtype eq 'success' ? 'success' : 'error';
    print "<div class='ui-state-$cls' style='padding:8px;margin:8px 0'>$message</div>\n";
}

# ── Tab: Konfiguration ──────────────────────────────────────────────────────
print <<HTML;
<div id="tabs">
  <ul>
    <li><a href="#tab-config">Konfiguration</a></li>
    <li><a href="#tab-devices">Geräte</a></li>
    <li><a href="#tab-gateway">Gateway</a></li>
    <li><a href="#tab-log">Log</a></li>
  </ul>

  <div id="tab-config">
    <form method="post">
    <input type="hidden" name="action" value="save_config">
    <table class="loxberry">
      <tr>
        <th>Cloud-Dienst</th>
        <td>
          <select name="cloud_service">
            <option value="dreame" @{[$cfg->{cloud_service} eq 'dreame' ? 'selected' : '']}>Dreame</option>
            <option value="mova"   @{[$cfg->{cloud_service} eq 'mova'   ? 'selected' : '']}>MOVA</option>
          </select>
        </td>
      </tr>
      <tr>
        <th>E-Mail</th>
        <td><input type="email" name="username" value="@{[$cfg->{username}]}" style="width:300px"></td>
      </tr>
      <tr>
        <th>Passwort</th>
        <td><input type="password" name="password" placeholder="Leer lassen wenn bereits eingeloggt" style="width:300px"></td>
      </tr>
      <tr>
        <th>MQTT Base-Topic</th>
        <td><input type="text" name="base_topic" value="@{[$cfg->{base_topic}]}" style="width:200px"></td>
      </tr>
      <tr>
        <th>Statistik-Intervall (Minuten)</th>
        <td><input type="number" name="polling_interval_min" value="@{[$cfg->{polling_interval_min}]}" min="5" max="1440"></td>
      </tr>
      <tr>
        <td></td>
        <td><input type="submit" value="Speichern" class="ui-button ui-widget"></td>
      </tr>
    </table>
    </form>
  </div>

  <div id="tab-devices">
    <table class="loxberry">
      <tr><th>Name</th><th>Modell</th><th>Typ</th><th>Status</th></tr>
HTML

for my $dev (@{$cfg->{devices}}) {
    my $online = $dev->{online} ? '<span style="color:green">Online</span>' : '<span style="color:grey">Offline</span>';
    my $type   = $dev->{device_type} eq 'mower' ? 'Mähroboter' : 'Saugroboter';
    print "      <tr><td>$dev->{name}</td><td>$dev->{model}</td><td>$type</td><td>$online</td></tr>\n";
}

if (!@{$cfg->{devices}}) {
    print "      <tr><td colspan='4'>Keine Geräte — Gateway starten um Geräteliste zu laden.</td></tr>\n";
}

print <<HTML;
    </table>
  </div>

  <div id="tab-gateway">
    <p>Status: $status_label$pid_info</p>
    <form method="post" style="display:inline">
      <input type="hidden" name="action" value="start_gateway">
      <input type="submit" value="Gateway starten" class="ui-button">
    </form>
    &nbsp;
    <form method="post" style="display:inline">
      <input type="hidden" name="action" value="stop_gateway">
      <input type="submit" value="Gateway stoppen" class="ui-button">
    </form>
    <p><small>Token-Status: @{[$cfg->{access_token} ? 'Vorhanden' : 'Nicht eingeloggt']}</small></p>
  </div>

  <div id="tab-log">
    <pre style="background:#111;color:#eee;padding:10px;height:400px;overflow:auto;font-size:11px">
HTML

if (-f $logfile) {
    my @lines = read_file($logfile, { binmode => ':utf8' });
    my @last = @lines > 200 ? @lines[-200..-1] : @lines;
    for my $line (@last) {
        $line =~ s/</&lt;/g;
        $line =~ s/>/&gt;/g;
        print $line;
    }
} else {
    print "Kein Log vorhanden.\n";
}

print <<HTML;
    </pre>
  </div>
</div>
</div>
HTML

LoxBerry::Web::lbfooter();
