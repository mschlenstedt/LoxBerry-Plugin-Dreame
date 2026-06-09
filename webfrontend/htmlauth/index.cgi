#!/usr/bin/perl

use CGI;
use LoxBerry::System;
use LoxBerry::Web;
use LoxBerry::JSON;
use LoxBerry::Log;
use File::Basename;
use warnings;
use strict;

my $cgi     = CGI->new;
my $q       = $cgi->Vars;
my $version = LoxBerry::System::pluginversion();
my $folder  = basename($lbpplugindir);

# Paths
my $pidfile = "/dev/shm/dreame_gateway.pid";
my $logfile = "$lbplogdir/dreame_gateway.log";
my $daemon  = "$lbhomedir/daemon/plugins/$folder/daemon.sh";

# Load config
my $jsonobj = LoxBerry::JSON->new();
my $cfg = $jsonobj->open(filename => "$lbpconfigdir/pluginconfig.json");
$cfg //= {};
$cfg->{cloud_service}        //= 'dreame';
$cfg->{username}             //= '';
$cfg->{base_topic}           //= 'dreame';
$cfg->{polling_interval_min} //= 30;
$cfg->{devices}              //= [];
$cfg->{access_token}         //= '';

# Active tab
my $form = $q->{form} // 'config';
$form = 'config' unless $form =~ /^(config|devices|gateway|log)$/;

# Navbar (rendered by lbheader via %navbar)
our %navbar;
$navbar{10}{Name}   = "Konfiguration";
$navbar{10}{URL}    = 'index.cgi?form=config';
$navbar{10}{active} = 1 if $form eq 'config';
$navbar{20}{Name}   = "Ger&auml;te";
$navbar{20}{URL}    = 'index.cgi?form=devices';
$navbar{20}{active} = 1 if $form eq 'devices';
$navbar{30}{Name}   = "Gateway";
$navbar{30}{URL}    = 'index.cgi?form=gateway';
$navbar{30}{active} = 1 if $form eq 'gateway';
$navbar{40}{Name}   = "Log";
$navbar{40}{URL}    = 'index.cgi?form=log';
$navbar{40}{active} = 1 if $form eq 'log';

# Actions
my $message = '';
my $msgtype = '';
my $action  = $q->{action} // '';

if ($action eq 'save_config') {
    $cfg->{cloud_service}        = $q->{cloud_service}        || 'dreame';
    $cfg->{username}             = $q->{username}             || '';
    $cfg->{base_topic}           = $q->{base_topic}           || 'dreame';
    $cfg->{polling_interval_min} = int($q->{polling_interval_min} || 30);
    if ($q->{password}) {
        $cfg->{_password_plain} = $q->{password};
        $cfg->{access_token}    = '';
        $cfg->{refresh_token}   = '';
        $cfg->{expires_at}      = 0;
    }
    $jsonobj->write();
    $message = 'Konfiguration gespeichert.';
    $msgtype = 'success';

} elsif ($action eq 'start_gateway') {
    system("bash $daemon start &");
    sleep(1);
    $message = 'Gateway gestartet.';
    $msgtype = 'success';

} elsif ($action eq 'stop_gateway') {
    system("bash $daemon stop");
    $message = 'Gateway gestoppt.';
    $msgtype = 'success';
}

# Gateway status
my $gw_status = 'stopped';
if (-f $pidfile) {
    my $pid;
    if (open my $fh, '<', $pidfile) { $pid = <$fh>; close $fh; chomp $pid if $pid; }
    $gw_status = ($pid && kill(0, $pid)) ? "running:$pid" : 'dead';
}
my $status_html = ($gw_status =~ /^running/)
    ? '<span style="color:green">&#x25CF; L&auml;uft</span>'
    : '<span style="color:red">&#x25CF; Gestoppt</span>';
my ($pid_label) = ($gw_status =~ /^running:(\d+)/);
$pid_label = $pid_label ? " (PID $pid_label)" : '';
my $token_label      = $cfg->{access_token} ? 'Vorhanden' : 'Nicht eingeloggt';
my $cloud_dreame_sel = $cfg->{cloud_service} eq 'dreame' ? ' selected' : '';
my $cloud_mova_sel   = $cfg->{cloud_service} eq 'mova'   ? ' selected' : '';
my $username_esc     = CGI::escapeHTML($cfg->{username});
my $topic_esc        = CGI::escapeHTML($cfg->{base_topic});
my $interval_esc     = int($cfg->{polling_interval_min});

# ── Render ────────────────────────────────────────────────────────────────────
LoxBerry::Web::lbheader("Dreame Gateway V$version", "dreame", "");

if ($message) {
    my $cls = $msgtype eq 'success' ? 'success' : 'error';
    print "<div class='loxberry $cls' style='padding:8px;margin:8px 0'>$message</div>\n";
}

# ── Tab: Konfiguration ────────────────────────────────────────────────────────
if ($form eq 'config') {
    print <<HTML;
<form method="post">
<input type="hidden" name="form"   value="config">
<input type="hidden" name="action" value="save_config">
<table class="loxberry">
  <tr>
    <th>Cloud-Dienst</th>
    <td>
      <select name="cloud_service">
        <option value="dreame"$cloud_dreame_sel>Dreame</option>
        <option value="mova"$cloud_mova_sel>MOVA</option>
      </select>
    </td>
  </tr>
  <tr>
    <th>E-Mail / Benutzername</th>
    <td><input type="email" name="username" value="$username_esc" style="width:300px"></td>
  </tr>
  <tr>
    <th>Passwort</th>
    <td><input type="password" name="password" placeholder="Leer lassen wenn bereits eingeloggt" style="width:300px"></td>
  </tr>
  <tr>
    <th>MQTT Base-Topic</th>
    <td><input type="text" name="base_topic" value="$topic_esc" style="width:200px"></td>
  </tr>
  <tr>
    <th>Statistik-Intervall (Minuten)</th>
    <td><input type="number" name="polling_interval_min" value="$interval_esc" min="5" max="1440"></td>
  </tr>
  <tr>
    <td></td>
    <td><input type="submit" value="Speichern" class="ui-button ui-widget"></td>
  </tr>
</table>
</form>
HTML

# ── Tab: Geräte ───────────────────────────────────────────────────────────────
} elsif ($form eq 'devices') {
    print "<table class=\"loxberry\">\n";
    print "  <tr><th>Name</th><th>Modell</th><th>Typ</th><th>Status</th></tr>\n";
    my @devices = (ref $cfg->{devices} eq 'ARRAY') ? @{$cfg->{devices}} : ();
    if (@devices) {
        for my $dev (@devices) {
            my $online = $dev->{online}
                ? '<span style="color:green">Online</span>'
                : '<span style="color:grey">Offline</span>';
            my $type  = ($dev->{device_type} // '') eq 'mower' ? 'M&auml;hroboter' : 'Saugroboter';
            my $name  = CGI::escapeHTML($dev->{name}  // '');
            my $model = CGI::escapeHTML($dev->{model} // '');
            print "  <tr><td>$name</td><td>$model</td><td>$type</td><td>$online</td></tr>\n";
        }
    } else {
        print "  <tr><td colspan='4'>Keine Ger&auml;te &mdash; Gateway starten um Ger&auml;teliste zu laden.</td></tr>\n";
    }
    print "</table>\n";

# ── Tab: Gateway ──────────────────────────────────────────────────────────────
} elsif ($form eq 'gateway') {
    print <<HTML;
<table class="loxberry">
  <tr><th>Gateway-Status</th><td>$status_html$pid_label</td></tr>
  <tr><th>Token</th><td>$token_label</td></tr>
</table>
<br>
<form method="post" style="display:inline">
  <input type="hidden" name="form"   value="gateway">
  <input type="hidden" name="action" value="start_gateway">
  <input type="submit" value="Gateway starten" class="ui-button">
</form>
&nbsp;
<form method="post" style="display:inline">
  <input type="hidden" name="form"   value="gateway">
  <input type="hidden" name="action" value="stop_gateway">
  <input type="submit" value="Gateway stoppen" class="ui-button">
</form>
HTML

# ── Tab: Log ──────────────────────────────────────────────────────────────────
} elsif ($form eq 'log') {
    print "<pre style='background:#111;color:#eee;padding:10px;height:500px;overflow:auto;font-size:11px'>\n";
    if (-f $logfile) {
        if (open my $fh, '<:utf8', $logfile) {
            my @lines = <$fh>;
            close $fh;
            my @last = @lines > 200 ? @lines[-200..-1] : @lines;
            for my $line (@last) {
                $line =~ s/&/&amp;/g;
                $line =~ s/</&lt;/g;
                $line =~ s/>/&gt;/g;
                print $line;
            }
        } else {
            print "Logdatei nicht lesbar.\n";
        }
    } else {
        print "Kein Log vorhanden.\n";
    }
    print "</pre>\n";
}

LoxBerry::Web::lbfooter();
exit;
