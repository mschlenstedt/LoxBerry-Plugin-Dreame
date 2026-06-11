#!/usr/bin/perl

use CGI;
use LoxBerry::System;
use LoxBerry::Log;
use LoxBerry::JSON;
use LoxBerry::IO;
use File::Basename;
use JSON;
use POSIX qw(setsid);
use warnings;
use strict;

my $cgi    = CGI->new;
my $action = $cgi->param('action') // '';
my $folder = basename($lbpplugindir);

my $pidfile      = '/dev/shm/dreame_gateway.pid';
my $stopped_flag = "$lbpconfigdir/gateway_stopped";
my $gateway      = "$lbhomedir/bin/plugins/$folder/dreame_gateway.py";
my $lbsconf      = "$lbhomedir/config/system";

print $cgi->header(-type => 'application/json', -charset => 'utf-8');

my $jsonobj = LoxBerry::JSON->new();
my $cfg = $jsonobj->open(filename => "$lbpconfigdir/pluginconfig.json", readonly => 1) // {};

sub read_pid {
    return undef unless -f $pidfile;
    open(my $fh, '<', $pidfile) or return undef;
    my $pid = <$fh>; close $fh; chomp $pid if $pid;
    return ($pid && $pid =~ /^\d+$/) ? $pid : undef;
}

sub pid_running {
    my ($pid) = @_;
    return 0 unless defined $pid;
    return kill(0, $pid) ? 1 : 0;
}

if ($action eq 'getpid') {
    my $pid = read_pid();
    print encode_json({ pid => (defined $pid && pid_running($pid) ? int($pid) : undef) });

} elsif ($action eq 'restart') {
    # Stop existing instance if running
    my $pid = read_pid();
    if (defined $pid && pid_running($pid)) {
        kill('TERM', $pid);
        for (1..10) { sleep 1; last unless pid_running($pid); }
        kill('KILL', $pid) if pid_running($pid);
    }
    unlink $pidfile if -f $pidfile;

    # Clear the manual-stop flag so the gateway may run (and start at next boot)
    unlink $stopped_flag if -f $stopped_flag;

    # Register log entry in LoxBerry log database so loglist_html() finds it
    my ($logfile, $logdbkey);
    eval {
        my $log = LoxBerry::Log->new(
            name    => 'dreame_gateway',
            package => $lbpplugindir,
            addtime => 1,
        );
        $log->LOGSTART("Dreame Gateway started");
        $logfile  = $log->{filename};
        $logdbkey = $log->{dbkey} // 0;
    };
    $logfile  //= "$lbplogdir/dreame_gateway.log";
    $logdbkey //= 0;

    unless (-f $gateway) {
        print encode_json({ ok => 0, error => "Gateway not found: $gateway" });
        return;
    }

    # Double-fork to detach gateway from CGI process
    my $child = fork();
    if (!defined $child) {
        print encode_json({ ok => 0, error => "fork failed: $!" });
        return;
    }
    if ($child == 0) {
        my $gc = fork();
        if (!defined $gc) { exit 1; }
        if ($gc == 0) {
            setsid();
            open(STDIN,  '<', '/dev/null');
            open(STDOUT, '>>', $logfile) or open(STDOUT, '>', '/dev/null');
            open(STDERR, '>>', $logfile) or open(STDERR, '>', '/dev/null');
            exec('python3', $gateway,
                '--logfile',   $logfile,
                '--logdbkey',  $logdbkey,
                '--configdir', $lbpconfigdir,
                '--lbsconfig', $lbsconf,
            ) or exit 1;
        }
        exit 0;
    }
    waitpid($child, 0);

    my $new_pid;
    for (1..10) {
        select(undef, undef, undef, 0.5);
        $new_pid = read_pid();
        last if defined $new_pid && pid_running($new_pid);
        $new_pid = undef;
    }

    if (defined $new_pid) {
        print encode_json({ ok => 1, pid => $new_pid+0 });
    } else {
        print encode_json({ ok => 0, error => 'Gateway did not start' });
    }

} elsif ($action eq 'stop') {
    my $pid = read_pid();
    unless (defined $pid && pid_running($pid)) {
        # Persist the stop even if nothing was running
        { open my $fh, '>', $stopped_flag }
        print encode_json({ ok => 1, msg => 'Not running' });
        return;
    }
    kill('TERM', $pid);
    for (1..10) { sleep 1; last unless pid_running($pid); }
    kill('KILL', $pid) if pid_running($pid);
    unlink $pidfile if -f $pidfile;
    # Persist the stop so the gateway stays down across reboots
    { open my $fh, '>', $stopped_flag }
    print encode_json({ ok => 1, msg => 'Stopped' });

} elsif ($action eq 'gettokenstatus') {
    # access_token/expires_at are ephemeral (memory only in the gateway). The
    # gateway publishes its auth status retained to {base_topic}/gateway; read
    # that via mqtt_get. has_refresh still comes from the on-disk config.
    my $base_topic  = $cfg->{base_topic}    // 'dreame';
    my $has_refresh = ($cfg->{refresh_token} // '') ne '' ? 1 : 0;

    my $raw = LoxBerry::IO::mqtt_get("$base_topic/gateway");
    if (!defined $raw || $raw eq '') {
        print encode_json({ ok => 0, has_refresh => $has_refresh, expires_in => 0 });
        return;
    }
    my $data       = eval { decode_json($raw) } // {};
    my $now        = time();
    my $expires_at = $data->{expires_at} // 0;
    my $ok         = ($data->{authenticated} && $now < $expires_at) ? 1 : 0;
    my $expires_in = $ok ? int($expires_at - $now) : 0;
    print encode_json({
        ok          => $ok,
        has_refresh => $has_refresh,
        expires_in  => $expires_in,
    });

} else {
    print encode_json({ error => 'unknown action' });
}
