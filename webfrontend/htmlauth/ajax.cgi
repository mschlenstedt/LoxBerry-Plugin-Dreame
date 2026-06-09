#!/usr/bin/perl

use CGI;
use LoxBerry::System;
use LoxBerry::JSON;
use File::Basename;
use JSON;
use warnings;
use strict;

my $cgi    = CGI->new;
my $action = $cgi->param('action') // '';
my $folder = basename($lbpplugindir);

my $pidfile = '/dev/shm/dreame_gateway.pid';
my $daemon  = "$lbhomedir/daemon/plugins/$folder/daemon.sh";

print $cgi->header(-type => 'application/json', -charset => 'utf-8');

my $jsonobj = LoxBerry::JSON->new();
my $cfg = $jsonobj->open(filename => "$lbpconfigdir/pluginconfig.json", readonly => 1) // {};

if ($action eq 'getpid') {
    my $pid;
    if (-f $pidfile) {
        if (open my $fh, '<', $pidfile) {
            $pid = <$fh>; close $fh; chomp $pid if $pid;
            $pid = undef unless $pid && kill(0, $pid);
        }
    }
    print encode_json({ pid => ($pid ? int($pid) : undef) });

} elsif ($action eq 'start') {
    system("bash $daemon start &");
    print encode_json({ ok => 1 });

} elsif ($action eq 'stop') {
    system("bash $daemon stop");
    print encode_json({ ok => 1 });

} elsif ($action eq 'gettokenstatus') {
    my $token      = $cfg->{access_token}  // '';
    my $refresh    = $cfg->{refresh_token} // '';
    my $expires_at = $cfg->{expires_at}    // 0;
    my $now        = time();
    my $ok         = ($token && $now < $expires_at) ? 1 : 0;
    my $expires_in = $ok ? int($expires_at - $now) : 0;
    print encode_json({
        ok          => $ok,
        has_refresh => ($refresh ? 1 : 0),
        expires_in  => $expires_in,
    });

} else {
    print encode_json({ error => 'unknown action' });
}
