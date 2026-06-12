#!/usr/bin/perl

use CGI;
use LoxBerry::System;
use LoxBerry::Web;
use LoxBerry::JSON;
use LoxBerry::Log;
use File::Basename;
use HTML::Template;
use warnings;
use strict;

my $cgi     = CGI->new;
my $q       = $cgi->Vars;
my $version = LoxBerry::System::pluginversion();
my $folder  = basename($lbpplugindir);

# Load config
my $jsonobj = LoxBerry::JSON->new();
my $cfg = $jsonobj->open(filename => "$lbpconfigdir/pluginconfig.json");
$cfg //= {};
$cfg->{cloud_service}              //= 'dreame';
$cfg->{username}                   //= '';
$cfg->{password_plain}             //= '';
$cfg->{base_topic}                 //= 'dreame';
$cfg->{statistic_poll_interval_sec} //= 300;
$cfg->{state_poll_interval_sec}    //= 60;
$cfg->{devices}                    //= [];

# Active tab (default: gateway)
my $form = $q->{form} // 'gateway';
$form = 'gateway' unless $form =~ /^(gateway|config|log)$/;

# Actions (form POST for config save only)
my $save_ok  = 0;
my $save_msg = '';
my $action   = $q->{action} // '';

if ($action eq 'save_config') {
    $cfg->{cloud_service}               = $q->{cloud_service}        || 'dreame';
    $cfg->{username}                    = $q->{username}             || '';
    $cfg->{base_topic}                  = $q->{base_topic}           || 'dreame';
    $cfg->{statistic_poll_interval_sec} = int($q->{statistic_poll_interval_sec} || 300);
    $cfg->{state_poll_interval_sec}     = int($q->{state_poll_interval_sec} || 60);
    # The password field is pre-filled, so it is sent on every save. Only act
    # when it actually changed: store the new plain password and drop the
    # persisted refresh_token so the next start does a fresh login.
    if (defined $q->{password} && $q->{password} ne ($cfg->{password_plain} // '')) {
        $cfg->{password_plain} = $q->{password};
        $cfg->{refresh_token}  = '';
    }
    $jsonobj->write();
    $save_ok  = 1;
    $save_msg = 'Konfiguration gespeichert.';
}

# Navbar (same pattern as Navimow)
our %navbar;
$navbar{10}{Name}   = "Gateway";
$navbar{10}{URL}    = 'index.cgi?form=gateway';
$navbar{10}{active} = 1 if $form eq 'gateway';
$navbar{20}{Name}   = "Konfiguration";
$navbar{20}{URL}    = 'index.cgi?form=config';
$navbar{20}{active} = 1 if $form eq 'config';
$navbar{30}{Name}   = "Log";
$navbar{30}{URL}    = 'index.cgi?form=log';
$navbar{30}{active} = 1 if $form eq 'log';

# Load template
my $templatefile = $form eq 'config' ? "$lbptemplatedir/config_tab.html"
                 : $form eq 'log'    ? "$lbptemplatedir/log_tab.html"
                 :                     "$lbptemplatedir/gateway_tab.html";

my $template_str = LoxBerry::System::read_file($templatefile) // '';
$template_str   .= LoxBerry::System::read_file("$lbptemplatedir/javascript.js") // '';

my $tmpl = HTML::Template->new_scalar_ref(
    \$template_str,
    global_vars       => 1,
    loop_context_vars => 1,
    die_on_bad_params => 0,
);

# Template parameters per tab
if ($form eq 'gateway') {
    my @devices = ();
    if (ref $cfg->{devices} eq 'ARRAY') {
        @devices = map { {
            DEVICE_NAME  => $_->{name}         // '',
            DEVICE_MODEL => $_->{model}        // '',
            DEVICE_TYPE  => (($_->{device_type}//'') eq 'mower') ? 'Mähroboter' : 'Saugroboter',
        } } @{ $cfg->{devices} };
    }
    $tmpl->param(HAS_DEVICES => scalar(@devices) ? 1 : 0);
    $tmpl->param(DEVICES     => \@devices);

} elsif ($form eq 'config') {
    $tmpl->param(CLOUD_SERVICE        => $cfg->{cloud_service});
    $tmpl->param(IS_DREAME            => $cfg->{cloud_service} eq 'dreame' ? 1 : 0);
    $tmpl->param(IS_MOVA              => $cfg->{cloud_service} eq 'mova'   ? 1 : 0);
    $tmpl->param(USERNAME                => CGI::escapeHTML($cfg->{username}));
    $tmpl->param(PASSWORD                => CGI::escapeHTML($cfg->{password_plain} // ''));
    $tmpl->param(BASE_TOPIC              => CGI::escapeHTML($cfg->{base_topic}));
    $tmpl->param(STATISTIC_POLL_INTERVAL_SEC => int($cfg->{statistic_poll_interval_sec}));
    $tmpl->param(STATE_POLL_INTERVAL_SEC => int($cfg->{state_poll_interval_sec}));
    $tmpl->param(SAVE_OK                 => $save_ok);
    $tmpl->param(SAVE_MSG             => $save_msg);

} elsif ($form eq 'log') {
    $tmpl->param(LOGLIST => LoxBerry::Web::loglist_html());
}

LoxBerry::Web::lbheader("Dreame Gateway V$version", "https://wiki.loxberry.de/plugins/dreame/start", "", "nojqm");
print $tmpl->output();
LoxBerry::Web::lbfooter();
exit;
