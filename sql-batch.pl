#!/usr/bin/perl
use strict;
use warnings FATAL => 'all';

# Version 0.9.1 (9. Feb 2014)

#######################################################
# These are some variables you probably want to set up.

# SQL server where domains table is found.
my %dbConfig = (host        => "mysql.your.host",
                port        => "3306",
                username    => "awstats",
                password    => "a_secure_password",
                schema      => "hosting",
                table       => "domains",
                type        => "mysql",
                sslKey      => "/etc/ssl/private/snakeoil.key",
                sslCert     => "/etc/ssl/certs/snakeoil.crt",
                sslCaPath   => "/etc/ssl/certs/",
                logSchema   => "apachelogs",    # If using mod_log_sql
                logTable    => "access_log");   # If using mod_log_sql

# Path configuration.
my %paths = (awstatsPl      => "/home/www/public/awstats/awstats.pl",
             staticPages    => "/usr/local/bin/awstats_buildstaticpages.pl",
             logResolve     => "/usr/local/bin/logresolvemerge.pl",
             awstatsConfigs => "/etc/awstats",
             temp           => "/tmp",
             apacheLogFiles => "/var/log/apache2");

my %config = (rotateDays    => 7,
              symlink       => 0,
              exitOnError   => 0,
              rmConfig      => 0,
              buildStatic   => 0);

# No further editing necessary.
#######################################################

setpriority 0, 0, 20;

use Getopt::Long;
use Date::Manip;
use DBI;
use feature 'say';
use POSIX qw/ strftime /;
use AwstatsSQL;

my %arguments;
my $i = 0;

&GetOptions("verbose=i" => \$arguments{verbose},
            "start=s"   => \$arguments{start},
            "end=s"     => \$arguments{end},
            "help"      => \$arguments{help},
            "static"    => \$arguments{static},
            "sqllog"    => \$arguments{sqllog},
            "logfile=s" => \$arguments{logfile},
            "domains=s" => \$arguments{domains});

# User wants help!
if ($arguments{help}) {
    print "AwStats/Apache log parser for mass virtual hosting.
Version 0.9.1 (9. Feb 2014)
Usage: $0 options\n
Command line options:
--help\t\tThis text.
--logfile\tParse a single log file.
--verbose level\tShow messages about what the script is doing (level 1) and AwStats output (level 2).
--start date\tParse old log files starting from defined date to present date or --end.
--end date\tParse old log files ending with defined date (implies --start).
--sqllog\tPull logs from an SQL database (Apache module mod_log_sql).
--domains\tParse only a certain domain names (separated by commas).
--static\tBuild static AwStats pages as opposed to dynamic.\n\n";
exit;
}

if ($arguments{logfile} && $arguments{sqllog}) {
    die("Error: arguments 'logfile' and 'sqllog' are mutually exclusive.");
}

# Pass arguments as references so that "shift" builtin works as intended.
my $sqlBatch = AwstatsSQL->new(\%dbConfig, \%paths, \%config, \%arguments);

if ($arguments{static} && $config{rmConfig}) {
    $sqlBatch->showDatetime("Warning: argument 'static' and option 'rmConfig' are mutually exclusive.");
    $sqlBatch->showDatetime("Disregarding option 'rmConfig' and will not remove config files.");
}

# Check that all required programs are available.
if ($arguments{static}) {
    $sqlBatch->checkProg($paths{staticPages});
}

$sqlBatch->checkProg($paths{awstatsPl});
$sqlBatch->checkProg($paths{logResolve});

# Symlink to current log file.
if ($config{symlink}) {
    $sqlBatch->doSymlink();
}

# Check if we need to get logs from database of from flat files.
if ($arguments{sqllog}) {

    $sqlBatch->parseSqlLogTable($arguments{sqllog});

} elsif ($arguments{logfile}) {

    $sqlBatch->parseSingleLogFile($arguments{logfile});
    $i++;

} else {

    # By default parse today's logs.
    my $daysToProcess = 0;

    # Generate the name of the log file which is to be processed,
    # if --start command line argument was defined.
    if ($arguments{start}) {
        # Get number of days between specified date and today.
        my $d1 = new Date::Manip::Date;
        my $d2 = new Date::Manip::Date;

        $d1->parse($arguments{start});
        $d2->parse("today");
        my $delta = $d1->calc($d2, 'exact');
        $daysToProcess = $delta->printf("%dyd");
    }

    while ($daysToProcess >= 0) {
        LINE: foreach my $time (qw/00 06 12 18/) {

            my $logfile;
            # Generate Apache log file name.
            if ($arguments{start}) {
                $logfile = &UnixDate((DateCalc("now", "-$daysToProcess days")), "$paths{apacheLogFiles}/%d-%m-%Y/$time-access.log");
            } else {
                $logfile = &UnixDate((DateCalc("now", "-6hours")), "$paths{apacheLogFiles}/%d-%m-%Y/%H-access.log");
            }

            $sqlBatch->parseSingleLogFile($logfile);
            $i++;

            # Break out of foreach if this is a normal update run.
            last LINE unless ($arguments{start});
        } # End foreach.

        $daysToProcess--;
    } # while $daysToProcess>0
}

# Delete all temporary Apache logs.
$sqlBatch->cleanTemp();

$sqlBatch->showDatetime("-------------------------------------------------------------------\nFinished all Awstats runs.\n", 1);
$sqlBatch->showDatetime("Updated a total of $i files.", 1);

#$sqlBatch->cleanOldApacheLogs();