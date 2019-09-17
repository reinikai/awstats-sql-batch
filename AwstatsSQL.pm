# The scope of the package definition extends to the end of the file,
# or until another package keyword is encountered.
use strict;
use warnings FATAL => 'all';
package AwstatsSQL;

# Constructor. This is a subroutine within a package that returns an object
# reference. The object reference is created by blessing a reference to the
# package's class. Most programmers choose to name this object constructor
# method "new", but in Perl one can use any name.
sub new
{
    # shift() is a built in Perl subroutine that takes an array as an argument,
    # then returns and deletes the first item in that array. It is common
    # practice to obtain all parameters passed into a subroutine with shift calls.
    my $class = shift;
    my $this  = { _dbConfig  => shift,
                  _paths     => shift,
                  _config    => shift,
                  _arguments => shift };

    # Perl provides a bless() function which is used to return a reference and
    # which becomes an object.
    bless $this, $class;
    return $this;
}


# Connect to database.
sub dbConnect
{
    my ($this) = @_;

    return DBI->connect("DBI:$this->{_dbConfig}{type}:database=$this->{_dbConfig}{schema};host=$this->{_dbConfig}{host};port=$this->{_dbConfig}{port};
                         mysql_ssl=1;
                         mysql_ssl_client_key=$this->{_dbConfig}{sslKey};
                         mysql_ssl_client_cert=$this->{_dbConfig}{sslCert};
                         mysql_ssl_ca_path=$this->{_dbConfig}{sslCaPath}",
        $this->{_dbConfig}{username}, $this->{_dbConfig}{password})
        || die("Error connecting to DB!");
}


# Get list of domains to parse from database.
sub getDomains
{
    my ($this, $domains) = @_;
    my $query = "SELECT DomainName,Aliases,Stats,Template,AwStatsMerge FROM $this->{_dbConfig}{table} WHERE Stats!='off'";

    if ($domains) {
        $query = $query . " AND (";

        # Split domains by comma.
        foreach my $domain (split(/,/, $domains)) {
            $query = $query . "DomainName='$domain' OR ";
        }

        # Trim last OR
        $query = substr($query, 0, -4);
        $query = $query . ")";
    }

    my $dbh = $this->dbConnect();
    $sth = $dbh->prepare($query);
    $sth->execute || die("Error preforming SQL query.\n");
    return $sth;
}


# Checks that specified program is executable.
sub checkProg
{
    my($this, $program) = @_;

    if (!-x $program) {
        $this->showDatetime("Error: $program is not executable by this user! Exiting.\n");
        exit;
    }
}

# Echos current date/time and a string - useful for info/error messages.
sub showDatetime {
    my ($this, $message, $logLevel) = @_;

    if (!$logLevel) {
        $logLevel = 0;
    }

    if ($this->{_arguments}{verbose} >= $logLevel) {
        my @now = localtime();
        printf ("\[%02d/%02d/%s %02d:%02d:%02d\] $message\n", @now[3], @now[4]+1, @now[5]+1900, @now[2], @now[1], @now[0]);
    }

    return;
}


# Symlinks active log file to /log/file/path/current.log.
sub doSymlink {
    my ($this) = @_;

    my @now = localtime();
    unlink("$this->{_paths}{apacheLogFiles}/current.log");
    symlink(sprintf("%s/%02d-%02d-%s/%02d-access.log %s/current.log", $this->{_paths}{apacheLogFiles}, @now[3], @now[4]+1, @now[5]+1900, @now[2], $this->{_paths}{apacheLogFiles}), $this->{_paths}{apacheLogFiles}."/current.log");

    if ($this->{_arguments}{verbose} eq "1" || $this->{_arguments}{verbose} eq "2") {
        $this->showDatetime(printf("Symlinked %s/current.log to %s/%02d-%02d-%s/%02d-access.log\n", $this->{_paths}{apacheLogFiles}, $this->{_paths}{apacheLogFiles}, @now[3], @now[4]+1, @now[5]+1900, @now[2]));
    }
}


sub parseSingleLogFile
{
    my ($this, $logFile) = @_;

    $this->cleanTemp();

    $this->showDatetime("Looking for log file $logFile");
    $this->splitLogByVhost($logFile);

    $this->showDatetime("Split Apache log file $logFile by virtualhost.", 1);
    $this->showDatetime("Begin AwStats update run.\n-------------------------------------------------------------------", 1);

    # Get domain names to parse from database.
    my $query = $this->getDomains($this->{_arguments}{domains});

    while (my @row = $query->fetchrow_array) {

        # Genetate/update template for this domainname.
        $this->parseTemplate($row[3], $row[0], "$row[1] $row[4]", $row[2]);

        # Where to direct AwStats output.
        my $output;

        if ($this->{_arguments}{verbose} eq "1") {
            $output = ">/dev/null 2>&1";
        }

        # Give AwStats the generated/updated config file.
        if ($this->{_arguments}{static}) {
            system $this->{_paths}{staticPages}." -config=$row[0] -update -awstatsprog=$awstats -dir=/home/www/$row[0]/public/awstats/ $output";
        } else {
            system $this->{_paths}{awstatsPl}." -config=$row[0] -update -showcorrupted $output";
        }

        $this->showDatetime("Parsed temporary config file for $row[0].", 1);

    } # End db while.

}


sub cleanTemp
{
    my ($this) = @_;

    # Empty temppath of old log files before update run!
    system("rm ".$this->{_paths}{temp}."/*.log");
    $this->showDatetime("Emptied ".$this->{_paths}{temp}." of old log files.", 1);
}


sub cleanOldApacheLogs
{
    # GET DATE IN THE PAST, SO THAT WE KNOW IF WE SHOULD DELETE OLD APACHE LOG FILES
    # AND THEIR DIRECTORIES
    #my @past = localtime(time-(86400 * $rotateDays));
    #my $oldlogs = sprintf("%s/%02d-%02d-%s", $logFilePath, @past[3], @past[4]+1, @past[5]+1900);

    # Remove old Apache log files.
    #if (!$sqllog && -d $oldlogs) {
    #   system ("rm -r $oldlogs");
    #   &showDatetime("Removed old log files from $oldlogs\n", 1);
    #}
}


sub parseSqlLogTable
{
    my ($this) = @_;

    my $cnx = $this->dbConnect();

    # Get domain names to parse from database.
    my $query = $this->getDomains($this->{_arguments}{domains});

    while (my @row = $query->fetchrow_array) {
        $this->showDatetime("Attempting to read log entries for $row[0] from database.", 1);

        my $data;
        my $query = "SELECT agent,bytes_sent,remote_host,request_method,referer,time_stamp,status,request_uri,remote_user FROM $logsDb.$logsTabl WHERE virtual_host='$row[0]'";

        if ($row[1] ne '') {
            $query .= " OR virtualhost='$row[1]'";
        }

        if ($row[4] ne '') {
            $query .= " OR virtualhost='$row[4]'";
        }

        $data = $cnx->prepare("$query ORDER BY timestamp ASC");
        $data->execute || die("Error preforming SQL query.\n");

        open (LOGFILE, ">>".$this->{_paths}{temp}."/$row[0].log");

        # Pull log data from SQL
        my $lastTimeStamp;

        while (my @foo = $data->fetchrow_array) {
            print LOGFILE "$foo[2] $foo[8] $foo[5] $foo[3] $foo[7] $foo[6] $foo[1] $foo[4] \"$foo[0]\"\n";
            $lastTimeStamp = $foo[5];
        }

        close (LOGFILE);

        # Clean SQL table
        $data = $cnx->prepare("DELETE FROM $logsDb.$logsTabl WHERE time_stamp<='$lastTimestamp' AND virtualhost='$row[0]'");
        $this->showDatetime("SQL: DELETE FROM $logsDb.$logsTabl WHERE time_stamp<='$lastTimestamp' AND virtualhost='$row[0]'", 2);
        $data->execute || die("Error preforming SQL query.\n");

        if ($row[1] ne '') {
            $data = $cnx->prepare("DELETE FROM $logsDb.$logsTabl WHERE timestamp<='$lastTimestamp' AND virtualhost='$row[1]'");
            $data->execute || die("Error preforming SQL query.\n");
            $this->showDatetime("SQL: DELETE FROM $logsDb.$logsTabl WHERE timestamp<='$lastTimestamp' AND virtualhost='$row[1]'", 2);
        }

        if ($row[4] ne '') {
            $data = $cnx->prepare("DELETE FROM $logsDb.$logsTabl WHERE timestamp<='$lastTimestamp' AND virtualhost='$row[4]'");
            $data->execute || die("Error preforming SQL query.\n");
            $this->showDatetime("SQL: DELETE FROM $logsDb.$logsTabl WHERE timestamp<='$lastTimestamp' AND virtualhost='$row[4]'", 2);
        }

        # Generate/update template for this domainname.
        $this->parseTemplate($row[3], $row[0], $row[1], $row[2]);

        # If logfile exists.
        if (-e $this->{_paths}{temp}."/$row[0].log") {

            # Where to direct AwStats output
            my $output;

            if ($this->{_arguments}{verbose} eq "1") {
                $output = ">/dev/null 2>&1";
            }

            # Give AwStats the generated/updated config file
            if ($this->{_arguments}{static}) {
                system $this->{_paths}{staticPages}." -config=$row[0] -update -awstatsprog=$awstats -dir=/home/www/$row[0]/public/awstats/ $output";
            } else {
                system $this->{_paths}{awstatsPl}." -config=$row[0] -update -lang=$row[2] $output";
            }

            $this->showDatetime("Parsed temporary config file for $row[0].", 1);

            # Can only remove config files if --static argument is given, since
            # the dynamic awstats.pl needs to config file to exist upon load.
            if ($this->{_arguments}{static} && $this->{_config}{rmConfig}) {
                system "rm ".$this->{_paths}{awstatsConfigs}."/awstats.$row[0].conf";
                $this->showDatetime("Removed temporary config file for $row[0].", 1);
            }

            $i++;
        } else {
            $this->showDatetime("Skipped $row[0] and all of it's host aliases, no hits.\n", 1);
        }
    }
}


sub parseTemplate {
    my ($this, $templatePath, $domain, $aliases, $language) = @_;
    my @Template;

    # Read the template.
    if (-e $templatePath) {
        if (-r $templatePath) {
            open(FILE, $templatePath);
            @Template = <FILE>;
            close FILE;
        } else {
            if ($this->{_config}{exitOnError}) {
                $this->showDatetime("Error: Cannot read template file for $domain. Exiting.\n");
                exit;
            } else {
                $this->showDatetime("Error: Cannot read template file for $domain.");
            }
        }
    } else {
        if ($this->{_config}{exitOnError}) {
            $this->showDatetime("Error: Template file for $domain does not exist. Exiting.\n");
            exit;
        } else {
            $this->showDatetime("Error: Template file for $domain does not exist.");
        }
    }

    # Build list of log files for logresolvemerge.pl
    my $logfiles = '';

    if ($aliases ne ' ') {
        # We have several log files, merge them.
        $logfiles = $this->{_paths}{logResolve}." -ignoremissing ".$this->{_paths}{temp}."/$domain.log ";

        # Split aliases by whitespace.
        foreach my $alias (split(/ /, $aliases)) {
            $logfiles = $logfiles . $this->{_paths}{temp}."/$alias.log ";
        }

        $logfiles = $logfiles . "|";
    } else {
        # Only one log file - we can skip logresolvemerge.pl.
        $logfiles = $this->{_paths}{temp}."/$domain.log";
    }

    # Parse the template.
    grep($_ =~ s/__SITEDOMAIN__/$domain/g, @Template);
    grep($_ =~ s/__HOSTALIASES__/$aliases /g, @Template); # Yes, there should be a space in this regexp.
    grep($_ =~ s/__LANGUAGE__/$language/g, @Template);
    grep($_ =~ s/__LOGFILES__/$logfiles/g, @Template);

    # Write parsed data to configuration file.
    my $path = $this->{_paths}{awstatsConfigs}."/awstats.$domain.conf";
    $this->showDatetime("Creating config file at $path");

    open(FILE,">$path") || die("Unable to create config file.");
    foreach my $Line (@Template) {
        print FILE $Line;
    }
    close FILE;

    $this->showDatetime("Generated temporary config file for $domain.", 1);
}


# SPLIT LOG FILES BY VIRTUAL HOST
# This method will take a combined Web server access log file and break its
# contents into separate files. It assumes that the first field of each line is
# the virtual host identity (put there by "%v"), and that the logfiles should be
# named that+".log".
sub splitLogByVhost
{
    my ($this, $logFile) = @_;

    if (-r $logFile) {
        open(LOGFILE, $logFile) || die("Unable to open logfile $logFile.");
    } else {
        $this->showDatetime("Error: No Apache log file named $logFile found or not readable.");
        if ($this->{_config}{exitOnError}) {
            $this->showDatetime("Error: Nothing to do. Please check configuration and paths.\n");
            exit;
        }
    }

    my %is_open = ();

    while (my $logLine = <LOGFILE>) {

        # Get the first token from the log record; it's the
        # identity of the virtual host to which the record
        # applies.

        my ($vhost) = split (/\s/, $logLine);

        # Normalize the virtual host name to all lowercase.
        # If it's blank, the request was handled by the default
        # server, so supply a default name.  This shouldn't
        # happen, but caution rocks.

        $vhost = lc ($vhost) || "access";

        # if the vhost contains a "/" or "\", it is illegal so just use
        # the default log to avoid any security issues due if it is interprted
        # as a directory separator.
        if ($vhost =~ m#[/\\]#) {
            $vhost = "access"
        }

        # If the log file for this virtual host isn't opened
        # yet, open it now.

        if (!$is_open{$vhost}) {
            open ($vhost, ">>".$this->{_paths}{temp}."/$vhost.log")
                || $this->displayError("Error: Cannot open ".$this->{_paths}{temp}."/$vhost.log");
            $is_open{$vhost} = 1;
        }

        # Strip off the first token (which may be null in the
        # case of the default server), and write the edited
        # record to the current log file.

        $logLine =~ s/^\S*\s+//;
        printf $vhost "%s", $logLine;
    }

    close LOGFILE;
}

1;