#!/usr/bin/perl

#
#       PBS Usage Report Generator ver 1.2
#       by Justin Lee, Sep.-Oct. 2013; user-module support added May 2015.
#
# This script processes the PBS accounting logs within a given date range,
# combining that with additional information from modules.log and host.map,
# and providing usage information for each completed job.
#
# Note that this requires CPAN modules installed and in the PERL5LIB path
#
# See the perldoc for more detailed information.


use Modern::Perl;
use Pod::Usage;
use Getopt::Long qw(:config auto_abbrev gnu_getopt auto_help);
use Data::Dumper;
use Clone qw(clone);
use File::Slurp;
use List::Util qw(reduce);
use syntax 'junction'; # provides keywords for any, all, none, or one
use DateTime;
use DateTime::Format::DateParse;


# NOTE: for debugging uncomment the below to die on Perl warnings
my $old_warn_handler = $SIG{__WARN__};
$SIG{__WARN__} = sub {
    die(@_);    
    $old_warn_handler->(@_) if $old_warn_handler;
};


# process the command line options
my %opt=getOptions();
my $debug = (defined($opt{debug})) ? $opt{debug} : 0;
my $matchon = (defined($opt{match})) ? quotemeta($opt{match}) : ".*";

# make sure the PBS accounting log files can be found, and get their filenames
my @logfiles = getLogFilenames(%opt);
# make sure the modules log file can be found, and read it into a hashref of jobid => module/version
my $modmap = getJobModules(%opt);
# make sure the host map file can be found, and read it into a hashref of node => scaling factor
my $hostmap = getNodeScalings(%opt);

my @jobusages; # where we store the extracted info for each job

# process each of the PBS accounting log files, adding information from modules.log and host.map
processLogs(\@logfiles, $modmap, $hostmap, \@jobusages);

# FOR DEBUGGING
map { validateJobInfo($_) } @jobusages if $debug > 1;

my @sorted_usages;
# either sort or total jobusage array, returning results in sorted_array
if (defined($opt{totalonly})) {
    totalUsages(\@jobusages, \@sorted_usages);
} else {
    sortUsages(\@jobusages, \@sorted_usages);
}

say STDERR "adding heading line.." if $debug;
unshift @sorted_usages, makeHeading() if defined($opt{heading});

# finally output the processed info to file or STDOUT
if (defined($opt{outfile})) {
    say STDERR "writing results to " . $opt{outfile} . " .." if $debug;
    write_file($opt{outfile}, map { formatJobUsage($_) . "\n" } @sorted_usages);
} else {
    say STDERR "outputting results ..\n" if $debug;
    foreach my $jobusage (@sorted_usages) {
        say formatJobUsage($jobusage);
    }
}
say STDERR "done!" if $debug;


# -------------- end of main() -----------------




# process each of the PBS accounting log files,
# using additional information from modules.log and host.map
# to create a jobusages array
sub processLogs {
    my ($logfiles, $modmap, $hostmap, $jobusages) = @_;

    my $linecnt=0;
    my $filecnt=0;

    foreach my $file (@{$logfiles}) {
        say STDERR "processing $opt{logdir}/$file .." if $debug;
        my @lines = read_file("$opt{logdir}/$file"); # File::Slurp will die if there's a problem
        say STDERR "read " . scalar(@lines) . " lines.." if $debug;
        $filecnt++;
        foreach my $line (grep { /^[0-9 \/\:]+;E/ } @lines) { # process exit states only
            $linecnt++;
            chomp($line);
            my $job_info = extractJobInfo($line, $modmap, $hostmap);
            push @{$jobusages}, $job_info if defined($job_info);
        }
    }

    say STDERR "Processed " . (scalar(@{$jobusages})) . " matching entries in $linecnt lines from $filecnt PBS accounting log files" if $debug;

}


# sort the jobusages arrayref by job, or by user, module or queue
# and secondly by job, and add these to the sorted_usages arrayref
sub sortUsages {
    my ($jobusages, $sorted_usages) = @_;

    if ($opt{orderby} eq "queue") {
        say STDERR "sorting jobusage array by queue, jobid.." if $debug;
        @{$sorted_usages} =  sort { $a->{queue} cmp $b->{queue} || jobOrder($a->{jobid}, $b->{jobid}) } @{$jobusages};
    } elsif ($opt{orderby} eq "user") {
        say STDERR "sorting jobusage array by user, jobid.." if $debug;
        @{$sorted_usages} =  sort { $a->{user} cmp $b->{user} || jobOrder($a->{jobid}, $b->{jobid}) } @{$jobusages};
    } elsif ($opt{orderby} eq "user-module") {
        say STDERR "sorting jobusage array by user, module.." if $debug;
        @{$sorted_usages} =  sort { $a->{user} cmp $b->{user} || $a->{module} cmp $b->{module} } @{$jobusages};
    } elsif ($opt{orderby} eq "module") {
        say STDERR "sorting jobusage array by module, jobid.." if $debug;
        @{$sorted_usages} =  sort { $a->{module} cmp $b->{module} || jobOrder($a->{jobid}, $b->{jobid}) } @{$jobusages};
    } else { # "job"
        say STDERR "sorting jobusage array by jobid.." if $debug;
        @{$sorted_usages} =  sort { jobOrder($a->{jobid}, $b->{jobid}) } @{$jobusages};
    }
}


# sort comparison function that allows job IDs to include an array job index, eg 123[45]
sub jobOrder {
    my ($a, $b) = @_;

    my ($ida) = $a =~ /^(\d+)/;
    my ($idb) = $b =~ /^(\d+)/;
    my $aidx = 0;
    my $bidx = 0;
    $aidx = $1 if ($a =~ /\[(\d*)\]/);
    $bidx = $1 if ($b =~ /\[(\d*)\]/);

    # simple case of no array jobs
    if ($ida < $idb || ($ida == $idb && $aidx < $bidx)) {
        return -1; 
    } elsif ($ida > $idb || ($ida == $idb && $aidx > $bidx)) {
        return 1; 
    } else {
        return 0; 
    }
}


# total the jobusages arrayref for user, user-module, module or queue, or for all jobs
# and add these totals to the sorted_usages arrayref
sub totalUsages {
    my ($jobusages, $sorted_usages) = @_;
    my %jobtotals;
    my $key = $opt{orderby} // 'job';
    say STDERR "totalling jobusages by $key.." if $debug;

    foreach my $usage (@{$jobusages}) {
        my $group = 0; # total for ALL jobs if key is 'job'
        if ($key eq 'user-module') {
            $group = $usage->{'user'} . ";" .  $usage->{'module'}; 
        } elsif ($key ne 'job') {
            $group = $usage->{$key};
        }

        if (defined($jobtotals{$group})) {
            addFieldsToTotal($jobtotals{$group}, $usage);
            say STDERR "added entry " . $jobtotals{$group}->{jobid} . " to jobtotals{$group}.." if $debug > 1;
        } else {
            say STDERR "creating new jobtotals entry for group $group.." if $debug;
            $jobtotals{$group} = clone($usage); # deep copy
            $jobtotals{$group}->{jobid} = 1; # this is a counter
            $jobtotals{$group}->{queue} = '';
            $jobtotals{$group}->{user} = '';
            $jobtotals{$group}->{module} = '';
            $jobtotals{$group}->{date_time} = '';
            $jobtotals{$group}->{exit_status} = 0;
            if ($key ne 'job') {
                $jobtotals{$group}->{$key} = $group; # the queue/user/user-module/module
            }
            # for user-module joined key, need the key parts for formatJobUsage()
            if ($key eq 'user-module') {
                $jobtotals{$group}->{user} = $usage->{'user'};
                $jobtotals{$group}->{module} = $usage->{'module'};
            }
            say STDERR "jobtotals{$group} created: " . Dumper($jobtotals{$group}) if $debug > 1;
        }
    }

    foreach my $group (sort { $a cmp $b } (keys %jobtotals)) {
        push @{$sorted_usages}, $jobtotals{$group};
    }
}


sub addFieldsToTotal {
    my ($total, $usage) = @_;

    $total->{jobid}             += 1; # counts number of entries in total
    $total->{queued_time}       += $usage->{queued_time};
    $total->{hpc_units}         += $usage->{hpc_units};
    $total->{hpc_unithours}      += $usage->{hpc_unithours};
    $total->{cpu_time}          += $usage->{cpu_time};
    $total->{walltime_cpus}     += $usage->{walltime_cpus};
    $total->{cpu_util}           = ($total->{walltime_cpus} > 0)
                                 ? $total->{cpu_time} / $total->{walltime_cpus}
                                  : 0;
    $total->{num_vnodes}         += $usage->{num_vnodes};
    $total->{mpi_procs}          += $usage->{mpi_procs};
    $total->{gpus}               += $usage->{gpus};
    $total->{ncpus_used}         += $usage->{ncpus_used};
    $total->{ncpus_requested}    += $usage->{ncpus_requested};
    $total->{ncpus_util}          = ($total->{ncpus_requested} > 0)
                                  ? $total->{ncpus_used} / $total->{ncpus_requested} 
                                  : 0;
    $total->{mem_used}           += $usage->{mem_used};
    $total->{mem_requested}      += $usage->{mem_requested};
    $total->{mem_util}            = ($total->{mem_requested} > 0)
                                  ? $total->{mem_used} / $total->{mem_requested} 
                                  : 0;
    $total->{walltime_used}      += $usage->{walltime_used};
    $total->{walltime_requested} += $usage->{walltime_requested};
    $total->{walltime_util}       =($total->{walltime_requested} > 0)
                                  ? $total->{walltime_used} / $total->{walltime_requested} 
                                  : 0;

}


# FOR DEBUGGING - ensure all fields are defined. ie no field name mismatches somewhere
sub validateJobInfo {
    my $jobinfo = shift;

    defined $jobinfo->{jobid} or die "jobid";
    defined $jobinfo->{queue} or die "queue";
    defined $jobinfo->{user} or die "user";
    defined $jobinfo->{module} or die "module";
    defined $jobinfo->{date_time} or die "date_time";
    defined $jobinfo->{queued_time} or die "queued_time";
    defined $jobinfo->{exit_status} or die "exit_status";
    defined $jobinfo->{hpc_units} or die "hpc_units";
    defined $jobinfo->{hpc_unithours} or die "hpc_unithours";
    defined $jobinfo->{cpu_time} or die "cpu_time";
    defined $jobinfo->{walltime_cpus} or die "walltime_cpus";
    defined $jobinfo->{cpu_util} or die "cpu_util";
    defined $jobinfo->{num_vnodes} or die "num_vnodes";
    defined $jobinfo->{mpi_procs} or die "mpi_procs";
    defined $jobinfo->{gpus} or die "gpus";
    defined $jobinfo->{ncpus_used} or die "ncpus_used";
    defined $jobinfo->{ncpus_requested} or die "ncpus_requested";
    defined $jobinfo->{ncpus_util} or die "ncpus_util";
    defined $jobinfo->{mem_used} or die "mem_used";
    defined $jobinfo->{mem_requested} or die "mem_requested";
    defined $jobinfo->{mem_util} or die "mem_util";
    defined $jobinfo->{walltime_used} or die "walltime_used";
    defined $jobinfo->{walltime_requested} or die "walltime_requested";
    defined $jobinfo->{walltime_util} or die "walltime_util";
    return 1;
}


sub makeHeading {
    # WARNING!!! ensure jobid field contains "job",
    # it is used in formatJobUsage() to check if this line is a heading or data
    my %jobinfo = (
        jobid              => "job ID",
        queue              => "queue",
        user               => "user",
        module             => "module",
        date_time          => "date-time",
        queued_time        => "queued time (hours)",
        exit_status        => "exit status",
        hpc_units          => "HPC units",
        hpc_unithours       => "HPC unit hours",
        cpu_time           => "cpu time (hours)",
        walltime_cpus      => "walltime cpus (hours)",
        cpu_util           => "cpu util",
        num_vnodes         => "num vnodes",
        mpi_procs          => "mpi procs",
        gpus               => "gpus",
        ncpus_used         => "ncpus used",
        ncpus_requested    => "ncpus requested",
        ncpus_util         => "ncpus util",
        mem_used           => "mem used (GB)",
        mem_requested      => "mem requested (GB)",
        mem_util           => "mem util",
        walltime_used      => "walltime used (hours)",
        walltime_requested => "walltime requested (hours)",
        walltime_util      => "walltime util",
    );

    $jobinfo{jobid} = "num jobs" if (defined($opt{totalonly}));
    # quote the value strings
    my %quoted_jobinfo = map { $_ => qq/"$jobinfo{$_}"/; } keys %jobinfo;
    return \%quoted_jobinfo;
}


# formatJobUsage takes a hashref of job information
# returns the following fields as a comma-delimited line of values:
#     job-id queue user module date-time queued-time exit-status
#            hpc-units hcp-walltime cpu-time walltime-cpus cpu-util
#            num-vnodes MPI-procs GPUs
#            ncpus-used ncpus-requested ncpu-util
#            mem-used mem-requested mem-util
#            walltime-used walltime-requested walltime-util

sub formatJobUsage {
    my $jobinfo = shift;

    # note that quoting text fields (qq)
    # and ensuring fractional fields are decimal not scientific notation (sprintf)
    return join(',',
        $jobinfo->{jobid},
        qq("$jobinfo->{queue}"),
        qq("$jobinfo->{user}"),
        qq("$jobinfo->{module}"),
        $jobinfo->{date_time},
        sprintf("%f", $jobinfo->{queued_time}),
        $jobinfo->{exit_status},
        $jobinfo->{hpc_units},
        $jobinfo->{hpc_unithours},
        $jobinfo->{cpu_time},
        $jobinfo->{walltime_cpus},
        sprintf("%f", $jobinfo->{cpu_util}),
        $jobinfo->{num_vnodes},
        $jobinfo->{mpi_procs},
        $jobinfo->{gpus},
        $jobinfo->{ncpus_used},
        $jobinfo->{ncpus_requested},
        sprintf("%f", $jobinfo->{ncpus_util}),
        sprintf("%f", $jobinfo->{mem_used}),
        $jobinfo->{mem_requested},
        sprintf("%f", $jobinfo->{mem_util}),
        $jobinfo->{walltime_used},
        $jobinfo->{walltime_requested},
        sprintf("%f", $jobinfo->{walltime_util})
    ) unless ($jobinfo->{jobid} =~ /job/i );

    # this is a heading line
    return join(',',
        $jobinfo->{jobid},
        $jobinfo->{queue},
        $jobinfo->{user},
        $jobinfo->{module},
        $jobinfo->{date_time},
        $jobinfo->{queued_time},
        $jobinfo->{exit_status},
        $jobinfo->{hpc_units},
        $jobinfo->{hpc_unithours},
        $jobinfo->{cpu_time},
        $jobinfo->{walltime_cpus},
        $jobinfo->{cpu_util},
        $jobinfo->{num_vnodes},
        $jobinfo->{mpi_procs},
        $jobinfo->{gpus},
        $jobinfo->{ncpus_used},
        $jobinfo->{ncpus_requested},
        $jobinfo->{ncpus_util},
        $jobinfo->{mem_used},
        $jobinfo->{mem_requested},
        $jobinfo->{mem_util},
        $jobinfo->{walltime_used},
        $jobinfo->{walltime_requested},
        $jobinfo->{walltime_util}
    );
}


# extractJobInfo takes a line from a PBS accounting log, and hashrefs of module loads and host scalings
# 
# The PBS accounting logs store information in ';' separated fields:
#     - Date Time: in format MMDDYYYY HH:MM:SS
#     - State: 'E' for exit (the only state of interest here)
#     - Job ID: <job_number>.pbsserver
#     - Data: data about the job in <key>=<value> pairs
# We extract / calculate the following:
#     job-id queue user module date-time queued-time exit-status
#            hpc-units hcp-walltime cpu-time walltime-cpus cpu-util
#            num-vnodes MPI-procs GPUs
#            ncpus-used ncpus-requested ncpu-util
#            mem-used mem-requested mem-util
#            walltime-used walltime-requested walltime-util

sub extractJobInfo {
    my ($line, $modmap, $hostmap) = @_;

    my @lineinfo = split(/;/, $line);
    # we only process node's exit states and skip array job summaries: jobid[]
    return undef if ($lineinfo[1] ne "E" || $lineinfo[2] =~ /\[\]\./ );

    # determine if this is an array job and extract jobid and array job index
    my $jobid;
    if ($lineinfo[2] =~ /(\d+)\[(\d+)\]\./) {
        # this is an array job
        $jobid = "$1\[$2\]";
    } else {
        # note psuedo-list context to capture regex match, rather than number of matches
        ($jobid) = $lineinfo[2] =~ /^(\d+)/;
    }
    my $module = $modmap->{$jobid} // "";

    # we will only process this line if matchon matches either line or module
    return undef unless ($line =~ /$matchon/i || $module =~ /$matchon/i);

    say STDERR "\nprocessing: $line" if $debug > 1;
    # some trickery is needed for the following to extract key1=value1 key2=value2 ..
    # we only want to split on space separated key=value pairs, but need to add
    # a dummy space at the start for the first key=value. then we get rid of the
    # empty first match (on the space) and cast the array to a hash
    my @data = split(/\s+([a-zA-Z_.]+)=/, " " . $lineinfo[3]); # add dummy space at start
    shift @data; # remove first, empty, field
    my %data = @data;
    say STDERR "extracted data fields: " . join("  ", map { "($_ => $data{$_})" } keys %data) if $debug > 1;

    my %jobinfo = (
        jobid             => $jobid,
        module             => $module,
        queue              => $data{'queue'},
        user               => $data{'user'},
        exit_status        => $data{'Exit_status'},
    );

    $jobinfo{date_time} = formatDate($lineinfo[0]);
    if (defined($data{'exec_vnode'})) {
        my @vn = split(/\+/, $data{'exec_vnode'});
        $jobinfo{num_vnodes} = scalar(@vn); # mem/cpu maybe spread over multiple
    } else {
        $jobinfo{num_vnodes} = 0;
    }

    $jobinfo{queued_time}        = ($data{'start'} - $data{'qtime'})/3600; # in hours
    $jobinfo{mpi_procs}          = $data{'Resource_List.mpiprocs'} // 0;
    $jobinfo{gpus}               = $data{'Resource_List.ngpus'} // 0;

    $jobinfo{ncpus_requested}    = $data{'Resource_List.ncpus'} // 0;
    $jobinfo{ncpus_used}         = $data{'resources_used.ncpus'} // 0;
    $jobinfo{ncpus_util}         = ($jobinfo{ncpus_requested} > 0)
                                 ? $jobinfo{ncpus_used} / $jobinfo{ncpus_requested}
                                 : 0;

    $jobinfo{mem_requested}      = toGB($data{'Resource_List.mem'});
    $jobinfo{mem_used}           = toGB($data{'resources_used.mem'});
    $jobinfo{mem_util}           = ($jobinfo{mem_requested} > 0)
                                 ? $jobinfo{mem_used} / $jobinfo{mem_requested}
                                 : 0;

    $jobinfo{walltime_requested} = hms2hours($data{'Resource_List.walltime'});
    $jobinfo{walltime_used}      = hms2hours($data{'resources_used.walltime'});
    $jobinfo{walltime_util}      = ($jobinfo{walltime_requested} > 0)
                                 ? $jobinfo{walltime_used} / $jobinfo{walltime_requested}
                                 : 0;

    $jobinfo{walltime_cpus}      = $jobinfo{walltime_used} * $jobinfo{ncpus_used};
    $jobinfo{cpu_time}           = hms2hours($data{'resources_used.cput'});
    $jobinfo{cpu_util}           = ($jobinfo{walltime_cpus} > 0)
                                 ? $jobinfo{cpu_time} / $jobinfo{walltime_cpus}
                                 : 0;

    $jobinfo{hpc_units}          = calcHPCunits($hostmap, $data{'exec_vnode'});
    $jobinfo{hpc_unithours}       = $jobinfo{hpc_units} * $jobinfo{walltime_used};

    say STDERR "extracted jobinfo data: " . join("  ", map { "($_ => $jobinfo{$_})" } keys %jobinfo) if $debug > 1;

    return \%jobinfo;
}


sub formatDate {
    my $datestr = shift;
    my $dt = DateTime::Format::DateParse->parse_datetime($datestr);
    return $dt->ymd('-') . ' ' . $dt->hms(':');
}


# scale cpus and by hostmap factor and sum to get total units of compute
# this receives a string like: (cl2n001[0]:mem=16777216kb:ncpus=6+cl2n001[1]:ncpus=2)
sub calcHPCunits {
    my ($hostmap, $exec_vnode) = @_;

    die "undefined exec_vnode" unless (defined $exec_vnode);
    $exec_vnode =~ s/[()]//g; # get rid of the surrounding brackets
    my $hpc_units = 0;
    my ($host, $cpu_units, $mem_units, $units, $factor, $core);

    say STDERR "calcHPCunits: $exec_vnode" if $debug > 1;
    foreach my $node (split(/\+/, $exec_vnode)) {
        $node =~ s/=/:/g;
        my @fields = split(/:/, $node);
        $host = shift @fields;
        #say STDERR "calcHPCunits: node: $node - fields:" . join(',', @fields) if $debug > 1;
        $host =~ s/\[(\d+)\]$//; # removing trailing [x], but keep core number in $1
        $core = $1;
        #$factor = $hostmap->{$host} // 1; # default to 1 if not in hostmap
        $factor = $hostmap->{$host} or die "host $host not found in hostmap: " . Dumper($hostmap);
        # have array like (ngpus, 0, ncpus, 1, mem, 13573584kb), cast to hash
        my %resources = @fields;
        #say STDERR "resources: " . join("  ", map { "($_ => $resources{$_})" } keys %resources) if $debug > 1;
        if (defined $resources{ncpus}) {
            $cpu_units = $resources{ncpus};
            say STDERR "calcHPCunits: $host core# $core cpu units: $cpu_units" if $debug > 1;
        } else {
            $cpu_units = 1;
        }
        
        # note that sometimes PBS is just utilising another CPU's memory
        if (defined $resources{mem}) {
            # we should do a lookup on the hostmap to see how much memory per core
            # but as we don't currently have that information, we'll use what we know
            # cl1n* have 24 GB spread between 12 cores => 2GB per core
            # cl2n* typically have 16 cores and 64 or 128 GB (or 32 cores and 256 GB)
            # but we are measuring against a standard, which we'll set as 4GB
            $mem_units = $resources{mem};
            $mem_units = toGB($mem_units) / 4000;
            say STDERR "calcHPCunits: $host core# $core RAM block units: $mem_units" if $debug > 1;
        } else {
            $mem_units = 1;
        }
        $units = ($cpu_units > $mem_units) ? $cpu_units : $mem_units;
        say STDERR "calcHPCunits: $host core# $core: adding units $units scaled by factor $factor = " . ($units * $factor) if $debug > 1;;
        $hpc_units += $units * $factor;
    }

    return $hpc_units;
}


# convert a numerical string suffixed with GB, MB, KB to the numerical value in GB
# note use of factors in base 10 not base 2 for memory
sub toGB {
    my $str = shift;
    return 0 unless defined($str);

    $str = uc($str);
    my $gb = undef;
    my %scale = ( 'B' => 1000000000, 'K' => 1000000, 'M' => 1000, 'G' => 1 );
    if ($str =~ /(\d+)(B|K|M|G)/ ) {
        $gb = $1 / $scale{$2};
    }
    return $gb;
}


# HH:MM:SS to hours
sub hms2hours {
    my $time = shift;
    return 0 unless defined($time);
    return reduce { $a/60 + $b } reverse(split(/:/, $time));
}

# HH:MM:SS to seconds
#sub hmsToSeconds {
#    my $time = shift;
#    return reduce { $a*60 + $b } split(/:/, $time);
#}


sub getJobModules {
    my %opt = @_;

    say STDERR "looking for module loads log file: " . $opt{modlog} . " .." if $debug;
    # make sure the modules log file can be found
    my $modlog=$opt{modlog};
    unless (-e $modlog) {
        # try in the log directory
        $modlog = "$opt{logdir}/$opt{modlog}";
        die "Modules log $opt{modlog} was not found!" unless (-e $modlog);
    }
    say STDERR "reading module loads from $modlog..." if $debug;

    # create map of jobid => module
    # WARNING: if jobids can be REUSED within the timeframe of the module.log file
    #          then we will NEED to create a key of jobid+date (for eg),
    #          or only store the parts of the log that are within the timeframe (see Date::Parse)
    #          but for simplicity, leave this out for now as job numbers don't appear
    #          to be reused within the recorded time frame
    #                                       module  user   jobid
    my %modmap = map { reverse /hpcsoftware: (\S+) - \w+ - (\d+)/ } read_file($modlog);

    say STDERR "read in " . scalar(keys %modmap) . " module loads from $modlog" if $debug;
    #say STDERR "modmap data: " . join("  ", map { "($_ => $modmap{$_})" } keys %modmap) if $debug > 1;

    return \%modmap;
}


sub getNodeScalings {
    my %opt = @_;

    say STDERR "looking for hostmap file " . $opt{hostmap} . " .." if $debug;
    # make sure the host map file can be found
    die "Host map file $opt{hostmap} was not found!" unless (-e $opt{hostmap});
    # create map of node name => decimal scaling factor
    my %hostmap = map { /(\w+)\s+([0-9.]+)/ } read_file($opt{hostmap});
    say STDERR "read in " . scalar(keys %hostmap) . " node scalings from " . $opt{hostmap} if $debug;

    return \%hostmap;
}


sub getLogFilenames {
    my %opt = @_;

    say STDERR "looking for log files in " . $opt{logdir} . " .." if $debug;
    opendir my($dh), $opt{logdir} or die("Couldn't open directory '" . $opt{logdir} . "': $!");

    # get a list of files from the directory with the form YYYYMMDD
    my @allfiles = grep { /^\d{8}$/ } readdir $dh;
    closedir $dh;
    say STDERR "found log files .." if $debug;

    # get the log files within the date range
    my @files = grep ($_ >= $opt{'start'} && $_ <= $opt{'end'}, @allfiles);
    @files = sort { $a <=> $b } @files;
    say STDERR "matching log files: " . join(', ', @files) if $debug;

    return @files;
}


sub getOptions {

    # Default options
    my %opt = (
        logdir     => '.',        # the directory this script is located in
        start      => '00000101', # 1st Jan 0 CE - ie no start bound
        end        => '99991231', # 31st Dec 9999 CE - ie no end bound
    );

    # Parse the command-line options
    pod2usage(-verbose => 1, -exitval => 2) unless GetOptions(\%opt,
        "debug:1",
        "help|h",
        # your options here
        "logdir=s",
        "start=s",
        "end=s",
        "match=s",
        "modlog=s",
        "hostmap=s",
        "outfile=s",
        "orderby=s",
        "totalonly",
        "heading",
    );
    pod2usage(-verbose => 1) if $opt{help}; # TODO fix -verbose => 2 doesn't work

    if (defined($opt{orderby}) && none(qw(job queue user user-module module)) eq $opt{orderby}) {
        say STDERR "Error! orderby should be one of: 'job' (default), 'queue', 'user', 'user-module' or 'module'!";
        pod2usage(-verbose => 1, -exitval => 2);
    } elsif (!defined($opt{orderby})) {
        $opt{orderby} = 'job';
    }

    $opt{modlog} = 'modules.log' if(!defined($opt{modlog}));
    $opt{hostmap} = 'host.map' if(!defined($opt{hostmap}));

    return %opt;
}

__END__


=head1 NAME

pbs_usage.pl: PBS Usage Report Generator


=head1 SYNOPSIS

pbs_usage.pl [ --help ] [ --debug[=LEVEL] ] [ --logdir DIRNAME ] [ --start YYYYMMDD ] [ --end YYYYMMDD ] [ --match STRING ] [ --modlog FILENAME ] [ --hostmap FILENAME ] [ --outfile FILENAME ] [ --heading ] [ --orderby ( job | queue | user | user-module | module ) ] [ --totalonly ]


=head1 ARGUMENTS

=over 4

=item B<--help>

Provides these helpful messages

=item B<--debug[=LEVEL]>

Provides debugging information to STDERR.
Defaults to level 1, showing script's progress and other high level output.
Levels above 1 provide more detailed debugging output.

=back

Other arguments modify the script's behaviour as below.

=over 4

=item B<--logdir DIRNAME>

Specify the directory that the PBS accounting log files are stored in (default: current directory)

=item B<--start YYYYMMDD --end YYYYMMDD>

These are used to specify the start and end date ranges too look at in the log files

=item B<--match STRING>

Only process log entries where either the log line matches the provided literal STRING
(case insensitive), or the loaded module does.

=item B<--modlog FILENAME>

Specify the name of the module load log file, used to map job IDs to modules (default: modules.log in logdir or current directory)

=item B<--hostmap FILENAME>

Specify the name of the host map file used for generating hpc-units (default: host.map in current directory)

=item B<--outfile FILENAME>

Optionally specifies a file for writing the results to. If not provided, STDOUT is used.

=item B<--heading>

Optionally output a heading row at the start.

=item B<--orderby ( job | queue | user | user-module | module )>

Specify the field by which the output is ordered (default: job)

=item B<--totalonly>

Only output the totals for each group as specified by the orderby field
(default: totals for all jobs, likewise if orderby job)
Note: jobid field gives number of jobs
      date and time field is set to empty and exit status is set to 0
      the queue/module/user fields that aren't sorted on are also empty

=back


=head1 DESCRIPTION

This script processes all the PBS accounting logs within a given date range,
providing usage for user and module. Module information is extracted from
the modules log file based on job-id, and HPC units use information from the hostmap file

The PBS accounting logs are stored on the file system with names in the form YYYYMMDD 
Within each log, the fields are ';' separated. The fields are:
    - Date Time: in format MMDDYYYY HH:MM:SS
    - State: 'E' for exit (the only state of interest here)
    - Job ID: <job_number>.pbsserver
    - Data: data about the job in <key>=<value> pairs

The modules log file contains module load information, with entries in space-delimited format:
    - Date Time: in format YYYY-MM-DD 'T' HH:MM:SS+GMT_HH:GMT_MM
                        or MTH Day HH:MM:SS (the older format)
    - Node name
    - 'hpcsoftware:'
    - module/version ' - '
    - user ' - '
    - job-id '.pbsserver - '
      optionally job-id may be suffixed by [array-job-index]
    - /path/to/module/version


OUTPUT: Utilisation information for each job, ordered by job-id (default),
        queue, user or module in lines with the comma-delimited format:

    job-id[array-job-index] queue user module date-time queued-time exit-status
           hpc-units hcp-walltime cpu-time walltime-cpus cpu-util
           num-vnodes MPI-procs GPUs
           ncpus-used ncpus-requested ncpu-util
           mem-used mem-requested mem-util
           walltime-used walltime-requested walltime-util


=head1 AUTHOR

Written by Justin Lee, Sep.-Oct. 2013 (Ver. 1.1).
user-module sort and total support added May 2015 (Ver 1.2).
Contact: jm.lee@qut.edu.au


=cut

