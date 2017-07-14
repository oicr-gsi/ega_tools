#! /usr/bin/perl

use strict;
use warnings;
use File::Basename;
use Data::Dumper;


### capture the command line for next option
my $commandline = join " ", $0, @ARGV;
my $pwd=`pwd`;chomp $pwd;





### script to encrypt and upload data to ega
### requires the following information, which can be stored in a configuration file
### folder with files to upload
### work folder (usually in scratch space)
### ega box + password
### path on the ega box
### flag indicating that encrypted data should be deleted after upload

#use Config::General qw(ParseConfig);
use Getopt::Long;
my %opts = ();
GetOptions(
	"config=s" 		=> \$opts{configfile},   ### configuration file where options are stored
	"qdir=s" 	=> \$opts{qdir},         ### folder with links to files that need encryption/upload
	"wdir=s" 	=> \$opts{wdir},         ### folder where work should be done, usually in scratch
	"box=s" 		=> \$opts{box},  
	"pw=s" 		=> \$opts{pw},
	"boxpath=s"	=> \$opts{boxpath},  
	"delete=s"		=> \$opts{delete},
	"next=s"		=> \$opts{next},
	"help" 			=> \$opts{help},
	"keys"			=> \$opts{keys},          ### comma separated list of public encryption keys
);
%opts=validate_options(%opts);


### grab ONE file from the queue direcotry
my $file=`ls $opts{qdir} | head -n 1`;chomp $file;   ### grab one file
my $fn=basename($file);
print STDERR "Processing next file in queue, $fn\n";

my $workdir=$opts{wdir} . "/$fn";
usage("$workdir already exists.  Upload of this file has completed.") if(-d $workdir);
mkdir $workdir;

print STDERR "Moving $file to $workdir for processing\n";
`mv to_upload/$file $workdir/$fn`;

print STDERR "Preparing process script under $workdir\n";

my $script_process="$workdir/process.sh";
my @keys=split /,/,$opts{keys};
my $keystring=join(" ",map{"-r $_"} @keys);
 
(open my $QSUB,">",$script_process) || die "could not open $script_process";
print $QSUB "qsub -cwd -b y -N ega.$fn.premd5                             \"md5sum $fn | cut -f1 -d\' \'> $fn.md5\"\n";
print $QSUB "qsub -cwd -b y -N ega.$fn.gpg  	                          \"gpg --trust-model always $keystring -o $fn.gpg -e $fn\"\n";
print $QSUB "qsub -cwd -b y -N ega.$fn.postmd5 -hold_jid ega.$fn.gpg      \"md5sum $fn.gpg | cut -f1 -d\' \'> $fn.gpg.md5\"\n";
print $QSUB "qsub -cwd -b y -N ega.$fn.upload  -hold_jid ega.$fn.postmd5  \"bash upload.sh\"\n";
print $QSUB "qsub -cwd -b y -N ega.$fn.rmgpg   -hold_jid ega.$fn.upload   \"rm $fn.gpg;\"\n"  if($opts{delete} eq "True");

if($opts{next} eq "True"){
	#### need to run the same command, is there way of echoing this in the program
	print $QSUB "qsub -cwd -b y  -N ega.$fn.startnext -hold_jid ega.$fn.upload  \"sleep 5m;cd $pwd;$commandline\"\n";
}
close $QSUB;


print STDERR "Preparing upload script under $workdir\n";

my $script_upload="$workdir/upload.sh";
(open my $UP,">",$script_upload) || die "unable to open $script_upload";
print $UP "ssh xfer.hpc.oicr.on.ca \"lftp -u $opts{box},$opts{pw} -e \\\"set ftp:ssl-allow false; mput $workdir/$fn.gpg $workdir/$fn*.md5 -O $opts{boxpath}; bye;\\\" ftp://ftp-private.ebi.ac.uk\"";
close $UP;


print STDERR "Start processing\n";
`(cd $workdir;bash process.sh)`;	
	


sub validate_options{
	my (%opts)=@_;
	usage("Help requested.") if($opts{help});
	
	if($opts{configfile}){
		 if( ! -e $opts{configfile} ){
			 usage("Configuration file not provided or not found.");
		 }else{
			 my %config;
			 my @recs=`cat $opts{configfile} | grep ":"`;
			 map{  chomp;
			       my($k,$v)=split /:/;
				   $config{$k}=$v
			 }@recs;
			 map{$opts{$_} = $opts{$_} || $config{$_}} keys %config;
		 }
	 }
	 
	
	if(! $opts{qdir} || ! -d $opts{qdir}){
		usage("Directory with queued files not provided or nor found.");
	}
	if(! $opts{wdir} || ! -d $opts{wdir}){
		usage("Work directory not provided or nor found.");
	}
	
	if(! $opts{box}){
		usage("ega-box not provided");
	}
	
	if(! $opts{pw}){
		usage("password for $opts{box} not provided");
	}
	
	if(! $opts{boxpath}){
		usage("path on $opts{box} not provided");
	}
	
	if(! $opts{keys}){
		usage("gpg encryption key(s) not provided");
	}
	
	if($opts{delete}){
		usage("Invalid option --delete") unless($opts{delete} eq "True" || $opts{delete} eq "False");
	}else{
		$opts{delete}="True";
	}

	if($opts{next}){
		usage("Invalid option --next") unless($opts{next} eq "True" || $opts{next} eq "False");
	}else{
		$opts{next}="True";
	}

	return %opts;

}

sub usage{
	print "\nega_lftp.pl [options]\n";
	print "Options are as follows:\n";
	print "\t--config String/filename. A configuration file, storing options as key value pairs\n";  ### currently not supported
	print "\t--qdir  String/directory name.  A directory with a list of files that need to be uploaded.\n";
	print "\t--wdir  String/directory name. A directory where the file will be copied, encrypted and md5summed, prior to upload.\n";
	print "\t--box String.  The ega-box for upload\n";
	print "\t--pw String. Password for the ega-box.\n";
	print "\t--boxpath String. Path on the ega-box where files should be uploaded.\n";
	print "\t--delete. True/False.  Should encrypted data be deleted after upload?  Default = True\n";
	print "\t--next. True/False.  Should the next file be processed once this one has completed.  Default = True.\n";
	print "\t--keys. String. A comma separated list of public gpg encryption keys..\n";
	print "\t--help displays this usage message.\n";

	die "\n@_\n\n";
}
