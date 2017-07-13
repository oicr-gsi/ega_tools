#! /usr/bin/perl

use strict;
use warnings;
use File::Basename;
use Data::Dumper;


### script to encrypt and upload data to ega
### requires the following information, which can be stored in a configuration file
### folder with files to upload
### work folder (usually in scratch space)
### ega box + password
### path on the ega box
### flag indicating that encrypted data should be deleted after upload

use Config::General qw(ParseConfig);
use Getopt::Long;
my %opts = ();
GetOptions(
	"config=s" 		=> \$opts{"configfile"},   ### configuration file where options are stored
	"dir_queue=s" 	=> \$opts{"qdir"},         ### folder with links to files that need encryption/upload
	"dir_work=s" 	=> \$opts{"wdir"},         ### folder where work should be done, usually in scratch
	"box=s" 		=> \$opts{"box"},  
	"box_pw=s" 		=> \$opts{"pass"},
	"box_path=s"	=> \$opts{"path"},  
	"delete=s"		=> \$opts{"delete"},
	"next=s"		=> \$opts{"next"},
	"help" 			=> \$opts{help},
	"keys"			=> \$opts{keys},          ### comma separated list of public encryption keys
);

### configuration options are overwritten by command line options
my %config = ParseConfig($opts{"configfile"});
map{$opts{$_} = $opts{$_} || $config{$_}} keys %config;
validate_options(\%opts);




#my $pwd=`pwd`;chomp $pwd;
#print STDERR "starting new job\n";

#my $remote_dir="/haltwg/bam";

#my $file=`ls to_upload | head -n 1`;chomp $file;   ### grab one file
#my $fn=basename($file);
#(my $id=$fn)=~s/\.fastq.*//;
#my $dir="scratch/$id";
#die "directory already exists for $id in scratch" if(-d $dir);
#mkdir $dir;
#print "$file\n";
#`mv to_upload/$file $dir/$file`;
 
#(open my $QSUB,">","$dir/process.sh") || die "could not open upload file";
#print $QSUB "qsub -cwd -b y -e $pwd/logs -o $pwd/logs -N ega.$id.premd5                              \"md5sum $fn | cut -f1 -d\' \'> $fn.md5\"\n";
#print $QSUB "qsub -cwd -b y -e $pwd/logs -o $pwd/logs -N ega.$id.gpg  	                             \"gpg --trust-model always -r EGA_Public_key -r SeqProdBio -o $fn.gpg -e $fn\"\n";
#print $QSUB "qsub -cwd -b y -e $pwd/logs -o $pwd/logs -N ega.$id.postmd5   -hold_jid ega.$id.gpg     \"md5sum $fn.gpg | cut -f1 -d\' \'> $fn.gpg.md5\"\n";
#print $QSUB "qsub -cwd -b y -e $pwd/logs -o $pwd/logs -N ega.$id.size      -hold_jid ega.$id.gpg     \"du $fn.gpg > $fn.gpg.size \"\n";
#print $QSUB "qsub -cwd -b y -e $pwd/logs -o $pwd/logs -N ega.$id.upload    -hold_jid ega.$id.postmd5 \"bash upload.sh\"\n";
#print $QSUB "qsub -cwd -b y -e $pwd/logs -o $pwd/logs -N ega.$id.rmgpg     -hold_jid ega.$id.upload  \"rm $fn.gpg;\"\n";
#print $QSUB "qsub      -b y -e $pwd/logs -o $pwd/logs -N ega.$id.finish    -hold_jid ega.$id.rmgpg   \"mv $pwd/$dir $pwd/uploaded/$id \"\n";
#print $QSUB "qsub -wd $pwd -b y -e $pwd/logs -o $pwd/logs -N ega.$id.startnext -hold_jid ega.$id.finish  \"sleep 5m;./ega.pl\"\n"; 
#print $QSUB "qsub -wd $pwd -b y -e $pwd/logs -o $pwd/logs -N ega.$id.startnext -hold_jid ega.$id.upload  \"sleep 5m;$pwd/ega.lftp.pl\"\n";
#close $QSUB;

#(open my $UP,">","$dir/upload.sh") || die "unable to open upload script";
#print $UP "ssh xfer.hpc.oicr.on.ca \"lftp -u ega-box-137,GnwyrZm6 -e \\\"set ftp:ssl-allow false; mput $pwd/$dir/$fn.gpg $pwd/$dir/$fn*.md5 -O $remote_dir; bye;\\\" ftp://ftp-private.ebi.ac.uk\"";
#close $UP;




#`(cd $dir;bash process.sh)`;	
	


sub validate_options{
	my ($opts)=@_;
	usage("Help requested.") if($opts{help});
	
	if(! $$opts{configfile} || ! -e $$opts{configfile}){
		usage("Configuration file not provided or not found.");
	}
	if(! $$opts{dir_queue} || ! -d $$opts{dir_queue}){
		usage("Directory with queued files not provided or nor found.");
	}
	if(! $$opts{dir_work} || ! -d $$opts{dir_work}){
		usage("Work directory not provided or nor found.");
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



}

sub usage{
	print "\nega_lftp.pl [options]\n";
	print "Options are as follows:\n";
	print "\t--config String/filename. A configuratino file, storing options as key value pairs\n";
	print "\t--dir_queue  String/directory name.  A directory with a list of files that need to be uploaded.\n";
	print "\t--dir_work  String/directory name. A directory where the file will be copied, encrypted and md5summed, prior to upload.\n";
	print "\t--box String.  The ega-box for upload\n";
	print "\t--box_pass String. Password for the ega-box.\n";
	print "\t--box_path String. Path on the ega-box where files should be uploaded.\n";
	print "\t--delete. True/False.  Should encrypted data be deleted after upload?  Default = True\n";
	print "\t--next. True/False.  Should the next file be processed once this one has completed.  Default = True.\n";
	print "\t--keys. String. A comma separated list of public gpg encryption keys..\n";
	print "\t--help displays this usage message.\n";

	die "\n@_\n\n";
}
