#! /usr/bin/perl

use strict;
use warnings;
use File::Basename;
use Data::Dumper;
use lib dirname (__FILE__);
use EGA_XML;


### capture the command line for next option
my $commandline = join " ", $0, @ARGV;
my $pwd=`pwd`;chomp $pwd;

my %p=init_parameters();



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
	"files=s"		=> \$opts{files}, ###comma separated list of files
	"file_type=s"   => \$opts{file_type}, ## fastq or bam
	"run_alias=s"	=> \$opts{run_alias},
	"run_date=s"	=> \$opts{run_date},
	"study_center=s" => \$opts{study_center},
	"run_center=s"	=> \$opts{run_center},
	
	
	"egax=s"			=> \$opts{egax},  ## the ega experiment accession the run object should be linked to
	"wdir=s" 		=> \$opts{wdir},         ### folder where work should be done, usually in scratch
	"egabox=s" 		=> \$opts{egabox},  
	"pw=s" 			=> \$opts{pw},
	"stage_path=s"	=> \$opts{stage_path},  
	"delete=s"		=> \$opts{delete},
	"help" 			=> \$opts{help},
	"keys=s"			=> \$opts{keys},          ### comma separated list of public encryption keys
	"xfer=s"			=> \$opts{xfer},		  ## the transfer box to use, defaults to xfer.sftp.oicr.on.ca			
	"xfer_method=s"   => \$opts{xfer_method},   ### lftp or aspera
	"aspera_pw=s"		=> \$opts{aspera_pw},
	
);



#print Dumper(%p);<STDIN>;
%p=(%p,validate_options(%opts));





my $workdir=$p{prep}{wdir} . "/$p{run_alias}";
#usage("$workdir already exists.") if(-d $workdir);

print STDERR "generating run files under $workdir\n";
mkdir "$workdir" || die "unable to create $workdir";


my($cmd,$rv);

### prepare the files


print STDERR "preparing files\n";
my $readN=0;
for my $file(@{$p{prep}{files}}){       ### this assumes fastq files to be R1,R2
	my $fn=basename($file);
	
	$readN++;
	my $readid="R$readN";
	$p{run}{$readid}{file}=$fn;
	
	print STDERR "calculating md5sum on $file\n";
	$cmd="md5sum $file | cut -f1 -d ' ' | tee $workdir/$fn.md5";
	my $md5=`$cmd`;chomp $md5;
	$p{run}{$readid}{md5}=$md5;
	
	print STDERR "encrypting $file\n";
	$cmd="gpg --trust-model always $p{prep}{keystring} -o $workdir/$fn.gpg -e $file";
	$rv=`$cmd`;
	
	$p{run}{$readid}{encrypted_file}="$workdir/$fn.gpg";
	
	print STDERR "calculating md5sum on $workdir/$fn.gpg\n";
	$cmd="md5sum $workdir/$fn.gpg | cut -f1 -d ' ' | tee $workdir/$fn.gpg.md5";
	$md5=`$cmd`;chomp $md5;
	$p{run}{$readid}{encrypted_md5}=$md5;
}

my $upload_cmd;
if($p{prep}{xfer_method} eq "lftp"){
	$upload_cmd="ssh $p{prep}{xfer} \"lftp -u $p{prep}{egabox},$p{prep}{pw} -e \\\"set ftp:ssl-allow false; mput $workdir/*.gpg $workdir/*.md5 -O $p{prep}{stage_path}; bye;\\\" ftp://ftp-private.ebi.ac.uk\"";
}elsif($p{prep}{xfer_method} eq "aspera"){
	$upload_cmd="ssh $p{xfer} \"export ASPERA_SCP_PASS=$p{aspera_pw};".
          		"~/.aspera/connect/bin/ascp -QT -l300M -L- -k2 $workdir/*.md5 $p{prep}{egabox}\@fasp.ega.ebi.ac.uk:$p{prep}{boxpath};".
          	  	"~/.aspera/connect/bin/ascp -QT -l300M -L- -k2 $workdir/*.gpg $p{prep}{egabox}\@fasp.ega.ebi.ac.uk:$p{prep}{boxpath};\"";
}

print STDERR "uploading files\n";
print STDERR "$upload_cmd\n";
$rv=`$upload_cmd`;
print STDERR "rv=$rv\n";

my $alias=$p{run_alias};
my $xml=run_xml_fastq($alias,\%p);
(open my $XML,">","$workdir/${alias}.xml") || die "unable to open run xml";
print $XML $xml->toString(1);
close $XML;




sub validate_options{
	my (%opts)=@_;
	usage("Help requested.") if($opts{help});
	
	my %param;  ### hash to be configured and returned
	
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
	
	if(! $opts{files}){
		usage("no files have been provided");
	}else{
		my @files=split /,/,$opts{files};
		for my $file(@files){
			if(! -e $file){
				usage("file $file not found");
			}
		}
		
		@{$param{prep}{files}}=@files;
	}
	
	if(! $opts{run_alias}){
		usage("run_alias not provided");
	}else{
		$param{run_alias}=$opts{run_alias};
	}

	if(! $opts{wdir} || ! -d $opts{wdir}){
		usage("Work directory not provided or nor found.");
	}else{
		$param{prep}{wdir}=$opts{wdir};
	}
	
	if(! $opts{egabox}){
		usage("egabox not provided");
	}else{
		$param{prep}{egabox}=$opts{egabox};
	}
	
	if(! $opts{pw}){
		usage("password for $opts{box} not provided");
	}else{
		$param{prep}{pw}=$opts{pw};
	}
	
	
	if(! $opts{stage_path}){
		usage("stage_path on $opts{box} not provided");
	}else{
		$param{prep}{stage_path}=$opts{stage_path};
		$param{run}{stage_path}=$opts{stage_path};
	}
	
	if(! $opts{keys}){
		usage("gpg encryption key(s) not provided");
	}else{
		my @keys=split /,/,$opts{keys};
		my $keystring=join(" ",map{"-r $_"} @keys);
		$param{prep}{keystring}=$keystring;
	}
	
	if(! $opts{egax}){
		usage("egax : a registered EGAX experiment accession with which to associate the run must be provided")
	}else{
		$param{run}{EGAX}=$opts{egax};
	}
	
	if(! $opts{run_date}){
		usage("run_date of the form YYMMDD must be provided");
	}else{
		$param{run}{run_date}=$opts{run_date};
	}
	
	if($opts{delete}){
		usage("Invalid value for option --delete, must be True or False") unless($opts{delete} eq "True" || $opts{delete} eq "False");
		$param{prep}{delete}=$opts{delete};
	}else{
		$param{prep}{delete}="True";
	}
	
	$param{study}{center_name}=$opts{study_center} || "OICR";
	$param{run}{run_center}=$opts{run_center} || "OICR";

	
    $param{prep}{xfer}=$opts{xfer} || "xfer.res.oicr.on.ca";
	$param{prep}{xfer_method}=$opts{xfer_method} || "lftp";
	
	return %param;

}

sub usage{
	print "\nupload_RUN.pl [options]\n";
	print "\nProvide file(s) and associated information to be incorporated into an EGA run object, encrypts, checksums and forms xml for metadata submission\n";
	print "Options are as follows:\n";
	print "\t--config String/filename. A configuration file, storing options as key value pairs\n";  
	print "\t--run_alias String.  An alias for the run object.  This must be unique to the ega-box, or it won't validate\n";
	print "\t--study_center String.  Where the study was conducted.  Default is OICR\n";
	print "\t--run_center String. Where the run was conducted. Default is OICR\n";
	print "\t--files String/filenames. Paths to files to incorporate into the run object, comma separated\n";
	print "\t--file_type String.  bam or fastq\n";
    print "\t--run_date String. provided as YYMMDD\n";
	print "\t--egax. The Registered EGAX accession with which to associate the run\n";
	print "\t--wdir  String/directory name. A directory where the file will be copied, encrypted and md5summed, prior to upload.\n";
	print "\t--egabox String.  The egabox for upload\n";
	print "\t--pw String. Password for the ega-box.\n";
	print "\t--stage_path String. Path on the ega-box where files should be uploaded.\n";
	print "\t--delete. True/False.  Should encrypted data be deleted after upload?  Default = True\n";
	print "\t--keys. String. A comma separated list of public gpg encryption keys..\n";
	print "\t--xfer. String.  The oicr xfer system to use for data upload.  Default : xfer.sftp.oicr.on.ca";
	print "\t--help displays this usage message.\n";

	die "\n@_\n\n";
}
