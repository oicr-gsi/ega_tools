#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long;
use File::Basename;
use lib dirname(__FILE__);
use EGA_XML;
use Data::Dumper;

## this script will generate bash scripts and organize to do the following
## 1. create a folder where to do the work
## 2. make a link to the fastq files
## 3. generate an md5sum
## 4. encrypt the files
## 5. generate an md5sum, on the gpgp encrypted file
## 6. upload all the files to the staging server
## 7. form the xml, using hte XML.pm library
## 8. check that the data is uploaded
## 9. submit the xml
## 10. capture the receipt

my %opts;

my($R1,$R2,$id,$stagepath,$workdir,$xmlini)=@_;
GetOptions(
	\%opts,
	'id=s'			=> \$id,
	'R1=s'			=> \$R1,
	'R2=s'			=> \$R2,
	'stage=s'		=> $stagepath,
	'workdir=s'		=> \$workdir,
	'xmlini' 		=> \$XMLini
	#'help|h!'		=> \$param{help}
);

### validation
die "R1 file $R1 does not exist" unless(-e $R1);
die "R2 file $R2 does not exist" unless(-e $R2);

die "id is not provided" unless($id);
die "path on stage is not provided" unless($stage);

die "workdir not provided" unless($workdir);
die "$workdir does not exist" unless(-d $workdir);

$workdir.="/".$id;
mkdir $workdir || die "unable to create $workdir";
mkdir "$workdir/logs" || die "unable to create logs dir under $workdir";

die "xmlini file is not valid" unless(validate_ini($xmlini));


### if here then all the data provided is valid.  now need to create the qsubs



(open my $UP,">","$workdir/upload.sh") || die "unable to open upload script";
print $UP "ssh xfer4.res.oicr.on.ca \"export ASPERA_SCP_PASS=pkcUoT6w;".
			"~/.aspera/connect/bin/ascp -QT -l300M -L- -k2 *.md5 ega-box-12\@fasp.ega.ebi.ac.uk:$stagepath;".
			"~/.aspera/connect/bin/ascp -QT -l300M -L- -k2 *.gpg ega-box-12\@fasp.ega.ebi.ac.uk:$stagepath;\"";
close $UP;


(open my $QSUB,">","$workdir/process.sh") || die "could not open upload file";
for my $R(qw/R1 R2/){
	my $file=$xml_param{run}{$R};
	
	die "$R file $file does not exist" unless(-e $file);
	
	my $fn=basename($file);

	`ln -s $file $workdir/$fn`;
	
	print $QSUB "qsub -cwd -b y -q default -e logs -o logs -N ega.ascp.$id.premd5 \"md5sum $fn | cut -f1 -d\' \'> $fn.md5\"\n";
	print $QSUB "qsub -cwd -b y -q default -e logs -o logs -N ega.ascp.$id.gpg \"gpg --trust-model always -r EGA_Public_key -r SeqProdBio -o $fn.gpg -e $fn\"\n";
	print $QSUB "qsub -cwd -b y -q default -e logs -o logs -N ega.ascp.$id.postmd5   -hold_jid ega.ascp.$id.gpg     \"md5sum $fn.gpg | cut -f1 -d\' \'> $fn.gpg.md5\"\n";
}	
print $QSUB "qsub -cwd -b y -q default -e log -o log -N ega.ascp.$id.upload  :--hold_jid ega.ascp.$id.postmd5 \"bash upload.sh\"\n";
#print $QSUB "qsub -cwd -b y -q default -e $pwd/log -o $pwd/log -N ega.ascp.$id.rmgpg     -hold_jid ega.ascp.$id.upload  \"rm $fn.gpg;\"\n";
#print $QSUB "qsub      -b y -q default -e $pwd/log -o $pwd/log -N ega.ascp.$id.finish    -hold_jid ega.ascp.$id.rmgpg   \"mkdir $pwd/uploaded/$id;mv $pwd/$workdir/* $pwd/uploaded/$id/ \"\n";
#print $QSUB "qsub -wd $pwd -b y -q default -e $pwd/log -o $pwd/log -N ega.ascp.$id.startnext -hold_jid ega.ascp.$id.finish  \"sleep 5m;./ega_aspera.pl\"\n"; 
close $QSUB;
close $UP;
#`(cd $workdir;bash process.sh)`;







