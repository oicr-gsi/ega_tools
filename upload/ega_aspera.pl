#! /usr/bin/perl

use strict;
use warnings;
use File::Basename;
use Data::Dumper;

PASS=""

my $pwd=`pwd`;chomp $pwd;
my $repo="/CPCG/BRCA/bam";


print STDERR "starting new job\n";



my $file=`ls to_upload | head -n 1`;chomp $file;   ### grab one file
(my $id=$file)=~s/\.bam//;
my $workdir="scratchprod/$id";
die "directory already exists for $id in scratch" if(-d $workdir);
mkdir $workdir;
my $fn=basename($file);
print "$file\n";
`mv to_upload/$file $workdir/$fn`;
 
(open my $QSUB,">","$workdir/process.sh") || die "could not open upload file";
print $QSUB "qsub -cwd -b y -q default -e $pwd/log -o $pwd/log -N ega.ascp.$id.premd5                                   \"md5sum $fn | cut -f1 -d\' \'> $fn.md5\"\n";
print $QSUB "qsub -cwd -b y -q default -e $pwd/log -o $pwd/log -N ega.ascp.$id.gpg  	                             \"gpg --trust-model always -r EGA_Public_key -r SeqProdBio -o $fn.gpg -e $fn\"\n";
print $QSUB "qsub -cwd -b y -q default -e $pwd/log -o $pwd/log -N ega.ascp.$id.postmd5   -hold_jid ega.ascp.$id.gpg     \"md5sum $fn.gpg | cut -f1 -d\' \'> $fn.gpg.md5\"\n";
print $QSUB "qsub -cwd -b y -q default -e $pwd/log -o $pwd/log -N ega.ascp.$id.size      -hold_jid ega.ascp.$id.gpg     \"du $fn.gpg > $fn.gpg.size \"\n";
print $QSUB "qsub -cwd -b y -q default -e $pwd/log -o $pwd/log -N ega.ascp.$id.upload    -hold_jid ega.ascp.$id.postmd5 \"bash upload.sh\"\n";
#print $QSUB "qsub -cwd -b y -q default -e $pwd/log -o $pwd/log -N ega.ascp.$id.rmgpg     -hold_jid ega.ascp.$id.upload  \"rm $fn.gpg;\"\n";
#print $QSUB "qsub      -b y -q default -e $pwd/log -o $pwd/log -N ega.ascp.$id.finish    -hold_jid ega.ascp.$id.rmgpg   \"mkdir $pwd/uploaded/$id;mv $pwd/$workdir/* $pwd/uploaded/$id/ \"\n";
#print $QSUB "qsub -wd $pwd -b y -q default -e $pwd/log -o $pwd/log -N ega.ascp.$id.startnext -hold_jid ega.ascp.$id.finish  \"sleep 5m;./ega_aspera.pl\"\n"; 
close $QSUB;

(open my $UP,">","$workdir/upload.sh") || die "unable to open upload script";
print $UP "ssh xfer4.res.oicr.on.ca \"export ASPERA_SCP_PASS=$PASS;".
          "~/.aspera/connect/bin/ascp -QT -l300M -L- -k2 $pwd/$workdir/$fn*.md5 ega-box-12\@fasp.ega.ebi.ac.uk:$repo;".
          "~/.aspera/connect/bin/ascp -QT -l300M -L- -k2 $pwd/$workdir/$fn.gpg ega-box-12\@fasp.ega.ebi.ac.uk:$repo;\"";
close $UP;
`(cd $workdir;bash process.sh)`;	
	





