#! /usr/bin/perl

use strict;
use warnings;
use File::Basename;
use Data::Dumper;

my $pwd=`pwd`;chomp $pwd;


print STDERR "starting new job\n";

my $remote_dir="/haltwg/bam";

my $file=`ls to_upload | head -n 1`;chomp $file;   ### grab one file
my $fn=basename($file);
(my $id=$fn)=~s/\.fastq.*//;
my $dir="scratch/$id";
die "directory already exists for $id in scratch" if(-d $dir);
mkdir $dir;
print "$file\n";
`mv to_upload/$file $dir/$file`;
 
(open my $QSUB,">","$dir/process.sh") || die "could not open upload file";
print $QSUB "qsub -cwd -b y -e $pwd/logs -o $pwd/logs -N ega.$id.premd5                              \"md5sum $fn | cut -f1 -d\' \'> $fn.md5\"\n";
print $QSUB "qsub -cwd -b y -e $pwd/logs -o $pwd/logs -N ega.$id.gpg  	                             \"gpg --trust-model always -r EGA_Public_key -r SeqProdBio -o $fn.gpg -e $fn\"\n";
print $QSUB "qsub -cwd -b y -e $pwd/logs -o $pwd/logs -N ega.$id.postmd5   -hold_jid ega.$id.gpg     \"md5sum $fn.gpg | cut -f1 -d\' \'> $fn.gpg.md5\"\n";
print $QSUB "qsub -cwd -b y -e $pwd/logs -o $pwd/logs -N ega.$id.size      -hold_jid ega.$id.gpg     \"du $fn.gpg > $fn.gpg.size \"\n";
print $QSUB "qsub -cwd -b y -e $pwd/logs -o $pwd/logs -N ega.$id.upload    -hold_jid ega.$id.postmd5 \"bash upload.sh\"\n";
#print $QSUB "qsub -cwd -b y -e $pwd/logs -o $pwd/logs -N ega.$id.rmgpg     -hold_jid ega.$id.upload  \"rm $fn.gpg;\"\n";
#print $QSUB "qsub      -b y -e $pwd/logs -o $pwd/logs -N ega.$id.finish    -hold_jid ega.$id.rmgpg   \"mv $pwd/$dir $pwd/uploaded/$id \"\n";
#print $QSUB "qsub -wd $pwd -b y -e $pwd/logs -o $pwd/logs -N ega.$id.startnext -hold_jid ega.$id.finish  \"sleep 5m;./ega.pl\"\n"; 
print $QSUB "qsub -wd $pwd -b y -e $pwd/logs -o $pwd/logs -N ega.$id.startnext -hold_jid ega.$id.upload  \"sleep 5m;$pwd/ega.lftp.pl\"\n";
close $QSUB;

(open my $UP,">","$dir/upload.sh") || die "unable to open upload script";
print $UP "ssh xfer.hpc.oicr.on.ca \"lftp -u ega-box-137,GnwyrZm6 -e \\\"set ftp:ssl-allow false; mput $pwd/$dir/$fn.gpg $pwd/$dir/$fn*.md5 -O $remote_dir; bye;\\\" ftp://ftp-private.ebi.ac.uk\"";
close $UP;




`(cd $dir;bash process.sh)`;	
	





