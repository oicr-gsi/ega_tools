#! /usr/bin/perl

use strict;
use warnings;
use Data::Dumper;


use Getopt::Long;
my %opts = ();
GetOptions(
	"user=s"	=> \$opts{user},  
	"pw=s"		=> \$opts{pw},
	"host=s"	=> \$opts{host},
	"path=s"	=> \$opts{path},
	"help" 			=> \$opts{help},
);

usage("Help requested.") if($opts{help});
$opts{host}= $opts{host} || "ftp://ftp-private.ebi.ac.uk";

for my $req_key(qw/user pw path/){
	usage("$req_key must be provided") unless($opts{$req_key});
}

my $cred="$opts{user}:$opts{pw}";
my $hostpath="$opts{host}/$opts{path}";

### get the directory listing
my $cmd="curl $hostpath/ --user $cred";
my @recs=`$cmd`;

for my $rec(@recs){
	chomp $rec;
	my @f=split /\s+/,$rec;
	
	### the filename is the last element in the listing
	my $fn=pop @f;
	

	if($fn=~/md5/){
		my $cmd="curl $hostpath/$fn --user $cred";
		my $md5=`$cmd`;chomp $md5;
		print "$fn\t$md5\n";
	}else{
		print "$fn\n";
	}

}

sub usage{
	print "\nget_ega-box_files.pl [options]\n";
	print "\nThis tool will provide a list of files on a path on a ega-box, and the contents of any md5 files\n";
	print "Options are as follows:\n";
	print "\t--user String. Required. The ega-box user\n";  
	print "\t--pw String. Required. The ega-box password\n";  
	print "\t--host String. Optional. The ega host, defaults to ftp://ftp-private.ebi.ac.uk\n";  
	print "\t--path String. Required. The path on the ega-box\n";  
	print "\t--help displays this usage message.\n";

	die "\n@_\n\n";
}

