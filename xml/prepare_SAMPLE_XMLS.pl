#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use XML::LibXML;
use XML::Simple;
use File::Basename;
use lib dirname (__FILE__);
use prepare_XML;

my %p=(

	xml=>{
		xmlns		=> "http://www.w3.org/2001/XMLSchema-instance",
		xsi			=> "ftp://ftp.sra.ebi.ac.uk/meta/xsd/sra_1_5/SRA.run.xsd",
	},
);


load_defaults("study.txt",\%p,"study");
load_defaults("sample.txt",\%p,"sample");



sub load_defaults{
	
	my ($fn,$p,$category)=@ARGV;
	(open my $FH,"<",$fn)  || die "unable to open filename";
	while(<STDIN>){
		chomp;
		my($k,$v)=split /\t/;
		$$p{$category}{$k}=$v;
	}
}






### get sample list from file
### sample list indicates libraries and runs for each sample

my ($table)=@ARGV;

die "no sample table provided" unless($table and -e $table);


my %samples;  ### this hash will store the meta data info associated with each sample
load_sample_info($table,\%samples);

my $XML;

for my $sid(sort keys %samples){
	print "generating xml for $sid\n";
	my $xml=sample_xml($sid,$samples{$sid},\%p);
	(open $XML,">","xmls/sample/${sid}.xml") || die "unable to open sample xml";
	print $XML $xml->toString(1);
	close $XML;
}







