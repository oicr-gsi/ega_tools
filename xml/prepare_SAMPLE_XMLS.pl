#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use XML::LibXML;
#use XML::Simple;
use File::Basename;
use lib dirname (__FILE__);
use EGA_XML;


## %p is a list of parameters.  Currently changes to this need to be edited in the script.  move this to a file, or set as defaults with ability to vary
my %p=(

	xml=>{
		xmlns		=> "http://www.w3.org/2001/XMLSchema-instance",
		xsi			=> "ftp://ftp.sra.ebi.ac.uk/meta/xsd/sra_1_5/SRA.run.xsd",
	},
	
	sample=>{
		taxon_id		=> 9606,
		scientific_name	=> "homo_sapiens",
		common_name		=> "human",
		title			=> "",  ### to be filled in
	},
	
	study=>{
		center_name		=>	"OICR_ICGC",
		run_center		=>	"OICR",
		study_id		=>	"",
		study_title		=>	"",
		study_design	=>	"",
		broker_name		=> 	"EGA",
	},	
	
	
	
);

## the table that is loaded are key value pairs
my ($table)=@ARGV;

die "no sample table provided" unless($table and -e $table);
my %samples=load_sample_attributes($table);


### load the registered samples, this will be a resource to look for reuse of aliases
my @recs=`cat EGA_Samples.txt`;chomp @recs;
my @h=split /\t/, shift @recs;
my %REG;
map{
	my %h;
	@h{@h}=split /\t/;
	$REG{$h{Alias}}=$h{EGAN};
}@recs;




for my $sid(sort keys %samples){
	print "generating xml for $sid\n";
	
	my $xml=sample_xml($sid,$samples{$sid},\%p);
	my $fn="XML/${sid}.xml";
	(open my $XML,">",$fn) || die "unable to open sample xml";
	print $XML $xml->toString(1);
	close $XML;
	
}







