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
		
	experiment=>{
		library_strategy => "WGS",
		library_source   => "GENOMIC",
		library_selection => "size fractionation",
		instrument_model => "Illumina HiSeq 2500",
		title=>"Library Preparation for Whole Genome Sequencing of CPCGene Prostate Cancer Samples",
	},
	
	study=>{
		center_name		=>	"OICR_ICGC",
		run_center		=>	"OICR",
		study_id		=>	"EGAS00001001615",
		study_title		=>	"MED12L Gene Alterations Define Aggressive BRCA2-Mutant Prostate Cancers",
		study_design	=>	"",
		broker_name		=> 	"EGA",
	},

	
);


### must provide a table of samples, and associated libraries and runs.  This must have columns library and run to be valid

my ($sample_xml,$seq_table)=@ARGV;
die "sample_xml folder not provided" unless($sample_xml && -d $sample_xml);
die "not sequencing table provided" unless($seq_table && -e $seq_table);
### get sample list from file
### sample list indicates libraries and runs for each sample

my %samples;  ### this hash will store the meta data info associated with each sample
load_sample_xml($sample_xml,\%samples);
load_sample_sequencing($seq_table,\%samples);

for my $sid(sort keys %samples){
	my @libs=keys %{$
	samples{$sid}{seq}};
	for my $lib(@libs){
		print "generating xml for $sid $lib\n";
		my $xml=experiment_xml($sid,$lib,\%p);
		(open my $XML,">","xmls/experiment/${lib}.xml") || die "unable to open experiment xml";
		print $XML $xml->toString(1);
		close $XML;
	}
}





