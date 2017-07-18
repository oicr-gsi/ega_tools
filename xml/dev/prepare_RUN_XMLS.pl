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
		
	study=>{
		center_name		=>	"OICR_ICGC",
		run_center		=>	"OICR",
		study_id		=>	"EGAS00001001615",
		study_title		=>	"MED12L Gene Alterations Define Aggressive BRCA2-Mutant Prostate Cancers",
		study_design	=>	"",
		broker_name		=> 	"EGA",
	},

	run=>{
		stage_path	=>"/CPCG/BRCA/fastq",
		title		=>"Sequencing runs for libraries generated from CPCGene Prostate Cancer Samples"
	},

);




### get sample list from file
### sample list indicates libraries and runs for each sample

my ($sample_xml,$experiment_xml,$seq_table,$fastq_table)=@ARGV;
die "sample_xml folder not provided" unless($sample_xml && -d $sample_xml);
die "experiment_xml folder not provided" unless($experiment_xml && -d $experiment_xml);
die "not sequencing table provided" unless($seq_table && -e $seq_table);




my %samples;  ### this hash will store the meta data info associated with each sample
load_sample_xml($sample_xml,\%samples);
load_experiment_xml($experiment_xml,\%samples);
load_sample_sequencing($seq_table,\%samples);



for my $sid(sort keys %samples){
	my @libs=keys %{$samples{$sid}{seq}};
	

	for my $lib(@libs){
		
		### do not proceed if the experiment xml doesn't exists
		if(!$samples{$sid}{exp}{$lib}){
			print "$lib has not yet been registered\n";
		}else{
			my @runs=keys %{$samples{$sid}{seq}{$lib}};
			for my $run(@runs){
				print "generating xml for $sid $lib $run\n";
				my $xml=run_xml($sid,$lib,$run,$samples{$sid}{seq}{$lib}{$run},\%p);
				(open my $XML,">","xmls/run/${lib}_${run}.xml") || die "unable to open run xml";
				print $XML $xml->toString(1);
				close $XML;				
				
		
				
			}
		}
	}
}





#my %experiments;
#load_registered_experiments("Experiments.Registered.txt",\%experiments);
#print Dumper(%samples);<STDIN>;

#my %fastq;
#load_fastq_files("fastq.md5.txt",\%fastq);













