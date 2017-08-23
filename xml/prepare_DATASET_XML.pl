#! /usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use XML::LibXML;
use File::Basename;
use lib dirname (__FILE__);
use EGA_XML;


### get these from the receipt, as all was registered in a single file

## %p is a list of parameters.  Currently changes to this need to be edited in the script.  move this to a file, or set as defaults with ability to vary



use Getopt::Long;
my %opts = ();
GetOptions(
	"config=s" 		=> \$opts{configfile},   ### configuration file where options are stored
	"study_center=s"	=> \$opts{center_name}, ### center to associate with the data, defaults to OICR
	"alias=s"		=> \$opts{alias},
	"title=s"		=> \$opts{title},
	"description=s"	=> \$opts{description},
	"type=s"		=> \$opts{type},
	"accession_list=s"	=> \$opts{accession_list},
	"out=s"			=> \$opts{xml},
	"policy=s"	=> \$opts{policy},
	"policy_center=s" => \$opts{policy_center},
	"submission_xml=s"=> \$opts{submission_xml},
	"submission_alias=s"=> \$opts{submission_alias},
	"help" 			=> \$opts{help},
);

my %p=validate_options(%opts);


my @acc=`cat $opts{accession_list}`;chomp @acc;
### accessions should all begin with EGAR/EGAZ
unless(@acc){
	usage("no accessions found in the accessions list");
}else{
	my @invalid;
	for my $acc(@acc){
		push(@invalid,$acc) unless($acc=~/^EGA[RZ]\d{11}$/);
	}
	if(@invalid){
		my $invalid_list=join(",",@invalid);
		usage("invalid accessions found : $invalid_list");
	}
}
my $xml=dataset_xml(\%p,@acc);

(open my $XMLOUT,">",$opts{xml}) || usage("unable to open output file $opts{out}");
print $XMLOUT $xml->toString(1);
close $XMLOUT;




sub dataset_xml{
	
	my($p,@accessions)=@_;
	
	
	
	### DATASET XML
	my $xml=XML::LibXML::Document->new('1.0','utf-8');
	my $root		= $xml->createElement("DATASETS");
	$root->setAttribute("xmlns:xsi"=>"http://www.w3.org/2001/XMLSchema-instance");
	$root->setAttribute("xsi:noNamespaceSchemaLocation"=>"ftp://ftp.sra.ebi.ac.uk/meta/xsd/sra_1_5/SRA.dataset.xsd");

	my $dataset		= $xml->createElement("DATASET");
	$dataset->setAttribute(alias        => $$p{dataset}{alias});
	$dataset->setAttribute(center_name  => $$p{study}{center_name});
	$dataset->setAttribute(broker_name   =>$$p{study}{broker_name});

	my $title		= $xml->createElement("TITLE");
	$title->appendTextNode($$p{dataset}{title});
	$dataset->appendChild($title);

	my $description		= $xml->createElement("DESCRIPTION");
	$description->appendTextNode($$p{dataset}{description});
	$dataset->appendChild($description);


	my $datasettype	= $xml->createElement("DATASET_TYPE");
	$datasettype->appendTextNode($$p{dataset}{type});
	$dataset->appendChild($datasettype);

	for my $acc(@accessions){
		my $analysis_ref	= $xml->createElement("ANALYSIS_REF");
		$analysis_ref->setAttribute(accession=>$acc);
		$dataset->appendChild($analysis_ref);
	}
	my $policy_ref		= $xml->createElement("POLICY_REF");
	$policy_ref->setAttribute(refname=>$$p{policy}{acc});
	$policy_ref->setAttribute(refcenter=>$$p{policy}{center});
	$dataset->appendChild($policy_ref);

	$root->appendChild($dataset);
	$xml->setDocumentElement($root);
	
	return $xml;
}

if($opts{submission_xml}){
	print STDERR "printing submission xml $opts{submission_xml}\n";
	(open my $SUB,">",$opts{submission_xml}) || die "unable to open merge file $opts{submission_xml}";
	print $SUB '<?xml version="1.0" encoding="utf-8"?>' . "\n";
	print $SUB '<SUBMISSION_SET xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="ftp://ftp.sra.ebi.ac.uk/meta/xsd/sra_1_5/SRA.submission.xsd">' ."\n";
	print $SUB '	<SUBMISSION alias="'.$opts{submission_alias} .'" center_name="' .$p{study}{center_name} . '" broker_name="' . $p{study}{broker_name} . '">' . "\n";
	print $SUB '		<ACTIONS>' . "\n";
	print $SUB '			<ACTION>' . "\n";

	### submission xml should contain only the name of the merge file, NOT the full path
	my $dataset_xml=basename($opts{xml});

	print $SUB '				<ADD source="' . $dataset_xml . '" schema="dataset"/>' . "\n";
	print $SUB '			</ACTION>' . "\n";
	print $SUB '			<ACTION>' . "\n";
	print $SUB '				<PROTECT/>' . "\n";
	print $SUB '			</ACTION>' . "\n";
	print $SUB '		</ACTIONS>' . "\n";
	print $SUB '	</SUBMISSION>' . "\n";
	print $SUB '</SUBMISSION_SET>' . "\n";
	close $SUB;
}




sub validate_options{
	my (%opts)=@_;
	
	usage("Help requested.") if($opts{help});
	
	my %param=init_parameters();
	
	for my $req_arg(qw/alias title description type/){
		if(! $opts{$req_arg}){
			usage("$req_arg must be provided.");
		}else{
			$param{dataset}{$req_arg}=$opts{$req_arg};
		}
	}	
	
	if(! $opts{xml}){
		usage("output xml file not provided");
	}elsif($opts{xml} !~/\.xml$/){
		usage("output file must have a .xml extension");
	}
	
	
	$param{study}{center_name}="OICR" unless($opts{center_name});
	## check the dataset type
	my %valid_types=map{$_,1} ("Whole genome sequencing","Exome sequencing","Genotyping by array","Transcriptome profiling by high-throughput sequencing",
	"Transcriptome profiling by array","Amplicon sequencing","Methylation binding domain sequencing","Methylation profiling by high-throughput sequencing",
	"Phenotype information","Study summary information","Genomic variant calling");
	
	if(! $opts{type}){
		usage("dataset type not provided");
	}elsif(!$valid_types{$opts{type}}){
		usage("invalid dataset type : $opts{type}")
	}else{
		$param{dataset}{type}=$opts{type};
	}
	
	if(! $opts{accession_list} || ! -e $opts{accession_list}){
		print usage("file with list of accessions not provided or not found");
	}
	
	if(! $opts{policy} || $opts{policy}!~/^EGAP\d{11}$/){
		print usage("a valid policy accession must be provided");
	}else{
		$param{policy}{acc}=$opts{policy};
	}
	if($opts{policy_center}){
		$param{policy}{center}=$opts{policy_center};
	}else{
		$param{policy}{center}=$param{study}{center_name};
	}
	
	
	if($opts{submission_xml} && $opts{submission_xml}!~/xml$/){
		usage("submission xml file should have and xml extension");
	}
	
	if($opts{submission_xml} && ! $opts{submission_alias}){
		usage("submission xml indicated but not alias provided");
	}

	return %param;
}


sub usage{
	print "\nprepare_DATASET_XMLS.pl [options]\n";
	print "Options are as follows:\n";
	print "\t--config String/filename. Optional. A file with configuration options\n";  
	print "\t--out String/filename.  Required. The output file, must have an .xml extension\n";
	print "\t--study_center String. Optional. Defaults to OICR\n";
	print "\t--alias String. Required.  A unique alias for the dataset\n";
	print "\t--title String. Required. A title for the dataset\n";
	print "\t--description String. Required. A description for the dataset\n";
	print "\t--type String.  Required. A valid dataset type. See EGA documentaion\n";  
	print "\t--accession_list String/filename A file with accessions to include in the dataset.  Must be EGAR or EGAZ accessions.\n";
	print "\t--policy  String. A registered policy accession EGAP\n";
	print "\t--policy_center. Optional. Default so study_center\n";
	print "\t--submission_xml  String/filename. A single file to save the submission xml from this process.\n";
	print "\t--submission_alias  String. A unique alias for the submission.  Required for submission_xml\n";
	
	print "\t--help displays this usage message.\n";

	die "\n@_\n\n";
}






