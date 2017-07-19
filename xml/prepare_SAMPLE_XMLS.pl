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
my %p=init_parameters();


use Getopt::Long;
my %opts = ();
GetOptions(
	"config=s" 		=> \$opts{configfile},   ### configuration file where options are stored

	"table=s" 		=> \$opts{table},         ### folder with links to files that need encryption/upload
	"out=s" 		=> \$opts{out},           ### folder where work should be done, usually in scratch
	"merge_xml=s" 		=> \$opts{merge_xml},         ### name of merge file, where all XML should be saved
	"submission_xml=s"	=> \$opts{submission_xml},
	"submission_alias=s"	=> \$opts{submission_alias},
	"registered_samples" => \$opts{registered_samples}, ## a table with registered samples, must have an EGAN and alias column


	"help" 			=> \$opts{help},
);
%opts=validate_options(%opts);

## the table that is loaded are key value pairs


my %samples=load_sample_attributes($opts{table});
my $samplecount=scalar keys %samples;
print STDERR "$samplecount samples loaded from $opts{table}\n";


my %reg;
if($opts{registered_samples}){
	my @recs=`cat $opts{registered_samples}`;chomp @recs;
	my @h=split /\t/, shift @recs;
	my %REG;
	map{
		my %h;
		@h{@h}=split /\t/;
		$reg{$h{Alias}}=$h{EGAN};
	}@recs;
	my $registered_samplecount=scalar keys %reg;
	print STDERR "$registered_samplecount Registered samples loaded";
}	





my %xmlmerge;
for my $sid(sort keys %samples){
	print "generating xml for $sid\n";

	
    ## check if the sample is registered
	if(%reg && $reg{$sid}){
		print STDERR "A sample with alias $sid has already been registered on this box";
	
	}else{
		my $xml=sample_xml($sid,$samples{$sid},\%p);
		my $fn=$opts{out}."/${sid}.xml";
		(open my $XML,">",$fn) || die "unable to open sample xml";
		print $XML $xml->toString(1);
		close $XML;
	
		if($opts{merge_xml}){
			my $xmlstring=$xml->toString(1);
			my @lines=split /\n/,$xmlstring;
			shift @lines;
			$xmlstring=join("\n",@lines);
			$xmlmerge{$sid}=$xmlstring;
		}

	}
	
	
}




if($opts{merge_xml}){
	print STDERR "printing merge xml file $opts{merge_xml}\n";
	(open my $MERGE,">",$opts{merge_xml}) || die "unable to open merge file $opts{merge}";
	print $MERGE '<?xml version="1.0" encoding="utf-8"?>' . "\n";
	print $MERGE '<SAMPLE_SET xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLoction="ftp://ftp.sra.ebi.ac.uk/meta/xsd/sra_1_5/SRA.run.xsd">' . "\n";
	for my $sid(sort keys %xmlmerge){
		print $MERGE $xmlmerge{$sid} . "\n";
	}
	print $MERGE '</SAMPLE_SET>' . "\n";
	close $MERGE;
}

if($opts{submission_xml}){
	print STDERR "printing submission xml $opts{submission_xml}\n";
	(open my $SUB,">",$opts{submission_xml}) || die "unable to open merge file $opts{submission_xml}";
	print $SUB '<?xml version="1.0" encoding="utf-8"?>' . "\n";
	print $SUB '<SUBMISSION_SET xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="ftp://ftp.sra.ebi.ac.uk/meta/xsd/sra_1_5/SRA.submission.xsd">' ."\n";
	print $SUB '	<SUBMISSION alias="'.$opts{submission_alias} .'" center_name="' .$p{study}{center_name} . '" broker_name="' . $p{study}{broker_name} . '">' . "\n";
	print $SUB '		<ACTIONS>' . "\n";
	print $SUB '			<ACTION>' . "\n";
	print $SUB '				<ADD source="' . $opts{merge_xml} . '" schema="sample"/>' . "\n";
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
	 
	
	if(! $opts{table} || ! -e $opts{table}){
		usage("Sample table not provided or not found.");
	}else{
		### check the header line of the sample table
		my %headers;
		my $headerline=`head -n1 $opts{table}`;
		chomp $headerline;
		map{$headers{$_}=1} split /\t/,$headerline;
		
		## checks on the headers
		my %missing;
		for my $key(qw/Sample donor_id Gender Phenotype/){
			$missing{$key}++ unless($headers{$key})
		}
		if(%missing){
			my $missingstring=join(",",sort keys %missing);
			usage("The following keyys are missing from the headerline of the sample table : $missingstring");
		}
	}
	if(! $opts{out} || ! -d $opts{out}){
		usage("Directory to save XML output not indicated or not found.");
	}
	if($opts{merge_xml} && $opts{merge_xml}!~/xml$/){
		usage("merge file should have and xml extension");
	}
	
	if($opts{submission_xml} && $opts{submission_xml}!~/xml$/){
		usage("submission xml file should have and xml extension");
	}
	
	if($opts{submission_xml} && ! $opts{submission_alias}){
		usage("submission xml indicated but not alias provided");
	}
	
	return %opts;
}

sub usage{
	print "\nprepare_SAMPLE_XMLS.pl [options]\n";
	print "Options are as follows:\n";
	print "\t--table String/filename. Sample table, with information required to form xml\n";  ### currently not supported
	print "\t--out  String/directory name.  A directory to save individual xmls for each sample.\n";
	print "\t--merge_xml  String/filename. A single file to save the merged xml from this process.\n";
	print "\t--submission_xml  String/filename. A single file to save the submission xml from this process.\n";
	print "\t--submission_alias  String. A unique alias for the submission.  Required for submission_xml\n";
	
	print "\t--help displays this usage message.\n";

	die "\n@_\n\n";
}





