#! /usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use XML::LibXML;
use File::Basename;
use lib dirname (__FILE__);
use EGA_XML;


## %p is a list of parameters.  Currently changes to this need to be edited in the script.  move this to a file, or set as defaults with ability to vary
my %p=init_parameters();


use Getopt::Long;
my %opts = ();
GetOptions(
	"config=s" 		=> \$opts{configfile},   ### configuration file where options are stored

	"study_id=s"	=> \$opts{study_id},  ### EGAS accession with which to associate this data
	"study_center=s"	=> \$opts{study_center}, ### center to associate with the study
	
	"center=s"	=> \$opts{center}, ### center where library prep was done

#	"title=s"		=> \$opts{title},
	"lib_strategy=s" => \$opts{lib_strategy},
	"lib_source=s"	=> \$opts{lib_source},
	"lib_selection=s" => \$opts{lib_selection},
	"instrument_model=s" => \$opts{instrument_model},
	
	"description=s"  => \$opts{description}, ## a global description common to all experiments
	
	"table=s"		=> \$opts{table},

	"merge_xml=s" 			=> \$opts{merge_xml},         ### name of merge file, where all XML should be saved
	"submission_xml=s"		=> \$opts{submission_xml},
	"submission_alias=s"	=> \$opts{submission_alias},

	"out=s"					=> \$opts{out},
	"help" 			=> \$opts{help},
);

%opts=validate_options(%opts);


$p{study}{center_name}=$opts{study_center} if($opts{study_center});

$p{experiment}={
	center_name=>$opts{center},
	study_id=>$opts{study_id},	
};

for my $key(qw/lib_strategy lib_source lib_selection intsrument_model description/){
	$p{experiment}{$key}=$opts{$key} || 0;
}




my %experiments=load_experiment_table($opts{table},\%p);

#print Dumper(%opts);exit;

my %xmlmerge;
for my $alias(sort keys %experiments){
		print "generating xml for $alias\n";
		
		my %p2=%p;
		map{$p2{experiment}{$_}=$experiments{$alias}{$_}} keys %{$experiments{$alias}};
		#print Dumper(%p2);
		#<STDIN>;

		my $xml=experiment_xml($alias,\%p2);
		(open my $XML,">","$opts{out}/${alias}.xml") || die "unable to open experiment xml";
		print $XML $xml->toString(1);
		close $XML;
		
		if($opts{merge_xml}){
			my $xmlstring=$xml->toString(1);
			my @lines=split /\n/,$xmlstring;
			shift @lines;
			$xmlstring=join("\n",@lines);
			$xmlmerge{$alias}=$xmlstring;
		}
}


if($opts{merge_xml}){
	print STDERR "printing merge xml file $opts{merge_xml}\n";
	(open my $MERGE,">",$opts{merge_xml}) || die "unable to open merge file $opts{merge_xml}";
	print $MERGE '<?xml version="1.0" encoding="utf-8"?>' . "\n";
	print $MERGE '<EXPERIMENT_SET xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLoction="ftp://ftp.sra.ebi.ac.uk/meta/xsd/sra_1_5/SRA.experiment.xsd">' . "\n";
	for my $sid(sort keys %xmlmerge){
		print $MERGE $xmlmerge{$sid} . "\n";
	}
	print $MERGE '</ANALYSIS_SET>' . "\n";
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

	### submission xml should contain only the name of the merge file, NOT the full path
	my $merge_xml=basename($opts{merge_xml});

	print $SUB '				<ADD source="' . $merge_xml . '" schema="experiment"/>' . "\n";
	print $SUB '			</ACTION>' . "\n";
	print $SUB '			<ACTION>' . "\n";
	print $SUB '				<PROTECT/>' . "\n";
	print $SUB '			</ACTION>' . "\n";
	print $SUB '		</ACTIONS>' . "\n";
	print $SUB '	</SUBMISSION>' . "\n";
	print $SUB '</SUBMISSION_SET>' . "\n";
	close $SUB;
}



sub load_experiment_table{
	my ($file,$p)=@_;
	
	#print Dumper($p);<STDIN>;
	
	my %table;

	(open my $FH,"<",$file) || usage("unable to open experiment table from $file");
	my $headerline=<$FH>;chomp $headerline;
	my @headings=split /\t/,$headerline;
	
	#### validate the table, ensure that the right columns are present
	
	my %headings;map{$headings{$_}++}@headings;
	for my $key(qw/EGAN alias insertsize lib/){
		usage("experiment table must include column $key") unless($headings{$key});
	}
	for my $key(qw/description lib_strategy lib_source lib_selection instrument_model/){
		usage("experiment table must include the column $key if not provided as a global argument") unless($headings{$key} || $$p{experiment}{$key});
	}
	### check that required headings are available
	while(my $rec=<$FH>){
		chomp $rec;
		my %h;
		@h{@headings}=split /\t/,$rec;
		
		%{$table{$h{alias}}}=%h;
	}
	return %table;
}






sub validate_options{
	my (%opts)=@_;
	usage("Help requested.") if($opts{help});
	

	### load options from config file if this is requested
	if($opts{configfile}){
		if( ! -e $opts{configfile} ){
			 usage("Configuration file not provided or not found.");
		 }else{
			 my %config;
			 my @recs=`cat $opts{configfile} | grep ":"`;
			 map{  chomp;
			       my($k,$v)=split /:/;
				   $config{$k}=$v
			 }@recs;
			 map{$opts{$_} = $opts{$_} || $config{$_}} keys %config;
		 }
	 }

	### ensure that required arguments have values
 	for my $req_arg(qw/study_id out table/){
 		if(! $opts{$req_arg}){
 		usage("$req_arg must be provided.");
 		}
	}
	
	$opts{center}="OICR" unless($opts{center_name});
	

	if(! $opts{table} || ! -e $opts{table}){
		usage("A file with a data table containing experiment information was not indicated or not found.");
	}

	if(! $opts{out} || ! -d $opts{out}){
		usage("Directory to save XML output not indicated or not found.");
	}
	
	
	if($opts{merge_xml}){
		usage("merge file should have and xml extension") if($opts{merge_xml}!~/xml$/);
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
	print "\nprepare_EXPERIMENT_XMLS.pl [options]\n";
	print "Options are as follows:\n";
	print "\t--table String/filename. Required. A file with a table experiments to register\n";  
	print "\t\tTable header must include : 'sample (EGAN accession or registered Alias), alias (for the analysis object), insertsize\n";  
	print "\t--study_id String. Required.  An EGAS accession\n";  
	print "\t--center_name String/filename. Required. The center where the expereiments were carried out. Defaults as OICR\n";  
	print "\t--description String. Optional/Required.  This can be specified globally for all experiments to be registered or as a column in the table\n";
	print "\t--lib_strategy String.  Optional/Required.  This can be specified globally for all experiments to be registered or as a column in the table\n";
	print "\t\tValid arguments : WGS WGA WXS RNA-Seq + others\n";
	print "\t--lib_source String.  Optional/Required.  This can be specified globally for all experiments to be registered or as a column in the table\n";
	print "\t\tValid arguments : GENOMIC TRANSCRIPTOMIC METAGNOMIC SYNTHETIC + others\n";
	print "\t--lib_selection  String.  Optional/Required.  This can be specified globally for all experiments to be registered or as a column in the table\n";
	print "\t\tValid arguments : RANDOM PCR 'size fractionation' + others\n";
	print "\t--instrument_model	String.  Optional/Required.  This can be specified globally for all experiments to be registered or as a column in the table\n";
	print "\t\tValid arguments : 'HiSeq X Ten' 'Illumina HiSeq 2500' 'Illumina HiSeq 2000' 'Illumina MiSeq' + others\n";
	
	print "\t--out  String/directory name.  Required. A directory to save individual xmls for each experiment.\n";
	print "\t--merge_xml  String/filename. A single file to save the merged xml from this process.\n";
	print "\t--submission_xml  String/filename. A single file to save the submission xml from this process.\n";
	print "\t--submission_alias  String. A unique alias for the submission.  Required for submission_xml\n";
	
	print "\t--help displays this usage message.\n";

	die "\n@_\n\n";
}

















