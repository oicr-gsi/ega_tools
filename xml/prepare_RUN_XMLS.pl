#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use XML::LibXML;
use File::Basename;
use lib dirname (__FILE__);
use EGA_XML;

my %p=init_parameters();


use Getopt::Long;
my %opts = ();
GetOptions(
	"config=s" 		=> \$opts{configfile},   ### configuration file where options are stored
	"study_center=s" => \$opts{study_center},  ### this can be provided as a global, or as a column in the file_table
	"run_center=s"	=> \$opts{run_center}, ### center to associate with the data, defaults to OICR
	"stage_path=s"  => \$opts{stage_path},  ### path ont he staging server where uploaded files can be found
	"file_table=s" 			=> \$opts{file_table},         ### folder with links to files that need encryption/upload
	"file_type=s"			=> \$opts{file_type},	
	"merge_xml=s" 			=> \$opts{merge_xml},         ### name of merge file, where all XML should be saved
	"submission_xml=s"		=> \$opts{submission_xml},
	"submission_alias=s"	=> \$opts{submission_alias},
	#"registered_samples=s" 	=> \$opts{registered_samples}, ## a table with registered samples, must have an EGAN and alias column

	"out=s"					=> \$opts{out},

	"help" 			=> \$opts{help},
);


%opts=validate_options(%opts);

$p{study}{center_name}=$opts{study_center} if($opts{study_center});
$p{run}{run_center}=$opts{run_center} if($opts{run_center});
$p{run}{stage_path}=$opts{stage_path};


my %runs=load_run_table($opts{file_table});



my %xmlmerge;
for my $alias(sort keys %runs){
	
	print "$alias\n";
	my %p2=%p;
	map{$p2{run}{$_}=$runs{$alias}{$_}} keys %{$runs{$alias}};
	
	
	$p2{run}{EGAX}=$p2{run}{experiment} if($p2{run}{experiment}=~/EGAX/);
	
	my $xml;
	if($opts{file_type} eq "fastq"){
		$xml=run_xml_fastq($alias,\%p2);
	}
	(open my $XML,">","$opts{out}/${alias}.xml") || die "unable to open run xml";
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








sub load_run_table{
	my ($file,$p)=@_;
	
	#print Dumper($p);<STDIN>;
	
	my %table;

	(open my $FH,"<",$file) || usage("unable to open run table from $file");
	my $headerline=<$FH>;chomp $headerline;
	my @headings=split /\t/,$headerline;
	
	#### validate the table, ensure that the right columns are present
	
	my %headings;
	map{$headings{$_}++} @headings;
	
	my @req_keys= (qw/experiment alias file run_date md5 encrypted_file encrypted_md5/);
	push(@req_keys,"readid") if($opts{file_type} eq "fastq");
			
	
	for my $key(@req_keys){
			usage("experiment table must include column $key") unless($headings{$key});
	}
	for my $key(qw/run_center/){
		usage("sample table must include the column $key if not provided as a global argument") unless($headings{$key} || $$p{run}{$key});
	}
	### check that required headings are available
	while(my $rec=<$FH>){
		chomp $rec;
		my %h;
		@h{@headings}=split /\t/,$rec;
		
		#print Dumper(%h);<STDIN>;
		my $alias=$h{alias};
		
		### check on the run data
		usage("rundate must be in for form YYMMDD")  unless($h{run_date}=~/^\d{6}$/);

		if($opts{file_type} eq "fastq"){
			my $readid=$h{readid};
			
			
			
			### rais an error if the alias ifnormation does not match
			map{
				if($table{$alias}{$_}){
					usage("inconsistent values for $_ in file table for alias $alias") unless($table{$alias}{$_} eq $h{$_});
				}else{
					$table{$alias}{$_}=$h{$_};
				}
			} qw/experiment run_center run_date/;
			
			map{$table{$alias}{$readid}{$_}=$h{$_}} qw/file md5 encrypted_file encrypted_md5/;
		}else{
			### not paired data, therefore bam?

			%{$table{$h{alias}}}=%h;
		}	
	}
	return %table;
}

if($opts{merge_xml}){
	print STDERR "printing merge xml file $opts{merge_xml}\n";
	(open my $MERGE,">",$opts{merge_xml}) || die "unable to open merge file $opts{merge_xml}";
	print $MERGE '<?xml version="1.0" encoding="utf-8"?>' . "\n";
	print $MERGE '<RUN_SET xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLoction="ftp://ftp.sra.ebi.ac.uk/meta/xsd/sra_1_5/SRA.run.xsd">' . "\n";
	for my $sid(sort keys %xmlmerge){
		print $MERGE $xmlmerge{$sid} . "\n";
	}
	print $MERGE '</RUN_SET>' . "\n";
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

	print $SUB '				<ADD source="' . $merge_xml . '" schema="run"/>' . "\n";
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
	
	

	
	if(! $opts{file_table} || ! -e $opts{file_table}){
		usage("file_table, a list of files, md5sums and other data, not provided or not found");
	}
	
	if(! $opts{stage_path}){
		usage("stage_path must be provided.");
	} 
	
	if(! $opts{file_type}){
		usage("--file_type must be specified, either bam or vcf");
	}elsif(!grep{$opts{file_type} eq $_} qw/fastq bam/){
		usage("--file_type must be either fastq or bam");
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
	print "\nprepare_SAMPLE_XMLS.pl [options]\n";
	print "Options are as follows:\n";
	print "\t--file_table String/filename. Required. A file with a table describing files that have been uploaded and are ready to register\n";  
	print "\t\tTable header must include : \n";
	print "\t\tfastq : 'experiment (EGAX accession or registered Alias), alias (for the run object),run_date,readid (R1/R2), file,md5,encrypted_file,encrypted_md5\n";  
	print "\t\tbam : 'experiment (EGAX accession or registered Alias), alias (for the run object),run_date,file,md5,encrypted_file,encrypted_md5\n";  
	
	print "\t--file_type String/filename. Required.  Either fastq or bam\n";
	print "\t--stage_path String. Required.  Path on the staging server where files have been uploaded\n";  
	
	print "\t--study_center String. Defaults to OICR, generally either OICR or OICR_ICGC\n";  
	print "\t--run_center String. Where the data was generated/sequenced.  Defaults to OICR\n";  
	
	print "\t--out  String/directory name.  Required. A directory to save individual xmls for each sample.\n";
	print "\t--merge_xml  String/filename. A single file to save the merged xml from this process.\n";
	print "\t--submission_xml  String/filename. A single file to save the submission xml from this process.\n";
	print "\t--submission_alias  String. A unique alias for the submission.  Required for submission_xml\n";
	
	print "\t--help displays this usage message.\n";

	die "\n@_\n\n";
}











