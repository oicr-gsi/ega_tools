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
	"reference=s"	=> \$opts{reference},   ### the reference sequence id to use and load into the xml
	"analysis=s"	=> \$opts{analysis},  ### a table describing the analysis, and attributes
	"study_id=s"	=> \$opts{study_id},  ### EGAS accession with which to associate this data
	"center_name=s"	=> \$opts{center_name}, ### center to associate with the data, defaults to OICR
	"stage_path=s"  => \$opts{stage_path},  ### path ont he staging server where uploaded files can be found
	"file_table=s" 			=> \$opts{file_table},         ### folder with links to files that need encryption/upload
	"merge_xml=s" 			=> \$opts{merge_xml},         ### name of merge file, where all XML should be saved
	"submission_xml=s"		=> \$opts{submission_xml},
	"submission_alias=s"	=> \$opts{submission_alias},
	"registered_samples=s" 	=> \$opts{registered_samples}, ## a table with registered samples, must have an EGAN and alias column
	"file_type=s"			=> \$opts{file_type},
	"out=s"					=> \$opts{out},

	"help" 			=> \$opts{help},
);
%opts=validate_options(%opts);

## the table that is loaded are key value pairs


$p{analysis}=load_analysis_info($opts{analysis});
$p{analysis}{center}="OICR";   ### hard coding this in here for now, should be loadable
$p{study}{study_id}=$opts{study_id};
$p{study}{center_name}=$opts{center_name} if($opts{center_name});

$p{file}{stage_path}=$opts{stage_path};
$p{analysis}{ref}=load_ref_info($opts{reference}) if($opts{reference});






sub load_ref_info{
	my ($file)=@_;


	my %h;

	(open my $FH,"<",$file) || die "unable to open file $file with reference sequence information";
	my $build_line=<$FH>;chomp $build_line;
	my($build,$acc)=split /\t/,$build_line;
	
	$h{name}=$build;
	$h{accession}=$acc;
	
	
	while(<$FH>){
		chomp;
		my($chrom,$acc)=split /\t/;
		$h{chromosomes}{$chrom}=$acc;
	}



	return \%h;
}


#print Dumper(%p);






sub load_analysis_info{
	my ($file)=@_;
	print STDERR "loading analysis info from $file\n";
	
	### loads : separated key value pairs.  
	### if key is attribute, these are key value pairs under the attribute section
	my %h;
	
	(open my $FH,"<",$file) || die "unable to open analysis information from file $file";
	while(<$FH>){
		chomp;
		my @f=split /:/,$_;
		
		my $key=shift @f;
		if($key eq "attribute"){
			my $akey=shift @f;
			my $aval=shift @f;
			$h{attributes}{$akey}=$aval;
			
		}else{
			my $val=shift @f;
			$h{$key}=$val;
		}
	}
	
	
	return \%h;
}

my %analysis_files=load_analysis_files($opts{file_table});



sub load_analysis_files{
	my ($file)=@_;
	(open my $FH,"<",$file) || die "unable to open analysis file";
	my $headerline=<$FH>;chomp $headerline;
	my @headers=split /\t/,$headerline;
	
	my %headers;
	map{  $headers{$_}=1;  } @headers;
	my %missing;
	map{
		$missing{$_}++ unless($headers{$_});
	} qw/sample alias file md5 encrypted_file encrypted_md5/;
	
	
	if(%missing){
		my $missing_headers=join(",",keys %missing);
		usage("missing headers in $file : $missing_headers");
	}
	

	my %hash;
	
	while(<$FH>){
		chomp;
		my %h;
		@h{@headers}=split /\t/;
		
		my $file=$h{file};
		%{$hash{$file}}=%h;
		


	}
	
	return %hash;
}



my %xmlmerge;
for my $file(sort keys %analysis_files){
	
	print "generating xml for $file\n";
	
	my %info=%p;
	$info{file}=$analysis_files{$file};
	$info{file}{stage_path}=$opts{stage_path};
	
	
	
	
	
#	if(%reg && $reg{$sid}){
#		print STDERR "A sample with alias $sid has already been registered on this box";
#	}
	my $xml;
		
	
	if($opts{file_type} eq "bam"){
		$xml=analysis_bam_xml($file,\%info);
	}elsif($opts{file_type} eq "vcf"){
		$xml=analysis_vcf_xml($file,\%info);
	}
	
	my $xmlfile=$opts{out} . "/" . "$file.xml";
	(open my $XML,">",$xmlfile) || die "unable to open xml file $xmlfile";
	print $XML $xml->toString(1);
	close $XML;		

	
	
	if($opts{merge_xml}){
		my $xmlstring=$xml->toString(1);
		my @lines=split /\n/,$xmlstring;
		shift @lines;
		$xmlstring=join("\n",@lines);
		$xmlmerge{$file}=$xmlstring;
	}
	
	
	
	
	
}











if($opts{merge_xml}){
	print STDERR "printing merge xml file $opts{merge_xml}\n";
	(open my $MERGE,">",$opts{merge_xml}) || die "unable to open merge file $opts{merge}";
	print $MERGE '<?xml version="1.0" encoding="utf-8"?>' . "\n";
	print $MERGE '<ANALYSIS_SET xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLoction="ftp://ftp.sra.ebi.ac.uk/meta/xsd/sra_1_5/SRA.run.xsd">' . "\n";
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

	print $SUB '				<ADD source="' . $merge_xml . '" schema="analysis"/>' . "\n";
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
	
	if(! $opts{study_id}){
		usage("study_id must be provided.");
	}
	
	if(! $opts{stage_path}){
		usage("stage_path must be provided.");
	} 
	
	if(! $opts{file_table} || ! -e $opts{file_table}){
		usage("file_table, a list of files and md5sums not provided or not found");
	}
	
	if(! $opts{analysis} || ! -e $opts{analysis}){
		usage("A file describing the analysis not provided or not found.");
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
	
	if(! $opts{file_type}){
		usage("--file_type must be specified, either bam or vcf");
	}elsif(!grep{$opts{file_type} eq $_} qw/bam vcf/){
		usage("--file_type must be either bam or vcf");
	}
	
	
	return %opts;
}



sub usage{
	print "\nprepare_SAMPLE_XMLS.pl [options]\n";
	print "Options are as follows:\n";
	print "\t--file_table String/filename. Required. A file with a table describing files to be uploaded\n";  
	print "\t\tTable header must include : 'sample (EGAN accession or registered Alias), alias (for the analysis object),file,md5,encrypteed_file,encrypted_md5\n";  
	print "\t--file_type String/filename. Required.  Either bam or vcf\n";
	print "\t--analysis String/filename. Required. A file with a table describing analysis parameters.  Key:value pairs\n";  
	print "\t--reference String/filename. Optional. A file with accessions for th reference sequence. First line is the build name and accession\n";  
	
	print "\t--center_name String. Defaults to OICR\n";  
	print "\t--study_id String. Required.  An EGAS accession\n";  
	print "\t--stage_path String. Required.  Path on the staging server where files have been uploaded\n";  
	
	print "\t--out  String/directory name.  Required. A directory to save individual xmls for each sample.\n";
	print "\t--merge_xml  String/filename. A single file to save the merged xml from this process.\n";
	print "\t--submission_xml  String/filename. A single file to save the submission xml from this process.\n";
	print "\t--submission_alias  String. A unique alias for the submission.  Required for submission_xml\n";
	
	print "\t--help displays this usage message.\n";

	die "\n@_\n\n";
}





