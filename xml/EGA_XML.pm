use strict;
use warnings;
use Data::Dumper;


#use lib "/u/lheisler/PERL/lib/perl5/x86_64-linux";
use XML::LibXML;
#use XML::Simple;
use File::Basename;
use Exporter;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);

$VERSION        =       1.00;
@ISA            =       qw(Exporter);
@EXPORT         =       qw(run_xml init_parameters experiment_xml analysis2_xml analysis_bam_xml);



sub init_parameters{

	my %p;
	$p{xml}={
		xmlns		=> "http://www.w3.org/2001/XMLSchema-instance",
		xsi			=> "ftp://ftp.sra.ebi.ac.uk/meta/xsd/sra_1_5/SRA.run.xsd",
	};

	$p{sample}={
		taxon_id		=> 9606,
		scientific_name	=> "homo sapiens",
		common_name		=> "human",
	},
	
	$p{study}={
		center_name		=>	"OICR",
		run_center		=>	"OICR",
		broker_name		=> 	"EGA",
	},	

	return %p;
}

sub run_xml{
	my($alias,$p)=@_;
	### validate components of $p

	my $XML=XML::LibXML::Document->new('1.0','utf-8');

	my $RUN=$XML->createElement("RUN");
	$RUN->setAttribute(alias        =>$alias);
	$RUN->setAttribute(center_name  =>$$p{study}{center_name});
	$RUN->setAttribute(run_center   =>$$p{study}{run_center});
	$RUN->setAttribute(run_date     =>$$p{run}{rundate});

	my $EXPERIMENT_REF=$XML->createElement("EXPERIMENT_REF");
	$EXPERIMENT_REF->setAttribute(accession=>$$p{run}{egax});
	$RUN->appendChild($EXPERIMENT_REF);
				
	my $FILES=$XML->createElement("FILES");
	my $fc=0;
	
	for my $R(qw/R1 R2/){
		my $FILE=$XML->createElement("FILE");
      	$FILE->setAttribute(filename             =>$$p{run}{stage_path}."/".$$p{run}{$R}{gpg});  ### indicate an encrypted file
      	$FILE->setAttribute(filetype             =>"Illumina_native_fastq");
      	$FILE->setAttribute(checksum_method      =>"MD5");
      	$FILE->setAttribute(checksum             =>$$p{run}{$R}{md5_encrypted});
      	$FILE->setAttribute(unencrypted_checksum =>$$p{run}{$R}{md5_unencrypted});
        
		$FILES->appendChild($FILE);
  			
		$fc++;
	}
    			
	#print "no files for run $alias\n" unless($fc);
	### DO NOT ADD THIS BLOCK UNLESS THERE ARE FILES
	next unless($fc);
				
	my $DATA_BLOCK=$XML->createElement("DATA_BLOCK");
   	$DATA_BLOCK->appendChild($FILES);
    $RUN->appendChild($DATA_BLOCK);
    $XML->setDocumentElement($RUN);
	return($XML);
}




sub load_sample_attributes{
	### this will load a table of sample information
	### column one will be the sample ID that needs to be registered
	### addition columns values.  the key for each key-value pair is hte header
	### reads a hash, with sample ID keys, and subhashes of key value pairs
	my($file)=@_;
	my %hash;
	my @recs=`cat $file | grep -v "^#"`;chomp @recs;
	

	
	my @headers=split /\t/,shift @recs;  ### headers is the first row
	map{
		my %H;
		@H{@headers}=split /\t/,$_;
		my $sid=$H{Sample};
		for my $tag(@headers){
			next if($tag eq "Sample");
			my $value=$H{$tag};

			if($tag=~/:Units/){
				(my $utag=$tag)=~s/:Units//;
				$hash{$sid}{units}{$utag}=$value;
			}else{
				$hash{$sid}{tags}{$tag}=$value;
			}	
		}
		
	}@recs;
	
	return %hash;

}


sub sample_xml{
	my ($sid,$attributes,$p)=@_;
	
	###  #initiate the xml object 
	my $XML=XML::LibXML::Document->new('1.0','utf-8');
    	my $sample_title=$$p{sample}{title};
	my $SAMPLE=$XML->createElement("SAMPLE");

	my $alias="${sid}";
	$$p{sample}{alias}{$sid}=$alias;

	$SAMPLE->setAttribute(alias       => $alias);
	$SAMPLE->setAttribute(center_name => $$p{study}{center_name});

	 my $TITLE=$XML->createElement("TITLE");
	if($$p{sample}{title}){	
		#my $TITLE=$XML->createElement("TITLE");
		$TITLE->appendTextNode($$p{sample}{title});
		#$SAMPLE->appendChild($TITLE);
	}
	$SAMPLE->appendChild($TITLE);

	my $SAMPLE_NAME=$XML->createElement("SAMPLE_NAME");
	my $TAXON_ID=$XML->createElement("TAXON_ID");
	$TAXON_ID->appendTextNode($$p{sample}{taxon_id});
	$SAMPLE_NAME->appendChild($TAXON_ID);

	my $SCIENTIFIC_NAME=$XML->createElement("SCIENTIFIC_NAME");
	$SCIENTIFIC_NAME->appendTextNode($$p{sample}{scientific_name});
	$SAMPLE_NAME->appendChild($SCIENTIFIC_NAME);	
	
	my $COMMON_NAME=$XML->createElement("COMMON_NAME");
	$COMMON_NAME->appendTextNode($$p{sample}{common_name});
	$SAMPLE_NAME->appendChild($COMMON_NAME);
	
	$SAMPLE->appendChild($SAMPLE_NAME);


	my $SAMPLE_ATTRIBUTES=$XML->createElement("SAMPLE_ATTRIBUTES");
	my @tags=sort keys %{$$attributes{tags}};

	if($$p{sample}{description}){
		my $DESCRIPTION=$XML->createElement("DESCRIPTION");
		$DESCRIPTION->appendTextNode($$p{sample}{description});
		$SAMPLE->appendChild($DESCRIPTION);
	}

		
	for my $tagname(@tags){    ### if tags info
		my $tagvalue=$$attributes{tags}{$tagname} || "";
		next unless($tagvalue);
		my $SAMPLE_ATTRIBUTE=$XML->createElement("SAMPLE_ATTRIBUTE");
		my $TAG=$XML->createElement("TAG");
		$TAG->appendTextNode($tagname);
		$SAMPLE_ATTRIBUTE->appendChild($TAG);
		
		my $VALUE=$XML->createElement("VALUE");
		$VALUE->appendTextNode($tagvalue);
		$SAMPLE_ATTRIBUTE->appendChild($VALUE);
			
		if($$attributes{units}{$tagname}){
			my $unitvalue=$$attributes{units}{$tagname};
			my $UNITS=$XML->createElement("UNITS");
			$UNITS->appendTextNode($unitvalue);
			$SAMPLE_ATTRIBUTE->appendChild($UNITS);
		}
		$SAMPLE_ATTRIBUTES->appendChild($SAMPLE_ATTRIBUTE);
	}	
	
	$SAMPLE->appendChild($SAMPLE_ATTRIBUTES) if(@tags);   ### only appedn this if thee are tags
    $XML->setDocumentElement($SAMPLE);

	return $XML;
}

sub experiment_xml{
	my ($alias,$p)=@_;    ### the alias and a hash with information


	#print Dumper($p);<STDIN>;
	

	###  EXPERIMENT XML 
	my $XML=XML::LibXML::Document->new('1.0','utf-8');
	
	my $EXPERIMENT=$XML->createElement("EXPERIMENT");
	$EXPERIMENT->setAttribute(alias        =>$alias);
	$EXPERIMENT->setAttribute(center_name  =>$$p{study}{center_name});

	### ADD TITLE IF AVAILABLE
	if($$p{experiment}{title}){
		my $TITLE=$XML->createElement("TITLE");
		$TITLE->appendTextNode($$p{experiment}{title});
		$EXPERIMENT->appendChild($TITLE);
	}
		
	## REQUIRED	
	my $STUDY_REF=$XML->createElement("STUDY_REF");
	$STUDY_REF->setAttribute(accession => $$p{experiment}{study_id});
	$EXPERIMENT->appendChild($STUDY_REF);
	
	
	## DESIGN SECTION
	my $DESIGN=$XML->createElement("DESIGN");
	
	## MANDATORY
	my $DESIGN_DESCRIPTION=$XML->createElement("DESIGN_DESCRIPTION");
	$DESIGN_DESCRIPTION->appendTextNode($$p{experiment}{description});
	$DESIGN->appendChild($DESIGN_DESCRIPTION);
	
	## REQUIRED, must reference a sample used for the experiment/library prep
	my $SAMPLE_DESCRIPTOR=$XML->createElement("SAMPLE_DESCRIPTOR");
	$SAMPLE_DESCRIPTOR->setAttribute(accession=>$$p{experiment}{EGAN});
	$DESIGN->appendChild($SAMPLE_DESCRIPTOR);
	
	my $LIBRARY_DESCRIPTOR=$XML->createElement("LIBRARY_DESCRIPTOR");
	my $LIBRARY_NAME=$XML->createElement("LIBRARY_NAME");
	$LIBRARY_NAME->appendTextNode($$p{experiment}{lib});
	$LIBRARY_DESCRIPTOR->appendChild($LIBRARY_NAME);

	
	if($$p{experiment}{lib_strategy}){
		my $LIBRARY_STRATEGY=$XML->createElement("LIBRARY_STRATEGY");
		$LIBRARY_STRATEGY->appendTextNode($$p{experiment}{lib_strategy});
		$LIBRARY_DESCRIPTOR->appendChild($LIBRARY_STRATEGY);
	}
	
	if($$p{experiment}{lib_source}){
		my $LIBRARY_SOURCE=$XML->createElement("LIBRARY_SOURCE");
		$LIBRARY_SOURCE->appendTextNode($$p{experiment}{lib_source});
		$LIBRARY_DESCRIPTOR->appendChild($LIBRARY_SOURCE);
	}
	
	## REQUIRED
	if($$p{experiment}{lib_selection}){
		my $LIBRARY_SELECTION=$XML->createElement("LIBRARY_SELECTION");
		$LIBRARY_SELECTION->appendTextNode($$p{experiment}{lib_selection});
		$LIBRARY_DESCRIPTOR->appendChild($LIBRARY_SELECTION);
	}
	
	### REQUIRED
	if(my $length=$$p{experiment}{insertsize}){
		my $LIBRARY_LAYOUT=$XML->createElement("LIBRARY_LAYOUT");
		my $PAIRED=$XML->createElement("PAIRED");
		$PAIRED->setAttribute(NOMINAL_LENGTH=>$length);
		$LIBRARY_LAYOUT->appendChild($PAIRED);
		$LIBRARY_DESCRIPTOR->appendChild($LIBRARY_LAYOUT);
	}
			
			
	$DESIGN->appendChild($LIBRARY_DESCRIPTOR);
    $EXPERIMENT->appendChild($DESIGN);

    my $PLATFORM=$XML->createElement("PLATFORM");
    my $PLATFORM_NAME=$XML->createElement("ILLUMINA");

    my $INSTRUMENT_MODEL=$XML->createElement("INSTRUMENT_MODEL");
    $INSTRUMENT_MODEL->appendTextNode($$p{experiment}{instrument_model});
	$PLATFORM_NAME->appendChild($INSTRUMENT_MODEL);
    $PLATFORM->appendChild($PLATFORM_NAME);
    		
			
	$EXPERIMENT->appendChild($PLATFORM);
	$XML->setDocumentElement($EXPERIMENT);

	
	return($XML);
}



sub experiment_xml_older{
	my ($EGAN,$lib,$alias,$p)=@_;

	###  EXPERIMENT XML 
	my $XML=XML::LibXML::Document->new('1.0','utf-8');
	
	my $EXPERIMENT=$XML->createElement("EXPERIMENT");
	$EXPERIMENT->setAttribute(alias        =>$alias);
	$EXPERIMENT->setAttribute(center_name  =>$$p{study}{center_name});

	### ADD TITLE IF AVAILABLE
	if($$p{experiment}{title}){
		my $TITLE=$XML->createElement("TITLE");
		$TITLE->appendTextNode($$p{experiment}{title});
		$EXPERIMENT->appendChild($TITLE);
	}
		
	## REQUIRED	
	my $STUDY_REF=$XML->createElement("STUDY_REF");
	$STUDY_REF->setAttribute(accession => $$p{study}{study_id});
	$EXPERIMENT->appendChild($STUDY_REF);
	
	
	## DESIGN SECTION
	my $DESIGN=$XML->createElement("DESIGN");
	
	## MANDATORY
	my $DESIGN_DESCRIPTION=$XML->createElement("DESIGN_DESCRIPTION");
	$DESIGN_DESCRIPTION->appendTextNode($$p{experiment}{design});
	$DESIGN->appendChild($DESIGN_DESCRIPTION);
	
	## REQUIRED, must reference a sample used for the experiment/library prep
	my $SAMPLE_DESCRIPTOR=$XML->createElement("SAMPLE_DESCRIPTOR");
	$SAMPLE_DESCRIPTOR->setAttribute(accession=>$EGAN);
	$DESIGN->appendChild($SAMPLE_DESCRIPTOR);
	
	my $LIBRARY_DESCRIPTOR=$XML->createElement("LIBRARY_DESCRIPTOR");
	my $LIBRARY_NAME=$XML->createElement("LIBRARY_NAME");
	$LIBRARY_NAME->appendTextNode($lib);
	$LIBRARY_DESCRIPTOR->appendChild($LIBRARY_NAME);

	
	if($$p{experiment}{library_strategy}){
		my $LIBRARY_STRATEGY=$XML->createElement("LIBRARY_STRATEGY");
		$LIBRARY_STRATEGY->appendTextNode($$p{experiment}{library_strategy});
		$LIBRARY_DESCRIPTOR->appendChild($LIBRARY_STRATEGY);
	}
	
	if($$p{experiment}{library_source}){
		my $LIBRARY_SOURCE=$XML->createElement("LIBRARY_SOURCE");
		$LIBRARY_SOURCE->appendTextNode($$p{experiment}{library_source});
		$LIBRARY_DESCRIPTOR->appendChild($LIBRARY_SOURCE);
	}
	
	## REQUIRED
	if($$p{experiment}{library_selection}){
		my $LIBRARY_SELECTION=$XML->createElement("LIBRARY_SELECTION");
		$LIBRARY_SELECTION->appendTextNode($$p{experiment}{library_selection});
		$LIBRARY_DESCRIPTOR->appendChild($LIBRARY_SELECTION);
	}
	
	### REQUIRED
	if(my $length=$$p{experiment}{nominal_length}){
		my $LIBRARY_LAYOUT=$XML->createElement("LIBRARY_LAYOUT");
		my $PAIRED=$XML->createElement("PAIRED");
		$PAIRED->setAttribute(NOMINAL_LENGTH=>$length);
		$LIBRARY_LAYOUT->appendChild($PAIRED);
		$LIBRARY_DESCRIPTOR->appendChild($LIBRARY_LAYOUT);
	}
			
			
	$DESIGN->appendChild($LIBRARY_DESCRIPTOR);
    $EXPERIMENT->appendChild($DESIGN);

    my $PLATFORM=$XML->createElement("PLATFORM");
    my $PLATFORM_NAME=$XML->createElement("ILLUMINA");

    my $INSTRUMENT_MODEL=$XML->createElement("INSTRUMENT_MODEL");
    $INSTRUMENT_MODEL->appendTextNode($$p{experiment}{instrument_model});
	$PLATFORM_NAME->appendChild($INSTRUMENT_MODEL);
    $PLATFORM->appendChild($PLATFORM_NAME);
    		
			
	$EXPERIMENT->appendChild($PLATFORM);
	$XML->setDocumentElement($EXPERIMENT);
	
	return($XML);
}


#### THIS IS DESIGNED AROUND BRCA, needs to be generalized

### analysis XML for a bam file
### needs to account for all readgroups in the bam file
### needs to reference the sample accessions
### will provide :
### 1. reference to a hash describing the bam file
### 2. reference to a hash describing the readgroups
sub analysis_bam_xml{
	my($bamfile,$baminfo)=@_;
	
	
	my $XML=XML::LibXML::Document->new('1.0','utf-8');
	my $ANALYSIS=$XML->createElement("ANALYSIS");
	
	
	### INFORMATION ABOUT THE STUDY CENTRE AND SUBMISSION
	$ANALYSIS->setAttribute(alias        	=>$$baminfo{file}{alias});
	$ANALYSIS->setAttribute(center_name  	=>$$baminfo{study}{center_name});
	$ANALYSIS->setAttribute(broker_name  	=>$$baminfo{study}{broker_name});
	$ANALYSIS->setAttribute(analysis_center =>$$baminfo{analysis}{center});
	#$ANALYSIS->setAttribute(analysis_date	=>$samples{$id}{$samp}{analysis_date});
	
	
	### INFORMATION ABOUT THE ANALYSIS
	my $TITLE=$XML->createElement("TITLE");
	$TITLE->appendTextNode($$baminfo{analysis}{title});
	$ANALYSIS->appendChild($TITLE);
	
	my $DESCRIPTION=$XML->createElement("DESCRIPTION");
	$DESCRIPTION->appendTextNode($$baminfo{analysis}{description});
	$ANALYSIS->appendChild($DESCRIPTION);
	
	### INFORMATION ABOUT THE STUDY
	my $STUDY_REF=$XML->createElement("STUDY_REF");
	$STUDY_REF->setAttribute(accession=>$$baminfo{study}{study_id});
	$STUDY_REF->setAttribute(refcenter=>$$baminfo{study}{center_name});
	$ANALYSIS->appendChild($STUDY_REF);
	
	
	
	### for each sample in teh readgroups information
	my $EGAN=$$baminfo{file}{sample} || "noacc";
	my $SAMPLE_REF=$XML->createElement("SAMPLE_REF");
	#$SAMPLE_REF->setAttribute(accession=>$EGAS);      ### accession is only available if previousl generated
	$SAMPLE_REF->setAttribute(accession=>$EGAN);
	$SAMPLE_REF->setAttribute(refcenter=>$$baminfo{study}{center_name});
	$ANALYSIS->appendChild($SAMPLE_REF);
		
	
	my $ANALYSIS_TYPE=$XML->createElement("ANALYSIS_TYPE");
	my $REFERENCE_ALIGNMENT=$XML->createElement("REFERENCE_ALIGNMENT");
	my $ASSEMBLY=$XML->createElement("ASSEMBLY");
	my $STANDARD=$XML->createElement("STANDARD");
	$STANDARD->setAttribute(refname=>$$baminfo{analysis}{ref}{name});
	$STANDARD->setAttribute(accession=>$$baminfo{analysis}{ref}{accession});
	$ASSEMBLY->appendChild($STANDARD);
	$REFERENCE_ALIGNMENT->appendChild($ASSEMBLY);

	for my $seqid(sort keys %{$$baminfo{analysis}{ref}{chromosomes}}){
		my $acc=$$baminfo{analysis}{ref}{chromosomes}{$seqid};
		my $SEQUENCE=$XML->createElement("SEQUENCE");
		$SEQUENCE->setAttribute(accession=>$acc);
		$SEQUENCE->setAttribute(label=>$seqid);
		$REFERENCE_ALIGNMENT->appendChild($SEQUENCE);
	}
	
	$ANALYSIS_TYPE->appendChild($REFERENCE_ALIGNMENT);
	$ANALYSIS->appendChild($ANALYSIS_TYPE);

	my $FILES=$XML->createElement("FILES");
	my $FILE=$XML->createElement("FILE");
	$FILE->setAttribute(filename=>$$baminfo{file}{stage_path} . "/". $$baminfo{file}{encrypted_file});
	$FILE->setAttribute(filetype=>"bam");
	$FILE->setAttribute(checksum_method=>"MD5");
	$FILE->setAttribute(checksum=>$$baminfo{file}{encrypted_md5});
	$FILE->setAttribute(unencrypted_checksum=>$$baminfo{file}{md5});
	$FILES->appendChild($FILE);
	$ANALYSIS->appendChild($FILES);

	my $ANALYSIS_ATTRIBUTES=$XML->createElement("ANALYSIS_ATTRIBUTES");
	for my $key(sort keys %{$$baminfo{analysis}{attributes}}){
		my $val=$$baminfo{analysis}{attributes}{$key};
		my $ANALYSIS_ATTRIBUTE=$XML->createElement("ANALYSIS_ATTRIBUTE");
		my $TAG=$XML->createElement("TAG");
		$TAG->appendTextNode($key);
		$ANALYSIS_ATTRIBUTE->appendChild($TAG);
		my $VALUE=$XML->createElement("VALUE");
		$VALUE->appendTextNode($val);
		$ANALYSIS_ATTRIBUTE->appendChild($VALUE);
		$ANALYSIS_ATTRIBUTES->appendChild($ANALYSIS_ATTRIBUTE);
	}
	
	### add in an analysis attribute indicating which sample it was co processed with
#	my $ANALYSIS_ATTRIBUTE=$XML->createElement("ANALYSIS_ATTRIBUTE");
#	my $TAG=$XML->createElement("TAG");
#	$TAG->appendTextNode("GATK preprocessed with");
#	$ANALYSIS_ATTRIBUTE->appendChild($TAG);
	
#	my $co_sid;
#	if($bamid=~/F1/){
#		($co_sid=$bamid)=~s/F1/B1/;
#	}else{
#		my ($co)=$bamid=~/_(.*)/;
#		($co_sid=$bamid)=~s/B1/$co/;
#	}
#
#	my $VALUE=$XML->createElement("VALUE");
#	$VALUE->appendTextNode($co_sid);
#	$ANALYSIS_ATTRIBUTE->appendChild($VALUE);
#	$ANALYSIS_ATTRIBUTES->appendChild($ANALYSIS_ATTRIBUTE);
	
	$ANALYSIS->appendChild($ANALYSIS_ATTRIBUTES);
	$XML->setDocumentElement($ANALYSIS);
	
	return($XML);
}


### this is different than the bam analysis xml, under analysis type (= sequence variation)
sub analysis_vcf_xml{
	my($vcfid,$vcfinfo)=@_;
	
	my $XML=XML::LibXML::Document->new('1.0','utf-8');
	my $ANALYSIS=$XML->createElement("ANALYSIS");
	
	
	### INFORMATION ABOUT THE STUDY CENTRE AND SUBMISSION
	$ANALYSIS->setAttribute(alias        	=>$vcfid);
	$ANALYSIS->setAttribute(center_name  	=>$$vcfinfo{study}{center_name});
	$ANALYSIS->setAttribute(broker_name  	=>$$vcfinfo{study}{broker_name});
	$ANALYSIS->setAttribute(analysis_center =>$$vcfinfo{study}{analysis_center});
	#$ANALYSIS->setAttribute(analysis_date	=>$samples{$id}{$samp}{analysis_date});
	
	
	### INFORMATION ABOUT THE ANALYSIS
	my $TITLE=$XML->createElement("TITLE");
	$TITLE->appendTextNode($$vcfinfo{analysis}{title});
	$ANALYSIS->appendChild($TITLE);
	
	my $DESCRIPTION=$XML->createElement("DESCRIPTION");
	$DESCRIPTION->appendTextNode($$vcfinfo{analysis}{description});
	$ANALYSIS->appendChild($DESCRIPTION);
	
	### INFORMATION ABOUT THE STUDY
	my $STUDY_REF=$XML->createElement("STUDY_REF");
	$STUDY_REF->setAttribute(accession=>$$vcfinfo{study}{study_id});
	$STUDY_REF->setAttribute(refcenter=>$$vcfinfo{study}{center_name});
	$ANALYSIS->appendChild($STUDY_REF);
	
	### for each sample in teh readgroups information
	my $EGAN=$$vcfinfo{sample}{accession} || "noacc";
	my $SAMPLE_REF=$XML->createElement("SAMPLE_REF");
	#$SAMPLE_REF->setAttribute(accession=>$EGAS);      ### accession is only available if previousl generated
	$SAMPLE_REF->setAttribute(accession=>$EGAN);
	$SAMPLE_REF->setAttribute(refcenter=>$$vcfinfo{study}{center_name});
	$ANALYSIS->appendChild($SAMPLE_REF);
		
	
	my $ANALYSIS_TYPE=$XML->createElement("ANALYSIS_TYPE");
	my $REFERENCE_ALIGNMENT=$XML->createElement("SEQUENCE_VARIATION");
	my $ASSEMBLY=$XML->createElement("ASSEMBLY");
	my $STANDARD=$XML->createElement("STANDARD");
	$STANDARD->setAttribute(refname=>$$vcfinfo{analysis}{ref}{name});
	$STANDARD->setAttribute(accession=>$$vcfinfo{analysis}{ref}{accession});
	$ASSEMBLY->appendChild($STANDARD);
	$REFERENCE_ALIGNMENT->appendChild($ASSEMBLY);

	for my $seqid(sort keys %{$$vcfinfo{analysis}{ref}{chromosomes}}){
		my $acc=$$vcfinfo{analysis}{ref}{chromosomes}{$seqid};
		my $SEQUENCE=$XML->createElement("SEQUENCE");
		$SEQUENCE->setAttribute(accession=>$acc);
		$SEQUENCE->setAttribute(label=>$seqid);
		$REFERENCE_ALIGNMENT->appendChild($SEQUENCE);
	}

	my $EXPERIMENT_TYPE=$XML->createElement("EXPERIMENT_TYPE");
	$EXPERIMENT_TYPE->appendTextNode($$vcfinfo{analysis}{experiment}{type});
	$REFERENCE_ALIGNMENT->appendChild($EXPERIMENT_TYPE);


	
	$ANALYSIS_TYPE->appendChild($REFERENCE_ALIGNMENT);
	$ANALYSIS->appendChild($ANALYSIS_TYPE);

	my $FILES=$XML->createElement("FILES");
	my $FILE=$XML->createElement("FILE");
	$FILE->setAttribute(filename=>$$vcfinfo{file}{stage_path} . "/". $$vcfinfo{file}{encrypted_vcf});
	$FILE->setAttribute(filetype=>"vcf");
	$FILE->setAttribute(checksum_method=>"MD5");
	$FILE->setAttribute(checksum=>$$vcfinfo{file}{encrypted_vcf_md5});
	$FILE->setAttribute(unencrypted_checksum=>$$vcfinfo{file}{md5});
	$FILES->appendChild($FILE);
	$ANALYSIS->appendChild($FILES);

	my $ANALYSIS_ATTRIBUTES=$XML->createElement("ANALYSIS_ATTRIBUTES");
	for my $key(sort keys %{$$vcfinfo{analysis}{attributes}}){
		my $val=$$vcfinfo{analysis}{attributes}{$key};
		my $ANALYSIS_ATTRIBUTE=$XML->createElement("ANALYSIS_ATTRIBUTE");
		my $TAG=$XML->createElement("TAG");
		$TAG->appendTextNode($key);
		$ANALYSIS_ATTRIBUTE->appendChild($TAG);
		my $VALUE=$XML->createElement("VALUE");
		$VALUE->appendTextNode($val);
		$ANALYSIS_ATTRIBUTE->appendChild($VALUE);
		$ANALYSIS_ATTRIBUTES->appendChild($ANALYSIS_ATTRIBUTE);
	}
	
	### add in an analysis attribute indicating which sample it was co processed with
#	my $ANALYSIS_ATTRIBUTE=$XML->createElement("ANALYSIS_ATTRIBUTE");
#	my $TAG=$XML->createElement("TAG");
#	$TAG->appendTextNode("GATK preprocessed with");
#	$ANALYSIS_ATTRIBUTE->appendChild($TAG);
	
#	my $co_sid;
#	if($bamid=~/F1/){
#		($co_sid=$bamid)=~s/F1/B1/;
#	}else{
#		my ($co)=$bamid=~/_(.*)/;
#		($co_sid=$bamid)=~s/B1/$co/;
#	}
#
#	my $VALUE=$XML->createElement("VALUE");
#	$VALUE->appendTextNode($co_sid);
#	$ANALYSIS_ATTRIBUTE->appendChild($VALUE);
#	$ANALYSIS_ATTRIBUTES->appendChild($ANALYSIS_ATTRIBUTE);
	
	$ANALYSIS->appendChild($ANALYSIS_ATTRIBUTES);
	$XML->setDocumentElement($ANALYSIS);
	
	return($XML);
}




sub analysis2_xml{
	
	
	##### CHECK ON RUN XML and EXPERIMENT XNML and SAMPLE XML - these need to exist!!!!
	my($baminfo,$EGAN,$p)=@_;
	
	### bam info is a hash with the location of the bam file and the md5 sums, encrypted and unencrypted
	### EGAN is the sample accession

	
	my $bamid=$$baminfo{id};

	my $XML=XML::LibXML::Document->new('1.0','utf-8');
	my $ANALYSIS=$XML->createElement("ANALYSIS");
	$ANALYSIS->setAttribute(alias        	=>$bamid);
	$ANALYSIS->setAttribute(center_name  	=>$$p{study}{center_name});
	$ANALYSIS->setAttribute(broker_name  	=>$$p{study}{broker_name});
	$ANALYSIS->setAttribute(analysis_center =>$$p{study}{analysis_center});
	#$ANALYSIS->setAttribute(analysis_date	=>$samples{$id}{$samp}{analysis_date});
	
	my $TITLE=$XML->createElement("TITLE");
	$TITLE->appendTextNode($$p{analysis}{title});
	$ANALYSIS->appendChild($TITLE);
	
	my $DESCRIPTION=$XML->createElement("DESCRIPTION");
	$DESCRIPTION->appendTextNode($$p{analysis}{description});
	$ANALYSIS->appendChild($DESCRIPTION);
	
	my $STUDY_REF=$XML->createElement("STUDY_REF");
	$STUDY_REF->setAttribute(accession=>$$p{study}{study_id});
	$STUDY_REF->setAttribute(refcenter=>$$p{study}{center_name});
	$ANALYSIS->appendChild($STUDY_REF);
	
	my $SAMPLE_REF=$XML->createElement("SAMPLE_REF");
	$SAMPLE_REF->setAttribute(accession=>$EGAN);
	$SAMPLE_REF->setAttribute(refcenter=>$$p{study}{center_name});
    $ANALYSIS->appendChild($SAMPLE_REF);

	my $ANALYSIS_TYPE=$XML->createElement("ANALYSIS_TYPE");	
	my $REFERENCE_ALIGNMENT=$XML->createElement("REFERENCE_ALIGNMENT");
	my $ASSEMBLY=$XML->createElement("ASSEMBLY");
	my $STANDARD=$XML->createElement("STANDARD");
	$STANDARD->setAttribute(refname=>$$p{analysis}{ref}{name});
	$STANDARD->setAttribute(accession=>$$p{analysis}{ref}{accession});
	$ASSEMBLY->appendChild($STANDARD);
	$REFERENCE_ALIGNMENT->appendChild($ASSEMBLY);

	for my $seqid(sort keys %{$$p{analysis}{ref}{chromosomes}}){
		my $acc=$$p{analysis}{ref}{chromosomes}{$seqid};
		my $SEQUENCE=$XML->createElement("SEQUENCE");
		$SEQUENCE->setAttribute(accession=>$acc);
		$SEQUENCE->setAttribute(label=>$seqid);
		$REFERENCE_ALIGNMENT->appendChild($SEQUENCE);
	}
	
	$ANALYSIS_TYPE->appendChild($REFERENCE_ALIGNMENT);
	$ANALYSIS->appendChild($ANALYSIS_TYPE);

	my $FILES=$XML->createElement("FILES");
	my $FILE=$XML->createElement("FILE");
	$FILE->setAttribute(filename=>$$p{analysis}{stage_path} . "/". "${bamid}.bam" .".gpg");
	$FILE->setAttribute(filetype=>"bam");
	$FILE->setAttribute(checksum_method=>"MD5");
	$FILE->setAttribute(checksum=>$$baminfo{md5}{encrypted});
	$FILE->setAttribute(unencrypted_checksum=>$$baminfo{md5}{unencrypted});
	$FILES->appendChild($FILE);
	$ANALYSIS->appendChild($FILES);

	my $ANALYSIS_ATTRIBUTES=$XML->createElement("ANALYSIS_ATTRIBUTES");
	for my $key(sort keys %{$$p{analysis}{attributes}}){
		my $val=$$p{analysis}{attributes}{$key};
		my $ANALYSIS_ATTRIBUTE=$XML->createElement("ANALYSIS_ATTRIBUTE");
		my $TAG=$XML->createElement("TAG");
		$TAG->appendTextNode($key);
		$ANALYSIS_ATTRIBUTE->appendChild($TAG);
		my $VALUE=$XML->createElement("VALUE");
		$VALUE->appendTextNode($val);
		$ANALYSIS_ATTRIBUTE->appendChild($VALUE);
		$ANALYSIS_ATTRIBUTES->appendChild($ANALYSIS_ATTRIBUTE);
	}
	$ANALYSIS->appendChild($ANALYSIS_ATTRIBUTES);
	$XML->setDocumentElement($ANALYSIS);
	
	return($XML);
}






	








