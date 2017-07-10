use strict;
use warnings;
use Data::Dumper;
use XML::LibXML;
use XML::Simple;
use File::Basename;
use Exporter;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);

$VERSION        =       1.00;
@ISA            =       qw(Exporter);
@EXPORT         =       qw(load_sample_info sample_xml experiment_xml load_sample_xml load_experiment_xml load_run_xml load_bam_rg load_bam_files 
							init_pareameters run_xml analysis_xml);



sub init_parameters{
	my %p;
	$p{xml}={
		xmlns		=> "http://www.w3.org/2001/XMLSchema-instance",
		xsi			=> "ftp://ftp.sra.ebi.ac.uk/meta/xsd/sra_1_5/SRA.run.xsd",
	};
	return \%p;
}



sub load_run_accessions{

	my($file,$hash)=@_;
	my @recs=`cat $file | grep -v "^#"`;chomp @recs;
    my @headers=split /\t/,shift @recs;  ### 
	map{
		my %H;
		@H{@headers}=split /\t/,shift @recs;
		
		
		$$hash{$H{Sample}}{acc}=$H{EGAN};
		$$hash{$H{Sample}}{Experiments}{$H{Experiment}}{acc}=$H{EGAX};
		$$hash{$H{Sample}}{Experiments}{$H{Experiment}}{Runs}{$H{Run}}{acc}=$H{EGAR};
	}@recs;
}



sub load_sample_info{
	### loads a table, first column are sample identifiere, remaining columns are values for header tags/keys that will form part of the sample xml
	my($file,$hash)=@_;
	
	my @recs=`cat sample_info.txt | grep -v "^#"`;chomp @recs;
	my @headers=split /\t/,shift @recs;  ### headers is the first row
	map{
		my %H;
		@H{@headers}=split /\t/,$_;
		my $sid=$H{Sample};
		for my $tag(@headers){
			next if($tag eq "sample");
			my $value=$H{$tag};

			if($tag=~/:Units/){
				(my $utag=$tag)=~s/:Units//;
				$$hash{$sid}{units}{$utag}=$value;
			}else{
				$$hash{$sid}{tags}{$tag}=$value;
			}	
		}
	}@recs;

}



sub load_sample_xml{
	my ($dir,$hash)=@_;
	my @files=`ls $dir/*.xml`;chomp @files;
	for my $file(@files){
		my $xml=XMLin($file);
		my $sid=$$xml{alias};
		$$hash{$sid}{xml}=1;
		
		## can we get teh accession number from downloaded xml?
	}
}

sub load_experiment_xml{
	my ($dir,$hash)=@_;
	my @files=`ls $dir/*.xml`;chomp @files;
	for my $file(@files){
		my $xml=XMLin($file);
		my $exp=$$xml{alias};
		my ($sid,$lib)=split /:/,$exp;
		
		$$hash{$sid}{exp}{$lib}{xml}=1;
	}
}

sub load_run_xml{
	my ($dir,$hash)=@_;
	my @files=`ls $dir/*.xml`;chomp @files;
	for my $file(@files){
		my $xml=XMLin($file);
		my $run=$$xml{alias};
		my ($sid,$lib,$rid)=split /:/,$run;
		
		$$hash{$sid}{exp}{$lib}{run}{$rid}{xml}=1;
	}
}


sub load_xml{
	
	## this process will review xml files
	## directory contains folder for different types, sample, experiment, run and xml files for each
	my ($dir,$hash)=@_;
	
	
	
	
	
}






sub load_sample_sequencing{
	
	my($file,$hash)=@_;
	my @recs=`cat sample_sequencing.txt | grep -v "^#"`;chomp @recs;   ### this table includes mapping from sample name to
	
	
	my @headers=split /\t/,shift @recs;
	shift @headers;
	map{
		my @f=split /\t/,$_;
		my $sid=shift @f;
		my %H;
		@H{@headers}=@f;
		
		

		my $lib=$H{library};
		my $run=$H{run};
	
		$$hash{$sid}{seq}{$lib}{$run}{count}++;
		
		for my $R(qw/R1 R2/){
			$$hash{$sid}{seq}{$lib}{$run}{$R}{file}=$H{"${R}_file"} || 0;
			$$hash{$sid}{seq}{$lib}{$run}{$R}{md5}=$H{"${R}_md5"} || 0;
			$$hash{$sid}{seq}{$lib}{$run}{$R}{md5_encrypted}=$H{"${R}_md5_encrypted"} || 0;
			
		}
	}@recs;
	
	

}

sub load_bam_files{
	my($file,$hash)=@_;
	
	my @recs=`cat $file | grep -v "^#"`;chomp @recs;
	my @headers=split /\t/,shift @recs;

	map{
		my %H;
		@H{@headers}=split /\t/,$_;
		my $file=$H{file};
	
		$$hash{$file}={md5=>$H{md5},md5_encrypted=>$H{md5_encrypted}};
		
		
	}@recs;
}

sub load_bam_rg{
	my($file,$hash)=@_;
	my @recs=`cat $file | grep -v "^#"`;chomp @recs;
	my @headers=split /\t/,shift @recs;
	
	map{
		my @f=split /\t/,$_;
		my %H;
		@H{@headers}=split /\t/,$_;
		my $file=$H{file};
		$$hash{$file}{rg}{$H{SM}}{$H{LB}}{$H{PU}}++;
	}@recs;
}

sub sample_xml{

	my ($sid,$sample,$p)=@_;
	
	###  #initiate the xml object 
	my $XML=XML::LibXML::Document->new('1.0','utf-8');
    my $sample_title=$$sample{Sample_type};
	my $SAMPLE=$XML->createElement("SAMPLE");

	my $alias="${sid}";
	$$p{sample}{alias}{$sid}=$alias;

	$SAMPLE->setAttribute(alias       => $alias);
	$SAMPLE->setAttribute(center_name => $$p{study}{center_name});
	
	my $TITLE=$XML->createElement("TITLE");
	$TITLE->appendTextNode($$p{sample}{title});
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
	my @tags=sort keys %{$$sample{tags}};
	
		
	for my $tagname(@tags){    ### if tags info
		my $tagvalue=$$sample{tags}{$tagname} || "";
		next unless($tagvalue);
		my $SAMPLE_ATTRIBUTE=$XML->createElement("SAMPLE_ATTRIBUTE");
		my $TAG=$XML->createElement("TAG");
		$TAG->appendTextNode($tagname);
		$SAMPLE_ATTRIBUTE->appendChild($TAG);
		
		my $VALUE=$XML->createElement("VALUE");
		$VALUE->appendTextNode($tagvalue);
		$SAMPLE_ATTRIBUTE->appendChild($VALUE);
			
		if($$sample{units}{$tagname}){
			my $unitvalue=$$sample{units}{$tagname};
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
	my ($sid,$lib,$p)=@_;

	###  EXPERIMENT XML 
	my $XML=XML::LibXML::Document->new('1.0','utf-8');
	
	my $alias="$sid:$lib";   ## ALIAS IS A COMBINATION OF SAMPLE ID AND LIBRARY NAME
	my $EXPERIMENT=$XML->createElement("EXPERIMENT");
	$EXPERIMENT->setAttribute(alias        =>$alias);
	$EXPERIMENT->setAttribute(center_name  =>$$p{study}{center_name});

	my $TITLE=$XML->createElement("TITLE");
	$TITLE->appendTextNode($$p{experiment}{title});
	$EXPERIMENT->appendChild($TITLE);
		
	my $STUDY_REF=$XML->createElement("STUDY_REF");
	$STUDY_REF->setAttribute(accession => $$p{study}{study_id});
	$EXPERIMENT->appendChild($STUDY_REF);
	
	my $DESIGN=$XML->createElement("DESIGN");
	my $DESIGN_DESCRIPTION=$XML->createElement("DESIGN_DESCRIPTION");
	$DESIGN_DESCRIPTION->appendTextNode($$p{study}{study_design});
	$DESIGN->appendChild($DESIGN_DESCRIPTION);
	
	my $SAMPLE_DESCRIPTOR=$XML->createElement("SAMPLE_DESCRIPTOR");
	$SAMPLE_DESCRIPTOR->setAttribute(refname=>$sid);
	$DESIGN->appendChild($SAMPLE_DESCRIPTOR);
	
			
	my $LIBRARY_NAME=$XML->createElement("LIBRARY_NAME");
	$LIBRARY_NAME->appendTextNode($lib);
			
	my $LIBRARY_STRATEGY=$XML->createElement("LIBRARY_STRATEGY");
	$LIBRARY_STRATEGY->appendTextNode($$p{experiment}{library_strategy});
	
	my $LIBRARY_SOURCE=$XML->createElement("LIBRARY_SOURCE");
	$LIBRARY_SOURCE->appendTextNode($$p{experiment}{library_source});
	
	my $LIBRARY_SELECTION=$XML->createElement("LIBRARY_SELECTION");
	$LIBRARY_SELECTION->appendTextNode($$p{experiment}{library_selection});
			
	my $PAIRED=$XML->createElement("PAIRED");
	my ($insert_size)=$lib=~/PE_(\d+)_/;
	$PAIRED->setAttribute(NOMINAL_LENGTH=>$insert_size);
			
	my $LIBRARY_LAYOUT=$XML->createElement("LIBRARY_LAYOUT");
	$LIBRARY_LAYOUT->appendChild($PAIRED);
			
			
	my $LIBRARY_DESCRIPTOR=$XML->createElement("LIBRARY_DESCRIPTOR");
	$LIBRARY_DESCRIPTOR->appendChild($LIBRARY_NAME);
	$LIBRARY_DESCRIPTOR->appendChild($LIBRARY_STRATEGY);
	$LIBRARY_DESCRIPTOR->appendChild($LIBRARY_SOURCE);
	$LIBRARY_DESCRIPTOR->appendChild($LIBRARY_SELECTION);
	$LIBRARY_DESCRIPTOR->appendChild($LIBRARY_LAYOUT);
			
	$DESIGN->appendChild($LIBRARY_DESCRIPTOR);
    $EXPERIMENT->appendChild($DESIGN);
 
    my $INSTRUMENT_MODEL=$XML->createElement("INSTRUMENT_MODEL");
    $INSTRUMENT_MODEL->appendTextNode($$p{experiment}{instrument_model});
			
    
    my $PLATFORM=$XML->createElement("PLATFORM");
    
	my $PLATFORM_NAME=$XML->createElement("ILLUMINA");
    $PLATFORM_NAME->appendChild($INSTRUMENT_MODEL);
    $PLATFORM->appendChild($PLATFORM_NAME);
    		
			
	$EXPERIMENT->appendChild($PLATFORM);
	$XML->setDocumentElement($EXPERIMENT);
	
	return($XML);
}

sub run_xml{
	my($alias,$egan,$egax,$run,$rundate,$fastq,$p)=@_;

	my $XML=XML::LibXML::Document->new('1.0','utf-8');

	

								

	my $RUN=$XML->createElement("RUN");
	$RUN->setAttribute(alias        =>$alias);
	$RUN->setAttribute(center_name  =>$$p{study}{center_name});
	$RUN->setAttribute(run_center   =>$$p{study}{run_center});
	$RUN->setAttribute(run_date     =>$rundate);

	my $EXPERIMENT_REF=$XML->createElement("EXPERIMENT_REF");
	$EXPERIMENT_REF->setAttribute(accession=>$egax);
	$RUN->appendChild($EXPERIMENT_REF);
				
	my $FILES=$XML->createElement("FILES");
	my $fc=0;
	
	for my $R(qw/R1 R2/){
		my %fq=%{$$fastq{$R}};  ### capture the hash with the info about the fastq file
		my $FILE=$XML->createElement("FILE");
      	$FILE->setAttribute(filename             =>$$p{run}{stage_path}."/".$fq{file}.".gpg");  ### indicate an encrypted file
      	$FILE->setAttribute(filetype             =>"Illumina_native_fastq");
      	$FILE->setAttribute(checksum_method      =>"MD5");
      	$FILE->setAttribute(checksum             =>$fq{md5_encrypted});
      	$FILE->setAttribute(unencrypted_checksum =>$fq{md5});
        
		$FILES->appendChild($FILE);
  			
		$fc++;
	}
    			
	print "no files for run $alias\n" unless($fc);
	### DO NOT ADD THIS BLOCK UNLESS THERE ARE FILES
	next unless($fc);
				
	my $DATA_BLOCK=$XML->createElement("DATA_BLOCK");
   	$DATA_BLOCK->appendChild($FILES);
    $RUN->appendChild($DATA_BLOCK);
	
	

    $XML->setDocumentElement($RUN);
	

	
	return($XML);
}


sub analysis_xml{
	
	
	##### CHECK ON RUN XML and EXPERIMENT XNML and SAMPLE XML - these need to exist!!!!
	my($prefix,$bamid,$baminfo,$sampleinfo,$p)=@_;
	
	#print Dumper($baminfo);<STDIN>;
	

	my $XML=XML::LibXML::Document->new('1.0','utf-8');
	my $ANALYSIS=$XML->createElement("ANALYSIS");
	$ANALYSIS->setAttribute(alias        	=>"${prefix}.${bamid}");
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
	
	for my $sid(sort keys %{$$baminfo{rg}}){
		
		my $EGAN=$$sampleinfo{$sid}{acc} || "noacc";
		
		my $SAMPLE_REF=$XML->createElement("SAMPLE_REF");
		#$SAMPLE_REF->setAttribute(accession=>$EGAS);      ### accession is only available if previousl generated
		$SAMPLE_REF->setAttribute(accession=>$EGAN);
		$SAMPLE_REF->setAttribute(refcenter=>$$p{study}{center_name});
		
		$ANALYSIS->appendChild($SAMPLE_REF);
	
		for my $lib(sort keys %{$$baminfo{rg}{$sid}}){
			next;  ### skip this part		
			my $EGAX=$$sampleinfo{$sid}{Experiments}{"$sid:$lib"}{acc} || "noacc";
			my $EXPERIMENT_REF=$XML->createElement("EXPERIMENT_REF");
			$EXPERIMENT_REF->setAttribute(accession=>$EGAX);
			$EXPERIMENT_REF->setAttribute(refcenter=>$$p{study}{center_name});
			$ANALYSIS->appendChild($EXPERIMENT_REF);
			
			for my $run(sort keys %{$$baminfo{rg}{$sid}{$lib}}){
				
				my $EGAR=$$sampleinfo{$sid}{Experiments}{"$sid:$lib"}{Runs}{"$sid:$lib:$run"}{acc} || "noacc";
				

				my $RUN_REF=$XML->createElement("RUN_REF");
				#$RUN_REF->setAttribute(refname=>$run_ref);
				$RUN_REF->setAttribute(accession=>$EGAR);
		
				$RUN_REF->setAttribute(refcenter=>$$p{study}{center_name});
				$ANALYSIS->appendChild($RUN_REF);
			}
		}
	}	
	
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
	$FILE->setAttribute(checksum=>$$baminfo{md5_encrypted});
	$FILE->setAttribute(unencrypted_checksum=>$$baminfo{md5});
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
	
	### add in an analysis attribute indicating which sample it was co processed with
	my $ANALYSIS_ATTRIBUTE=$XML->createElement("ANALYSIS_ATTRIBUTE");
	my $TAG=$XML->createElement("TAG");
	$TAG->appendTextNode("GATK preprocessed with");
	$ANALYSIS_ATTRIBUTE->appendChild($TAG);
	
	my $co_sid;
	if($bamid=~/F1/){
		($co_sid=$bamid)=~s/F1/B1/;
	}else{
		my ($co)=$bamid=~/_(.*)/;
		($co_sid=$bamid)=~s/B1/$co/;
	}
	
	my $VALUE=$XML->createElement("VALUE");
	$VALUE->appendTextNode($co_sid);
	$ANALYSIS_ATTRIBUTE->appendChild($VALUE);
	$ANALYSIS_ATTRIBUTES->appendChild($ANALYSIS_ATTRIBUTE);
	
	$ANALYSIS->appendChild($ANALYSIS_ATTRIBUTES);
	$XML->setDocumentElement($ANALYSIS);
	
	return($XML);
}





	








