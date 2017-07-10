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
		study_id=>"EGAS00001001615",
		broker_name=>"EGA",
		analysis_center=>"OICR",
		center_name=>"OICR_ICGC",
	},

	analysis=>{

		title=>"Alignment of sequence data from CPCGene Prostate Cancer Samples",
		description=>"lane level reads were aligned against the reference sequence;".
					  "multiple lane alignments from the same library bams were merged with duplicate marking;".
					  "multiple library alignments were merged with no duplicate marking;".
					  "sample level aligments were processed through GATK realignment and base call recalibration as tumour normal pairs;".
					  "details are indicated in the analysis attributes",
		attributes=>{
			aligner => "bwa aln",
			aligner_ver => "0.7.10",
			mark_duplicates => "picard",
			mark_duplicates_ver => "1.8.7",
			realign_recalibration => "GATK",
			realign_recalibration_ver =>"2.4.9",
		},
		stage_path=>"/CPCG/BRCA/bam",
		ref=>{
			name=>"GRCh37",
			accession=>"GCA_000001405.1",
			chromosomes=>{
				chr1=>"CM000663.1",chr2=>"CM000664.1",chr3=>"CM000665.1",chr4=>"CM000666.1",chr5=>"CM000667.1",chr6=>"CM000668.1",chr7=>"CM000669.1",
				chr8=>"CM000670.1",chr9=>"CM000671.1",chr10=>"CM000672.1",chr11=>"CM000673.1",chr12=>"CM000674.1",chr13=>"CM000675.1",chr14=>"CM000676.1",
				chr15=>"CM000677.1",chr16=>"CM000678.1",chr17=>"CM000679.1",chr18=>"CM000680.1",chr19=>"CM000681.1",chr20=>"CM000682.1",chr21=>"CM000683.1",
				chr22=>"CM000684.1",chrX=>"CM000685.1",chrY=>"CM000686.1",chrM=>"J01415.2",
				chr1_gl000191_random=>"GL000191.1",chr1_gl000192_random=>"GL000192.1",
				chr4_ctg9_hap1=>"GL000257.1",chr4_gl000193_random=>"GL000193.1",chr4_gl000194_random=>"L000194.1",
				chr6_apd_hap1=>"GL000250.1",chr6_cox_hap2=>"GL000251.1",chr6_dbb_hap3=>"GL000252.1",chr6_mann_hap4=>"GL000253.1",
				chr6_mcf_hap5=>"GL000254.1",chr6_qbl_hap6=>"GL000255.1",chr6_ssto_hap7=>"GL000256.1",
				chr7_gl000195_random=>"GL000195.1",
				chr8_gl000196_random=>"GL000196.1",chr8_gl000197_random=>"GL000197.1",
				chr9_gl000198_random=>"GL000198.1",chr9_gl000199_random=>"GL000199.1",chr9_gl000200_random=>"GL000200.1",chr9_gl000201_random=>"GL000201.1",
				chr11_gl000202_random=>"GL000202.1",
				chr17_ctg5_hap1=>"GL000258.1",chr17_gl000203_random=>"GL000203.1",chr17_gl000204_random=>"GL000204.1",chr17_gl000205_random=>"GL000205.1",chr17_gl000206_random=>"GL000206.1",
				chr18_gl000207_random=>"GL000207.1",
				chr19_gl000208_random=>"GL000208.1",chr19_gl000209_random=>"GL000209.1",
				chr21_gl000210_random=>"GL000210.1",
				chrUn_gl000211=>"GL000211.1",chrUn_gl000212=>"GL000212.1",chrUn_gl000213=>"GL000213.1",chrUn_gl000214=>"GL000214.1",chrUn_gl000215=>"GL000215.1",
				chrUn_gl000216=>"GL000216.1",chrUn_gl000217=>"GL000217.1",chrUn_gl000218=>"GL000218.1",chrUn_gl000219=>"GL000219.1",chrUn_gl000220=>"GL000220.1",
				chrUn_gl000221=>"GL000221.1",chrUn_gl000222=>"GL000222.1",chrUn_gl000223=>"GL000223.1",chrUn_gl000224=>"GL000224.1",chrUn_gl000225=>"GL000225.1",
				chrUn_gl000226=>"GL000226.1",chrUn_gl000227=>"GL000227.1",chrUn_gl000228=>"GL000228.1",chrUn_gl000229=>"GL000229.1",chrUn_gl000230=>"GL000230.1",
				chrUn_gl000231=>"GL000231.1",chrUn_gl000232=>"GL000232.1",chrUn_gl000233=>"GL000233.1",chrUn_gl000234=>"GL000234.1",chrUn_gl000235=>"GL000235.1",
				chrUn_gl000236=>"GL000236.1",chrUn_gl000237=>"GL000237.1",chrUn_gl000238=>"GL000238.1",chrUn_gl000239=>"GL000239.1",chrUn_gl000240=>"GL000240.1",
				chrUn_gl000241=>"GL000241.1",chrUn_gl000242=>"GL000242.1",chrUn_gl000243=>"GL000243.1",chrUn_gl000244=>"GL000244.1",chrUn_gl000245=>"GL000245.1",
				chrUn_gl000246=>"GL000246.1",chrUn_gl000247=>"GL000247.1",chrUn_gl000248=>"GL000248.1",chrUn_gl000249=>"GL000249.1",
			},
		},	
	},
	
	
	
);

my ($run_file,$bam_rg,$bam_md5)=@ARGV;
die "run_file not provided" unless($run_file && -e $run_file);
die "no bam readgroups provided" unless($bam_rg && -e $bam_rg);
die "no bam md5 provided" unless($bam_md5 && -e $bam_md5);


### need a table prepared with accessions EGAN, EGAX, EGAR and identfiiers Sample, Experiment and Run

my %samples;
load_run_accessions($run_file,\%samples);

my %bams;

load_bam_files($bam_md5,\%bams);
load_bam_rg($bam_rg,\%bams);


### sample names in the bam file are OICR names, want to switch to boutros lab names
##
for my $bam(sort keys %bams){
	my $SM=$bam;
	$SM=~s/\.bam//;
	$SM=~s/_.*//;
	
	### get the sample name for the bam rg
	my $SMX=( keys %{$bams{$bam}{rg}} )[0];
		
	
	
	$bams{$bam}{rg}{$SM}= delete $bams{$bam}{rg}{$SMX};
	
	
}

for my $bam(sort keys %bams){
	
	(my $bamid=$bam)=~s/\.bam//;
	print "generating xml for $bam\n";
	my $xml=analysis_xml("BRCA",$bamid,$bams{$bam},\%samples,\%p);
	(open my $XML,">","xmls/analysis/${bam}.xml") || die "unable to open bam xml";
	print $XML $xml->toString(1);
	close $XML;		
	
}

















