use strict;
use warnings;
use JSON::PP;
use Data::Dumper;
use XML::Simple;

### LOGIN to API
my $path="https://ega.crg.eu/submitterportal/v1";
my $username="";   ## eg. ega-box-12
my $password="";   ## password for box


### get the token
my $cmd="curl -X POST $path/login -d username=$username --data-urlencode password=\"$password\" -d loginType=\"submitter\"";
print "$cmd\n";
my $json=`$cmd`;
my $login_response=decode_json($json);
my $result=shift @{$$login_response{response}{result}};

my $token=$$result{session}{sessionToken} || 0;
die "no token obtained" unless($token);

print "Session Token = $token\n";

get_study_accessions($token);
get_dataset_accessions($token);
get_sample_accessions($token);
get_experiment_accessions($token);
get_run_accessions($token);
get_analysis_accessions($token);

### NOTE there is not an api to get all EGAF file accessions,  these come down wiht queries to datasets. 
#get_file_accessions($token);

### LOGOUT
$cmd="curl -X DELETE -H \"X-Token: $token\" $path/logout";
$json=`$cmd`;
print "Session $token ended\n";


sub get_study_accessions{
	my ($token)=@_;
	
	print "\nAccessing study accessions\n";
	my $cmd="curl -X GET -H \"X-Token: $token\" \"$path/studies?status=SUBMITTED&skip=0&limit=0\"";
	my $json=`$cmd`;
	my $json_hash=decode_json($json);

	#print Dumper($json_hash);
	my $numTotalResults=$$json_hash{response}{numTotalResults};
	my @results=@{$$json_hash{response}{result}};
	my $count=scalar @results;
	print "$count results obtained from $numTotalResults Total Results\n";
	(open my $OUT,">","EGA_Studies.txt") || die "could not open EGA_Studies";
	print $OUT "ID\tAlias\tEGAS\tTitle\tStudyType\tSubmission_date\n";
	for my $result(@results){
         my $id=$$result{id} || 0;
         my $egaS=$$result{egaAccessionId} || 0;
         my $alias=$$result{alias} || 0;
         my $creationTime=$$result{creationTime};
         my ($sec, $min, $hour, $day,$month,$year) = (localtime($creationTime/1000))[0,1,2,3,4,5];
         $year+=1900;
         $month++;
         $day++;
         my $time="${year}-${month}-${day}";
		 my $title=$$result{title} || 0;
		 my $studytype=$$result{studyType} || 0;
         print $OUT "$id\t$alias\t$egaS\t$studytype\t$time\n";
	 }
	 close $OUT;
 }


#exit;


sub get_dataset_accessions{
	my ($token)=@_;
	print "\nAccessing dataset accessions\n";
	my $cmd="curl -X GET -H \"X-Token: $token\" \"$path/datasets?status=SUBMITTED&skip=0&limit=0\"";
	my $json=`$cmd`;
	my $json_hash=decode_json($json);

	my $numTotalResults=$$json_hash{response}{numTotalResults};
	my @results=@{$$json_hash{response}{result}};
	my $count=scalar @results;
	print "$count results obtained from $numTotalResults Total Results\n";
	(open my $OUT,">","EGA_Datasets.txt") || die "could not open EGA_Datasets";
	print $OUT "ID\tAlias\tEGAD\tSubmission_date\n";
	for my $result(@results){
		my $id=$$result{id} || 0;
		my $egaD=$$result{egaAccessionId} || 0;
		my $alias=$$result{alias} || 0;
		my $creationTime=$$result{creationTime};
		my ($sec, $min, $hour, $day,$month,$year) = (localtime($creationTime/1000))[0,1,2,3,4,5];
		$year+=1900;
		$month++;
		$day++;
		my $time="${year}-${month}-${day}";
		print $OUT "$id\t$alias\t$egaD\t$time\n";
	}
	close $OUT;
}

sub get_sample_accessions{
	my ($token)=@_;
	print "\nAccessing sample accessions\n";
	#$cmd="curl -X GET -H \"X-Token: $token\" $path/samples?status=SUBMITTED\\&skip=0\\&limit=0";
	my $cmd="curl -X GET -H \"X-Token: $token\" \"$path/samples?status=SUBMITTED&skip=0&limit=0\"";
	my	$json=`$cmd`;
	my $json_hash=decode_json($json);
	my $numTotalResults=$$json_hash{response}{numTotalResults};
	my @results=@{$$json_hash{response}{result}};
	my $count=scalar @results;
	print "$count results obtained from $numTotalResults Total Results\n";
	(open my $OUT,">","EGA_Samples.txt") || die "could not open EGA_Samples";
	print $OUT "ID\tAlias\tEGAN\tSubmission_date\n";
	for my $result(@results){
		my $id=$$result{id} || 0;
		my $egaN=$$result{egaAccessionId} || 0;
		my $alias=$$result{alias} || 0;
		my $creationTime=$$result{creationTime};
		my ($sec, $min, $hour, $day,$month,$year) = (localtime($creationTime/1000))[0,1,2,3,4,5]; 
		$year+=1900;
		$month++;
		$day++;
		my $time="${year}-${month}-${day}";
	print $OUT "$id\t$alias\t$egaN\t$time\n";
	}
	close $OUT;
}

sub get_experiment_accessions{
	my ($token)=@_;
	print "\nAccessing experiment accessions\n";
	#$cmd="curl -X GET -H \"X-Token: $token\" $path/experiments?status=SUBMITTED\\&skip=0\\&limit=0";
	my $cmd="curl -X GET -H \"X-Token: $token\" \"$path/experiments?status=SUBMITTED&skip=0&limit=0\"";
	my $json=`$cmd`;
	my $json_hash=decode_json($json);


	my $numTotalResults=$$json_hash{response}{numTotalResults};
	my @results=@{$$json_hash{response}{result}};
	my $count=scalar @results;
	print "$count results obtained from $numTotalResults Total Results\n";

	(open my $OUT,">","EGA_Experiments.txt") || die "could not open EGA_Experiments";
	print $OUT "ID\tAlias\tEGAX\tSubmission_date\tSample\tStudy\tLib_Strategy\n";
	for my $result(@results){
		#print Dumper($result);<STDIN>;
		my $id=$$result{id} || 0;
		my $egaX=$$result{egaAccessionId} || 0;
		my $alias=$$result{alias} || 0;
		my $creationTime=$$result{creationTime};
		my ($sec, $min, $hour, $day,$month,$year) = (localtime($creationTime/1000))[0,1,2,3,4,5]; 
		$year+=1900;
		$month++;
		$day++;
		my $time="${year}-${month}-${day}";
	
		my $sid=$$result{sampleId} || 0;
		my $study=$$result{studyId} || 0;
		my $strategy=$$result{libraryStrategy} || 0;


		print $OUT "$id\t$alias\t$egaX\t$time\t$sid\t$study\t$strategy\n";
	
	}
	close $OUT;
}


sub get_run_accessions{
	my ($token)=@_;
	print "\nAccessing run accessions\n";
	#$cmd="curl -X GET -H \"X-Token: $token\" $path/runs?status=SUBMITTED\\&skip=0\\&limit=0";
	my $cmd="curl -X GET -H \"X-Token: $token\" \"$path/runs?status=SUBMITTED&skip=0&limit=0\"";
	my $json=`$cmd`;
	my $json_hash=decode_json($json);

	my $numTotalResults=$$json_hash{response}{numTotalResults};
	my @results=@{$$json_hash{response}{result}};
	my $count=scalar @results;
	print "$count results obtained from $numTotalResults Total Results\n";

	(open my $OUT,">","EGA_Runs.txt") || die "could not open EGA_Runs";
	print $OUT "ID\tAlias\tEGAR\tExperiment\tSubmission_date\tfiles\n";
	for my $result(@results){

		my $id=$$result{id} || 0;
		my $egaR=$$result{egaAccessionId} || 0;
		my $alias=$$result{alias} || 0;
		my $creationTime=$$result{creationTime};
		my ($sec, $min, $hour, $day,$month,$year) = (localtime($creationTime/1000))[0,1,2,3,4,5]; 
		$year+=1900;
		$month++;
		$day++;
		my $time="${year}-${month}-${day}";
		my @files=map{ $$_{fileName} } @{$$result{files}};
		my $filelist=join(";",@files);
	
		my $expid=$$result{experimentId} || 0;
	
		print $OUT "$id\t$alias\t$egaR\t$expid\t$time\t$filelist\n";
	
		#print Dumper($result);<STDIN>;
	}
	close $OUT;
}

sub get_analysis_accessions{
	my ($token)=@_;
	print "\nAccessing analysis accessions\n";
	#$cmd="curl -X GET -H \"X-Token: $token\" $path/analyses?status=SUBMITTED\\&skip=0\\&limit=0";
	my $cmd="curl -X GET -H \"X-Token: $token\" \"$path/analyses?status=SUBMITTED&skip=0&limit=0\"";
	my $json=`$cmd`;
	my $json_hash=decode_json($json);


	my $numTotalResults=$$json_hash{response}{numTotalResults};
	my @results=@{$$json_hash{response}{result}};
	my $count=scalar @results;
	print "$count results obtained from $numTotalResults Total Results\n";

	(open my $OUT,">","EGA_Analyses.txt") || die "could not open EGA_Analysis";
	print $OUT "Alias\tEGAZ\tSubmission_date\tfiles\tStudy\tSample\n";
	for my $result(@results){
		
		### the result returned do NOT provide a link to the sampel accession
		### they do however provide the XML, which includes an SAMPLE_REF tag.  can extract from here, 
		my $xmlstring=$$result{xml};
		my $xml=XMLin($xmlstring);
		my $sample_ref=$$xml{ANALYSIS}{SAMPLE_REF}{accession} || 0;
		my $egaZ=$$result{egaAccessionId} || 0;
		my $alias=$$result{alias} || 0;
		my $creationTime=$$result{creationTime};
		my ($sec, $min, $hour, $day,$month,$year) = (localtime($creationTime/1000))[0,1,2,3,4,5]; 
		$year+=1900;
		$month++;
		$day++;
		my $time="${year}-${month}-${day}";
		my @files=map{ $$_{fileName} } @{$$result{files}};
		my $filelist=join(";",@files);

		my $study=$$result{studyId} || 0;
		print $OUT "$alias\t$egaZ\t$time\t$filelist\t$study\t$sample_ref\n";
		#print Dumper($result);<STDIN>;
	}
	close $OUT;
}








