use strict;
use warnings;
use Data::Dumper;
use JSON::PP;
use XML::Simple;

use Exporter;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);

$VERSION        =       1.00;
@ISA            =       qw(Exporter);
@EXPORT         =       qw(get_token delete_token get_submission_id delete_object view_objects submit_object validate_submission);


my %http_response_codes=(200=>'OK',400=>'Bad Request',401=>"Unauthorized/Forbidden",404=>"NOT FOUND",500=>"INTERNAL SERVER ERROR");

sub get_token{
	my($api,$username,$password)=@_;
	print "obtaining token\n";
	my $cmd="curl -s -X POST $api/login -d username=$username --data-urlencode password=\"$password\" -d loginType=\"submitter\"";
	#print "$cmd\n";
	my $json=`$cmd`;
	my $response=decode_json($json);
	check_response($response);
	my $result=shift @{$$response{response}{result}};
	my $token=$$result{session}{sessionToken} || 0;
	die "no token obtained" unless($token);
	print STDERR "Obtained Session Token = $token\n";
	return $token;
}

sub delete_token{
	my($api,$token)=@_;
	my $cmd="curl -H \“X-Token: $token\” -X DELETE  $api/logout";
	#print "$cmd\n";
	my $json=`$cmd`;
	my $response=decode_json($json);
	my $result=shift @{$$response{response}{result}};
	#print Dumper($response);
	print STDERR "Session $token ended\n";
}
sub get_submission_id{
	my($api,$token,$json_sub)=@_;
	my $cmd="curl -H \"X-Token: $token\" -H \"Content-type: application/json\" -X POST $api/submissions -d '$json_sub'";
	#print "$cmd\n";

	my $json=`$cmd`;
	my $response=decode_json($json);
	my $result=shift @{$$response{response}{result}};
	my $submissionId=$$result{id};
	print STDERR "SubmissionId=$submissionId\n";
	return $submissionId;
}
sub check_response{
	my ($response)=@_;
	#print Dumper($response);<STDIN>;
	my $http_response_code=$$response{header}{code};
	my $http_response_text=$http_response_codes{$http_response_code} || "";
	if($http_response_code ne '200'){
		my $userMessage=$$response{header}{userMessage} || "";
		my $devMessage=$$response{header}{developerMessage} || "";
		print "$http_response_code : $http_response_text";
		print "userMessage:$userMessage\n";
		print "devMessage:$devMessage\n";
		exit;
	}else{
		return 1;
	}
}
sub delete_object{
	my($api,$token,$object_type,$id)=@_;
	print STDERR "attempting to delete object $id from $object_type\n";
	my $cmd="curl -H \"X-Token: $token\" -X DELETE $api/$object_type/{$id}";
	my $json=`$cmd`;
	my $response=decode_json($json);
	check_response($response);
	print Dumper($response);
}



sub view_objects{
	my($api,$token,$object_type,$status)=@_;
	print STDERR "getting objects of type:$object_type ; status:$status\n";
	
	my %statuses=map{$_=>1}qw/DRAFT VALIDATED VALIDATED_WITH_ERRORS SUBMITTED NOT_SUBMITTED/;
	my %object_types=map{$_=>1}qw/analyses dacs datasets experiments policies runs samples studies/;

	die "invalid object_type request $object_type" unless($object_type && $object_types{$object_type});
	die "invalid status request $status" unless($status && $statuses{$status});
	
	my $cmd;
	if($status ne "NOT_SUBMITTED"){
		### query for the specific status
		$cmd="curl -s -H \"X-Token: $token\" -X GEt $api/$object_type?status=$status";
	}else{
		### query for without status, will return all but SUBMITTED
		$cmd="curl -s -H \"X-Token: $token\" -X GEt $api/$object_type";
	}
	
	my $json=`$cmd`;
	my $response=decode_json($json);
	
	#print Dumper($response);
	check_response($response);
	#print Dumper($response);
	my $result_count=$$response{response}{numTotalResults};
	print "$result_count results\n";
		
	my @results=@{$$response{response}{result}};
	my $n=0;my %xml;
	for my $result(@results){
		$n++;
		#print Dumper($result);
		my $status=$$result{status} || 0;
		my $id=$$result{id} || 0;
		my $submissionId=$$result{submissionId} || 0;
		my $alias=$$result{alias} || 0;
		print "$n : $status : $id : $submissionId : $alias\n"; 
		
		$xml{$id}=$$result{xml};
		
	}
	return %xml;
	
	
}


sub submit_object{
	my($api,$token,$submissionId,$object_type,$structure_type,$file)=@_;
	### object types : analyses,dacs,datasets,experiments,policies,,runs,samples,studies
	### structure_type : xml,json
	my %paths=(
		json=>{
			analyses=>'analyses',
			dacs=>'dacs',
			datasets=>'datasets',
			experiments=>'experiments',
			policies=>'policies',
			runs=>'runs',
			samples=>'samples',
			studies=>'studies',
		},
		xml=>{
			analyses=>'analyses/xml',
			dacs=>'dacs/xml',
			datasets=>'datasets/xml',
			experiments=>'experiments/xml',
			policies=>'policies/xml',
			runs=>'runs/sequencing/xml',
			samples=>'samples/xml',
			studies=>'studies/xml',
		},
	
	);
	
	
	
	
	my $path=$paths{$structure_type}{$object_type} || 0;
	if($path){
		my $cmd="curl -H \"X-Token: $token\" -H \"Content-type: application/$structure_type \" -X POST $api/submissions/$submissionId/$path -d \@$file";
		#print "$cmd\n";
		my $json=`$cmd`;
		my $response=decode_json($json);
		check_response($response);
		my $result_count=$$response{response}{numTotalResults};
		print "$result_count results\n";
			
		my @results=@{$$response{response}{result}};
		my $n=0;
		for my $result(@results){
			$n++;
			#print Dumper($result);
			my $status=$$result{status} || 0;
			print "$n : $status\n"; 
		}
	}else{
		die "no path could be formed from structure and object type";
	}
}

sub validate_submission{
	my ($api,$token,$submissionId)=@_;
	my $cmd="curl -H \"X-Token: $token\" -X PUT $api/submissions/$submissionId/?action=VALIDATE";
	my $json=`$cmd`;
	my $response=decode_json($json);
	check_response($response);
	#print Dumper($response);
	my $result_count=$$response{response}{numTotalResults};
	print "$result_count results\n";
		
	my @results=@{$$response{response}{result}};
	my $n=0;
	for my $result(@results){
		$n++;
		#print Dumper($result);
		my $status=$$result{status} || 0;
		print STDERR "$n : $status\n"; 
		
		if($status eq "VALIDATED_WITH_ERRORS"){
			my @validationErrors=@{$$result{validationError}};
			print STDERR "Validation Errors:\n";
			print STDERR join("\n",@validationErrors) . "\n";
		}
		
	}
}














