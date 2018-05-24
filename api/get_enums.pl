#! /usr/bin/perl

use strict;
use warnings;
use JSON::PP;
use Data::Dumper;
use XML::Simple;

### LOGIN to API
my $api="https://ega.crg.eu/submitterportal/v1";
my $username="ega-box-137";   ## eg. ega-box-12
my $password="GnwyrZm6";   ## password for box



my @enums=qw/analysis_file_types analysis_types case_control dataset_types experiment_types file_types genders instrument_models library_selections library_sources library_strategies reference_chromosomes reference_genomes study_types/;
for my $enum(@enums){
	#$cmd="curl -H \"X-Token: $token\" -X GET $api/enums/experiment_types";
	my $cmd="curl -s -X GET $api/enums/$enum";
	
	my $json=`$cmd`;
	my $response=decode_json($json);
	#print Dumper($response);<STDIN>;
	
	my $count=$$response{response}{numTotalResults};
	print "\n$enum $count\n";
	my $len=length($enum) +1 + length($count);
	print "=" x $len . "\n";
	
	my @results=@{$$response{response}{result}};
	for my $result(sort{ $$a{tag}<=>$$b{tag} } @results){
		my $tag=$$result{tag};
		my $value=$$result{value};
		print "$tag\t$value\n";
	}
	#print Dumper($result);<STDIN>;
	
	
	
	
	
}





### get the token
#my $cmd="curl -X POST $path/login -d username=$username --data-urlencode password=\"$password\" -d loginType=\"submitter\"";
#print "$cmd\n";
#my $json=`$cmd`;
#my $response=decode_json($json);
#my $result=shift @{$$response{response}{result}};

#my $token=$$result{session}{sessionToken} || 0;
#die "no token obtained" unless($token);

#print "Session Token = $token\n";


#$cmd="curl -X DELETE -H \“X-Token: $token\”  $path/logout";
#print "$cmd\n";
#$json=`$cmd`;
#$response=decode_json($json);
#$result=shift @{$$response{response}{result}};
#print Dumper($response);
#print "Session $token ended\n";













