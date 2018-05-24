#! /usr/bin/perl

use strict;
use warnings;

use File::Basename;
use lib dirname(__FILE__);
use EGA_API;

use Data::Dumper;

use Getopt::Long;
my %opts = ();
GetOptions(
	"api=s" 		=> \$opts{api},
	"username|u=s"	=> \$opts{username},
	"password|p=s"	=> \$opts{password},
	"action|a=s"    => \$opts{action},
	
	"object_type=s" => \$opts{object_type},
	"status=s"      => \$opts{status},
	"xml=s"         => \$opts{xml},
	

	"help" 			=> \$opts{help},
);


%opts=validate_options(%opts);


my $token=get_token($opts{api},$opts{username},$opts{password});

if($opts{action} eq "view"){
	#my $to_delete='5b05caa18d962971c1f66d4d';
	#delete_object($path,$token,'datasets',$to_delete);

	my %xml=view_objects($opts{api},$token,$opts{object_type},$opts{status});
	
	if($opts{xml}){
		for my $id(keys %xml){
			(open my $XML,">","$opts{xml}/${id}.xml") || die "unable to open xml file at $opts{xml}/${id}.xml";
			print $XML $xml{$id};
			close $XML;
		}
	}
}

#my $json_sub="{\"title\" : \"test\",\"description\": \"test\"}";
#my $submissionId=get_submission_id($path,$token,$json_sub);


#exit;
#my $file="Dataset.GATCI_TEST.1.xml";
#submit_object($path,$token,$submissionId,'datasets','xml',$file);

#validate_submission($path,$token,$submissionId);






sub validate_options{
	my (%opts)=@_;
	usage("Help requested.") if($opts{help});
	
	$opts{api}=$opts{api} || "https://ega.crg.eu/submitterportal/v1";
	
	if(! $opts{username} && ! $opts{password}){
		usage("username (ega-box) and password required");
	}

	my %valid_actions=map{$_=>1} qw/open submit view delete/;
	## open : opens a submission
	## submit : submits an object to an open submission
	## view : views objects
	## delete : deletes objects
	if(!$opts{action} || !$valid_actions{$opts{action}}){
		usage("must provide a valid action");
	}
	
	if($opts{action} eq "view"){
		usage("object_type to view not provided") unless($opts{object_type});
		usage("status to view not provided") unless($opts{status});
		
		if($opts{xml}){
			usage("directory to save xml $opts{xml} not found") unless(-d $opts{xml});
		}
		
	}

	
	return %opts;
}



sub usage{
	print "\nega_api.pl [options]\n";
	print "Options are as follows:\n";
	print "\t--api String/address. Optional. The address of the EGA api, defaults to https://ega.crg.eu/submitterportal/v1\n";
	print "\t--username|-u String. Required.  The name of the ega-box (eg. ega-box-12)\n";
	print "\t--password|-p String. Required.  Password for the ega-box\n";
	print "\n";
	print "\t--action|a String.  Required.  One of open|submit|view|delete\n";
	print "\t\topen     : open a data submission, returns a submissionId\n";
	print "\t\tsubmit   : submit an object, must provide submissionID and objecttype\n";
	print "\t\tview     : view registered object ids and status\n";
	print "\t\t           --object_type analyses|dacs|datasets|experiments|policies|runs|samples|studies\n";
	print "\t\t           --status DRAFT|VALIDATED|VALIDATED_WITH_ERRORS|SUBMITTED|NOT_SUBMITTED\n";
	print "\t\t           --xml directory to save xml, named by object id\n";
	
	
	print "\t\tdelete   : delete objects by id\n";
	
	
	print "\t--help displays this usage message.\n";

	die "\n@_\n\n";
}








