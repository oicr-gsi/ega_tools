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

	"object_type=s" => \$opts{object_type},  ## action = view, create, delete
	"status=s"      => \$opts{status},       ## action = view
	"xml=s"         => \$opts{xml},					## action = view,create
	"json=s"				=> \$opts{json},        ## action = open
	"id=s"          => \$opts{id},           ## action = create, delete, validate

	"submission_id=s"  => \$opts{submission_id},
	"object_id=s" 		 => \$opts{object_id},

	"help" 			=> \$opts{help},
);


%opts=validate_options(%opts);


my $token=get_token($opts{api},$opts{username},$opts{password});

if($opts{action} eq "view"){
	#my $to_delete='5b05caa18d962971c1f66d4d';
	#delete_object($path,$token,'datasets',$to_delete);



	my %xml=view_objects($opts{api},$token,$opts{object_type},$opts{object_id},$opts{submission_id},$opts{status});

	if($opts{xml}){
		for my $id(keys %xml){
			(open my $XML,">","$opts{xml}/${id}.xml") || die "unable to open xml file at $opts{xml}/${id}.xml";
			print $XML $xml{$id};
			close $XML;
		}
	}
}

if($opts{action} eq "open"){
	my $submissionId=open_submission($opts{api},$token,$opts{json});
	print "opened Submission, ID=$submissionId\n";
}


if($opts{action} eq "create"){
	create_objects($opts{api},$token,$opts{id},$opts{object_type},$opts{structure_type},$opts{xml});
}

if($opts{action} eq "delete"){
	delete_object($opts{api},$token,$opts{object_type},$opts{object_id});
}

if($opts{action} eq "validate"){
	validate_submission($opts{api},$token,$opts{id});
}

if($opts{action} eq "submit"){
	print "request to submit data, needs work";

}






sub validate_options{
	my (%opts)=@_;
	usage("Help requested.") if($opts{help});

	$opts{api}=$opts{api} || "https://ega.crg.eu/submitterportal/v1";

	if(! $opts{username} && ! $opts{password}){
		usage("username (ega-box) and password required");
	}

	my %valid_actions=map{$_=>1} qw/open create view delete validate/;
	## open : opens a submission
	## submit : submits an object to an open submission
	## view : views objects
	## delete : deletes objects
	if(!$opts{action} || !$valid_actions{$opts{action}}){
		usage("must provide a valid action");
	}

	if($opts{action} eq "view"){
		usage("object_type to view not provided") unless($opts{object_type});

		if($opts{status} && $opts{object_id}){
			usage("requested view should be for status or object_id, not both");
		}
		if($opts{submission_id} && $opts{object_id}){
			usage("requested view should be for submission_id or object_id, not both");
		}

		### status reques is NOT_SUBMITTED by defaults
		$opts{status}="ALL" unless($opts{status});
		$opts{object_id}=0 unless($opts{object_id});
		$opts{submission_id}=0 unless($opts{submission_id});


		if($opts{xml}){
			usage("directory to save xml $opts{xml} not found") unless(-d $opts{xml});
		}

	}

	if($opts{action} eq "open"){
		usage("json file describing submission not provided or not found") unless($opts{json} && -e $opts{json});
	}

	if($opts{action} eq "create"){
		usage("must provide a valid submissionId") unless($opts{id});
		usage("object_type to create not provided") unless($opts{object_type});
		usage("xml file not provided or not found") unless($opts{xml} && -e $opts{xml});

		$opts{structure_type}="xml";  ### will expand later to allow json
	}

	if($opts{action} eq "delete"){
		usage("must provide an objectId") unless($opts{object_id});
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
	print "\t--action|a String.  Required.  One of open|create|validate|view|delete\n";
	print "\t\topen     : open a data submission, returns a submissionId\n";
	print "\t\t           --json json file describing the submission. Include a title and description\n";
	print "\t\tcreate   : create an object, must provide submissionID and objecttype\n";
	print "\t\t           --id a valid submissionId\n";
	print "\t\t           --object_type analyses|dacs|datasets|experiments|policies|runs|samples|studies\n";
	print "\t\t           --xml file describing the object(s) to be created\n";
	print "\t\tvalidate : validate objects in a submission, must provide submissionID\n";
	print "\t\t           --id a valid submissionId\n";
	print "\t\tsubmit   : submit validated objects, must provide submissionID\n";
	print "\t\t           --submission_id a valid submissionId\n";
	print "\t\tdelete   : delete an object, must provide object_typeID and objecttype\n";
	print "\t\t           --object_id a valid objectId\n";
	print "\t\t           --object_type analyses|dacs|datasets|experiments|policies|runs|samples|studies\n";
	print "\t\tview     : view registered objects by type and status\n";
	print "\t\t           --object_type analyses|dacs|datasets|experiments|policies|runs|samples|studies\n";
	print "\t\t           --object_id a valid objectID (OPTIONAL)\n";
	print "\t\t           --submission_id a valid submissionID (OPTIONAL)\n";
	print "\t\t           --status DRAFT|VALIDATED|VALIDATED_WITH_ERRORS|PARTIALLY_SUBMITTED|SUBMITTED|ALL(default)\n";
	print "\t\t           --xml directory to save xml files which will be named by object id\n";
	print "\t\t              Notes:\n";
	print "\t\t               if object_id is supplied, status is ignored\n";
	print "\t\t              	status ALL includes all non-submitted objects\n";




	print "\t\tdelete   : delete objects by id\n";


	print "\t--help displays this usage message.\n";

	die "\n@_\n\n";
}
