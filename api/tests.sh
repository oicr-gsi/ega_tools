### view
#./ega_api.pl -u ega-box-137 -p GnwyrZm6 -a view --object_type datasets --status NOT_SUBMITTED --xml temp


## open submission
#./ega_api.pl -u ega-box-137 -p GnwyrZm6 -a open
#./ega_api.pl -u ega-box-137 -p GnwyrZm6 -a open --json test_submission.json


subId=5b06bb678d962971c1f66de1
xml=Dataset.GATCI_TEST.1.xml
./ega_api.pl -u ega-box-137 -p GnwyrZm6 -a create --object_type=datasets  --id $subId --xml $xml
