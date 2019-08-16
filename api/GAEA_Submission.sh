#### 1. Declare variables ####
# decare array with box names
declare -a boxes=("ega-box-12" "ega-box-137" "ega-box-1269")
# provide path to python submission script
SubmissionScript=/.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/Submission_Tools/Gaea.py
# provide path to credential file
credentials=/.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/Submission_Tools/.EGA_metData
# provide path to encryption keys
EncryptionKeys=/.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/publickeys/public_keys.gpg
# provide portal url
portal=https://ega.crg.eu/submitterportal/v1
# provide path to python 
PythonPath=/.mounts/labs/PDE/Modules/sw/python/Python-3.6.4/bin/python3.6


#### 2. LIST FILES ON STAGING SERVERS ####

# list files on the staging server of available boxes
for boxname in "${boxes[@]}"; do
    echo "listing files on staging server for "$boxname""
    module load python-gsi/3.6.4; python3.6 $SubmissionScript StagingServer -b "$boxname" -c $credentials -s EGASUB -m EGA --RunsTable Runs --AnalysesTable Analyses --StagingTable StagingServer --FootprintTable FootPrint;
done;

#### 3. FORM JSON FOR EACH OBJECT #### 

# loop over boxes
for boxname in "${boxes[@]}"; do
    echo "forming json for "$boxname""
    
    # form analyses json in EGASUb db
    module load python-gsi/3.6.4; python3.6 $SubmissionScript FormJson -c $credentials -t Analyses -m EGA -s EGASUB -b "$boxname" -p AnalysesProjects -a AnalysesAttributes -k $EncryptionKeys -f FootPrint -o analyses -q production -u aspera -d 10 --Mem 10 --Max 10 --MaxFootPrint 15 --Remove;

    # form datasets json in EGASUB db
    module load python-gsi/3.6.4; python3.6 $SubmissionScript FormJson -c $credentials -t Datasets -m EGA -s EGASUB -b "$boxname" -o datasets;

    # form policies json in EGASUB db 
    module load python-gsi/3.6.4; python3.6 $SubmissionScript FormJson -c $credentials -t Policies -m EGA -s EGASUB -b "$boxname" -o policies;

    # form samples json in EGASUB db
    module load python-gsi/3.6.4; python3.6 $SubmissionScript FormJson -c $credentials -t Samples -m EGA -s EGASUB -b "$boxname" -a SamplesAttributes  -o samples;

    # form experiments  json in EGASUB db
    module load python-gsi/3.6.4; python3.6 $SubmissionScript FormJson -c $credentials -t Experiments -m EGA -s EGASUB -b "$boxname" -o experiments;

    # form runs json in EGASUb db
    module load python-gsi/3.6.4; python3.6 $SubmissionScript FormJson -c $credentials -t Runs -m EGA -s EGASUB -b "$boxname" -k $EncryptionKeys -f FootPrint -o runs -q production -u aspera -d 10 --Mem 10 --Max 10 --MaxFootPrint 15 --Remove;

    # form dacs json in EGASUB db
    module load python-gsi/3.6.4; python3.6 $SubmissionScript FormJson -c $credentials -t Dacs -m EGA -s EGASUB -b "$boxname" -o dacs;

    # form studies json in EGASUB db
    module load python-gsi/3.6.4; python3.6 $SubmissionScript FormJson -c $credentials -t Studies -m EGA -s EGASUB -b "$boxname" -o studies;

done;

#### 4. SUBMIT METADATA OF EACH OBJECT TO EGA ####

# loop over boxes
for boxname in "${boxes[@]}"; do
    echo "submitting metadata for "$boxname"";

    # connect to xfer4 and submit analyses metadata from EGASUB db
    ssh xfer4 "$PythonPath $SubmissionScript RegisterObject -c $credentials -t Analyses -m EGA -s EGASUB -b "$boxname" -o analyses --Portal $portal";

    # connect to xfer4 and submit datasets metadata from EGASUB db
    ssh xfer4 "$PythonPath $SubmissionScript RegisterObject -c $credentials -t Datasets -m EGA  -s EGASUB -b "$boxname" -o datasets --Portal $portal";

    # connect to xfer4 and submit policies metadata in ega-box-12 from EGASUB db
    ssh xfer4 "$PythonPath $SubmissionScript RegisterObject -c $credentials -t Policies -m EGA -s EGASUB -b "$boxname" -o policies --Portal $portal";

    # connect to xfer4 and submit samples metadata from EGASUB db
    ssh xfer4 "$PythonPath $SubmissionScript RegisterObject -c $credentials -t Samples -m EGA -s EGASUB -b "$boxname" -o samples --Portal $portal";

    # connect to xfer4 and submit experiments metadata from EGASUB db
    ssh xfer4 "$PythonPath $SubmissionScript RegisterObject -c $credentials -t Experiments -m EGA -s EGASUB -b "$boxname" -o experiments --Portal $portal";

    # connect to xfer4 and submit runs metadata from EGASUB db
    ssh xfer4 "$PythonPath $SubmissionScript RegisterObject -c $credentials -t Runs -m EGA -s EGASUB -b "$boxname" -o runs --Portal $portal";

    # connect to xfer4 and submit dacs metadata from EGASUB db
    ssh xfer4 "$PythonPath $SubmissionScript RegisterObject -c $credentials -t Dacs -m EGA -s EGASUB -b "$boxname" -o dacs --Portal $portal";

    # connect to xfer4 and submit studies metadata from EGASUB db
    ssh xfer4 "$PythonPath $SubmissionScript RegisterObject -c $credentials -t Studies -m EGA -s EGASUB -b "$boxname" -o studies --Portal $portal";  

done;

#### 5. DOWNLOAD METADATA TO EACH BOX IN EGA DB ####
for boxname in "${boxes[@]}"; do
    echo "downloading metadata for "$boxname""    
    ssh xfer4 "/.mounts/labs/PDE/Modules/sw/python/Python-3.6.4/bin/python3.6 /.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/Submission_Tools/DownloadEGAMetaData.py -c $credentials -b "$boxname"";
done;

