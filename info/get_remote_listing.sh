box=""
pw=""
path=""
ssh xfer.hpc.oicr.on.ca "lftp -u $box,$pw -e \" set ftp:ssl-allow false;ls $path; bye; \" ftp://ftp-private.ebi.ac.uk"
