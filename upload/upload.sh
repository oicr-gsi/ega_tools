PASS=""
ssh xfer4.res.oicr.on.ca "export ASPERA_SCP_PASS=$PASS;~/.aspera/connect/bin/ascp -QT -l300M -L- -k2 *.md5 ega-box-12@fasp.ega.ebi.ac.uk:;~/.aspera/connect/bin/ascp -QT -l300M -L- -k2 *.gpg ega-box-12@fasp.ega.ebi.ac.uk:;"
