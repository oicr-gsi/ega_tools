xml_merge=
set=SAMPLE_SET
xml_dir=XML

echo '<?xml version="1.0" encoding="utf-8"?>' > $xml_merge
echo '<$set xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLoction="ftp://ftp.sra.ebi.ac.uk/meta/xsd/sra_1_5/SRA.run.xsd">' >> $xml_merge
cat xml_dir/*.xml | grep -v utf-8  >> $xml_merge
echo '</$set>' >> $xml_merge
