echo '<?xml version="1.0" encoding="utf-8"?>' > xmls/Analysis.2.xml
echo '<ANALYSIS_SET xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLoction="ftp://ftp.sra.ebi.ac.uk/meta/xsd/sra_1_5/SRA.run.xsd">' >> xmls/Analysis.2.xml
cat xmls/analysis/*.xml | grep -v utf-8  >> xmls/Analysis.2.xml
echo '</ANALYSIS_SET>' >> xmls/Analysis.2.xml
