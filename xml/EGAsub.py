# -*- coding: utf-8 -*-
"""
Created on Tue Sep 11 13:37:40 2018

@author: rjovelin
"""


# import modules
import json
import subprocess
import time
import xml.etree.ElementTree as ET
import pymysql
import sys
import os
import time
import argparse



# use this function to extract credentials from file
def ExtractCredentials(CredentialFile):
    '''
    (file) -> tuple
    Take a file with database credentials and return a dictionary
    with the credentials as key:value pairs
    '''
    
    Credentials = {}            
    infile = open(CredentialFile)            
    for line in infile:
        if line.rstrip() != '':
            line = line.rstrip().split('=')
            Credentials[line[0]] = line[1]
    infile.close()        
    return Credentials


# use this function to connect to the gsi database
def EstablishConnection(CredentialFile, database):
    '''
    (list, str) -> connection object    
    Take a file with database credentials and a string specifying if the connection
    if made to the Metadata or Submission database
    '''
    
    # extract database credentials from the command
    Credentials = ExtractCredentials(CredentialFile)
    # determine the database name
    if database == 'Metadata':
        DbName = Credentials['DbMet']
    elif database == 'Submission':
        DbName = Credentials['DbSub']
    # connnect to the database
    conn = pymysql.connect(host = Credentials['DbHost'], user = Credentials['DbUser'], password = Credentials['DbPasswd'], db = DbName, charset = "utf8")
    return conn 


# use this function to download a database table as a flat file
def DownloadDbTable(args):
    '''
    (list) -> None
    Take a list of command line arguments and download data for Table of interest
    to the specified outputfile
    '''
    
    # connect to database
    conn = EstablishConnection(args)
    
    # list all tables
    cur = conn.cursor()
    cur.execute('SHOW TABLES')
    Tables = [i[0] for i in cur]
    # check that table of interest exists
    if args.table in Tables:
        # extract all data from tables
        cur.execute('SELECT * FROM {0}'.format(args.table))
        # get the header
        header = '\t'.join([i[0] for i in cur.description])
        # open file for writting
        newfile = open(args.outputfile, 'w')
        # write header and data to file
        newfile.write(header + '\n')
        for data in cur:
            newfile.write('\t'.join(data) + '\n')
        # close file and connection
        newfile.close()
        conn.close()
    else:
        print('table {0} is not in Database'.format(args.table))
        # close connection and exit
        conn.close()
        sys.exit(2)
        

# use this function to specify column type in database table
def FormatTableHeader(L, table):
    '''
    (list, str) -> list
    Take a list of column fields for a given table and return a string
    that specify the column type in SQL
    '''
    # all columns hold string data, add 
    Cols = []
    
    pass 

#    for i in range(1, len(L)):
#        if L[i] in ('title', 'description', 'designDescription'):
#            if i == len(L) -1:
#                Cols.append(L[i] + ' TEXT NULL')
#            else:
#                Cols.append(L[i] + ' TEXT NULL,')
#        elif L[i] in ('files', 'xml'):
#            if i == len(L) -1:
#                Cols.append(L[i] + ' MEDIUMTEXT NULL')
#            else:
#                Cols.append(L[i] + ' MEDIUMTEXT NULL,')
#        else:
#            if i == len(L) -1:
#                Cols.append(L[i] + ' VARCHAR(100) NULL')
#            else:
#                Cols.append(L[i] + ' VARCHAR(100) NULL,')
#    # first column holds primary key
#    Cols.insert(0, L[0] + ' VARCHAR(100) PRIMARY KEY UNIQUE,')
#    return ' '.join(Cols)




# use this function to upload content and replace table database
def UploadDbTable(args):
    '''
    (file) -> None
    Take a list of command line arguments including credentials to connect to 
    database, a tab-delimited file and a table name as it appears in the databse
    and replace this table with the file content if table format is compatible
    Note: This potentially results in information loss as it will replace
    any information in the database table with the file content,
    even if the database table contains more information than the file
    '''
    
    # connect to database
    conn = EstablishConnection(args)
        # list all tables
    cur = conn.cursor()
    cur.execute('SHOW TABLES')
    Tables = [i[0] for i in cur]
    
    # check that table to be uploaded is present in database
    if args.table in Tables:
        # compare headers between input file and table in database
        cur.execute('SELECT * from {0}'.format(args.table))
        fields = [i[0] for i in cur.description]
        infile = open(args.inputfile)
        header = infile.readline().rstrip().split('\t')
        if fields != header:
            print('table headers should be identical')
            conn.close()
            sys.exit(2)
        else:
            # extract the file content as a list of lines
            content = infile.read().rstrip().split('\n')
            # specify the column type of each column
            Columns = FormatTableHeader(fields, args.table)            
            SqlCommands = ['DROP TABLE IF EXISTS {0}'.format(args.table),
                           'CREATE TABLE {0} ({1})'.format(args.table, Columns)]
            # execute each command in turn with a new cursor
            for i in range(len(SqlCommands)):
                with conn.cursor() as cur:
                    cur.execute(SqlCommand[i])
                    conn.commit()
            # insert data in table
            cur = conn.cursor()
            # make a string with column names
            ColumnNames = ', '.join(header)
            assert ColumnNames == ', '.join(fields)
            # loop over list of lines from file content
            for i in range(len(content)):
                Values = ()
                # convert line string to list of column entries
                line = content[i].split('\t')
                # loop over line entries, dump values into a tuple
                for j in range(len(line)):
                    if line[j] == '' or line[j] == None or line[j] == 'NA':
                        Values = Values.__add__(('NULL',))
                    else:
                        Values = Values.__add__((line[j],))
                # check that all column data has been recorded for the current line
                assert len(Values) == len(fields)
                # add values into table
                cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(args.table, ColumnNames, Values))
                conn.commit()
            # close connection
            conn.close()
    else:
        print('table {0} is not in Database'.format(args.table))
        # close connection and exit
        conn.close()
        sys.exit(2)


# use this function to parse the input samples table
def ParseSamplesInputTable(TableFile):
    '''
    (file) -> dict
    Take a file with sample information and return a dictionary with sample alias
    as key and a dictionary of attributes as value. Non-valid attributes are
    ignored and missing attributes are allowed
    '''

    # parse the input file
    # varying content is allowed:
    # missing key value pairs are allowed, non-valid key-value pairs are ignored
    
    infile = open(TableFile)
    # lower caps in header to allow for variation in spelling
    header = infile.readline().rstrip().lower().split('\t')
    # create a dictionary of dictionaries {sample: {key:values}}
    Table = {}
    
    # create a list of valid fields
    ValidFields = ['alias', 'taxonId', 'speciesName', 'species', 'gender', 'gender:units',
                   'subjectId', 'ExternalDataset', 'source', 'sourceId', 'bioSampleId', 'SRASample',
                   'anonymizedName', 'phenotype', 'description', 'title',
                   'attributes', 'caseOrControl', 'cellLine', 'organismPart',
                   'region', 'sampleAge', 'sampleDetail', 'brokerName', 'centerName', 'runCenter']

    for line in infile:
        if line.rstrip() != '':
            line = line.rstrip().split('\t')
            # check that required fields are present
            required_fields = ['alias', 'gender', 'phenotype', 'title', 'description']
            for field in required_fields:
                if field not in header:
                    print('table format is not valid, 1 or more fields are missing')
                    sys.exit(2)
            # extract key-value pairs, ignore non-valid fields
            # get the sample alias
            alias = line.pop(header.index('alias'))
            # insert alias in 1st position
            line.insert(0, alias)
            # initialize inner dict with sample alias
            assert alias not in Table 
            Table[alias] = {}
            for i in range(len(line)):
                # check that key is valid
                if header[i] in ValidFields:
                    Table[alias][header[i]] = line[i]
    infile.close()
    
    # add empty or hard-coded values to missing keys
    hard_coded_fields = {'taxonId': '9606', 'speciesName': 'Homo sapiens',
                         'species': 'human', 'brokerName': 'EGA', 'centerName': 'OICR',
                         'runCenter': 'OICR'}
    for sample in Table:
        # compare sample attributes to valid fields
        if set(Table[sample].keys()) != set(ValidFields):
            for field in ValidFields:
                # check if field if key in inner dict
                if field not in list(Table[sample].keys()):
                    # check if field is hard-coded or not
                    if field in list(hard_coded_fiels.keys()):
                        # add hard-coded fields
                        Table[sample][field] = hard_coded_fields[field]
                    else:
                        Table[sample][field] = ''
    return Table
                        
                        

# use this function to add data to Samples Table
def AddSamples(args):
    '''
    
    
    '''

    # connect to database
    conn = EstablishConnection(args)
    
    # parse input table, create a dict {sample: {attributes:values}}
    Samples = ParseSamplesInputTable(args.intable)
    
    # add sample information to database
    cur = conn.cusor()
    # list all tables in database
    cur.execute('SHOW TABLES')
    Tables = [i[0] for i in cur]
    # check if Samples table exists
    if 'Samples' in Tables:
        # download data from table
        cur.execute('SELECT * FROM {0}'.format(args.table))
        # convert 
        
        
        # check if samples already exist
        # existing samples cannot be replaced
        
        
        
        
    else:
        
        
    
    
    
    
    # check if table already exists
    
    
    
    
    
    
    # pull down data from table
    
    
    # update
    
    
    # reinject
    
    
    # how top simply update fields for given sample without having to drop and re-create table?
    
    
    # need a working directory to save the json/xml
    
    
    # need a submission xml: json?
    
    
    # get receipt, store in db
    
    
    # extract egan and strore in db


    # check if analysis table exists
    
    
    # process to download table as flat file and reupload the entire file







center_name='OICR_ICGC' accession='ERS1020778' broker_name='EGA' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'>    <IDENTIFIERS>      <PRIMARY_ID>ERS1020778</PRIMARY_ID>      <SUBMITTER_ID namespace='OICR_ICGC'>184ND</SUBMITTER_ID>    </IDENTIFIERS>    <SAMPLE_NAME>      <TAXON_ID>9606</TAXON_ID>      <SCIENTIFIC_NAME>Homo sapiens</SCIENTIFIC_NAME>      <COMMON_NAME>human</COMMON_NAME>    </SAMPLE_NAME>    <SAMPLE_ATTRIBUTES>      <SAMPLE_ATTRIBUTE>        <TAG>gender</TAG>        <VALUE>unknown</VALUE>      </SAMPLE_ATTRIBUTE>      <SAMPLE_ATTRIBUTE>        <TAG>phenotype</TAG>        <VALUE>Matched Blood Normal</VALUE>      </SAMPLE_ATTRIBUTE>      <SAMPLE_ATTRIBUTE>        <TAG>subject_id</TAG>        <VALUE>CLL184</VALUE>      </SAMPLE_ATTRIBUTE>      <SAMPLE_ATTRIBUTE>        <TAG>ENA-CHECKLIST</TAG>        <VALUE>ERC000026</VALUE>      




ebiId	alias	attributes	caseOrControl	centerName	creationTime	description	egaAccessionId	gender	phenotype	status	subjectId	title	xml	egaBox


CPCG_External_Baca_T_01-28_WGS  01-28   Baca    https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/study.cgi?study_id=phs000447.v1.p1    T_01-28_WGS     Prostate tumor       male    X/Y     SAMN00848211    SRS306791
Sample  donor_id        External_dataset        source  SourceID        phenotype       Gender  Gender:Units    BiosampleID     SRA Sample      sample_uuid     EGA_accession








ebiId	alias	attributes	caseOrControl	centerName	creationTime	description	egaAccessionId	gender	phenotype	status	subjectId	title	xml	egaBox
ERS1020778	184ND	NULL	NULL	OICR_ICGC	2015-12-15	NULL	EGAN00001356756	unknown	Matched Blood Normal	SUBMITTED	NULL	NULL	<SAMPLE_SET>  <SAMPLE alias='184ND' center_name='OICR_ICGC' accession='ERS1020778' broker_name='EGA' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'>    <IDENTIFIERS>      <PRIMARY_ID>ERS1020778</PRIMARY_ID>      <SUBMITTER_ID namespace='OICR_ICGC'>184ND</SUBMITTER_ID>    </IDENTIFIERS>    <SAMPLE_NAME>      <TAXON_ID>9606</TAXON_ID>      <SCIENTIFIC_NAME>Homo sapiens</SCIENTIFIC_NAME>      <COMMON_NAME>human</COMMON_NAME>    </SAMPLE_NAME>    <SAMPLE_ATTRIBUTES>      <SAMPLE_ATTRIBUTE>        <TAG>gender</TAG>        <VALUE>unknown</VALUE>      </SAMPLE_ATTRIBUTE>      <SAMPLE_ATTRIBUTE>        <TAG>phenotype</TAG>        <VALUE>Matched Blood Normal</VALUE>      </SAMPLE_ATTRIBUTE>      <SAMPLE_ATTRIBUTE>        <TAG>subject_id</TAG>        <VALUE>CLL184</VALUE>      </SAMPLE_ATTRIBUTE>      <SAMPLE_ATTRIBUTE>        <TAG>ENA-CHECKLIST</TAG>        <VALUE>ERC000026</VALUE>      </SAMPLE_ATTRIBUTE>    </SAMPLE_ATTRIBUTES>  </SAMPLE></SAMPLE_SET>	ega-box-12






ebiId	alias	attributes	caseOrControl	centerName	creationTime	description	egaAccessionId	gender	phenotype	status	subjectId	title	xml	egaBox
ERS1020778	184ND	NULL	NULL	OICR_ICGC	2015-12-15	NULL	EGAN00001356756	unknown	Matched Blood Normal	SUBMITTED	NULL	NULL	<SAMPLE_SET>  <SAMPLE alias='184ND' center_name='OICR_ICGC' accession='ERS1020778' broker_name='EGA' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'>    <IDENTIFIERS>      <PRIMARY_ID>ERS1020778</PRIMARY_ID>      <SUBMITTER_ID namespace='OICR_ICGC'>184ND</SUBMITTER_ID>    </IDENTIFIERS>    <SAMPLE_NAME>      <TAXON_ID>9606</TAXON_ID>      <SCIENTIFIC_NAME>Homo sapiens</SCIENTIFIC_NAME>      <COMMON_NAME>human</COMMON_NAME>    </SAMPLE_NAME>    <SAMPLE_ATTRIBUTES>      <SAMPLE_ATTRIBUTE>        <TAG>gender</TAG>        <VALUE>unknown</VALUE>      </SAMPLE_ATTRIBUTE>      <SAMPLE_ATTRIBUTE>        <TAG>phenotype</TAG>        <VALUE>Matched Blood Normal</VALUE>      </SAMPLE_ATTRIBUTE>      <SAMPLE_ATTRIBUTE>        <TAG>subject_id</TAG>        <VALUE>CLL184</VALUE>      </SAMPLE_ATTRIBUTE>      <SAMPLE_ATTRIBUTE>        <TAG>ENA-CHECKLIST</TAG>        <VALUE>ERC000026</VALUE>      </SAMPLE_ATTRIBUTE>    </SAMPLE_ATTRIBUTES>  </SAMPLE></SAMPLE_SET>	ega-box-12












if __name__ == '__main__':

    # create top-level parser
    parser = argparse.ArgumentParser(prog = 'EGAsub.py', description='manages submission to EGA')
    subparsers = parser.add_subparsers(title='sub-commands', description='valid sub-commands', help = 'sub-commands help')

    # Download sub-commands
    Download_parser = subparsers.add_parser('DownloadTable', help ='Download database table to flat file')
    Download_parser.add_argument('-c', '--Credentials', dest=args.credential, help='file with database credentials')
    Download_parser.add_argument('-t', '--Table', dest=table, help='database table to be downloaded')
    Download_parser.add_argument('-o', '--Output', dest=outputfile, help='path to the tab-delimited file with database table content')
    Download_parser.set_defaults(func=DownloadDbTable)

    
    
  
    
    # AddSamples sub-commands
    Samples_parser = subparsers.add_parser('UpdateSamples', help ='Download database table to flat file')
    
    # box name
    # input table
    # directory where to save the json
    # submission xml or json
    # submission alias
    
    
      
    
    
    
    # get arguments from the command line
    args = parser.parse_args()
    # pass the args to the default function
    args.func(args)
