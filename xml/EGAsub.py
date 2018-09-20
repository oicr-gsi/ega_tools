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


# use this function to create the table headers
def FormatDbTableHeader(Table):
    '''
    (str) -> str
    Take a database Table string name and return a string with column headers and datatype
    to be used in  SQL command to create a tabkle and its header
    '''
    
    # create a list of columns and datatype
    Columns = []
    
    # check the table in database
    if Table == 'Samples':
        # create a list of valid fields
        ValidFields = ['alias', 'taxonId', 'speciesName', 'species', 'gender', 'gender:units',
                       'subjectId', 'ExternalDataset', 'source', 'sourceId', 'bioSampleId', 'SRASample',
                       'anonymizedName', 'phenotype', 'description', 'title',
                       'attributes', 'caseOrControl', 'cellLine', 'organismPart',
                       'region', 'sampleAge', 'sampleDetail', 'brokerName', 'centerName', 'runCenter',
                       'creationTime', 'json', 'egaAccessionId']
        # format columns with data type
        Columns = []
        for i in range(len(ValidFields)):
            if ValidFields[i] == 'json':
                Columns.append(ValidFields[i] + ' MEDIUMTEXT NULL')
            else:
                Columns.append(L[i] + ' TEXT NULL')
    
    # convert list to string    
    Columns = ' '.join(Columns)        
    return Columns


# use this function to insert data in a database table
def FormatDbTableData(L):
    '''
    (list) -> tuple
    Take a list of data and return a tuple to be inserted in a database table 
    '''
    
    # create a tuple of strings data values
    Values = ()
    # loop over data 
    for i in range(len(L)):
        if L[i] == '' or L[i] == None or L[i] == 'NA':
            Values = Values.__add__(('NULL',))
        else:
            Values = Values.__add__((str(L[i]),))
    return Values


# use this function to download a database table as a flat file
def DownloadDbTable(args):
    '''
    (list) -> None
    Take a list of command line arguments and download data for Table of interest
    to the specified outputfile
    '''
    
    # connect to database
    conn = EstablishConnection(args.credential, args.database)
    
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
    conn = EstablishConnection(args.credential, args.database)
        # list all tables
    cur = conn.cursor()
    cur.execute('SHOW TABLES')
    Tables = [i[0] for i in cur]
    
    # check if table exists in database
    if args.table not in Tables:
        # create table with column headers
        Columns = FormatDbTableHeader(args.table)
        cur = conn.cursor()
        cur.execute('CREATE TABLE {0} ({1})'.format(args.table, Columns))
        conn.commit()
    
    # compare headers between input file and table in database
    cur = conn.cursor()
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
        Columns = FormatDbTableHeader(args.table)            
        SqlCommands = ['DROP TABLE IF EXISTS {0}'.format(args.table),
                       'CREATE TABLE {0} ({1})'.format(args.table, Columns)]
        # execute each command in turn with a new cursor
        for i in range(len(SqlCommands)):
            with conn.cursor() as cur:
                cur.execute(SqlCommands[i])
                conn.commit()
        # insert data in table
        cur = conn.cursor()
        # make a string with column names
        ColumnNames = ', '.join(header)
        assert ColumnNames == ', '.join(fields)
        # loop over list of lines from file content
        for i in range(len(content)):
            # convert line string to list of column entries
            line = content[i].split('\t')
            # get the values to be added to database table
            Values = FormatDbTableData(line)
            # check that all column data has been recorded for the current line
            assert len(Values) == len(fields)
            # add values into table
            cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(args.table, ColumnNames, Values))
            conn.commit()
        # close connection
        conn.close()
    
    
# use this function to parse the input sample table
def ParseSamplesInputTable(TableFile):
    '''
    (file) -> dict
    Take a tab-delimited file with sample information and return a dictionary
    with sample alias as key and a dictionary of attributes as value.
    Precondition: "alias" is a required in the table header 
    '''
    
    Data = {}
    infile = open(TableFile)
    header = infile.readline().rstrip().split('\t')
    assert 'alias' in header
    for line in infile:
        if line.rstrip() != '':
            line = line.rstrip().split('\t')
            alias = line[header.index('alias')]
            assert alias not in Data
            Data[alias] = {}
            for i in range(len(line)):
                Data[alias][header[i]] = line[i]
    infile.close()
    return Data


# use this function to parse the input samples table
def LoadSamples(TableFile, ValidFields):
    '''
    (file, list) -> dict
    Take a file with sample information and a list of valid column fields in the
    database Sample Table and return a dictionary with sample alias as key and
    a dictionary of attributes as value. Non-valid attributes are ignored and
    missing attributes are allowed
    '''

    # parse the input table file
    Table = ParseSamplesInputTable(TableFile)
    # get the fields of the input table and convert to lower cap
    FileFields = list(map(lambda x: x.lower(), list(Table[list(Table.keys())[0]].keys())))
    
    # create a dictionary of dictionaries {sample: {key:values}}
    Data = {}
    # make a list of valid fields in lower case for comparison with fields in file
    FieldsLower = '\t'.join(ValidFields).lower().split('\t')
    # check if required fields are present
    required_fields = ['alias', 'gender', 'phenotype', 'title', 'description']
    for field in required_fields:
        if field.lower() not in FileFields:
            print('table format is not valid, 1 or more fields are missing')
            return Data

    # for each sample in input table:
    # 1) remove non-valid fields: non-valid fields are ignored
    # 2) add hard-coded fields: some fields are required but may be omitted in the input table
    # 3) add empty string to missing fields: missing values are allowed for some fields
    
    # make a set of non-supported fields
    NonSupportedFields = set()
    # remove non-valid fields 
    for sample in Table:
        # initialize data inner dict
        Data[sample] = {}
        # loop over sample fields 
        for field in Table[sample]:
            # check if valid field, convert to lower caps to allow for mis-spelling in input table
            if field.lower() in FieldsLower:
                # valid field, get field valid name
                field_name = ValidFields[FieldsLower.index(field)]
                # add value to dict
                Data[sample][field_name] = Table[sample][field]
            else:
                # print message for field only once
                if field not in NonSupportedFields:
                    print('{0} column is not supported in Samples table'.format(field))
                    NonSupportedFields.add(field)
    # add values to missing columns 
    # some required fields are hard-coded and can be omitted from the input table
    hard_coded_fields = {'taxonId': '9606', 'speciesName': 'Homo sapiens', 'species': 'human',
                         'brokerName': 'EGA', 'centerName': 'OICR', 'runCenter': 'OICR'}
    for sample in Data:
        for field in ValidFields:
            if field not in Data[sample]:
                if field in hard_coded_fields:
                    Data[sample][field] = hard_coded_fields[field]
                else:
                    Data[sample][field] = ''
    return Data
                        
  

# use this function to parse a database table into a dictionary
def ParseMetaDataDatabaseTable(CredentialFile, Database='EGA', Table):
    '''
    (file, str, str) -> dict
    Take a file with credentials to connect to Metadata Database and return a dictionary
    with all the data in the Metadata Database Table with ebiId as key of columns:value pairs
    '''

    # connect to database
    conn = EstablishConnection(CredentialFile, Database)
    # select all fields from table
    cur = conn.cursor()
    cur.execute('SELECT * FROM {0}'.format(Table))
    
    # create a dict {ebiId: {column:value}}
    TableData = {}
    
    # get the table header
    header = [i[0] for i in cur.description]
    
    # loop over table records
    for i in cur:
        ebiId = i[header.index('ebiId')]
        # check that key is unique in database
        assert ebiId not in TableData
        # initialize inner dict
        TableData[ebiId] = {}
        # populate inner dict with columns: value
        for j in range(len(i)):
            TableData[ebiId][header[j]] = i[j]
    # close connection
    conn.close()
    
    return TableData


# use this function to add data to Samples Table
def AddSamples(args):
    '''
    
    
    '''

    
    # parse input table, create a dict {sample: {attributes:values}}
    Samples = ParseSamplesInputTable(args.intable)
    
    # look for samples in the metadata database
    Metadata = ParseMetaDaDatabaseTable(args.credential, 'EGA', 'Samples')
    # create a dict of samples with accessions {alias: ebiId}
    RecordedSamples = {}
    for i in Metadata:
        j = Metadata[i]['alias']
        assert j not in RecordedSamples
        RecordedSamples[j] = i
    
    # check if samples already have a accession number
    for sample in Samples:
        if sample in RecordedSamples:
            # sample already has an accession number
            # replace column:values for that sample with info from the Metadata db
            # make a list of valid database table columns
            
            # if column is key in ebiid dict: replace value
            
            # if not, leave empty or hard-coded value
    
    
    
    
    
    
    
    
    # connect to database
    conn = EstablishConnection(args.credential, args.database)
    cur = conn.cusor()
    # list all tables in database
    cur.execute('SHOW TABLES')
    Tables = [i[0] for i in cur]
    # check if Samples table exists
    if 'Samples' in Tables:
        # make a list of samples that are already recorded in the Samples table
        cur.execute('SELECT Samples.alias FROM Samples')
        PresentSamples = [i[0] for i in cur]
    else:
        PresentSamples = []
        
    # check if samples already exist
    for sample in Samples:
        if sample in PresentSamples:
            print('sample {0} is already recorded'.format(sample))
        else:
            # need to add sample and its information to database
    
    
    
    
        # existing samples cannot be replaced
        
        # if exist, do nothing
        
        # if doesn't exist add info to table
        
    
    
    
    
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









if __name__ == '__main__':

    # create top-level parser
    parser = argparse.ArgumentParser(prog = 'EGAsub.py', description='manages submission to EGA')
    subparsers = parser.add_subparsers(title='sub-commands', description='valid sub-commands', help = 'sub-commands help')

    # Download sub-commands
    Download_parser = subparsers.add_parser('DownloadTable', help ='Download database table to flat file')
    Download_parser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    Download_parser.add_argument('-d', '--Database', dest='database', default='EGAsub', help='database name. default is EGAsub')
    Download_parser.add_argument('-t', '--Table', dest='table', help='database table to be downloaded', required=True)
    Download_parser.add_argument('-o', '--Output', dest='outputfile', help='path to the tab-delimited file with database table content', required=True)
    Download_parser.set_defaults(func=DownloadDbTable)

    # Upload sub-commands
    Upload_parser = subparsers.add_parser('UploadTable', help ='Upload file to database table. This will either replace the entire table or update records')
    Upload_parser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    Upload_parser.add_argument('-d', '--Database', dest='database', default='EGAsub', help='database name. default is EGAsub')
    Upload_parser.add_argument('-t', '--Table', dest='table', help='database table to be modified', required=True)
    Upload_parser.add_argument('-i', '--InputFile', dest='inputfile', help='file to upload to database table', required=True)
    Upload_parser.set_defaults(func=UploadDbTable)
    
    
    
  
    
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
