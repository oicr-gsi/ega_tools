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




# resource for jaon formatting and api submission
#https://ega-archive.org/submission/programmatic_submissions/json-message-format
#https://ega-archive.org/submission/programmatic_submissions/submitting-metadata




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


# use this function to parse the input sample table
def ParseSampleTable(Table):
    '''
    (file) -> list
    Take a tab-delimited file with sample information and return a list of dictionaries,
    each dictionary storing the information of a unique sample
    Preconditions: Required fields must be present or returned list is empty,
    and missing entries are not permitted (can be '', NA or anything else)
    '''
    
    # create list of dicts to store the sample data {sample: {attribute: key}}
    L = []
    
    infile = open(Table)
    # get file header
    Header = infile.read().rstrip().split('\n')
    # check that required fields are present
    Missing = [i for i in ['alias', 'subjectId', 'gender', 'phenotype'] if i not in Header]
    if len(Missing) != 0:
        print('These required fields are missing: {0}'.format(', '.join(Missing)))
    else:
        # required fields are present, read the content of the file
        Content = infile.read().rstrip().split('\n')
        for S in Content:
            S = S.split('\t')
            # missing values are not permitted
            assert len(Header) == len(S)
            # create a dict to store the key: value pairs
            D = {}
            # get the sample name
            sample = S[Header.index('alias')]
            D[sample] = {}
            for i in range(len(Header)):
                assert Header[i] not in D[sample]
                D[sample][Header[i]] = S[i]    
            L.append(D)
    infile.close()
    return L        
            
 
# use this function to parse the sample config file
def ParseSampleConfig(Config):
    '''
    (file) -> dict
    Take a config file and return a dictionary of key: value pairs
    '''
    
    infile = open(Config)
    Header = infile.readline().rstrip().split('\t')
    # create a dict {key: value}
    D = {}
    # check that required fields are present
    RequiredFields = ['xmlns', 'xsi', 'taxon_id', 'scientific_name', 'common_name', 'title',
                      'center_name', 'run_center', 'study_id', 'study_title', 'study_design', 'broker_name']
    Missing = [i for i in RequiredFields if i not in Header]
    if len(Missing) != 0:
        print('These required fields are missing: {0}'.format(', '.join(Missing)))
    else:
        for line in infile:
            if line.rstrip() != '':
                line = line.rstrip().split('\t')
                for i in range(len(Header)):
                    D[Header[i]] = line[i]
    infile.close()
    return D


# use this function convert data into data to be instered in a database table
def FormatData(L):
    '''
    (list) -> tuple
    Take a list of data and return a tuple to be inserted in a database table 
    '''
    
    # create a tuple of strings data values
    Values = []
    # loop over data 
    for i in range(len(L)):
        if L[i] == '' or L[i] == None or L[i] == 'NA':
            Values.append('NULL')
        else:
            Values.append(str(L[i]))
    return tuple(Values)


# use this function to add data to the sample table
def AddSampleInfo(args):
    '''
    (list) -> None
    Take a list of command line arguments and add sample information
    to the Sample Table of the EGAsub database if samples are not already registered
    '''
    
    # connect to submission database
    conn = EstablishConnection(args.credential, args.database)
    
    # parse input table [{sample: {key:value}}] 
    Data = ParseSampleTable(args.table)

    # create table if table doesn't exist
    cur = conn.cursor()
    cur.execute('SHOW TABLES')
    Tables = [i[0] for i in cur]
    if args.table not in Tables:
        Fields = ['alias', 'subjectId', 'title', 'description', 'caseOrControlId',  
                  'gender', 'organismPart', 'cellLine', 'region', 'phenotype',
                  'anonymizedName', 'biosampleId', 'sampleAge',
                  'sampleDetail', 'attributes', 'Species', 'Taxon',
                  'ScientificName', 'SampleTitle', 'Center', 'RunCenter',
                  'StudyId', 'ProjectId', 'StudyTitle', 'StudyDesign', 'Broker',
                  'Json', 'Receipt', 'CreationTime', 'egaAccessionId', 'Box']
        # format colums with datatype
        Columns = []
        for i in range(len(Fields)):
            if Fields[i] == 'Json' or Fields[i] == 'Receipt':
                Columns.append(Fields[i] + ' MEDIUMTEXT NULL')
            else:
                Columns.append(Fields[i] + ' TEXT NULL')
        # convert list to string    
        Columns = ' '.join(Columns)        
        # create table with column headers
        cur = conn.cursor()
        cur.execute('CREATE TABLE {0} ({1})'.format(args.table, Columns))
        conn.commit()
    else:
        # get the column headers from the table
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='{0}'".format(args.table))
        Fields = [i[0] for i in cur]
        ColumnNames = ', '.join(Fields)
        
    # pull down sample alias and egaId from metadata db, alias should be unique
    cur.execute('SELECT {0}.alias, {0}.egaAccessionId from {0} WHERE {0}.egaBox=\"{1}\"'.format(args.table, args.box)) 
    # create a dict {alias: accession}
    Registered = {}
    for i in cur:
        assert i[0] not in Registered
        Registered[i[0]] = i[1]
            
    # check that samples are not already in the database for that box
    for D in Data:
        # get sample alias
        sample = list(D.keys())[0]
        if sample in Registered:
            # skip sample, already registered
            print('{0} is already registered in box {1} under accession {2}'.format(sample, args.box, Registered[sample]))
        else:
            # add box to sample data
            D[sample]['Box'] = args.box
            # list values according to the table column order
            L = [D[sample][field] for field in Fields]
            # convert data to strings, converting missing values to NULL
            Values = FormatData(L)
            cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(args.table, ColumnNames, Values))
            conn.commit()
    
    conn.close()



######### review code below



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
    
    # create table and its header in database if it doesn't exist
    FirstTimeCreateTable(args.credential, args.database, args.table)
    
    # connect to database
    conn = EstablishConnection(args.credential, args.database)
       
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
    
    

# use this function to parse a database table into a dictionary
def ParseDatabaseTable(CredentialFile, Database, Table, Info):
    '''
    (file, str, str, str) -> dict
    Take a file with credentials to connect to Database and return a dictionary
    with all the data in the Database Table with a MasterKey that depends on the
    database type (Info) and the table as key to columns:value pairs
    '''

    # connect to database
    conn = EstablishConnection(CredentialFile, Database)
    # select all fields from table
    cur = conn.cursor()
    cur.execute('SELECT * FROM {0}'.format(Table))
    
    # create a dict {MasterKey: {column:value}}
    TableData = {}
    
    # check which should be the Master Key
    if Info == 'Metadata':
        # master key is ebiId, create a dict {ebiId: {column:value}}
        MasterKey = 'ebiId'
    elif Info == 'Submission':
        # master key depends on table
        if Table == 'Samples':
            # Master key is alias
            MasterKey = 'alias'
    
    # get the table header
    header = [i[0] for i in cur.description]
    
    # loop over table records
    for i in cur:
        j = i[header.index(MasterKey)]
        # check that key is unique in database
        assert j not in TableData
        # initialize inner dict
        TableData[j] = {}
        # populate inner dict with columns: value
        for k in range(len(i)):
            TableData[j][header[k]] = i[k]
    # close connection
    conn.close()
    
    return TableData








if __name__ == '__main__':

    # create top-level parser
    parser = argparse.ArgumentParser(prog = 'EGAsub.py', description='manages submission to EGA')
    subparsers = parser.add_subparsers(title='sub-commands', description='valid sub-commands', help = 'sub-commands help')

    # add samples to Samples Table
    AddSamples = subparsers.add_parser('AddSamples', help ='Add sample information')
    AddSamples.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    AddSamples.add_argument('-d', '--Database', dest='database', default='EGAsub', help='database name. Default is EGAsub')
    AddSamples.add_argument('-t', '--Table', dest='table', default='Samples', help='Samples table. Default is Samples')
    AddSamples.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
    AddSamples.add_argument('--Species', dest='species', default='Human', help='common species name')
    AddSamples.add_argument('--Taxon', dest='taxon', default='9606', help='species ID')    
    AddSamples.add_argument('--Name', dest='name', default='Homo sapiens', help='Species scientific name')
    AddSamples.add_argument('--SampleTitle', dest='sampleTitle', help='Title associated with submission', required=True)
    AddSamples.add_argument('--Center', dest='center', default='OICR_ICGC', help='Center name. Default is OICR_ICGC')
    AddSamples.add_argument('--RunCenter', dest='run', default='OICR', help='Run center name. Default is OICR')
    AddSamples.add_argument('--Study', dest='study', default=' EGAS00001000900', help='Study ID. default is  EGAS00001000900')
    AddSamples.add_argument('--StudyTitle', dest='studyTitle', help='Title associated with study', required=True)
    AddSamples.add_argument('--Design', dest='design', help='Study design')
    AddSamples.add_argument('--Broker', dest='broker', default='EGA', help='Broker name. Default is EGA')
    AddSamples.add_argument('--Xlmns', dest='xmlns', default='http://www.w3.org/2001/XMLSchema-instance', help='Xml schema. Default is http://www.w3.org/2001/XMLSchema-instance')
    AddSamples.add_argument('--Xsi', dest='xsi', default='ftp://ftp.sra.ebi.ac.uk/meta/xsd/sra_1_5/SRA.run.xsd', help='Xsi model. Default is ftp://ftp.sra.ebi.ac.uk/meta/xsd/sra_1_5/SRA.run.xsd')
    AddSamples.set_defaults(func=AddSampleInfo)


################### code below requires review
















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
    Samples_parser = subparsers.add_parser('AddSamples', help ='Add sample information to Samples table')
    
    # box name
    # input table
    # directory where to save the json
    # submission xml or json
    # submission alias
    
    Samples_parser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    Samples_parser.add_argument('-d', '--Database', dest='database', default='EGAsub', help='database name. default is EGAsub')
    Samples_parser.add_argument('-t', '--Table', dest='table', default='Samples', help='database table to be modified')
    Samples_parser.add_argument('-i', '--InputFile', dest='intable', help='file with sample information to add to the Samples table', required=True)
    
    
    
    
    
    
    
    
    
    # get arguments from the command line
    args = parser.parse_args()
    # pass the args to the default function
    args.func(args)
