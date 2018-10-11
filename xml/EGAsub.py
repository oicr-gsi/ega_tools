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
def ParseSampleInputTable(Table):
    '''
    (file, str) -> list
    Take a tab-delimited file and return a list of dictionaries, each dictionary
    storing the information for a uniqe sample
    Preconditions: Required fields must be present or returned list is empty,
    and missing entries are not permitted (e.g. can be '', NA)
    '''
    
    # create list of dicts to store the object info {alias: {attribute: key}}
    L = []
    
    infile = open(Table)
    # get file header
    Header = infile.read().rstrip().split('\n')
    # check that required fields are present
    Missing = [i for i in ['alias', 'subjectId', 'genderId', 'phenotype'] if i not in Header]
    if len(Missing) != 0:
        print('These required fields are missing: {0}'.format(', '.join(Missing)))
    else:
        # required fields are present, read the content of the file
        Content = infile.read().rstrip().split('\n')
        for S in Content:
            S = list(map(lambda x: x.strip(), S.split('\t')))
            # missing values are not permitted
            assert len(Header) == len(S)
            # create a dict to store the key: value pairs
            D = {}
            # get the alias name
            alias = S[Header.index('alias')]
            D[alias] = {}
            for i in range(len(S)):
                if 'attribute' in S[i] or 'unit' in S[i]:
                    # D[alias][attributes] is a dict {tag: {tag: val, value: val, unit: val}}
                    assert ':' in S[i]
                    # initialize list
                    if 'attributes' not in D[alias]:
                        D[alias]['attributes'] = {}
                    if S[i].split(':')[1] not in D[alias]['attributes']:
                        D[alias]['attributes'][S[i].split(':')[1]] = {}
                    if 'attribute' in S[i]:
                        assert S[i].split(':')[0] == 'attribute'
                        D[alias]['attributes'][S[i].split(':')[1]]['tag'] = S[i].split(':')[1]
                        D[alias]['attributes'][S[i].split(':')[1]]['value'] = S[i].split(':')[2]
                    elif 'unit' in S[i]:
                        assert S[i].split(':')[0] == 'unit'
                        D[alias]['attributes'][S[i].split(':')[1]]['tag'] = S[i].split(':')[1]
                        D[alias]['attributes'][S[i].split(':')[1]]['unit'] = S[i].split(':')[2]
                else:
                    assert Header[i] not in D[alias]
                    D[alias][Header[i]] = S[i]    
            L.append(D)
    infile.close()
    return L        


# use this function to parse the input analysis table
def ParseAnalysisInputTable(Table):
    '''
    (file) -> list
    Take a tab-delimited file and return a list of dictionaries, each dictionary
    storing the information for a uniqe analysis object
    Preconditions: Required fields must be present or returned list is empty,
    and missing entries are not permitted (e.g. can be '', NA)
    '''
    
    # create a dict to store the information about the files
    D = {}
    
    infile = open(Table)
    # get file header
    Header = infile.read().rstrip().split('\n')
    # check that required fields are present
    Missing =  [i for i in ['alias', 'sampleAlias', 'filePath', 'unencryptedChecksum', 'encryptedPath', 'checksum'] if i not in Header]
    if len(Missing) != 0:
        print('These required fields are missing: {0}'.format(', '.join(Missing)))
    else:
        # required fields are present, read the content of the file
        Content = infile.read().rstrip().split('\n')
        for S in Content:
            S = S.split('\t')
            # missing values are not permitted
            assert len(Header) == len(S), 'missing values should be "" or NA'
            # extract variables from line
            L = ['alias', 'sampleAlias', 'filePath', 'unencryptedChecksum', 'encryptedPath', 'checksum']
            alias, sampleAlias, filePath, originalmd5, encryptedPath, encryptedmd5 = [S[Header.index(L[i])] for i in range(len(L))]
            # check if alias already recorded ( > 1 files for this alias)
            if alias not in D:
                # create inner dict, record sampleAlias and create files dict
                D[alias] = {}
                # record sampleAlias
                D[alias]['sampleAlias'] = sampleAlias
                D[alias]['files'] = {}
            else:
                # check that sample alias is the same as recorded for this alias
                assert D[alias]['sampleAlias'] == sampleAlias
                # record file info, filepath shouldn't be recorded already 
                assert filePath not in D[alias]['files']
                D[alias]['files'][filePath] = {'filePath': filePath, 'unencryptedChecksum': originalmd5,
                 'encryptedPath': encryptedPath, 'checksum': encryptedmd5}
                       
    infile.close()

    # create list of dicts to store the info under a same alias
    # [{alias: {'sampleAlias':sampleAlias, 'files': {filePath: {attributes: key}}}}]
    L = [{alias: D[alias]} for alias in D]             
    return L        

 
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
    
    # connect to metadata database
    conn = EstablishConnection(args.credential, args.metadatadb)
    cur = conn.cursor()
    # pull down sample alias and egaId from metadata db, alias should be unique
    cur.execute('SELECT {0}.alias, {0}.egaAccessionId from {0} WHERE {0}.egaBox=\"{1}\"'.format(args.table, args.box)) 
    # create a dict {alias: accession}
    Registered = {}
    for i in cur:
        assert i[0] not in Registered
        Registered[i[0]] = i[1]
    conn.close()

    # connect to submission database
    conn = EstablishConnection(args.credential, args.subdb)    
    
    # parse input table [{sample: {key:value}}] 
    Data = ParseSampleInputTable(args.table)

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
    
    # create a string with column headers
    ColumnNames = ', '.join(Fields)
        
    # pull down sample alias from submission db. alias may be recorded but not submitted yet. aliases must be unique and not already recorded in the same box
    cur.execute('SELECT {0}.alias from {0} WHERE {0}.egaBox=\"{1}\"'.format(args.table, args.box)) 
    # create a dict {alias: accession}
    Recorded = [i[0] for i in cur]
                
    # check that samples are not already in the database for that box
    for D in Data:
        # get sample alias
        sample = list(D.keys())[0]
        if sample in Registered:
            # skip sample, already registered
            print('{0} is already registered in box {1} under accession {2}'.format(sample, args.box, Registered[sample]))
        elif alias in Recorded:
            # skip analysis, already recorded in submission database
            print('{0} is already recorded for box {1} in the submission database'.format(alias, args.box))
        else:
            # add fields from the command
            for i in [['Box', args.box], ['Species', args.species], ['Taxon', args.name],
                      ['Name', args.name], ['SampleTitle', args.sampleTitle], ['Center', args.center],
                      ['RunCenter', args.run], ['StudyId', args.study], ['StudyTitle', args.studyTitle],
                      ['StudyDesign', args.design], ['Broker', args.broker]]:
                if i[0] not in D[sample]:
                    D[sample][i[0]] = i[1]
            # list values according to the table column order
            L = []
            for field in Fields:
                if field not in D[sample]:
                    L.append('')
                else:
                    if field == 'attributes':
                        attributes = [D[sample]['attributes'][i] for i in D[sample]['attributes']]
                        attributes = ';'.join(list(map(lambda x: str(x), attributes))).replace("'", "\"")            
                        L.append(attributes)
                    else:
                        L.append(D[sample][field])
            # convert data to strings, converting missing values to NULL
            Values = FormatData(L)
            cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(args.table, ColumnNames, Values))
            conn.commit()
    
    conn.close()

    
# use this function to parse the analysis config file
def ParseAnalysisConfig(Config):
    '''
    (file) -> dict
    Take a config file and return a dictionary of key: value pairs
    '''
    
    infile = open(Config)
    Content = infile.read().rstrip().split('\n')
    infile.close()
    # create a dict {key: value}
    D = {}
    # check that required fields are present
    Expected = ['title', 'description', 'reference']
    Fields = [S.split(':')[0].strip() for S in Content if ':' in S]
    Missing = [i for i in Expected if i not in Fields]
    if len(Missing) != 0:
        print('These required fields are missing: {0}'.format(', '.join(Missing)))
    else:
        for S in Content:
            if ':' in S:
                S = list(map(lambda x: x.strip(), S.split(':')))
                if S[0] not in ['attribute', 'unit']:
                    assert S[0] in Expected and len(S) == 2
                    D[line[0]] = line[1]
                else:
                    assert len(S) == 3
                    if 'attributes' not in D:
                        D['attributes'] = {}
                    if line[1] not in D['attributes']:
                        D['attributes'][line[1]] = {}    
                    if line[0] == 'attribute':
                        if 'tag' not in D['attributes'][line[1]]:
                            D['attributes'][line[1]]['tag'] = line[1]
                        else:
                            assert D['attributes'][line[1]]['tag'] == line[1]
                        D['attributes'][line[1]]['value'] = line[2]
                    elif line[0] == 'unit':
                        if 'tag' not in D['attributes'][line[1]]:
                            D['attributes'][line[1]]['tag'] = line[1]
                        else:
                            assert D['attributes'][line[1]]['tag'] == line[1]
                        D['attributes'][line[1]]['unit'] = line[2]
    infile.close()
    return D
    
    
# use this function to add data to the analysis table
def AddAnalysesInfo(args):
    '''
    (list) -> None
    Take a list of command line arguments and add analysis information
    to the Analysis Table of the EGAsub database if files are not already registered
    '''
    
    # connect to metadata database
    conn = EstablishConnection(args.credential, args.metadatadb)
    cur = conn.cursor()
    # pull down analysis alias and egaId from metadata db, alias should be unique
    cur.execute('SELECT {0}.alias, {0}.egaAccessionId from {0} WHERE {0}.egaBox=\"{1}\"'.format(args.table, args.box)) 
    # create a dict {alias: accession}
    Registered = {}
    for i in cur:
        assert i[0] not in Registered
        Registered[i[0]] = i[1]
    conn.close()
    
    # connect to submission database
    conn = EstablishConnection(args.credential, args.subdb)
       
    # parse input table [{alias: {'sampleAlias':sampleAlias, 'files': {filePath: {attributes: key}}}}]
    Data = ParseAnalysisInputTable(args.table)

    # parse config table 
    Config = ParseAnalysisConfig(args.config)

    "fileId", "fileName", "checksum", "unencryptedChecksum", "fileTypeId"

    # create table if table doesn't exist
    cur = conn.cursor()
    cur.execute('SHOW TABLES')
    Tables = [i[0] for i in cur]
    if args.table not in Tables:
        Fields = ["alias", "sampleAlias", "sampleEgaAccessionsId", "title",
                  "description", "studyId", "sampleReferences", "analysisCenter",
                  "analysisDate", "analysisTypeId", "files", "attributes",
                  "genomeId", "chromosomeReferences", "experimentTypeId",
                  "platform", "ProjectId", "StudyTitle",
                  "StudyDesign", "Broker", "StagePath", "filePath", "encryptedPath",
                  "Json", "Receipt", "CreationTime", "egaAccessionId", "Box", "Status"]
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
    
    # create a string with column headers
    ColumnNames = ', '.join(Fields)
    
    # pull down analysis alias from submission db. alias may be recorded but not submitted yet. aliases must be unique and not already recorded in the same box
    cur.execute('SELECT {0}.alias from {0} WHERE {0}.egaBox=\"{1}\"'.format(args.table, args.box)) 
    # create a dict {alias: accession}
    Recorded = [i[0] for i in cur]
        
    # check that analyses are not already in the database for that box
    for D in Data:
        # get analysis alias
        alias = list(D.keys())[0]
        if alias in Registered:
            # skip analysis, already registered in EGA
            print('{0} is already registered in box {1} under accession {2}'.format(alias, args.box, Registered[alias]))
        elif alias in Recorded:
            # skip analysis, already recorded in submission database
            print('{0} is already recorded for box {1} in the submission database'.format(alias, args.box))
        else:
            # add fields from the command
            for i in [['Box', args.box], ['StagePath', args.stagepath], ['analysisCenter', args.center],
                      ['studyId', args.study], ['Broker', args.broker], ['experimentTypeId', args.experiment],
                      ['analysisTypeId', args.analysistype]]:
                if i[0] not in D[sample]:
                    D[sample][i[0]] = i[1]
            # add fields from the config
            for i in Config:
                if i not in D[alias]:
                    if i == 'reference':
                        D[alias]['genomeId'] = Config['reference']
                    elif i == 'experiment':
                        D[alias]['experimentTypeId'] = Config['experiment']
                    elif i == 'attributes':
                        attributes = [Config['attributes'][j] for j in Config['attributes']]
                        attributes = ';'.join(list(map(lambda x: str(x), attributes))).replace("'", "\"")
                        D[alias]['attributes'] = attributes
                    else:
                        D[alias][i] = Config[i]
            # add fileTypeId to each file
            
            
            
            
            
            fileTypeId, analysisTypeId = '', ''
            if 'vcf' in D[alias]['filePath']:
                assert 'vcf' in D[alias]['fileLink'] and 'vcf' in D[alias]['encryptedPath']
                fileTypeId, analysisTypeId = 'vcf', 'Sequence variation (VCF)'
            elif 'bam' in D[alias]['filePath']:
                assert 'bam' in D[alias]['fileLink'] and 'bam' in D[alias]['encryptedPath']
                fileTypeId, analysisTypeId = 'bam', 'Reference Alignment (BAM)'
            elif 'bai' in D[alias]['filePath']:
                assert 'bai' in D[alias]['fileLink'] and 'bai' in D[alias]['encryptedPath']
                fileTypeId, analysisTypeId = 'bai', 'Reference Alignment (BAM)'
            assert fileTypeId != '' and analysisTypeId != ''
            if 'fileTypeId' not in D[alias]:
                    D[alias]['fileTYpeId'] = fileTypeId
            if 'analysisTypeId' not in D[alias]:
                D[alias]['analysisTypeId'] = analysisTypeId
                    
            # list values according to the table column order
            L = [D[alias][field] if field in D[alias] else '' for field in Fields]
            # convert data to strings, converting missing values to NULL                    L
            Values = FormatData(L)        
            cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(args.table, ColumnNames, Values))
            conn.commit()
            
    conn.close()            



#############check attributes below ###################################

# use this function to format the sample json
def FormatJson(D, ObjectType):
    '''
    (dict, str) -> dict
    Take a dictionary with information for an object and string describing the
    object type and return a dictionary with the expected format for that object
    '''
    
    # create a dict to be strored as a json. note: strings should have double quotes
    J = {}
    
    # check object type
    if ObjectType == 'sample':
        JsonKeys = ["alias", "title", "description", "caseOrControlId", "genderId",
                    "organismPart", "cellLine", "region", "phenotype", "subjectId",
                    "anonymizedName", "biosampleId", "sampleAge", "sampleDetail", "attributes"]
        for field in D:
            if field in JsonKeys:
                if D[field] == 'NULL':
                    J[field] = ""
                else:
                    if field == 'attributes':
                        J[field] = []
                        attributes = D[field].replace("'", "\"")
                        # convert string to dict
                        if ';' in D[field]:
                            attributes = attributes.split(';')
                            for i in range(len(attributes)):
                                J[field].append(json.loads(attributes[i]))
                        else:
                            J[field].append(json.loads(attributes))
                    else:
                        J[field] = D[field]
    elif ObjectType == 'analysis':
        JsonKeys = ["alias", "title", "description", "studyId", "sampleReferences",
                    "analysisCenter", "analysisDate", "analysisTypeId", "files",
                    "attributes", "genomeId", "chromosomeReferences", "experimentTypeId", "platform"]

        for key in D:
            if field in JsonKeys:
                if field == 'sampleReference':
                    
                    
                    
                    
                    
                    
                elif field == 'files':
                    
                
                    
                elif field == 'attributes':
                    




       
Analysis
{
  "alias": "",
  "title": "",
  "description": "",
  "studyId": "",
  "sampleReferences": [
    {
      "value": "",
      "label": ""
    }
  ],
  "analysisCenter": "",
  "analysisDate": "",
  "analysisTypeId": "", → /enums/analysis_types
  "files": [
    {
      "fileId ": "",
      "fileName": "",
      "checksum": "",
      "unencryptedChecksum": ""
      "fileTypeId":"" -> /enums/analysis_file_types
    }
  ],
  "attributes": [
    {
      "tag": "",
      "value": "",
      "unit": ""
    }
  ],
  "genomeId": "", → /enums/reference_genomes
  "chromosomeReferences": [ → /enums/reference_chromosomes
    {
      "value": "",
      "label": ""
    }
  ],
  "experimentTypeId": [ "" ], → /enums/experiment_types
  "platform": ""
}











    return J                
    
 
  
# use this function to update table with json
def AddJsonToTable(args):
    '''
    (list) -> None
    Take a list of command line arguments and insert an object-formatted json as 
    string in the Json column of each object missing the json and an accession Id
    '''
  
    # connect to submission database
    conn = EstablishConnection(args.credential, args.database)
    
    # pull data for samples without json and accession Id
    cur = conn.cursor()
    cur.execute('SELECT * FROM {0} WHERE {0}.Json=\"''\" and {0}.egaAccessionId=\"''\"'.format(args.table))
    # get column headers
    Header = [i[0] for i in cur.description]           
    
    # extract all information from the pull down
    Data = cur.fetchall()
    # check that some objects are missing Jsons
    if len(Data) != 0 and len(Header) != 0:
        # create a list of dicts to sore the object info
        L = []
        for i in Data:
            D = {}
            assert len(i) == len(Header)
            for j in range(len(i)):
                D[Header[j]] = i[j]
            L.append(D)
        # create json from each dict formatted for the given object
        Jsons = [FormatJson(D, args.object) for D in L]
        # add json back to table
        for D in Jsons:
            # get the sample alias
            alias = D["alias"]
            # add json in table for that sample
            # string need to be in double quotes for storing json as string 
            cur.execute('UPDATE {0} SET {0}.Json=\"{1}\" WHERE {0}.alias=\"{2}\";'.format(args.table, str(D).replace('\'', '\"'), alias))
            conn.commit()
    conn.close()
    
    
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
        conn.close()
        
        


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
    AddSamples.set_defaults(func=AddSampleInfo)

    # add analyses to Analyses Table
    AddAnalyses = subparsers.add_parser('AddAnalyses', help ='Add analysis information')
    AddAnalyses.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    AddAnalyses.add_argument('-d', '--Database', dest='database', default='EGAsub', help='database name. Default is EGAsub')
    AddAnalyses.add_argument('-t', '--Table', dest='table', default='Analyses', help='Samples table. Default is Analyses')
    AddAnalyses.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
    AddAnalyses.add_argument('--Config', dest='config', help='Path to config file', required=True)
    AddAnalyses.add_argument('--StagePath', dest='stagepath', help='Path on the staging server', required=True)
    AddAnalyses.add_argument('--Center', dest='center', default='OICR_ICGC', help='Name of the Analysis Center')
    AddAnalyses.add_argument('--StudyId', dest='study', help='Study accession Id', required =True)
    AddAnalyses.add_argument('--Broker', dest='broker', default='EGA', help='Broker name. Default is EGA')
    AddAnalyses.add_argument('--Experiment', dest='experiment', default='Whole genome sequencing', choices=['Genotyping by array', 'Exome sequencing', 'Whole genome sequencing', 'transcriptomics'], help='Experiment type. Default is Whole genome sequencing')
    AddAnalyses.set_defaults(func=AddAnalysesInfo)

    # add jsons to table 
    AddJson = subparsers.add_parser('AddJson', help ='Add object-formatted json to table for objects missing json and accession Id')
    AddJson.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    AddJson.add_argument('-d', '--Database', dest='database', default='EGAsub', help='database name. Default is EGAsub')
    AddJson.add_argument('-t', '--Table', dest='table', help='Database table', required=True)
    AddJson.add_argument('-o', '--Object', dest='object', choice=['sample', 'analysis', 'run', 'experiment'], help='Object type', required=True)
    AddJson.set_defaults(func=AddJsonToTable)
 
    # Download sub-commands
    Download = subparsers.add_parser('DownloadTable', help ='Download database table to flat file')
    Download.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    Download.add_argument('-d', '--Database', dest='database', default='EGAsub', help='database name. default is EGAsub')
    Download.add_argument('-t', '--Table', dest='table', help='database table to be downloaded', required=True)
    Download.add_argument('-o', '--Output', dest='outputfile', help='path to the tab-delimited file with database table content', required=True)
    Download.set_defaults(func=DownloadDbTable)

    # get arguments from the command line
    args = parser.parse_args()
    # pass the args to the default function
    args.func(args)
