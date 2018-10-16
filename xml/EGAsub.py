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
import requests



# resource for jaon formatting and api submission
#https://ega-archive.org/submission/programmatic_submissions/json-message-format
#https://ega-archive.org/submission/programmatic_submissions/submitting-metadata




# use this function to extract credentials from file
def ExtractCredentials(CredentialFile):
    '''
    (file) -> dict
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
    Missing =  [i for i in ['alias', 'sampleAlias', 'filePath', 'unencryptedChecksum', 'encryptedName', 'checksum'] if i not in Header]
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
            L = ['alias', 'sampleAlias', 'filePath', 'unencryptedChecksum', 'encryptedName', 'checksum']
            alias, sampleAlias, filePath, originalmd5, encryptedName, encryptedmd5 = [S[Header.index(L[i])] for i in range(len(L))]
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
                 'encryptedName': encryptedName, 'checksum': encryptedmd5}
                       
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
        elif sample in Recorded:
            # skip analysis, already recorded in submission database
            print('{0} is already recorded for box {1} in the submission database'.format(sample, args.box))
        else:
            # add fields from the command
            for i in [['Box', args.box], ['Species', args.species], ['Taxon', args.name],
                      ['Name', args.name], ['SampleTitle', args.sampleTitle], ['Center', args.center],
                      ['RunCenter', args.run], ['StudyId', args.study], ['StudyTitle', args.studyTitle],
                      ['StudyDesign', args.design], ['Broker', args.broker]]:
                if i[0] not in D[sample]:
                    D[sample][i[0]] = i[1]
            # set Status to ready
            D[sample]["Status"] = "ready"
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
                    D[S[0]] = S[1]
                else:
                    assert len(S) == 3
                    if 'attributes' not in D:
                        D['attributes'] = {}
                    if S[1] not in D['attributes']:
                        D['attributes'][S[1]] = {}    
                    if S[0] == 'attribute':
                        if 'tag' not in D['attributes'][S[1]]:
                            D['attributes'][S[1]]['tag'] = S[1]
                        else:
                            assert D['attributes'][S[1]]['tag'] == S[1]
                        D['attributes'][S[1]]['value'] = S[2]
                    elif S[0] == 'unit':
                        if 'tag' not in D['attributes'][S[1]]:
                            D['attributes'][S[1]]['tag'] = S[1]
                        else:
                            assert D['attributes'][S[1]]['tag'] == S[1]
                        D['attributes'][S[1]]['unit'] = S[2]
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

    # create table if table doesn't exist
    cur = conn.cursor()
    cur.execute('SHOW TABLES')
    Tables = [i[0] for i in cur]
    if args.table not in Tables:
        Fields = ["alias", "sampleAlias", "sampleEgaAccessionsId", "title",
                  "description", "studyId", "sampleReferences", "analysisCenter",
                  "analysisDate", "analysisTypeId", "files", "FileDirectory", "attributes",
                  "genomeId", "chromosomeReferences", "experimentTypeId",
                  "platform", "ProjectId", "StudyTitle",
                  "StudyDesign", "Broker", "StagePath", "filePath", "encryptedName",
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
                      ['analysisTypeId', args.analysistype], ['FileDir', args.filedir]]:
                if i[0] not in D[alias]:
                    D[alias][i[0]] = i[1]
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
            for filePath in D[alias]['files']:
                if 'vcf' in filePath:
                    fileTypeId = 'vcf'
                elif 'bam' in filePath:
                    fileTypeId = 'bam'
                elif 'bai' in filePath:
                    fileTypeId = 'bai'
                # check that file type Id is also in the encrypted file
                assert fileTypeId in D[alias]['files'][filePath]['encryptedName'], '{0} should be part of the encrypted file name'.format(fileTypeId)
                # add fileTypeId to dict
                assert 'fileTypeId' not in D[alias]['files'][filePath] 
                D[alias]['files'][filePath]['fileTypeId'] = fileTypeId
            # set Status to ready
            D[alias]["Status"] = "ready"
            
            # list values according to the table column order
            L = [D[alias][field] if field in D[alias] else '' for field in Fields]
            # convert data to strings, converting missing values to NULL                    L
            Values = FormatData(L)        
            cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(args.table, ColumnNames, Values))
            conn.commit()
            
    conn.close()            


# use this function to format the sample json
def FormatJson(D, ObjectType):
    '''
    (dict, str) -> dict
    Take a dictionary with information for an object and string describing the
    object type and return a dictionary with the expected format for that object
    or dictionary with the alias only if required fields are missing
    Precondition: strings in D have double-quotes
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
                    # some fields are required, return empty dict if field is emoty
                    if field in ["alias", "title", "description", "genderId", "phenotype", "subjectId"]:
                        # erase dict and add alias
                        J = {}
                        J["alias"] = D["alias"]
                        # return dict with alias only if required fields are missing
                        return J
                    else:
                        J[field] = ""
                else:
                    if field == 'attributes':
                        J[field] = []
                        attributes = D[field]
                        # convert string to dict
                        if ';' in attributes:
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
                if D[field] == 'NULL':
                    # some fields are required, return empty dict if field is empty
                    if field in ["alias", "title", "description", "studyId", "sampleReferences",
                    "analysisCenter", "analysisTypeId", "files", "attributes", "genomeId", "experimentTypeId"]:
                        # erase dict and add alias
                        J = {}
                        J["alias"] = D["alias"]
                        # return dict with alias only if required fields are missing
                        return J
                    else:
                        J[field] = ""
                else:
                    if field == 'sampleReference':
                        J[field] = []
                        if ';' in D[field]:
                            for accession in D[field].split(';'):
                                J[field].append({"value": accession.strip(), "label":""})
                        else:
                            J[field].append({"value": D[field], "label":""})
                    elif field == 'files':
                        assert D[field] != 'NULL'
                        J[field] = []
                        # convert string to dict
                        files = json.loads(D[field])
                        # loop over file name
                        for filePath in files:
                            # create a dict to store file info
                            d = {"fileName": files[filePath]['encryptedName'],
                                 "checksum": files[filePath]['checksum'],
                                 "unencryptedChecksum": files[filePath]['unencryptedChecksum'],
                                 "fileTypeId": files[filePath]["fileTypeId"]}
                            J[field].append(d)
                    elif field == 'attributes':
                        assert D[field] != 'NULL'
                        J[field] = []
                        # ensure strings are double-quoted
                        attributes = D[field].replace("'", "\"")
                        # convert string to dict
                        if ';' in attributes:
                            for i in range(len(attributes)):
                                J[field].append(json.loads(attributes[i]))
                        else:
                            J[field].append(json.loads(attributes))
                    else:
                        J[field] = D[field]
                    
    return J                
    
 
    
# use this script to generate qsubs to encrypt the files and do a checksum
def EncryptFiles(args):
    '''
    (list) -> None
    Take a list of command-line arguments and write bash and scripts to do a checksum
    on specified files, encryption and checksum of the encrypted file
    '''

    # command to do a cheksum and encryption
    MyCmd = 'md5sum {0} | cut -f1 -d \' \' > {1}.md5; \
    gpg --trust-model always -r EGA_Public_key -r SeqProdBio -o {1}.gpg -e {0} && \
    md5sum {1}.gpg | cut -f1 -d \' \' > {1}.gpg.md5'

    if args.inputdir:
        # files to encrypt are in the input directory    
        # make a list of files to encrypt
        Files = [os.path.join(args.inputdir, filename) for filename in os.listdir(args.inputdir)]
    elif args.inputfile:
        # files to encrypt are listed in a file
        # make a list of files to encrypt
        infile = open(args.inputfile)
        Files = infile.read().rstrip().split('\n')
        infile.close()
    
    # check that files are valid
    to_remove = [i for i in Files if os.path.isfile(i) == False]
    if len(to_remove) != 0:
        for i in to_remove:
            print('skipping {0}, file does not exist'.format(i))
            Files.remove(i)
    if len(Files) != 0:
        # check if date is added to output directory
        if args.time == True:
            # get the time year_month_day
            Time = time.strftime('%Y-%m-%d', time.localtime(time.time()))
            # add time to directory name
            args.outdir = os.path.join(args.outdir, 'EGA_submission_' + Time)
        # create outputdir if doesn't exist
        if os.path.isdir(args.outdir) == False:
            os.mkdir(args.outdir)
        # make a directory to save the qsubs
        qsubdir = os.path.join(args.outdir, 'qsub')
        if os.path.isdir(qsubdir) == False:
            os.mkdir(qsubdir)
        # create a log dir and a directory to keep qsubs already run
        for i in ['log', 'done']:
            if i not in os.listdir(qsubdir):
                os.mkdir(os.path.join(qsubdir, i))
            assert os.path.isdir(os.path.join(qsubdir, i))
        
        # loop over files
        for filePath in Files:
            # get file name
            assert filePath[-1] != '/'
            fileName = os.path.basename(filePath)
            OutFile = os.path.join(args.outdir, fileName)
            BashScript = os.path.join(qsubdir, fileName + '_encrypt.sh')
            newfile = open(BashScript, 'w')
            newfile.write(MyCmd.format(filePath, OutFile) + '\n')
            newfile.close()
            QsubScript = os.path.join(qsubdir, fileName + '_encrypt.qsub')
            newfile = open(QsubScript, 'w')
            LogDir = os.path.join(qsubdir, 'log')
            newfile.write("qsub -cwd -b y -q {0} -l h_vmem={1}g -N md5sum.{2} -e {3} -o {3} \"bash {4}\"".format(args.queue, args.mem, OutFile, LogDir, BashScript))
            newfile.close()
            
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
    

# use this function to submit Sample objects
def SubmitSamples(args):
    
    '''
    (list) -> None
    Take a list of command line arguments and submit samples to EGA following
    in sequential steps that depend on the sample status mode
    '''
   
    # workflow for submitting samples:
    # add sample info to sample table -> set status to ready
    # form json for samples in ready mode and store in table -> set status to submit
   
    # connect to the database
    conn = EstablishConnection(args.credential, args.database)
      
    # check if Sample table exists
    cur = conn.cursor()
    Tables = [i[0] for i in cur.execute('SHOW TABLES')]
    if args.table in Tables:
       
        ## form json for samples in ready mode, add to table and update status -> submit
              
        # pull data for samples with ready Status
        cur.execute('SELECT * FROM {0} WHERE {0}.Status=\"ready\"'.format(args.table))
        # get column headers
        Header = [i[0] for i in cur.description]
        # extract all information 
        Data = cur.fetchall()
        # check that samples are in ready mode
        if len(Data) != 0:
            # create a list of dicts storing the sample info
            L = []
            for i in Data:
                D = {}
                assert len(i) == len(Header)
                for j in range(len(i)):
                    D[Header[j]] = i[j]
                L.append(D)
            # create sample-formatted jsons from each dict 
            Jsons = [FormatJson(D, 'sample') for D in L]
            # add json back to sample table and update status
            for D in Jsons:
                # check if json is correctly formed (ie. required fields are present)
                if len(D) == 1:
                    print('cannot form json for sample {0}, required field(s) missing'.format(D['alias']))
                else:
                    # add json back in table and update status
                    alias = D['alias']
                    # string need to be in double quote
                    cur.execute('UPDATE {0} SET {0}.Json=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.Status=\"ready\";'.format(args.table, str(D).replace("'", "\""), alias))
                    conn.commit()
                    # update status
                    cur.execute('UPDATE {0} SET {0}.Status=\"submit\" WHERE {0}.alias="\{1}\" AND {0}.Status=\"ready\";'.format(args.table, alias))
                    conn.commit()
                   
        ## submit samples with submit status                

        # pull json for samples with ready Status
        cur.execute('SELECT {0}.Json FROM {0} WHERE {0}.Status=\"submit\"'.format(args.table))
        # get column headers
        Header = [i[0] for i in cur.description]
        # extract all information 
        Data = cur.fetchall()
        # check that samples are in submit mode
        if len(Data) != 0:
            # make a list of jsons
            L = [json.loads(i) for i in Data]
            assert len(L) == len(Data)

            # connect to EGA and get a token
            # parse credentials to get userName and Password
            Credentials = ExtractCredentials(args.credential)
            if args.box == 'ega-box-12':
                MyPassword, UserName = Credentials['MyPassWordBox12'], Credentials['UserNameBox12']
            elif args.box == 'ega-box-137':
                MyPassword, UserName = Credentials['MyPassWordBox137'], Credentials['UserNameBox137']
            
            # get the token
            data = {"username": UserName, "password": MyPassword, "loginType": "submitter"}
            # get the adress of the submission portal
            if args.portal[-1] == '/':
                URL = args.portal[:-1]
            Login = requests.post(URL + '/login', data=data)
            # check that response code is OK
            if Login.status_code == requests.codes.ok:
                # response is OK, get Token
                Token = Login.json()['response']['result'][0]['session']['sessionToken']
            
                # open a submission for each sample
                for J in L:
                    headers = {"Content-type": "application/json", "X-Token": Token}
                    submissionJson = {"title": "sample submission", "description": "opening a submission for sample {0}".format(J["alias"])}
                    submissionJson = str(submissionJson).replace("'", "\"")
                    OpenSubmission = requests.post(URL + '/submissions', headers=headers, data=submissionJson)
                    # check if submission is successfully open
                    if OpenSubmission.status_code == requests.codes.ok:
                        # get submission Id
                        submissionId = OpenSubmission.json()['response']['result'][0]['id']
                        # create sample object
                        SampleCreation = requests.post(URL + '/submissions/{0}/samples'.format(submissionId), headers=headers, data=J)
                        # check response code
                        if SampleCreation.status_code == requests.codes.ok:
                            # validate, get status (VALIDATED or VALITED_WITH_ERRORS) 
                            sampleId = SampleCreation.json()['response']['result'][0]['Id']
                            submissionStatus = SampleCreation.json()['response']['result'][0]['status']
                            assert submissionStatus == 'DRAFT'
                            # validate sample
                            SampleValidation = requests.put(URL + '/samples/sampleId?action=VALIDATE', headers=headers)
                            # check code and validation status
                            if SampleValidation.status_code == requests.codes.ok:
                                # get sample status
                                sampleStatus=SampleValidation.json()['response']['result'][0]['status']
                                if sampleStatus == 'VALIDATED':
                                    # submit sample
                                    SampleSubmission = requests.put(URL + '/samples/{0}?action=SUBMIT'.format(sampleId), headers=headers)
                                    # check if successfully submitted
                                    if SampleSubmission.status_code == requests.codes.ok:
                                        # check status
                                        Status = SampleValidation.json()['response']['result'][0]['status']
                                        if Status == 'SUBMITTED':
                                            # get the receipt, and the accession id
                                            Receipt, egaAccessionId = SampleSubmission.json(), SampleSubmission['response']['result'][0]['egaAccessionId']
                                            # add Receipt and accession to table and change status
                                            cur.execute('UPDATE {0} SET {0}.Receipt=\"{1}\" WHERE {0}.alias=\"{2}\";'.format(args.table, str(Receipt).replace("'", "\""), J["alias"]))
                                            conn.commit()
                                            cur.execute('UPDATE {0} SET {0}.egaAccessionId=\"{1}\" WHERE {0}.alias="\{2}\";'.format(args.table, egaAccessionId, J["alias"]))
                                            conn.commit()
                                            cur.execute('UPDATE {0} SET {0}.Status=\"{1}\" WHERE {0}.alias=\"{2}\";'.format(args.table, Status, J["alias"]))
                                            conn.commit()
                                        else:
                                            # delete sample
                                            SampleDeletion = requests.delete(URL + '/samples/{0}'.format(sampleId), headers=headers)
                                            print('deleting sample {0} because status is {1}'.format(J["alias"], Status))
                                    else:
                                        print('cannot submit sample {0}'.format(J["alias"]))
                                else:
                                    #delete sample
                                    print('deleting sample {0} because status is {1}'.format(J["alias"], sampleStatus))
                                    SampleDeletion = requests.delete(URL + '/samples/{0}'.format(sampleId), headers=headers)
                            else:
                                print('cannot validate sample {0}'.format(J["alias"]))
                        else:
                            print('cannot create sample object for {0}'.format(J["alias"]))
                    else:
                        print('cannot open a submission for sample {0}'.format(J["alias"]))
            
                # disconnect by removing token
                response = requests.delete(URL + '/logout', header={"X-Token": Token})     
            else:
                print('could not obtain a token')
    else:
        print('{0} table is not the submission database. Insert data first'.format(args.table))
    conn.close()
   
   
# use this function to submit Analyses objects
def SubmitAnalyses(args):
    '''
    
    
    
    '''
    
    
    # connect to the databse
    
    
    
    # status: ready -> grab_sample_ids -> upload files -> check_uploading -> check all_info -> form_json -> store_json
    
    

    pass








    
if __name__ == '__main__':

    # create top-level parser
    parser = argparse.ArgumentParser(prog = 'EGAsub.py', description='manages submission to EGA')
    subparsers = parser.add_subparsers(title='sub-commands', description='valid sub-commands', help = 'sub-commands help')

    # add samples to Samples Table
    AddSamples = subparsers.add_parser('AddSamples', help ='Add sample information')
    AddSamples.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    AddSamples.add_argument('-t', '--Table', dest='table', default='Samples', help='Samples table. Default is Samples')
    AddSamples.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
    AddSamples.add_argument('-m', '--MetadataDb', dest='metadatadb', default='EGA', help='Name of the database collection EGA metadata. Default is EGA')
    AddSamples.add_argument('-s', '--SubDb', dest='subdb', default='EGAsub', help='Name of the database used to store object information for submission to EGA. Default is EGAsub')
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
    AddAnalyses.add_argument('-t', '--Table', dest='table', default='Analyses', help='Samples table. Default is Analyses')
    AddAnalyses.add_argument('-m', '--MetadataDb', dest='metadatadb', default='EGA', help='Name of the database collection EGA metadata. Default is EGA')
    AddAnalyses.add_argument('-s', '--SubDb', dest='subdb', default='EGAsub', help='Name of the database used to object information for submission to EGA. Default is EGAsub')
    AddAnalyses.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
    AddAnalyses.add_argument('-f', '--FileDir', dest='filedir', help='Directory with md5sums and encrypted files', required=True)
    AddAnalyses.add_argument('--Config', dest='config', help='Path to config file', required=True)
    AddAnalyses.add_argument('--StagePath', dest='stagepath', help='Path on the staging server', required=True)
    AddAnalyses.add_argument('--Center', dest='center', default='OICR_ICGC', help='Name of the Analysis Center')
    AddAnalyses.add_argument('--StudyId', dest='study', help='Study accession Id', required =True)
    AddAnalyses.add_argument('--Broker', dest='broker', default='EGA', help='Broker name. Default is EGA')
    AddAnalyses.add_argument('--Experiment', dest='experiment', default='Whole genome sequencing', choices=['Genotyping by array', 'Exome sequencing', 'Whole genome sequencing', 'transcriptomics'], help='Experiment type. Default is Whole genome sequencing')
    AddAnalyses.add_argument('--AnalysisType', dest='analysistype', choices=['Reference Alignment (BAM)', 'Sequence variation (VCF)'], help='Analysis type', required=True)
    AddAnalyses.set_defaults(func=AddAnalysesInfo)

    # encrypt files and do a checksum
    Encryption = subparsers.add_parser('Encryption', help ='Encrypt files and do a checksum')    
    Encryption.add_argument('-d', '--InputDir', dest='inputdir', help='Directory with files to encrypt')
    Encryption.add_argument('-f', '--InputFile', dest='inputfile', help='File with full paths of files to encrypt')
    Encryption.add_argument('-o', '--OutDir', dest='outdir', default='/scratch2/groups/gsi/bis/ega', help='Directory where encrypted files and md5sums are saved. Default is /scratch2/groups/gsi/bis/ega')
    Encryption.add_argument('-t', '--Time', dest='time', action='store_true', help='Add date to OutputDirectory if used')
    Encryption.add_argument('-q', '--Queue', dest='queue', default='production', help='Queue, default is production')
    Encryption.add_argument('-m', '--Mem', dest='mem', default='10', help='Memory, default is 10g')
    Encryption.set_defaults(func=EncryptFiles)

    # submit samples to EGA
    SampleSubmission = subparsers.add_parser('SampleSubmission', help ='Submit samples to EGA')
    SampleSubmission.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    SampleSubmission.add_argument('-t', '--Table', dest='table', default='Analyses', help='Samples table. Default is Analyses')
    SampleSubmission.add_argument('-d', '--Database', dest='database', default='EGAsub', help='Name of the database used to store object information for submission to EGA. Default is EGAsub')
    SampleSubmission.add_argument('-p', '--Portal', dest='portal', default='https://ega.crg.eu/submitterportal/v1', help='EGA submission portal. Default is https://ega.crg.eu/submitterportal/v1')
    SampleSubmission.set_defaults(func=SubmitSamples)


    # get arguments from the command line
    args = parser.parse_args()
    # pass the args to the default function
    args.func(args)
