# -*- coding: utf-8 -*-
"""
Created on Tue Feb  6 15:13:26 2018

@author: RJovelin
"""

# import modules
import json
import subprocess
import time
import xml.etree.ElementTree as ET
import pymysql

# This script is used to pull metadata from the EGA API and store it into a database 
# usage: python EGAMetDataToDB.py 

### 1) set up credentials
URL = "https://ega.crg.eu/submitterportal/v1"

DbName = '****'
UserName = "****"
MyPassWord = "****"
DbHost = '****'
DbUser = '****'
DbPasswd = "****"

### 2) Functions used in this script

# use this function to capture the information of a given object
def GetObjectFields(L, Data):
    '''
    (list, dict) -> list
    Take a list of strings representing fields of interest and a dictionary with 
    all info for all entries obtained from the EGA api
    Precondition: 2 filenames are present for runs, 1 for read1 and 1 for read2
    '''
    # create a list to store dicts of individual entries
    Entries = []
    # loop over dicts in list of entries
    for item in Data['response']['result']:
        # collect info for each entry of each object
        D = {}
        # loop over fields of interest
        for field in L:
            # check if field is creationTime
            if field == 'creationTime':
                # convert system time to readable time
                EpochTime = int(item[field]) / 1000
                ReadableTime = time.strftime('%Y-%m-%d', time.localtime(EpochTime))
                D[field] = str(ReadableTime)
            # check if field is file
            elif field == 'files':
                # more than 1 file name may recorded
                if len(item[field]) == 1:
                    # grab the file name
                    file = item[field][0]['fileName']
                else:
                    assert len(item[field]) > 1
                    # separate file names with ;
                    file = ';'.join([item[field][i]['fileName'] for i in range(len(item[field]))])
                D[field] = file
            else:
                # check if value is string or None
                if item[field] == None:
                    D[field] = item[field]
                else:
                    D[field] = str(item[field])
        Entries.append(D)
    return Entries

# use this function to match egaAccessionId to id
def MatchIds(D):
    '''
    (dict) -> dict
    Take the dictionary of json EGA object and return a dictionary of matching
    egaAccessionId: id pairs for that object
    '''    
    Ref = {}
    a, b = [], []
    for item in D['response']['result']:
        # check that egaAccession id is uniquely matched to an id
        Ref[item['egaAccessionId']] = item['ebiId']
        a.append(item['egaAccessionId'])
        b.append(item['ebiId'])
    # check that egaAccessionIds and ids are unique
    assert len(a) == len(b) == len(set(a)) == len(set(b))
    return Ref

# use this function to retrieve the reference to an object when no object id
# is present in dataset instance
def RetrieveObjectRef(Info, TagPath, MatchingIDs):
    '''
    (list, str, dict) -> dict
    Take a list of dicts, one for each dataset, a path in the dataset xml,
    and a dict of matching ids for object_2 and return a dict of dataset id  
    paired with a list of the corresponding object_2_ids
    '''
    
    # extract object_2 IDs for dataset, map each object_2_id to dataset    
    # note that dataset instance may have multiple references to object_2 (and vice versa)
    IdToId = {}
    for item in Info:
        # make an element oject of the xml
        tree = ET.fromstring(item['xml'])
        # make a list of ids
        IDs = []
        for run in tree.findall(TagPath):
            ega_id = run.get('accession')
            # convert ega_id to id
            assert ega_id in MatchingIDs, 'no id match of ega_id'
            IDs.append(MatchingIDs[ega_id])
        # populate dict with dataset id: [Object_2_ids]
        assert item['ebiId'] not in IdToId, 'id already recorded' 
        IdToId[item['ebiId']] = IDs
    return IdToId


# use this function to reorganize the object fields
def ReorderFields(L, field):
    '''
    (list, str) -> list
    Take a list of column fields and a given field name and return the modified
    list with the given field in a different position
    '''
    # move field id to first position in list L
    if field == 'ebiId':
        L.insert(0, L.pop(L.index(field)))
    else:
        # move foreign keys to last positions in list L
        L.insert(len(L), L.pop(L.index(field)))
    return L

# use this function to specify column type in database
def SpecifyColumnType(L):
    '''
    (list) -> list
    Take a list of fields for a given object and return a SQL string with 
    column name and column type
    Preconditions: all column entries are string, and first column is primary key
    '''
    # all columns hold string data, add 
    Cols = []
    for i in range(1, len(L)):
        if L[i] in ('title', 'description', 'designDescription'):
            if i == len(L) -1:
                Cols.append(L[i] + ' TEXT NULL')
            else:
                Cols.append(L[i] + ' TEXT NULL,')
        elif L[i] in ('files', 'xml'):
            if i == len(L) -1:
                Cols.append(L[i] + ' MEDIUMTEXT NULL')
            else:
                Cols.append(L[i] + ' MEDIUMTEXT NULL,')
        else:
            if i == len(L) -1:
                Cols.append(L[i] + ' VARCHAR(100) NULL')
            else:
                Cols.append(L[i] + ' VARCHAR(100) NULL,')
    # first column holds primary key
    Cols.insert(0, L[0] + ' VARCHAR(100) PRIMARY KEY UNIQUE,')
    return ' '.join(Cols)


### 3) get the token
LogInCmd = "curl -X POST " + URL + "/login -d username=" + UserName + " --data-urlencode password=\"" + MyPassWord + "\"" + " -d loginType=\"submitter\""
# extract data from call and convert str to dict
LogData = subprocess.check_output(LogInCmd, shell=True)
LogData = json.loads(LogData)
# get token
Token = LogData['response']['result'][0]['session']['sessionToken']

### 4) extract metadata for all objects

# make a list of objects of interest
Objects = ["studies", "runs", "samples", "experiments", "datasets", "analyses"]
# make a parallel list of dicts for each object
MetaData = []
for name in Objects:
    # build command
    # IMPORTANT: by default only 10 results are returned. Set both parameters skip and limit to 0
    MyCommand = "curl -X GET -H \"X-Token: " + Token + "\" " + "\"" + URL + "/" + name + "?status=SUBMITTED&skip=0&limit=0\""
    # convert str output to dict
    MyData = subprocess.check_output(MyCommand, shell=True)
    MyJsonData = json.loads(MyData)
    # do some QC by checking the number of entries
    assert MyJsonData['response']['numTotalResults'] == len(MyJsonData['response']['result']), "entries count does not match total # entries"
    # keep track of dicts
    MetaData.append(MyJsonData)

# Do some QC, some id fields are not valid EGA object ID, use ebiId instead, check that ebiId is a valid EGA object ID
IdCode = ['ERP', 'ERR', 'ERS', 'ERX', 'EGAD', 'ERZ']
for i in range(len(MetaData)):
    for item in MetaData[i]['response']['result']:
        assert str(item['ebiId']).startswith(IdCode[i])
print('fetched metadata from the API')

### 5) capture the fields of interest for each EGA object

# make lists of fields of interest for each object
StudyFields = ['alias', 'centerName', 'creationTime', 'egaAccessionId', 'ebiId',
               'shortName', 'status', 'studyType', 'title', 'xml']
SampleFields = ['alias', 'attributes', 'caseOrControl', 'centerName',
                'creationTime', 'description', 'egaAccessionId', 'gender',
                'ebiId', 'phenotype', 'status', 'subjectId', 'title', 'xml']
ExperimentFields = ['alias', 'centerName', 'creationTime', 'designDescription',
                    'egaAccessionId', 'ebiId', 'instrumentModel', 'instrumentPlatform',
                    'libraryLayout', 'libraryName', 'librarySelection', 'librarySource',
                    'libraryStrategy', 'pairedNominalLength', 'sampleId', 'status', 'studyId', 'title', 'xml']
RunFields = ['alias', 'centerName', 'creationTime', 'egaAccessionId', 'experimentId',
             'files', 'ebiId', 'runFileType', 'sampleId', 'status', 'xml']
AnalysisFields = ['alias', 'analysisCenter', 'analysisDate', 'analysisFileType',
                  'analysisType', 'attributes', 'centerName', 'creationTime',
                  'description', 'egaAccessionId', 'files', 'ebiId', 'platform', 'status',
                  'studyId', 'title', 'xml']
DataSetFields = ['alias', 'attributes', 'centerName', 'creationTime', 'datasetTypes',
                 'description', 'egaAccessionId', 'ebiId', 'policyId', 'status', 'title', 'xml']

# make a list for each object 
Fields = [StudyFields, RunFields, SampleFields, ExperimentFields, DataSetFields, AnalysisFields]

# capture the fields of interest for each object 
StudyInfo = GetObjectFields(Fields[0], MetaData[0])
RunInfo = GetObjectFields(Fields[1], MetaData[1])
SampleInfo = GetObjectFields(Fields[2], MetaData[2])
ExperimentInfo = GetObjectFields(Fields[3], MetaData[3])
DataSetInfo = GetObjectFields(Fields[4], MetaData[4])
AnalysisInfo = GetObjectFields(Fields[5], MetaData[5])         

### 6) add fields to link tables that are found only in the xml

# sampleId is not a field for EGA analysis but can be found in the xml
for i in range(len(AnalysisInfo)):
    # extract sampleId from xml
    assert AnalysisInfo[i]['xml'].count('SAMPLE_REF') == 2
    tree = ET.ElementTree(ET.fromstring(AnalysisInfo[i]['xml']))
    sample_ref = tree.find('.//SAMPLE_REF')
    accession = sample_ref.attrib['accession']
    assert 'ERS' in accession, 'not a valid sample Id'
    # populate dict
    AnalysisInfo[i]['sampleId'] = accession

# add sampleId field to list of analysis fields
Fields[-1].append('sampleId')

# runId is not a field for EGA dataset but can found in the xml FOR SOME DATASETS
# the run ID in the dataset xml is EGAR, it needs to be mapped to ERR ID
# extract run IDs for each dataset, map each run id (err) to dataset id (egad)    
DatasetToRun = RetrieveObjectRef(DataSetInfo, './DATASET/RUN_REF', MatchIds(MetaData[1]))

# analysisId is not a field for EGA dataset but can be found in the xml FOR SOME DATASETS
# the analysis ID in the dataset xml is EGAZ, it needs to be mapped to ERZ ID
# extract analysis IDs for each dataset, map each analysis id (erz) to dataset id (egad)
DatasetToAnalysis = RetrieveObjectRef(DataSetInfo, './DATASET/ANALYSIS_REF', MatchIds(MetaData[-1]))

print('extracted metadata of interest')

### 7) connect to database

#reorder fields so that primary key is first
for i in range(len(Fields)):
    Fields[i] = ReorderFields(Fields[i], 'ebiId')
# reorganize fields so that foreign keys, if present, are last
Fields[3] = ReorderFields(Fields[3], 'sampleId')
Fields[3] = ReorderFields(Fields[3], 'studyId')
Fields[1] = ReorderFields(Fields[1], 'sampleId')
Fields[1] = ReorderFields(Fields[1], 'experimentId')
Fields[-1] = ReorderFields(Fields[-1], 'studyId')
Fields[-1] = ReorderFields(Fields[-1], 'sampleId')

# make SQL command to specifiy the columns datatype    
Columns = []
for i in range(len(Fields)):
    Columns.append(SpecifyColumnType(Fields[i]))


# connect to the database
conn = pymysql.connect(host = DbHost, user = DbUser, password = DbPasswd, db = DbName, charset = "utf8")
cur = conn.cursor()

# Drop existing tables if present and create new ones
SqlCommand = ['DROP TABLE IF EXISTS Experiments', 'DROP TABLE IF EXISTS Runs', 'DROP TABLE IF EXISTS Samples',
              'DROP TABLE IF EXISTS Analyses', 'DROP TABLE IF EXISTS Datasets', 'DROP TABLE IF EXISTS Studies',
              'DROP TABLE IF EXISTS Datasets_Runs', 'DROP TABLE IF EXISTS Datasets_Analyses', 
              'CREATE TABLE Studies ({0})', 'CREATE TABLE Runs ({1})', 'CREATE TABLE Samples ({2})',
              'CREATE TABLE Experiments ({3})', 'CREATE TABLE Datasets ({4})', 'CREATE TABLE Analyses ({5})',
              'CREATE TABLE Datasets_Runs (datasetId VARCHAR(100), runId VARCHAR(100), PRIMARY KEY (datasetId, runId))',
              'CREATE TABLE Datasets_Analyses (datasetId VARCHAR(100), analysisId VARCHAR(100), PRIMARY KEY (datasetId, analysisId))']
SqlCommand = '; '.join(SqlCommand).format(Columns[0], Columns[1], Columns[2], Columns[3], Columns[4], Columns[5])

cur.execute(SqlCommand)
conn.commit()
            
print('Dropped existing tables and created new tables')

# make a list of lists with object info parallel to field list
AllInfo = [StudyInfo, RunInfo, SampleInfo, ExperimentInfo, DataSetInfo, AnalysisInfo]     
# make a parallel list of table names
Tables = ['Studies', 'Runs', 'Samples', 'Experiments', 'Datasets', 'Analyses']

# Insert data into tables
# loop over objects
for i in range(len(AllInfo)):
    print('Inserting data for table {0}'.format(Tables[i]))
    # loop over instances of given object
    for j in range(len(AllInfo[i])):
        # dump values into a tuple
        Values = ()
        for field in Fields[i]:
            if AllInfo[i][j][field] == '' or AllInfo[i][j][field] == None:
                Values = Values.__add__(('NULL',))
            else:
                Values = Values.__add__((AllInfo[i][j][field],))
        assert len(Values) == len(Fields[i])        
        # make a string with column names
        names = ', '.join(Fields[i])
        # get table name
        TableName = Tables[i]
        # add values into table
        cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(TableName, names, Values))
        conn.commit()

# Insert data into junction tables
print('Inserting data into Datasets_Runs table')
for egad_id in DatasetToRun:
    for err_id in DatasetToRun[egad_id]:
        cur.execute('INSERT INTO Datasets_Runs (datasetId, runId) VALUES {0}'.format((egad_id, err_id)))         
        conn.commit()

print('Inserting data into Datasets_Analyses table')
for egad_id in DatasetToAnalysis:
    for egaz_id in DatasetToAnalysis[egad_id]:
        cur.execute('INSERT INTO Datasets_Analyses (datasetId, analysisId) VALUES {0}'.format((egad_id, egaz_id)))
        conn.commit()

print('Inserted data into all tables')

# close connection
conn.close()

  
### 8) log out
LogOutCmd = "curl -X DELETE -H \"X-Token: " + Token + "\" " + URL + "/logout"
logout = subprocess.call(LogOutCmd, shell=True)
# check that returncode is success
assert logout == 0, "did not successfully log out"

