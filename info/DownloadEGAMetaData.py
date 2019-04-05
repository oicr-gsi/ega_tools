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
import sys
import getopt


# This script is used to pull metadata from the EGA API and store it into a database 
# usage: python EGAMetDataToDB.py [-h|--Help] -c|--Credentials

# define script usage
def Usage():
    print("""
    usage: EGAMetDataToDB.py [-h|--Help|-c|--Credentials]
    -h, --Help: help
    -c, --Credentials: file with database credentials
    """)

try:
    opts, args = getopt.getopt(sys.argv[1:], 'hc:', ['Help', 'Credentials='])
except getopt.GetoptError:
    Usage()
    sys.exit(2)
else:
    for opt, val in opts:
        if opt in ('-h', '--Help'):
            Usage()
            sys.exit(2)
        elif opt in ('-c', '--Credentials'):
            CredentialFile = val


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
                # check that value is list
                elif type(item[field]) == list:
                    # replace empty lists with None value
                    if len(item[field]) == 0:
                        D[field] = None
                    else:
                        D[field] = item[field]
                else:
                    D[field] = str(item[field])
        Entries.append(D)
    return Entries


# use this function to add box of origin to an object
def AddBoxOrigin(Info, BoxName):
    '''
    (list, str) -> list
    Take a list of dictionaries for a given ega object and return a modified list
    in which a 'egaBox: BoxName key: value pair is added to each dictionary
    '''
    for i in range(len(Info)):
        Info[i]['egaBox'] = BoxName
    return Info


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
            if ega_id in MatchingIDs:
                IDs.append(MatchingIDs[ega_id])
        # populate dict with dataset id: [Object_2_ids]
        assert item['ebiId'] not in IdToId, 'id already recorded' 
        IdToId[item['ebiId']] = IDs
    return IdToId

# use this function to add sampleID field to the analysis object
def ExtractSampleIDsFromAnalysisXml(AnalysisInfo):
    '''
    (list) -> list
    Take a list of dictionaries of analysis object and return a modified list
    in which a 'sampleId: [accession_id key] value-pair is added to each dictionary
    '''
    
    # create a dict {analysis ebiID: [list of sample EbiID]}
    AnalysisIDs = {}
    
    # sampleId is not a field for EGA analysis but can be found in the xml
    for i in range(len(AnalysisInfo)):
        # extract sampleId from xml
        tree = ET.ElementTree(ET.fromstring(AnalysisInfo[i]['xml']))
        sample_ref = tree.findall('.//SAMPLE_REF')
        # capture all sample IDs in a list, there mayy be more than 1 for vcf files
        accessions = [sample_ref[j].attrib['accession'] for j in range(len(sample_ref))]
        assert AnalysisInfo[i]['ebiId'] not in AnalysisIDs
        AnalysisIDs[AnalysisInfo[i]['ebiId']] = accessions
    return AnalysisIDs


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
        elif L[i] in ('files', 'xml', 'policyText', 'contact'):
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


# use this function to extract contact information from dac object
def GetContactInfo(S):
    '''
    (str) -> str
    Take a string representation of an xml and return a semi-colon separated string
    of dictionaries with contact infor for each person in the DAC    
    '''
    
    # parse the xml string into a xml tree object
    root = ET.fromstring(S)
    # make a list of contacts
    contacts = [str(name.attrib) for name in root.iter('CONTACT')]
    return ';'.join(contacts)    
 
    
# use this function to get the dacId from the policy xml
def ExtractDacId(S):
   '''
   (str) -> str
   Take a string representation of a policy xml and return the dacId accession string 
   corresponding to that policy
   '''    

   root = ET.fromstring(S)
   accession = [item.attrib for item in root.iter('DAC_REF')][0]['accession']
   return accession

# use this function to get the policy Id from the dataset xml
def ExtractPolicyId(S):
    '''
    Take a string representation of a dataset xml and return the policy accession string 
    corresponding to that dataset
    '''
    root = ET.fromstring(S)
    accession = [item.attrib for item in root.iter('POLICY_REF')][0]['accession']
    return accession

    
### 1) set up credentials
URL = "https://ega.crg.eu/submitterportal/v1"

# open the dot file, and retrieve the credentials
Credentials = {}            
infile = open(CredentialFile)            
for line in infile:
    if line.rstrip() != '':
        line = line.rstrip().split('=')
        Credentials[line[0]] = line[1]
infile.close()        

# extract credential values
UserNameBox12, MyPassWordBox12 = Credentials['UserNameBox12'], Credentials['MyPassWordBox12']
UserNameBox137, MyPassWordBox137 = Credentials['UserNameBox137'], Credentials['MyPassWordBox137']
DbHost, DbName = Credentials['DbHost'], Credentials['DbMet']
DbUser, DbPasswd = Credentials['DbUser'], Credentials['DbPasswd']

### 2) Log in ega-box-12 ans ega-box-137

LogInCmd1 = "curl -X POST " + URL + "/login -d username=" + UserNameBox12 + " --data-urlencode password=\"" + MyPassWordBox12 + "\"" + " -d loginType=\"submitter\""
# extract data from call and convert str to dict
LogData1 = subprocess.check_output(LogInCmd1, shell=True)
LogData1 = json.loads(LogData1)
# get token
TokenBox12 = LogData1['response']['result'][0]['session']['sessionToken']

LogInCmd2 = "curl -X POST " + URL + "/login -d username=" + UserNameBox137 + " --data-urlencode password=\"" + MyPassWordBox137 + "\"" + " -d loginType=\"submitter\""
# extract data from call and convert str to dict
LogData2 = subprocess.check_output(LogInCmd2, shell=True)
LogData2 = json.loads(LogData2)
# get token
TokenBox137 = LogData2['response']['result'][0]['session']['sessionToken']


### 3) extract metadata for all objects

# make a list of objects of interest
Objects = ["studies", "runs", "samples", "experiments", "datasets", "analyses", "policies", "dacs"]

# make a parallel list of dicts for each object in list Objects
MetaDataBox12 = []
for name in Objects:
    # build command
    # IMPORTANT: by default only 10 results are returned. Set both parameters skip and limit to 0
    MyCommand = "curl -X GET -H \"X-Token: " + TokenBox12 + "\" " + "\"" + URL + "/" + name + "?status=SUBMITTED&skip=0&limit=0\""
    # convert str output to dict
    MyData = subprocess.check_output(MyCommand, shell=True)
    MyJsonData = json.loads(MyData)
    # do some QC by checking the number of entries
    assert MyJsonData['response']['numTotalResults'] == len(MyJsonData['response']['result']), "entries count does not match total # entries"
    # keep track of dicts
    MetaDataBox12.append(MyJsonData)


# make a parallel list of dicts for each object in list Objects
MetaDataBox137 = []
for name in Objects:
    # build command
    # IMPORTANT: by default only 10 results are returned. Set both parameters skip and limit to 0
    MyCommand = "curl -X GET -H \"X-Token: " + TokenBox137 + "\" " + "\"" + URL + "/" + name + "?status=SUBMITTED&skip=0&limit=0\""
    # convert str output to dict
    MyData = subprocess.check_output(MyCommand, shell=True)
    MyJsonData = json.loads(MyData)
    # do some QC by checking the number of entries
    assert MyJsonData['response']['numTotalResults'] == len(MyJsonData['response']['result']), "entries count does not match total # entries"
    # keep track of dicts
    MetaDataBox137.append(MyJsonData)

print('fetched metadata from the API')


# reformat dac data to add contact info as a field in jsons
for i in range(len(MetaDataBox12[-1]['response']['result'])):
    contacts = GetContactInfo(MetaDataBox12[-1]['response']['result'][i]['xml'])
    MetaDataBox12[-1]['response']['result'][i]['contact'] = contacts
for i in range(len(MetaDataBox137[-1]['response']['result'])):
    contacts = GetContactInfo(MetaDataBox137[-1]['response']['result'][i]['xml'])
    MetaDataBox137[-1]['response']['result'][i]['contact'] = contacts

### 4) capture the fields of interest for each EGA object

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
PolicyFields = ['alias', 'ebiId', 'centerName', 'egaAccessionId', 'title', 'policyText', 'url', 
                'status', 'creationTime', 'xml', 'dacId']
DacFields = ['ebiId', 'alias', 'title', 'egaAccessionId', 'contact', 'creationTime']

# make a list for each object 
Fields = [StudyFields, RunFields, SampleFields, ExperimentFields, DataSetFields, AnalysisFields, PolicyFields, DacFields]

# capture the fields of interest for each object 
InfoBox12, InfoBox137 = [], []
for i in range(len(Fields)):
    InfoBox12.append(GetObjectFields(Fields[i], MetaDataBox12[i]))
    InfoBox137.append(GetObjectFields(Fields[i], MetaDataBox137[i]))


### 5) add fields to link tables that are found only in the xml

# dacId is an empty field for EGA policy but it can be retrieved from the xml
# loop over policies in each box, extract dacId and replace empty field with accession    
for i in range(len(InfoBox12[6])):    
    accession = ExtractDacId(InfoBox12[6][i]['xml'])
    InfoBox12[6][i]['dacId'] = accession
for i in range(len(InfoBox137[6])):    
    accession = ExtractDacId(InfoBox137[6][i]['xml'])
    InfoBox137[6][i]['dacId'] = accession

# policyId is an empty field for EGA datasets but it can retrieved from the xml
# loop over datsets in each box, extraxt policyId and replace empty field with accession
for i in range(len(InfoBox12[4])):    
    accession = ExtractPolicyId(InfoBox12[4][i]['xml'])
    InfoBox12[4][i]['policyId'] = accession
for i in range(len(InfoBox137[4])):    
    accession = ExtractPolicyId(InfoBox137[4][i]['xml'])
    InfoBox137[4][i]['policyId'] = accession

# runId is not a field for EGA dataset but can found in the xml FOR SOME DATASETS
# the run ID in the dataset xml is EGAR, it needs to be mapped to ERR ID
# extract run IDs for each dataset, map each run id (err) to dataset id (egad)    
DatasetToRunBox12 = RetrieveObjectRef(InfoBox12[4], './DATASET/RUN_REF', MatchIds(MetaDataBox12[1]))
DatasetToRunBox137 = RetrieveObjectRef(InfoBox137[4], './DATASET/RUN_REF', MatchIds(MetaDataBox137[1]))

# analysisId is not a field for EGA dataset but can be found in the xml FOR SOME DATASETS
# the analysis ID in the dataset xml is EGAZ, it needs to be mapped to ERZ ID
# extract analysis IDs for each dataset, map each analysis id (erz) to dataset id (egad)
DatasetToAnalysisBox12 = RetrieveObjectRef(InfoBox12[4], './DATASET/ANALYSIS_REF', MatchIds(MetaDataBox12[-1]))
DatasetToAnalysisBox137 = RetrieveObjectRef(InfoBox137[4], './DATASET/ANALYSIS_REF', MatchIds(MetaDataBox137[-1]))

# sampleId is not a field for EGA analysis but can be found in the xml
# because there may be more than 1 sampleId for a given analysisID,
# a junction table with analysisID and sampleID is necessary
AnalysisToSampleBox12 = ExtractSampleIDsFromAnalysisXml(InfoBox12[5])
AnalysisToSampleBox137 = ExtractSampleIDsFromAnalysisXml(InfoBox137[5])

# add box of origin to each object
for i in range(len(InfoBox12)):
    InfoBox12[i] = AddBoxOrigin(InfoBox12[i], 'ega-box-12')
for i in range(len(InfoBox137)):
    InfoBox137[i] = AddBoxOrigin(InfoBox137[i], 'ega-box-137')    
    
# add Ega-Box field to each field list
for i in range(len(Fields)):
    Fields[i].append('egaBox')

print('extracted metadata of interest')


### 6) connect to database

#reorder fields so that primary key is first
for i in range(len(Fields)):
    Fields[i] = ReorderFields(Fields[i], 'ebiId')
# reorganize fields so that foreign keys, if present, are last
Fields[3] = ReorderFields(Fields[3], 'sampleId')
Fields[3] = ReorderFields(Fields[3], 'studyId')
Fields[1] = ReorderFields(Fields[1], 'sampleId')
Fields[1] = ReorderFields(Fields[1], 'experimentId')
Fields[5] = ReorderFields(Fields[5], 'studyId')
Fields[6] = ReorderFields(Fields[6], 'dacId')

# make SQL command to specifiy the columns datatype    
Columns = []
for i in range(len(Fields)):
    Columns.append(SpecifyColumnType(Fields[i]))

# connect to the database
conn = pymysql.connect(host = DbHost, user = DbUser, password = DbPasswd, db = DbName, charset = "utf8")

SqlCommand = ['DROP TABLE IF EXISTS Experiments', 'DROP TABLE IF EXISTS Runs', 'DROP TABLE IF EXISTS Samples',
              'DROP TABLE IF EXISTS Analyses', 'DROP TABLE IF EXISTS Datasets', 'DROP TABLE IF EXISTS Studies',
              'DROP TABLE IF EXISTS Policies', 'DROP TABLE IF EXISTS Dacs',
              'DROP TABLE IF EXISTS Datasets_RunsAnalysis', 'DROP TABLE IF EXISTS Analyses_Samples',
              'CREATE TABLE Studies ({0})'.format(Columns[0]), 'CREATE TABLE Runs ({0})'.format(Columns[1]),
              'CREATE TABLE Samples ({0})'.format(Columns[2]), 'CREATE TABLE Experiments ({0})'.format(Columns[3]),
              'CREATE TABLE Datasets ({0})'.format(Columns[4]), 'CREATE TABLE Analyses ({0})'.format(Columns[5]),
              'CREATE TABLE Policies ({0})'.format(Columns[6]), 'CREATE TABLE Dacs ({0})'.format(Columns[7]),
              'CREATE TABLE Datasets_RunsAnalysis (datasetId VARCHAR(100), ebiId VARCHAR(100), PRIMARY KEY (datasetId, ebiId))',
              'CREATE TABLE Analyses_Samples (analysisId VARCHAR(100), sampleId  VARCHAR(100), PRIMARY KEY (analysisId, sampleId))']


# execute each sql command in turn with a new cursor
for i in range(len(SqlCommand)):
    with conn.cursor() as cur:
        cur.execute(SqlCommand[i])
        conn.commit()

print('Dropped existing tables and created new tables')

# open new cursor to instert data into tables
cur = conn.cursor()

# make a list of table names parallel to object lists
Tables = ['Studies', 'Runs', 'Samples', 'Experiments', 'Datasets', 'Analyses', 'Policies', 'Dacs']

# Insert data into tables
# loop over objects
for i in range(len(InfoBox12)):
    print('Inserting data from ega-box-12 for table {0}'.format(Tables[i]))
    # loop over instances of given object
    for j in range(len(InfoBox12[i])):
        # dump values into a tuple
        Values = ()
        for field in Fields[i]:
            if InfoBox12[i][j][field] == '' or InfoBox12[i][j][field] == None:
                Values = Values.__add__(('NULL',))
            else:
                Values = Values.__add__((InfoBox12[i][j][field],))
        assert len(Values) == len(Fields[i])        
        # make a string with column names
        names = ', '.join(Fields[i])
        # get table name
        TableName = Tables[i]
        # add values into table
        cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(TableName, names, Values))
        conn.commit()

for i in range(len(InfoBox137)):
    print('Inserting data from ega-box-137 for table {0}'.format(Tables[i]))
    # loop over instances of given object
    for j in range(len(InfoBox137[i])):
        # dump values into a tuple
        Values = ()
        for field in Fields[i]:
            if InfoBox137[i][j][field] == '' or InfoBox137[i][j][field] == None:
                Values = Values.__add__(('NULL',))
            else:
                Values = Values.__add__((InfoBox137[i][j][field],))
        assert len(Values) == len(Fields[i])        
        # make a string with column names
        names = ', '.join(Fields[i])
        # get table name
        TableName = Tables[i]
        # add values into table
        cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(TableName, names, Values))
        conn.commit()

# Insert data into junction tables
print('Inserting data into Datasets_RunsAnalysis table')
for egad_id in DatasetToRunBox12:
    for err_id in DatasetToRunBox12[egad_id]:
        cur.execute('INSERT INTO Datasets_RunsAnalysis (datasetId, ebiId) VALUES {0}'.format((egad_id, err_id)))         
        conn.commit()
for egad_id in DatasetToRunBox137:
    for err_id in DatasetToRunBox137[egad_id]:
        cur.execute('INSERT INTO Datasets_RunsAnalysis (datasetId, ebiId) VALUES {0}'.format((egad_id, err_id)))         
        conn.commit()
for egad_id in DatasetToAnalysisBox12:
    for egaz_id in DatasetToAnalysisBox12[egad_id]:
        cur.execute('INSERT INTO Datasets_RunsAnalysis (datasetId, ebiId) VALUES {0}'.format((egad_id, egaz_id)))
        conn.commit()
for egad_id in DatasetToAnalysisBox137:
    for egaz_id in DatasetToAnalysisBox137[egad_id]:
        cur.execute('INSERT INTO Datasets_RunsAnalysis (datasetId, ebiId) VALUES {0}'.format((egad_id, egaz_id)))
        conn.commit()

print('Inserting data into Analyses_Samples table')
for erz_id in AnalysisToSampleBox12:
    for ers_id in AnalysisToSampleBox12[erz_id]:
        cur.execute('INSERT INTO Analyses_Samples (analysisId, sampleId) VALUES {0}'.format((erz_id, ers_id)))
        conn.commit()
for erz_id in AnalysisToSampleBox137:
    for ers_id in AnalysisToSampleBox137[erz_id]:
        cur.execute('INSERT INTO Analyses_Samples (analysisId, sampleId) VALUES {0}'.format((erz_id, ers_id)))
        conn.commit()


print('Inserted data into all tables')

# close connection
conn.close()

 
### 8) log out
LogOutCmd = "curl -X DELETE -H \"X-Token: " + TokenBox12 + "\" " + URL + "/logout"
logout = subprocess.call(LogOutCmd, shell=True)
# check that returncode is success
assert logout == 0, "did not successfully log out from box-12"

LogOutCmd = "curl -X DELETE -H \"X-Token: " + TokenBox137 + "\" " + URL + "/logout"
logout = subprocess.call(LogOutCmd, shell=True)
# check that returncode is success
assert logout == 0, "did not successfully log out from box-137"