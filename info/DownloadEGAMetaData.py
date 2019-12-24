# -*- coding: utf-8 -*-
"""
Created on Fri Aug  9 10:54:33 2019

@author: rjovelin
"""


# import modules
import json
import subprocess
import time
import xml.etree.ElementTree as ET
import pymysql
import sys
import argparse
import requests


# This script is used to pull metadata from the EGA API and store it into a database 
# usage: python EGAMetDataToDB.py [-h|--Help] -c|--Credentials




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
                        # convert to string
                        D[field]  = ';'.join(list(map(lambda x: str(x), item[field])))
                else:
                    D[field] = str(item[field])
        Entries.append(D)
    return Entries



# use this function to capture egaAccessionId for Experiment Objects
def CaptureExperimentAccession(D):
    '''
    (dict) --> dict
    Take a dictionary with information for a single experiment and replace
    egaAccessionId with accession in egaAccessionIds field if None and return a 
    modified dictionary D
    '''
    if D['egaAccessionId'] == None and 'egaAccessionIds' in D:
        if type(D['egaAccessionIds']) == list:
            # get accession fron egaAccessionIds
            egaAccessionId  = D['egaAccessionIds'][0]
            assert egaAccessionId.startswith('EGAX')
        else:
            egaAccessionId  = D['egaAccessionIds']
        # replace accession
        D['egaAccessionId'] = egaAccessionId
    assert D['egaAccessionId'].startswith('EGAX')
    return D


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
        elif L[i] in ('files', 'xml', 'policyText', 'contact', 'attributes'):
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
    (str) -> str
    Take a string representation of a dataset xml and return the policy accession string 
    corresponding to that dataset
    '''
    root = ET.fromstring(S)
    accession = [item.attrib for item in root.iter('POLICY_REF')][0]['accession']
    return accession


# use this function to extract credentials
def ExtractCredentials(CredentialFile):
    '''
    (str) -> dict
    
    Take a file with EGA credentials and return a dictionary with credentials 
    '''
    
    # open the dot file, and retrieve the credentials
    Credentials = {}            
    infile = open(CredentialFile)            
    for line in infile:
        if line.rstrip() != '':
            line = line.rstrip().split('=')
            Credentials[line[0]] = line[1]
    infile.close()        
    
    return Credentials


# use this function to download metadata and add to EGA tables for a given box
def DownloadMetadata(args):
    '''
    (list) -> None
    
    Take a list of command line argument incuding the file with EGA credentials 
    and a given ega box and download metadata into the EGA database
    '''
    
    
    ### 1) extract credentials
    Credentials = ExtractCredentials(args.credential)
   
    boxname = args.box[args.box.index('b'):].title().replace('-', '')
    UserKey, PassWordKey = 'UserName' +  boxname, 'MyPassWord' +  boxname
    UserName, MyPassWord = Credentials[UserKey], Credentials[PassWordKey]
    DbHost, DbName = Credentials['DbHost'], Credentials['DbMet']
    DbUser, DbPasswd = Credentials['DbUser'], Credentials['DbPasswd']

    ### 2) connect to EGA api

    URL = "https://ega.crg.eu/submitterportal/v1"

    LogInCmd = "curl -X POST " + URL + "/login -d username=" + UserName + " --data-urlencode password=\"" + MyPassWord + "\"" + " -d loginType=\"submitter\""
    # extract data from call and convert str to dict
    LogData = subprocess.check_output(LogInCmd, shell=True)
    LogData = json.loads(LogData)
    # get token
    Token = LogData['response']['result'][0]['session']['sessionToken']

    ### 3) extract metadata for all objects

    # make a list of objects of interest
    Objects = ["studies", "runs", "samples", "experiments", "datasets", "analyses", "policies", "dacs"]

    # make a parallel list of dicts for each object in list Objects
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

    print('fetched metadata from the API')

    # reformat dac data to add contact info as a field in jsons
    for i in range(len(MetaData[-1]['response']['result'])):
        contacts = GetContactInfo(MetaData[-1]['response']['result'][i]['xml'])
        MetaData[-1]['response']['result'][i]['contact'] = contacts
    
    ### 4) capture the fields of interest for each EGA object

    # make lists of fields of interest for each object
    StudyFields = ['alias', 'centerName', 'creationTime', 'egaAccessionId', 'ebiId',
                   'shortName', 'status', 'studyType', 'title', 'xml']
    SampleFields = ['alias', 'attributes', 'caseOrControl', 'centerName',
                    'creationTime', 'description', 'egaAccessionId', 'gender',
                    'ebiId', 'phenotype', 'status', 'subjectId', 'title', 'xml']
    ExperimentFields = ['alias', 'centerName', 'creationTime', 'designDescription',
                        'egaAccessionId', 'egaAccessionIds', 'ebiId', 'instrumentModel',
                        'instrumentPlatform', 'libraryLayout', 'libraryName', 'librarySelection',
                        'librarySource', 'libraryStrategy', 'pairedNominalLength', 'sampleId',
                        'status', 'studyId', 'title', 'xml']
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
    InfoBox = []
    for i in range(len(Fields)):
        InfoBox.append(GetObjectFields(Fields[i], MetaData[i]))
    
    # capture experiment egaAccessionId 
    # experiments submitted through the api have egaAccessionid set to None
    # but the Id can be retrieved from a list, in the egaAccessionIds field
    for i in range(len(InfoBox[3])):
        InfoBox[3][i] = CaptureExperimentAccession(InfoBox[3][i])
    
    # remove egaAccessionIds from field list
    ExperimentFields.remove('egaAccessionIds')

    ### 5) add fields to link tables that are found only in the xml

    # dacId is an empty field for EGA policy but it can be retrieved from the xml
    # loop over policies in each box, extract dacId and replace empty field with accession    
    for i in range(len(InfoBox[6])):    
        accession = ExtractDacId(InfoBox[6][i]['xml'])
        InfoBox[6][i]['dacId'] = accession
    
    # policyId is an empty field for EGA datasets but it can retrieved from the xml
    # loop over datsets in each box, extraxt policyId and replace empty field with accession
    for i in range(len(InfoBox[4])):    
        accession = ExtractPolicyId(InfoBox[4][i]['xml'])
        InfoBox[4][i]['policyId'] = accession
    
    # runId is not a field for EGA dataset but can found in the xml FOR SOME DATASETS
    # the run ID in the dataset xml is EGAR, it needs to be mapped to ERR ID
    # extract run IDs for each dataset, map each run id (err) to dataset id (egad)    
    DatasetToRun = RetrieveObjectRef(InfoBox[4], './DATASET/RUN_REF', MatchIds(MetaData[1]))
    
    # analysisId is not a field for EGA dataset but can be found in the xml FOR SOME DATASETS
    # the analysis ID in the dataset xml is EGAZ, it needs to be mapped to ERZ ID
    # extract analysis IDs for each dataset, map each analysis id (erz) to dataset id (egad)
    DatasetToAnalysis = RetrieveObjectRef(InfoBox[4], './DATASET/ANALYSIS_REF', MatchIds(MetaData[5]))
        
    # sampleId is not a field for EGA analysis but can be found in the xml
    # because there may be more than 1 sampleId for a given analysisID,
    # a junction table with analysisID and sampleID is necessary
    AnalysisToSample = ExtractSampleIDsFromAnalysisXml(InfoBox[5])
    
    # add box of origin to each object
    for i in range(len(InfoBox)):
        InfoBox[i] = AddBoxOrigin(InfoBox[i], args.box)
        
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
    conn = pymysql.connect(host = DbHost, user = DbUser, password = DbPasswd,
                           db = DbName, charset = "utf8", port=3306,
                           unix_socket='/var/run/mysqld/mysqld.sock')
    cur = conn.cursor()
                            
    # make a list of tables
    cur.execute('SHOW TABLES')
    dbTables = [i[0] for i in cur]
       
    # make a list of table names parallel to Columns list and to object list
    TableNames =  ['Studies', 'Runs', 'Samples', 'Experiments', 'Datasets', 'Analyses', 'Policies', 'Dacs', 'Datasets_RunsAnalysis', 'Analyses_Samples']
    
    # create or update table
    for i in range(len(TableNames)):
        # create table if doesn't exist
        if TableNames[i] not in dbTables:
            if TableNames[i] not in ['Datasets_RunsAnalysis', 'Analyses_Samples']:
                with conn.cursor() as cur:
                    cur.execute('CREATE TABLE {0} ({1})'.format(TableNames[i], Columns[i]))
                    conn.commit()
            elif TableNames[i] == 'Datasets_RunsAnalysis':
                with conn.cursor() as cur:
                    cur.execute('CREATE TABLE Datasets_RunsAnalysis (datasetId VARCHAR(100), ebiId VARCHAR(100), egaBox VARCHAR(100), PRIMARY KEY (datasetId, ebiId))')
                    conn.commit()            
            elif TableNames[i] == 'Analyses_Samples':
                with conn.cursor() as cur:
                    cur.execute('CREATE TABLE Analyses_Samples (analysisId VARCHAR(100), sampleId  VARCHAR(100), egaBox VARCHAR(100), PRIMARY KEY (analysisId, sampleId))')
                    conn.commit()
            print('created {0}'.format(TableNames[i]))
        # delete rows corresponding to box if table exists
        else:
            with conn.cursor() as cur:
                cur.execute('DELETE FROM {0} WHERE {0}.egaBox=\"{1}\"'.format(TableNames[i], args.box))
                conn.commit()
            print('deleting rows for {0} in {1}'.format(args.box, TableNames[i]))
    
    
    # open new cursor to instert data into tables
    cur = conn.cursor()
    
    # make a list of table names parallel to object list
    Tables =  ['Studies', 'Runs', 'Samples', 'Experiments', 'Datasets', 'Analyses', 'Policies', 'Dacs']

    # Insert data into tables
    # loop over objects
    for i in range(len(InfoBox)):
        print('Inserting data from {0} for table {1}'.format(args.box, Tables[i]))
        # loop over instances of given object
        for j in range(len(InfoBox[i])):
            # dump values into a tuple
            Values = ()
            for field in Fields[i]:
                if InfoBox[i][j][field] == '' or InfoBox[i][j][field] == None:
                    Values = Values.__add__(('NULL',))
                else:
                    Values = Values.__add__((InfoBox[i][j][field],))
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
    for egad_id in DatasetToRun:
        for err_id in DatasetToRun[egad_id]:
            cur.execute('INSERT INTO Datasets_RunsAnalysis (datasetId, ebiId, egaBox) VALUES {0}'.format((egad_id, err_id, args.box)))         
            conn.commit()
    for egad_id in DatasetToAnalysis:
        for egaz_id in DatasetToAnalysis[egad_id]:
            cur.execute('INSERT INTO Datasets_RunsAnalysis (datasetId, ebiId, egaBox) VALUES {0}'.format((egad_id, egaz_id, args.box)))
            conn.commit()
    print('Inserting data into Analyses_Samples table')
    for erz_id in AnalysisToSample:
        for ers_id in AnalysisToSample[erz_id]:
            cur.execute('INSERT INTO Analyses_Samples (analysisId, sampleId, egaBox) VALUES {0}'.format((erz_id, ers_id, args.box)))
            conn.commit()
    print('Inserted data into all tables')

    # close connection
    conn.close()

 
    ### 8) log out
    LogOutCmd = "curl -X DELETE -H \"X-Token: " + Token + "\" " + URL + "/logout"
    logout = subprocess.call(LogOutCmd, shell=True)
    # check that returncode is success
    errmssg = "did not successfully log out from {0}".format(args.box)
    assert logout == 0, errmssg

if __name__ == '__main__':

    # create main parser
    main_parser = argparse.ArgumentParser(prog = 'DownloadEGAMetaData.py', description='Download metadata from EGA to GSI EGA database', add_help=True)
    main_parser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    main_parser.add_argument('-b', '--Box', dest='box', choices=['ega-box-12', 'ega-box-137', 'ega-box-1269'], help='EGA box', required=True)
    main_parser.set_defaults(func=DownloadMetadata)
       
    # get arguments from the command line
    args = main_parser.parse_args()
    # pass the args to the default function
    args.func(args)

