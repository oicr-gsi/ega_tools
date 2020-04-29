# -*- coding: utf-8 -*-
"""
Created on Thu Apr 23 16:14:03 2020

@author: rjovelin
"""

import time
import xml.etree.ElementTree as ET
import pymysql
import requests
import uuid
import json



def ExtractCredentials(CredentialFile):
    '''
    (str) -> dict
    
    Return a dictionary with the EGA and BIS database credentials
    '''
    
    D = {}            
    infile = open(CredentialFile)            
    for line in infile:
        if line.rstrip() != '':
            line = line.rstrip().split('=')
            D[line[0].strip()] = line[1].strip()
    infile.close()        
    return D


def GetSubmissionBoxCredentials(CredentialFile):
    '''
    (str) -> dict
    
    Return a dictionary with EGA box, password key, value pairs 
    '''
    
    D = ExtractCredentials(CredentialFile)
    Boxes = list(set([i[i.lower().index('b'):].title().replace('-', '') for i in D if 'box' in i.lower()]))
    K = {}
    for i in range(len(Boxes)):
        for j in D:
            if 'passwordbox' in j.lower():
                if j[j.index('Box'):] == Boxes[i]:
                    boxname = 'ega-' + Boxes[i][:3].lower() + '-' + Boxes[i][3:]    
                    K[boxname] = D[j]   
    return K


def FormatURL(URL):
    '''
    (str) -> str
    
    Return the URL ending with a slash
    '''
    
    if URL[-1] != '/':
        URL = URL + '/'
    return URL
    

def ConnectToAPI(Username, Password, URL):
    '''
    (str, str, str) -> str    
    
    Connect to the API at URL using the Username and Password for a given box
    '''

    URL = FormatURL(URL)
    data = {'username': Username, 'password': Password, 'loginType': 'submitter'}
    Login = requests.post(URL + 'login', data=data)
    Token = Login.json()['response']['result'][0]['session']['sessionToken']
    return Token
    

def CloseAPIConnection(Token, URL):
    '''
    (str, str) -> None
    
    Close the connection to URL bu deleting Token
    '''
        
    URL = FormatURL(URL)
    headers = {'X-Token': Token}
    response = requests.delete(URL + 'logout', headers=headers)

    
def CountObjects(Username, Password, URL):
    '''
    (str, str, str) -> dict
    
    Return a dictionary with the counts of each object with SUBMITTED status
    in box defined by Username and Password
    '''
    
    # connect to API
    Token = ConnectToAPI(Username, Password, URL)
    # make a list of objects of interest
    L = ["studies", "runs", "samples", "experiments", "datasets", "analyses", "policies", "dacs"]
    # store the count of each object for the given box
    D = {}
    headers = {'X-Token': Token}
    URL = FormatURL(URL)
    for i in L:
        response = requests.get(URL + i + '?status=SUBMITTED&skip=0&limit=10', headers=headers)
        D[i] = response.json()['response']['numTotalResults']
    # close connection
    CloseAPIConnection(Token, URL)
    return D


def GetUpperLimit(Count, chunk_size):
    '''
    (int, int) -> int
    
    Take the number of objects to download, the size of the chunck
    and return the upper limit of the range of chunks
    '''
    
    i = Count / chunk_size
    if '.' in str(i):
        num, dec = str(i)[:str(i).index('.')], str(i)[str(i).index('.'):]
    else:
        num, dec = i, 0
    if 0 < float(dec) < 0.5:
        u = round(int(num) + float(dec) + 0.5)
    elif float(dec) > 0.5:
        u = round(i)
    elif float(dec) == 0:
        u = i + 1
    return u    


def DownloadMetadata(Username, Password, URL, Object, Count, chunk_size):
    '''
    (str, str, str, str, dict, int) -> list
    
    Return a list of dictionaries with all instances of Object, downloaded in 
    chunks of size chunk_size from URL for a given box
    '''
    
    URL = FormatURL(URL)
    
    # connect to API
    Token = ConnectToAPI(Username, Password, URL)
    headers = {'X-Token': Token}
    # collect all objects  
    L = []
    
    # get the right range limit
    right = GetUpperLimit(Count[Object], chunk_size)
    
    # download objects in chuncks of chunk_size
    for i in range(0, right):
        response = requests.get(URL + Object + '?status=SUBMITTED&skip={0}&limit={1}'.format(i, chunk_size), headers=headers)
        L.extend(response.json()['response']['result'])
    # close connection
    CloseAPIConnection(Token, URL)
    
    # make a list of accession Id
    accessions = [i['egaAccessionId'] for i in L]
    assert len(accessions) == Count[Object]
    return L
   
    
def RelevantInfo():
    '''
    () -> dict
    
    Return a dictionary with fields of interest for each EGA object
    '''
    
    # map objects with relevant keys
    Info = {'studies': ['ebiId', 'alias', 'centerName', 'creationTime', 'egaAccessionId',
                        'shortName', 'status', 'studyType', 'title', 'xml', 'submitterId'],
            'samples': ['ebiId', 'alias', 'attributes', 'caseOrControl', 'centerName',
                        'creationTime', 'description', 'egaAccessionId', 'gender',
                        'phenotype', 'status', 'subjectId', 'title', 'xml', 'submitterId'],
            'experiments': ['ebiId', 'alias', 'centerName', 'creationTime', 'designDescription',
                            'egaAccessionId', 'egaAccessionIds', 'instrumentModel',
                            'instrumentPlatform', 'libraryLayout', 'libraryName', 'librarySelection',
                            'librarySource', 'libraryStrategy', 'pairedNominalLength', 
                            'status', 'title', 'xml', 'submitterId', 'sampleId', 'studyId'],
            'runs': ['ebiId', 'alias', 'centerName', 'creationTime', 'egaAccessionId', 'experimentId',
                     'files', 'runFileType', 'status', 'xml', 'submitterId', 'sampleId'],
            'analyses': ['ebiId', 'alias', 'analysisCenter', 'analysisDate', 'analysisFileType', 'analysisType',
                         'attributes', 'centerName', 'creationTime', 'description', 'egaAccessionId',
                         'files', 'platform', 'status', 'title', 'xml', 'submitterId', 'studyId'],
            'datasets': ['ebiId', 'alias', 'attributes', 'centerName', 'creationTime', 'datasetTypes',
                         'description', 'egaAccessionId', 'status', 'title', 'xml', 'submitterId', 'policyId'],
            'policies': ['ebiId', 'alias', 'centerName', 'egaAccessionId', 'title', 'policyText', 'url',
                         'status', 'creationTime', 'xml', 'submitterId', 'dacId'],
            'dacs': ['ebiId', 'alias', 'title', 'egaAccessionId', 'contacts', 'creationTime', 'submitterId']}

    return Info


    
def ExtractInfo(Metadata, Object):
    '''
    (list, str) -> list
    
    Return a list of dictionaries of Object with information of interest
    '''
    
    # get relevant Info
    Info = RelevantInfo()[Object]

    # make a list of dicts with relevant info
    L = []
    for d in Metadata:
        m = {}
        # loop over relevant keys
        for j in Info:
            if d[j] == None:
                m[j] = d[j]
            elif type(d[j]) == list:
                if len(d[j]) == 0:
                    m[j] = None
                else:
                    m[j] = ';'.join(list(map(lambda x: str(x), d[j])))
            else:
                # convert epoch time to readabale format
                if j == 'creationTime':
                    EpochTime = int(d[j]) / 1000
                    m[j] = str(time.strftime('%Y-%m-%d', time.localtime(EpochTime)))
                # record all file names
                elif j == 'files':
                    m[j] = ';'.join([k['fileName'] for k in d['files']])
                # add egaBox
                elif j == 'submitterId':
                    m['egaBox'] = d[j]
                    m[j] = d[j]
                # ebiId is sometimes assigned to None upon submission
                # assign a random string, gets replaced once EGA updates
                elif j == 'ebiId' and d[j] == None:
                    m[j] = str(uuid.uuid4())
                elif j == 'egaAccessionId':
                    # if egaAccessionId is None, it can be retrieved from the list of Ids
                    if d[j] == None and 'egaAccessionIds' in d:
                        if type(d['egaAccessionIds']) == list:
                            m[j] = d['egaAccessionIds'][0]
                        else:
                            m[j] = d['egaAccessionIds']
                    else:
                        m[j] = d[j]
                else:
                    m[j] = str(d[j])
        L.append(m)  
    return L


def MapEgaIdToEbiId(CredentialFile, Object, box, URL, chunk_size):
    '''
    (str, str, str, str, int) -> dict
    
    Return a dictionary of egaAccessionId: ebiId key, value pairs
    for all objects in box for which metadata is downloaded in chunks of chunk_size
    '''
    
    # download metadata for the given object
    # get the submission boxes credentials
    BoxCredentials = GetSubmissionBoxCredentials(CredentialFile)
    # count all objects registered in box
    Counts = CountObjects(box, BoxCredentials[box], URL)
    # download all metadata for Object in chunks
    L = DownloadMetadata(box, BoxCredentials[box], URL, Object, Counts, chunk_size)
    
    # create a dict with {egaAccessionId : ebiId}
    D = {}
    for i in L:
        egaAccessionId, ebiId = i['egaAccessionId'], i['ebiId']
        # ebiId is sometimes assigned to None upon submission
        # assign a random string, gets replaced once EGA updates
        if ebiId == None:
            ebiId = str(uuid.uuid4())
        D[egaAccessionId] = ebiId
    return D


def MapDatasetsToRunsAnalyses(CredentialFile, box, URL, chunk_size, DatasetMetadata):
    '''
    (str, str, str, int, list) -> dict
    
    Take the dataset metadata and return a dictionary of dataset ebiId, list of runs
    and analyses ebiId key, value pairs 
    '''
    
    # ebiId could be the key in the link table
    # but for unknown reasons, many runs objects have ebiId set to None
    # uses egaAccessionId as key instead
    
    # map analyses and runs egaAccessionId to their ebiId
    #analyses = MapEgaIdToEbiId(CredentialFile, 'analyses', box, URL, chunk_size)
    #runs = MapEgaIdToEbiId(CredentialFile, 'runs', box, URL, chunk_size)
    
    D = {}
    for i in DatasetMetadata:
        # get dataset Id
#        if i['ebiId'] == None:
#            datasetId = str(uuid.uuid4())
#        else:
#            datasetId = i['ebiId']
        if i['egaAccessionId'] == None:
            datasetId = str(uuid.uuid4())
        else:
            datasetId = i['egaAccessionId']
        
        # make a list of analyses ebId
        # analysisReferences are egaAccessionId
        #a = [analyses[j] for j in i['analysisReferences']]
        a = i['analysisReferences']
        # runsReferences are egaAccessionId or random string
        # some run accessions may not have a ebiId
        #r = [runs[j] for j in i['runsReferences']]
        r = i['runsReferences']
        #r = [runs[j] if j in runs else j for j in i['runsReferences']]
        assert not (len(a) == 0 and len(r) == 0)
        # make a list of analyses and runs ebiId
        D[datasetId] = a + r         
    return D


def MapAnalysesToSamples(AnalysisMetadata):
    '''
    (list) -> dict
    
    Take the list of analysis metadata and return a dictionary of analysis ebiId,
    list of samples ebiId key, value pairs 
    '''
    
    # create a dict {analysis_ebiId: [sample_ebiId]}
    D = {}
    
    for i in AnalysisMetadata:
        # samples are not defined in sampleRefences list
        # but can be extracted from the xml
        tree = ET.ElementTree(ET.fromstring(i['xml']))
        sample_ref = tree.findall('.//SAMPLE_REF')
        # capture all sample IDs in a list, there mayy be more than 1 for vcf files
        accessions = [sample_ref[j].attrib['accession'] for j in range(len(sample_ref))]
        # get the analysis ebiId
        ebiId = i['ebiId']
        D[ebiId] = accessions
    return D


def ConnectToDatabase(CredentialFile):
    '''
    (str) -> pymysql.connections.Connection
    
    Open a connection to the EGA database by parsing the CredentialFile
    '''

    # get the database credentials
    Credentials = ExtractCredentials(CredentialFile)
    DbHost, DbName = Credentials['DbHost'], Credentials['DbMet']
    DbUser, DbPasswd = Credentials['DbUser'], Credentials['DbPasswd']
    
    try:
        conn = pymysql.connect(host = DbHost, user = DbUser, password = DbPasswd,
                               db = DbName, charset = "utf8", port=3306,
                               unix_socket='/var/run/mysqld/mysqld.sock')
    except:
        try:
            conn = pymysql.connect(host=DbHost, user=DbUser, password=DbPasswd, db=DbName)
        except:
            raise ValueError('cannot connect to {0} database'.formatDbName)
    return conn
    
    
def ShowTables(CredentialFile):
    '''
    (str) -> list
    
    Return a list of tables in the EGA database
    '''
    
    # connect to EGA database
    conn = ConnectToDatabase(CredentialFile)
    # make a list of database tables
    cur = conn.cursor()
    cur.execute('SHOW TABLES')
    tables = [i[0] for i in cur]
    conn.close()
    return tables


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
    
  
def CreateTable(CredentialFile, Object):
    '''
    (str, str) -> None
    
    Create a table for Object in EGA database
    '''
    
    # Get the relevant metadata fields
    Info = RelevantInfo()[Object]
    # add egaBox field
    Info.append('egaBox')
    
    # determine column types
    columns = SpecifyColumnType(Info)
    
    # get table name
    table_name = Object.title()

    # connect to database
    conn = ConnectToDatabase(CredentialFile)
    cur = conn.cursor()
    # create table
    cur.execute('CREATE TABLE {0} ({1})'.format(table_name, columns))
    conn.commit()
    conn.close()

    
def DeleteRecords(CredentialFile, Table, Box):
    '''
    (str, str, str) -> None
    
    Delete rows in Table corresponding to Box
    '''

    # connect to database
    conn = ConnectToDatabase(CredentialFile)
    cur = conn.cursor()

    # delete rows corresponding to box
    cur.execute('DELETE FROM {0} WHERE {0}.egaBox=\"{1}\"'.format(Table, Box))
    conn.commit()
    conn.close()
    
    
def CreateLinktable(CredentialFile, Object):
    '''
    (str, str) -> None
    
    Create link table for datasets or analyses Object
    '''

    # connect to database
    conn = ConnectToDatabase(CredentialFile)
    cur = conn.cursor()
    
    # use egaAcessionId for runs and analyses Id in Datasets_RunsAnalyses junction table
    # because many datasets, runs ebiId are None
    if Object == 'datasets':
        cur.execute('CREATE TABLE Datasets_RunsAnalysis (datasetId VARCHAR(100), egaAccessionId VARCHAR(100), egaBox VARCHAR(100), PRIMARY KEY (datasetId, egaAccessionId))')
        conn.commit()            
    elif Object == 'analyses':
        cur.execute('CREATE TABLE Analyses_Samples (analysisId VARCHAR(100), sampleId  VARCHAR(100), egaBox VARCHAR(100), PRIMARY KEY (analysisId, sampleId))')
        conn.commit()
    conn.close()
    

def InsertMetadataTable(CredentialFile, Object, Metadata):
    '''
    (str, str, list) -> None
    
    
    Take a list of dictionaries with Objects metadata and insert it 
    into the corresponding table
    '''

    # connect to database
    conn = ConnectToDatabase(CredentialFile)    
    cur = conn.cursor()
    
    # get relevant metadata fields
    Info = RelevantInfo()[Object]
    # add egaBox
    Info.append('egaBox')
    
    # get table name
    table_name = Object.title()
    
    for d in Metadata:
        # add values to a tuple
        Values = ()
        for i in Info:
            if d[i] == '' or d[i] == None:
                Values = Values.__add__(('NULL',))
            else:
                 Values = Values.__add__((d[i],))
        assert len(Values) == len(Info)        
        # make a string with column names
        col_names = ', '.join(Info)
        # add values into table
        cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(table_name, col_names, Values))
        conn.commit()
    conn.close()
    

def InstertInfoLinktable(CredentialFile, TableName, D, box):
    '''
    (str, str, dict, str) -> None
    
    Insert objects accession Ids in D into the junction table TableName for the given box 
    '''
        
    # connect to database
    conn = ConnectToDatabase(CredentialFile)
    cur = conn.cursor()
    
    if TableName == 'Datasets_RunsAnalysis':
        for i in D:
            for j in D[i]:
                cur.execute('INSERT INTO Datasets_RunsAnalysis (datasetId, egaAccessionId, egaBox) VALUES {0}'.format((i, j, box)))         
                conn.commit()
    elif TableName == 'Analyses_Samples':
        for i in D:
            # the same sample could be linked to the same study multiple times
            # remove duplicate sample Ids
            for j in list(set(D[i])):
                cur.execute('INSERT INTO Analyses_Samples (analysisId, sampleId, egaBox) VALUES {0}'.format((i, j, box)))
                conn.commit()
    conn.close()


def GetUniqueRecords(L):
    '''
    (list) -> list
    
    Return the list of unique dictionaries with metadata.
    A single record is kept when duplicate entries with same egAccessionId   
    '''

    D = {}
    for i in L:
        accession = i['egaAccessionId']
        D[accession] = i
    K = [D[i] for i in D]
    return K


def CollectMetadata(CredentialFile, box, Object, chunk_size, URL="https://ega-archive.org/submission-api/v1"):
    '''
    (str, str, int, str, )
    
    Dowonload the Object's metadata in chuncks of chunksize for a given box from
    the EGA API at URL and instert it into the EGA database 
    '''
    
    # get the submission boxes credentials
    BoxCredentials = GetSubmissionBoxCredentials(CredentialFile)
    
    # count all objects registered in box
    Counts = CountObjects(box, BoxCredentials[box], URL)
    
    # process if objects exist
    if Counts[Object] != 0:
        # download all metadata for Object in chunks
        M = DownloadMetadata(box, BoxCredentials[box], URL, Object, Counts, chunk_size)
        print('downloaded {0} metadata from the API'.format(Object))
   
        # keep records with unique accessions
        L = GetUniqueRecords(M)
        if len(L) != len(M):
            print('removed {0} duplicate records'.format(Object))
               
        # extract relevant information
        Metadata = ExtractInfo(L, Object)
        print('collected relevant {0} information'.format(Object))

        # get the table name    
        table_name = Object.title()   
        # make a list of tables
        Tables = ShowTables(CredentialFile)
        if table_name not in Tables:
            # create table
            CreateTable(CredentialFile, Object)
            print('created table {0}'.format(table_name))
            InsertMetadataTable(CredentialFile, Object, Metadata)         
            print('inserted data in table {0} for box {1}'.format(table_name, box))    
        else:
            # update table
            DeleteRecords(CredentialFile, table_name, box)
            print('deleted rows in table {0} for box {1}'.format(table_name, box))
            InsertMetadataTable(CredentialFile, Object, Metadata)         
            print('inserted data in table {0} for box {1}'.format(table_name, box))
        # collect data to form Link Tables    
        if Object == 'datasets':
            # map dataset Ids to runs and analyses Ids
            D = MapDatasetsToRunsAnalyses(CredentialFile, box, URL, chunk_size, L)
            print('mapped datasets to runs and analyses Ids')
            # check if link table needs created or updated
            if 'Datasets_RunsAnalysis' not in Tables:
                CreateLinktable(CredentialFile, Object)
                print('created Datasets_RunsAnalysis junction table')
                # instert data into junction table
                InstertInfoLinktable(CredentialFile, 'Datasets_RunsAnalysis', D, box)
                print('inserted data in Datasets_RunsAnalysis junction table')
            else:
                DeleteRecords(CredentialFile, 'Datasets_RunsAnalysis', box)
                print('deleted rows in Datasets_RunsAnalysis junction table')
                # instert data into junction table
                InstertInfoLinktable(CredentialFile, 'Datasets_RunsAnalysis', D, box)
                print('inserted data in Datasets_RunsAnalysis junction table')
        elif Object == 'analyses':
            # map analyses Ids to sample Ids    
            D = MapAnalysesToSamples(L)
            print('mapped analyses to samples Ids')
            # check if link table needs created or updated
            if 'Analyses_Samples' not in Tables:
                CreateLinktable(CredentialFile, Object)
                print('created Analyses_Samples junction table')
                # instert data into junction table
                InstertInfoLinktable(CredentialFile, 'Analyses_Samples', D, box)
                print('inserted data in Analyses_Samples junction table')
            else:
                DeleteRecords(CredentialFile, 'Analyses_Samples', box)
                print('deleted rows in Analyses_Samples junction table')
                # instert data into junction table
                InstertInfoLinktable(CredentialFile, 'Analyses_Samples', D, box)
                print('inserted data in Analyses_Samples junction table')


if __name__ == '__main__':
    import os
    # make a list of boxes
    Boxes = ['ega-box-137', 'ega-box-1269', 'ega-box-12']
    # make a list of objects
    Objects = ['studies', 'runs', 'samples', 'experiments', 'datasets', 'analyses', 'policies', 'dacs']
    # get path to Credential File
    CredentialFile = '.EGA_metData'
    if os.path.isfile(CredentialFile) == False:
        CredentialFile = '/.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/Submission_Tools/.EGA_metData'
    for box in Boxes:
        for Object in Objects:
            try:
                CollectMetadata(CredentialFile, box, Object, 500, "https://ega-archive.org/submission-api/v1")
            except:
                print('## ERROR ## Could not add {0} metadata for box {1} into EGA database'.format(Object, box))
           