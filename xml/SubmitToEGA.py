# -*- coding: utf-8 -*-
"""
Created on Tue Sep 11 13:37:40 2018

@author: rjovelin
"""


# import modules
import json
import subprocess
import time
import pymysql
import os
import argparse
import requests
import uuid


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

# use this function to extract password and username for a given box from the credential file
def ParseCredentials(CredentialFile, Box):
    '''
    (str, str) -> tuple
    Take the file with credentials to connect to the database, and return the 
    username and password of the given Box
    '''
    
    # parse the crdential file, get username and password for given box
    Credentials = ExtractCredentials(CredentialFile)
    if Box == 'ega-box-12':
        MyPassword, UserName = Credentials['MyPassWordBox12'], Credentials['UserNameBox12']
    elif Box == 'ega-box-137':
        MyPassword, UserName = Credentials['MyPassWordBox137'], Credentials['UserNameBox137']
    return UserName, MyPassword


# use this function to connect to the gsi database
def EstablishConnection(CredentialFile, database):
    '''
    (list, str) -> connection object    
    Take a file with database credentials and the name of the database
    '''
    
    # extract database credentials from the command
    Credentials = ExtractCredentials(CredentialFile)
    # get the database name
    assert database in [Credentials['DbMet'], Credentials['DbSub']]
    # connnect to the database
    conn = pymysql.connect(host = Credentials['DbHost'], user = Credentials['DbUser'],
                           password = Credentials['DbPasswd'], db = database, charset = "utf8",
                           port=3306, unix_socket='/var/run/mysqld/mysqld.sock')
    return conn 


# use this function to list tables in the database
def ListTables(CredentialFile, DataBase):
    '''
    (file, str) -> list
    Take the file with credentials to connect to DataBase and return a list
    of tables in the Database
    '''
    
    # connect to database
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
    cur.execute('SHOW TABLES')
    Tables = [i[0] for i in cur]
    conn.close()
    return Tables



# use this function to to generate a working directory to save the encrypted and md5sums 
def GetWorkingDirectory(S, WorkingDir = '/scratch2/groups/gsi/bis/EGA_Submissions'):
    '''
    (str, str) -> str
    Returns a working directory where to save the encrypted and md5sum files
    by appending str S to WorkingDir
    '''
    
    return os.path.join(WorkingDir, S)
    

# use this function to add a working directory for each alias
def AddWorkingDirectory(CredentialFile, DataBase, Table, Box):
    '''
    (str, str, str, str) --> None
    Take the file with credentials to connect to Database, create unique directories
    in file system for each alias in Table with ready Status and Box and record       
    working directory in Table
    '''
    
    # check if table exists
    Tables = ListTables(CredentialFile, DataBase)
    
    if Table in Tables:
        # connect to db
        conn = EstablishConnection(CredentialFile, DataBase)
        cur = conn.cursor()
        # get the title project and the attributes for that alias
        cur.execute('SELECT {0}.alias FROM {0} WHERE {0}.Status=\"valid\" and {0}.egaBox=\"{1}\"'.format(Table, Box))
        Data = cur.fetchall()
        if len(Data) != 0:
            # loop over alias
            for i in Data:
                alias = i[0]
                # create working directory with random unique identifier
                UID = str(uuid.uuid4())             
                # record identifier in table, create working directory in file system
                cur.execute('UPDATE {0} SET {0}.WorkingDirectory=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, UID, alias, Box))  
                conn.commit()
                # create working directories
                WorkingDir = GetWorkingDirectory(UID, WorkingDir = '/scratch2/groups/gsi/bis/EGA_Submissions')
                os.makedirs(WorkingDir)
        conn.close()
        
        # check that working directory was recorded and created
        conn = EstablishConnection(CredentialFile, DataBase)
        cur = conn.cursor()
        # get the title project and the attributes for that alias
        cur.execute('SELECT {0}.alias, {0}.WorkingDirectory FROM {0} WHERE {0}.Status=\"valid\" and {0}.egaBox=\"{1}\"'.format(Table, Box))
        Data = cur.fetchall()
        if len(Data) != 0:
            for i in Data:
                Error = []
                alias = i[0]
                WorkingDir = GetWorkingDirectory(i[1])
                if i[1] in ['', 'NULL', '(null)']:
                    Error.append('Working directory does not have a valid Id')
                if os.path.isdir(WorkingDir) == False:
                    Error.append('Working directory not generated')
                # check if error message
                if len(Error) != 0:
                    # error is found, record error message, keep status valid --> valid
                    cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, Error, alias, Box))  
                    conn.commit()
                else:
                    # no error, update Status valid --> start
                    cur.execute('UPDATE {0} SET {0}.Status=\"start\", {0}.errorMessages=\"None\" WHERE {0}.alias=\"{1}\" AND {0}.egaBox=\"{2}\"'.format(Table, alias, Box))  
                    conn.commit()
        conn.close()            
        

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
    Header = infile.readline().rstrip().split('\t')
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
    storing the information for a unique analysis object
    Preconditions: Required fields must be present or returned list is empty,
    and missing entries are not permitted (e.g. can be '', NA)
    '''
    
    # create a dict to store the information about the files
    D = {}
    
    infile = open(Table)
    # get file header
    Header = infile.readline().rstrip().split('\t')
    # check that required fields are present
    Missing =  [i for i in ['alias', 'sampleAlias', 'filePath'] if i not in Header]
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
            if 'fileName' not in Header:
                if 'analysisDate' in Header:
                    L = ['alias', 'sampleAlias', 'filePath', 'analysisDate']
                    alias, sampleAlias, filePath, analysisDate = [S[Header.index(L[i])] for i in range(len(L))]
                else:
                    L = ['alias', 'sampleAlias', 'filePath']
                    alias, sampleAlias, filePath = [S[Header.index(L[i])] for i in range(len(L))]
                    analysisDate = ''
                # file name is not supplied, use filename in filepath             
                assert filePath != '/' and filePath[-1] != '/'
                fileName = os.path.basename(filePath)                
            else:
                # file name is supplied, use filename
                if 'analysisDate' in Header:
                    L = ['alias', 'sampleAlias', 'filePath', 'fileName', 'analysisDate']
                    alias, sampleAlias, filePath, fileName, analysisDate = [S[Header.index(L[i])] for i in range(len(L))]
                else:
                    L = ['alias', 'sampleAlias', 'filePath', 'fileName']
                    alias, sampleAlias, filePath, fileName = [S[Header.index(L[i])] for i in range(len(L))]
                    analysisDate = ''
                # check if fileName is provided for that alias
                if fileName in ['', 'NULL', 'NA']:
                    fileName = os.path.basename(filePath)
            # check if alias already recorded ( > 1 files for this alias)
            if alias not in D:
                # create inner dict, record sampleAlias and create files dict
                D[alias] = {}
                # record alias
                D[alias]['alias'] = alias
                D[alias]['analysisDate'] = analysisDate
                # record sampleAlias. multiple sample alias are allowed, eg for VCFs
                D[alias]['sampleAlias'] = [sampleAlias]
                D[alias]['files'] = {}
                D[alias]['files'][filePath] = {'filePath': filePath, 'fileName': fileName}
            else:
                assert D[alias]['alias'] == alias
                # record sampleAlias
                D[alias]['sampleAlias'].append(sampleAlias)
                # record file info, filepath shouldn't be recorded already 
                assert filePath not in D[alias]['files']
                D[alias]['files'][filePath] = {'filePath': filePath, 'fileName': fileName}
                     
    infile.close()

    # create list of dicts to store the info under a same alias
    # [{alias: {'sampleAlias':sampleAlias, 'files': {filePath: {attributes: key}}}}]
    L = [{alias: D[alias]} for alias in D]             
    return L        


# use this function to parse the AnalysisAttributes input file
def ParseAnalysesAccessoryTables(Table, TableType):
    '''
    (file, str) -> dict
    Read Table and returns of key: value pairs for Projects or Attributes Tables
    '''
    
    infile = open(Table)
    Content = infile.read().rstrip().split('\n')
    infile.close()
    # create a dict {key: value}
    D = {}
    # check that required fields are present
    if TableType == 'Attributes':
        Expected = ['alias', 'title', 'description', 'genomeId', 'StagePath']
    elif TableType == 'Projects':
        Expected = ['alias', 'analysisCenter', 'studyId', 'Broker', 'analysisTypeId',
                    'experimentTypeId'] 
    Fields = [S.split(':')[0].strip() for S in Content if ':' in S]
    Missing = [i for i in Expected if i not in Fields]
    if len(Missing) != 0:
        print('These required fields are missing: {0}'.format(', '.join(Missing)))
    else:
        for S in Content:
            S = list(map(lambda x: x.strip(), S.split(':')))
            if S[0] not in ['attributes', 'units']:
                assert len(S) == 2
                D[S[0]] = S[1]
            else:
                assert len(S) == 3
                if 'attributes' not in D:
                    D['attributes'] = {}
                if S[1] not in D['attributes']:
                    D['attributes'][S[1]] = {}    
                if S[0] == 'attributes':
                    if 'tag' not in D['attributes'][S[1]]:
                        D['attributes'][S[1]]['tag'] = S[1]
                    else:
                        assert D['attributes'][S[1]]['tag'] == S[1]
                    D['attributes'][S[1]]['value'] = S[2]
                elif S[0] == 'units':
                    if 'tag' not in D['attributes'][S[1]]:
                        D['attributes'][S[1]]['tag'] = S[1]
                    else:
                        assert D['attributes'][S[1]]['tag'] == S[1]
                    D['attributes'][S[1]]['unit'] = S[2]
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


# use this function to format the sample json
def FormatSampleJson(D):
    '''
    (dict) -> dict
    Take a dictionary with information for a sample object and return a dictionary
    with the expected format or dictionary with the alias only if required fields are missing
    Precondition: strings in D have double-quotes
    '''
    
    # create a dict to be strored as a json. note: strings should have double quotes
    J = {}
    
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
    return J                


# use this function to print a dict the enumerations from EGA to std output
def GrabEgaEnums(args):
    '''
    (str) -> dict
    Take the URL for a given enumeration and return and dict of tag: value pairs
    '''
    
    # create a dict to store the enumeration info {value: tag}
    Enum = {}
    # connect to the api, retrieve the information for the given enumeration
    response = requests.get(args.url)
    # check response code
    if response.status_code == requests.codes.ok:
        # loop over dict in list
        for i in response.json()['response']['result']:
            assert i['value'] not in Enum
            Enum[i['value']] = i['tag']
    print(Enum)

# use this function to format the analysis json
def FormatAnalysisJson(D):
    '''
    (dict) -> dict
    Take a dictionary with information for an analysis object and return a dictionary
    with the expected format or dictionary with the alias only if required fields are missing
    Precondition: strings in D have double-quotes
    '''
    
    # get the enumerations
    ExperimentTypes = GrabEgaEnums('https://ega-archive.org/submission-api/v1/enums/experiment_types')
    AnalysisTypes =  GrabEgaEnums('https://ega-archive.org/submission-api/v1/enums/analysis_types')
    FileTypes = GrabEgaEnums('https://ega-archive.org/submission-api/v1/enums/analysis_file_types')
    
    # create a dict to be strored as a json. note: strings should have double quotes
    J = {}
    
    JsonKeys = ["alias", "title", "description", "studyId", "sampleReferences",
                "analysisCenter", "analysisDate", "analysisTypeId", "files",
                "attributes", "genomeId", "chromosomeReferences", "experimentTypeId", "platform"]
    # loop over required json keys
    for field in JsonKeys:
        if field in D:
            if D[field] == 'NULL':
                # some fields are required, return empty dict if field is empty
                if field in ["alias", "title", "description", "studyId", "analysisCenter",
                             "analysisTypeId", "files", "genomeId", "experimentTypeId", "StagePath"]:
                    # erase dict and add alias
                    J = {}
                    J["alias"] = D["alias"]
                    # return dict with alias only if required fields are missing
                    return J
                # other fields can be missing, either as empty list or string
                else:
                    # chromosomeReferences is hard-coded as empty list
                    if field == "chromosomeReferences" or field == "attributes":
                        J[field] = []
                    else:
                        J[field] = ""
            else:
                if field == 'files':
                    assert D[field] != 'NULL'
                    J[field] = []
                    # convert string to dict
                    files = D[field].replace("'", "\"")
                    files = json.loads(files)
                    # loop over file name
                    for filePath in files:
                        # create a dict to store file info
                        # check that fileTypeId is valid
                        if files[filePath]["fileTypeId"].lower() not in FileTypes:
                            # cannot obtain fileTypeId. erase dict and add alias
                            J = {}
                            J["alias"] = D["alias"]
                            # return dict with alias only if required fields are missing
                            return J
                        else:
                            fileTypeId = FileTypes[files[filePath]["fileTypeId"].lower()]
                        # create dict with file info, add path to file names
                        d = {"fileName": os.path.join(D['StagePath'], files[filePath]['encryptedName']),
                             "checksum": files[filePath]['checksum'],
                             "unencryptedChecksum": files[filePath]['unencryptedChecksum'],
                             "fileTypeId": fileTypeId}
                        J[field].append(d)
                elif field == 'attributes':
                    # ensure strings are double-quoted
                    attributes = D[field].replace("'", "\"")
                    # convert string to dict
                    # loop over all attributes
                    attributes = attributes.split(';')
                    J[field] = [json.loads(attributes[i].strip().replace("'", "\"")) for i in range(len(attributes))]
                elif field == "experimentTypeId":
                    # check that experimentTypeId is valid
                    if D[field] not in ExperimentTypes:
                        # cannot obtain experimentTypeId. erase dict and add alias
                        J = {}
                        J["alias"] = D["alias"]
                        # return dict with alias only if required fields are missing
                        return J
                    else:
                        J[field] = [ExperimentTypes[D[field]]]
                elif field == "analysisTypeId":
                    # check that analysisTypeId is valid
                    if D[field] not in AnalysisTypes:
                        # cannot obtain analysisTypeId. erase dict and add alias
                        J = {}
                        J["alias"] = D["alias"]
                        # return dict with alias only if required fields are missing
                        return J
                    else:
                        J[field] = AnalysisTypes[D[field]]
                else:
                    J[field] = D[field]
        else:
            if field == 'sampleReferences':
                # populate with sample accessions
                J[field] = []
                if ':' in D['sampleEgaAccessionsId']:
                    for accession in D['sampleEgaAccessionsId'].split(':'):
                        J[field].append({"value": accession.strip(), "label":""})
                else:
                    J[field].append({"value": D['sampleEgaAccessionsId'], "label":""})
    return J                

# use this function to extract ega accessions from metadata database
def ExtractAccessions(CredentialFile, DataBase, Box, Table):
    '''
    (file, str, str, str) -> dict
    Take a file with credentials to connect to DataBase and return a dictionary
    with alias: accessions registered in Box for the given object/Table
    '''
    
    # connect to metadata database
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
    # pull down analysis alias and egaId from metadata db, alias should be unique
    cur.execute('SELECT {0}.alias, {0}.egaAccessionId from {0} WHERE {0}.egaBox=\"{1}\"'.format(Table, Box)) 
    # create a dict {alias: accession}
    # some PCSI aliases are not unique, 1 sample is chosen arbitrarily
    Registered = {}
    for i in cur:
        Registered[i[0]] = i[1]
    conn.close()
    return Registered

# use this function to check that root is not given as a parameter for stagepath
def RejectRoot(S):
    '''
    (str) -> str
    Take the string name of the staging server on EGA and return this or raise
    a value error if the name is the root
    '''

    if S == '/':
        raise ValueError('The root is not allowed for the staging server')
    return S

# use this function to form jsons and store to submission db
def AddSampleJsonToTable(CredentialFile, DataBase, Table, Box):
    '''
    (file, str, str, str) -> None
    Take the file with credentials to connect to DataBase and update the Table
    for Object with json and new status if json is formed correctly
    '''
    
    
    # check if Sample table exists
    Tables = ListTables(CredentialFile, DataBase)

    # connect to the database
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
    
    if Table in Tables:
        ## form json, add to table and update status -> submit
        # pull data for objects with ready Status for sample and uploaded Status for analyses
        cur.execute('SELECT * FROM {0} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{1}\"'.format(Table, Box))

        # get column headers
        Header = [i[0] for i in cur.description]
        # extract all information 
        Data = cur.fetchall()
        # check that samples are in ready mode
        if len(Data) != 0:
            # create a list of dicts storing the object info
            L = []
            for i in Data:
                D = {}
                assert len(i) == len(Header)
                for j in range(len(i)):
                    D[Header[j]] = i[j]
                L.append(D)
            # create object-formatted jsons from each dict 
            Jsons = [FormatSampleJson(D) for D in L]
            # add json back to table and update status
            for D in Jsons:
                # check if json is correctly formed (ie. required fields are present)
                if len(D) == 1:
                    print('cannot form json for {0}, required field(s) missing'.format(D['alias']))
                else:
                    # add json back in table and update status
                    alias = D['alias']
                    # string need to be in double quote
                    cur.execute('UPDATE {0} SET {0}.Json=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\";'.format(Table, str(D), alias, Box))
                    conn.commit()
                    # update status to submit
                    cur.execute('UPDATE {0} SET {0}.Status=\"submit\" WHERE {0}.alias="\{1}\" AND {0}.egaBox=\"{2}\";'.format(Table, alias, Box))
                    conn.commit()
    else:
        print('Table {0} does not exist')
    conn.close()

    
# use this function to form jsons and store to submission db
def AddAnalysisJsonToTable(CredentialFile, DataBase, Table, AttributesTable, ProjectsTable, Box):
    '''
    (str, str, str, str, str, str) -> None
    Form a json for Analyses Objects in the given Box and add it to Table by
    quering required information from the Analysis, Projects and Attributes Tables 
    using the file with credentials to connect to Database update the analysis
    status if json is formed correctly
    '''
    
    # check if Sample table exists
    Tables = ListTables(CredentialFile, DataBase)

    # connect to the database
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
    
    if Table in Tables and AttributesTable in Tables and ProjectsTable in Tables:
        ## form json, add to table and update status -> submit
        cur.execute('SELECT {0}.alias, {0}.sampleEgaAccessionsId, {0}.analysisDate, {0}.files, \
                    {1}.title, {1}.description, {1}.attributes, {1}.genomeId, {1}.chromosomeReferences, {1}.StagePath, {1}.platform, \
                    {2}.studyId, {2}.analysisCenter, {2}.Broker, {2}.analysisTypeId, {2}.experimentTypeId  \
                    FROM {0} JOIN {1} JOIN {2} WHERE {0}.Status=\"uploaded\" AND {0}.egaBox=\"{3}\" AND {0}.attributes = {1}.alias \
                    AND {0}.projects = {2}.alias'.format(Table, AttributesTable, ProjectsTable, Box))

        # get column headers
        Header = [i[0] for i in cur.description]
        # extract all information 
        Data = cur.fetchall()
        # check that samples are in ready mode
        if len(Data) != 0:
            # create a list of dicts storing the object info
            L = []
            for i in Data:
                D = {}
                assert len(i) == len(Header)
                for j in range(len(i)):
                    D[Header[j]] = i[j]
                L.append(D)
            # create object-formatted jsons from each dict 
            Jsons = [FormatAnalysisJson(D) for D in L]
            # add json back to table and update status
            for D in Jsons:
                # check if json is correctly formed (ie. required fields are present)
                if len(D) == 1:
                    Error = 'Cannot form json, required field(s) missing'
                    # add error in table and keep status uploaded --> uploaded
                    alias = D['alias']
                    cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, Error, alias, Box))
                    conn.commit()
                else:
                    # add json back in table and update status
                    alias = D['alias']
                    cur.execute('UPDATE {0} SET {0}.Json=\"{1}\", {0}.errorMessages=\"None\", {0}.Status=\"submit\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\";'.format(Table, str(D), alias, Box))
                    conn.commit()
    conn.close()
    

# use this function to add sample accessions to Analysis Table in the submission database
def AddSampleAccessions(CredentialFile, MetadataDataBase, SubDataBase, Box, Table):
    '''
    (file, str, str, str, str) -> None
    Take a file with credentials to connect to metadata and submission databases
    and update the Table in the submission table with the sample accessions
    and update the analyses status to upload
    '''
    
    # grab sample EGA accessions from metadata database, create a dict {alias: accession}
    Registered = ExtractAccessions(CredentialFile, MetadataDataBase, Box, 'Samples')
            
    # connect to the submission database
    conn = EstablishConnection(CredentialFile, SubDataBase)
    cur = conn.cursor()
    # pull alias, sampleEgacessions for analyses with ready status for given box
    cur.execute('SELECT {0}.sampleAlias, {0}.sampleEgaAccessionsId, {0}.alias FROM {0} WHERE {0}.Status=\"start\" AND {0}.egaBox=\"{1}\"'.format(Table, Box))
    Data = cur.fetchall()
    
    # create a dict {alias: [sampleaccessions, ErrorMessage]}
    Samples = {}
    # check if alias are in ready status
    if len(Data) != 0:
        for i in Data:
            # make a list of sampleAlias
            sampleAlias = i[0].split(':')
            # make a list of sample accessions
            sampleAccessions = [Registered[j] for j in sampleAlias if j in Registered]
            # record error if sample aliases have missing accessions
            if len(sampleAlias) != len(sampleAccessions):
                Error = 'Sample accessions not available'
            else:
                Error = ''
            Samples[i[2]] = [':'.join(sampleAccessions), Error]
        if len(Samples) != 0:
            for alias in Samples:
                # update status start --> encrypt if no error
                if Samples[alias][1] == '':
                    # update sample accessions and status start --> encrypt
                    cur.execute('UPDATE {0} SET {0}.sampleEgaAccessionsId=\"{1}\", {0}.errorMessages=\"None\", {0}.Status=\"encrypt\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, Samples[alias][0], alias, Box)) 
                    conn.commit()
                else:
                    # record error message and keep status start --> start
                    cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, Error, alias, Box)) 
                    conn.commit()
    conn.close()    


# use this script to launch qsubs to encrypt the files and do a checksum
def EncryptAndChecksum(alias, filePath, fileName, KeyRing, OutDir, Queue, Mem):
    '''
    (file, str, str, str, str, str) -> tuple
    Take the full path to file, the name of the output file, the path to the
    keys used during encryption, the directory where encrypted and cheksums are saved, 
    the queue and memory allocated to run the jobs and return the exit codes 
    specifying if the jobs were launched successfully or not and the job names
    '''

    MyCmd1 = 'md5sum {0} | cut -f1 -d \' \' > {1}.md5'
    MyCmd2 = 'gpg --no-default-keyring --keyring {2} -r EGA_Public_key -r SeqProdBio --trust-model always -o {1}.gpg -e {0}'
    MyCmd3 = 'md5sum {0}.gpg | cut -f1 -d \' \' > {0}.gpg.md5'

    # check that FileName is valid
    if os.path.isfile(filePath) == False:
        # return error that will be caught if file doesn't exist
        return [-1], [-1] 
    else:
        # check if OutDir exist
        if os.path.isdir(OutDir) == False:
            return [-1], [-1] 
        else:
            # make a directory to save the scripts
            qsubdir = os.path.join(OutDir, 'qsubs')
            if os.path.isdir(qsubdir) == False:
                os.mkdir(qsubdir)
            # create a log dir
            logDir = os.path.join(qsubdir, 'log')
            if os.path.isdir(logDir) == False:
                os.mkdir(logDir)
        
            # get name of output file
            OutFile = os.path.join(OutDir, fileName)
            # put commands in shell script
            BashScript1 = os.path.join(qsubdir, alias + '_' + fileName + '_md5sum_original.sh')
            BashScript2 = os.path.join(qsubdir, alias + '_' + fileName + '_encrypt.sh')
            BashScript3 = os.path.join(qsubdir, alias + '_' + fileName + '_md5sum_encrypted.sh')
            with open(BashScript1, 'w') as newfile:
                newfile.write(MyCmd1.format(filePath, OutFile) + '\n')
            with open(BashScript2, 'w') as newfile:
                newfile.write(MyCmd2.format(filePath, OutFile, KeyRing) + '\n')
            with open(BashScript3, 'w') as newfile:
                newfile.write(MyCmd3.format(OutFile) + '\n')
        
            # launch qsub directly, collect job names and exit codes
            JobName1 = 'Md5sum.original.{0}'.format(alias + '__' + fileName)
            QsubCmd1 = "qsub -b y -q {0} -l h_vmem={1}g -N {2} -e {3} -o {3} \"bash {4}\"".format(Queue, Mem, JobName1, logDir, BashScript1)
            job1 = subprocess.call(QsubCmd1, shell=True)
                
            JobName2 = 'Encrypt.{0}'.format(alias + '__' + fileName)
            QsubCmd2 = "qsub -b y -q {0} -hold_jid {1} -l h_vmem={2}g -N {3} -e {4} -o {4} \"bash {5}\"".format(Queue, JobName1, Mem, JobName2, logDir, BashScript2)
            job2 = subprocess.call(QsubCmd2, shell=True)
        
            JobName3 = 'Md5sum.encrypted.{0}'.format(alias + '__' + fileName)
            QsubCmd3 = "qsub -b y -q {0} -hold_jid {1} -l h_vmem={2}g -N {3} -e {4} -o {4} \"bash {5}\"".format(Queue, JobName2, Mem, JobName3, logDir, BashScript3)
            job3 = subprocess.call(QsubCmd3, shell=True)
        
            return [job1, job2, job3], [JobName1, JobName2, JobName3]


# use this function to encrypt files and update status to encrypting
def EncryptFiles(CredentialFile, DataBase, Table, Box, KeyRing, Queue, Mem, DiskSpace):
    '''
    (file, str, str, str, str, str, int) -> None
    Take a file with credentials to connect to Database, encrypt files of aliases
    only if DiskSpace (in TB) is available in scratch after encryption and update
    file status to encrypting if encryption and md5sum jobs are successfully
    launched using the specified queue and memory
    '''
    
    # create a list of aliases for encryption 
    Aliases = SelectAliasesForEncryption(CredentialFile, DataBase, Table, Box, DiskSpace)
           
    # check if Table exist
    Tables = ListTables(CredentialFile, DataBase)
    if Table in Tables:
        # connect to database
        conn = EstablishConnection(CredentialFile, DataBase)
        cur = conn.cursor()
        # pull alias, files and working directory for status = encrypt
        cur.execute('SELECT {0}.alias, {0}.files, {0}.WorkingDirectory FROM {0} WHERE {0}.Status=\"encrypt\" AND {0}.egaBox=\"{1}\"'.format(Table, Box))
        Data = cur.fetchall()
        conn.close()
        
        # check that some files are in encrypt mode
        if len(Data) != 0:
            for i in Data:
                alias = i[0]
                # encrypt only files of aliases that were pre-selected
                if alias in Aliases:
                    # get working directory
                    WorkingDir = GetWorkingDirectory(i[2])
                    # create working directory if doesn't exist
                    if os.path.isdir(WorkingDir) == False:
                        os.makedirs(WorkingDir)
                    assert '/scratch2/groups/gsi/bis/EGA_Submissions' in WorkingDir
                    # convert single quotes to double quotes for str -> json conversion
                    files = json.loads(i[1].replace("'", "\""))
                    # store the job names and exit codes for that alias
                    JobCodes, JobNames = [], []
                    # loop over files for that alias
                    for file in files:
                        # get the filePath and fileName
                        filePath = files[file]['filePath']
                        fileName = files[file]['fileName']
                        # encrypt and run md5sums on original and encrypted files
                        j, k = EncryptAndChecksum(alias, filePath, fileName, KeyRing, WorkingDir, Queue, Mem)
                        JobCodes.extend(j)
                        JobNames.extend(k)
                    # check if encription was launched successfully
                    if len(set(JobCodes)) == 1 and list(set(JobCodes))[0] == 0:
                        # store the job names
                        JobNames = ';'.join(JobNames)
                        # encryption and md5sums jobs launched succcessfully, update status -> encrypting
                        conn = EstablishConnection(CredentialFile, DataBase)
                        cur = conn.cursor()
                        cur.execute('UPDATE {0} SET {0}.Status=\"encrypting\", {0}.JobNames=\"{1}\", {0}.errorMessages=\"None\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, JobNames, alias, Box))
                        conn.commit()
                        conn.close()
                    else:
                        # store error message and jobnames, keep status encrypt --> encrypt
                        JobNames = ';'.join(JobNames)
                        Error = 'Could not launch encryption jobs'
                        conn = EstablishConnection(CredentialFile, DataBase)
                        cur = conn.cursor()
                        cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\", {0}.JobNames=\"{2}\" WHERE {0}.alias=\"{3}\" AND {0}.egaBox=\"{4}\"'.format(Table, Error, JobNames, alias, Box))
                        conn.commit()
                        conn.close()
                        

# use this function to check if a job is still running
def CheckRunningJob(JobName):
    '''
    (str) -> bool
    Take the name of a job and the description of all running jobs and return
    True in the given job name is this description and false otherwise
    '''
    
    # store the content of the job description 
    JobDetails = []
    # get the list of job Ids
    JobIds = subprocess.check_output("qstat | tail -n +3 | cut -d ' ' -f1", shell=True).decode('utf-8').rstrip().split('\n')
    # check if jobs are running
    if len(JobIds) != 0:
        for i in JobIds:
            # check if the listed job is still running
            try:
                content = subprocess.check_output('qstat -j {0}'.format(i), shell=True).decode('utf-8').rstrip()
            # job may have ended between job_Ids collection and view on running jobs, collect empty string
            except:
                content = ''
            JobDetails.append(content)
    # check f Job name is in JobDetails        
    JobDetails = ','.join(JobDetails)        
    return JobName in JobDetails

                
# use this function to check that encryption is done
def CheckEncryption(CredentialFile, DataBase, Table, Box):
    '''
    (file, str, str, str, str, str) -> None
    Take the file with DataBase credentials, the tables in this db used to pull
    information to extract the working directory and files in encrypting status
    and update status to upload and files with md5sums when encrypting is done
    '''        
        
    # check that table exists
    Tables = ListTables(CredentialFile, DataBase)
    
    if Table in Tables:
        # connect to database
        conn = EstablishConnection(CredentialFile, DataBase)
        cur = conn.cursor()
        # pull alias and files and encryption job names for status = encrypting
        cur.execute('SELECT {0}.alias, {0}.files, {0}.JobNames, {0}.WorkingDirectory FROM {0} WHERE {0}.Status=\"encrypting\" AND {0}.egaBox=\"{1}\"'.format(Table, Box))
        Data = cur.fetchall()
        conn.close()
        # check that some files are in encrypting mode
        if len(Data) != 0:
            for i in Data:
                alias = i[0]
                # get the working directory for that alias
                WorkingDir = GetWorkingDirectory(i[3])
                # convert single quotes to double quotes for str -> json conversion
                files = json.loads(i[1].replace("'", "\""))
                # get the job names
                jobNames = i[2]
                # create a dict to store the updated file info
                Files = {}
                
                # check that all jobs are done running                 
                StillRunning = [CheckRunningJob(JobName) for JobName in jobNames.split(';')]
                if True not in StillRunning:
                    # create boolean, update when md5sums and encrypted file not found for at least one file under the same alias 
                    Encrypted = True
                    for file in files:
                        # get the fileName
                        fileName = files[file]['fileName']
                        fileTypeId = files[file]['fileTypeId']
                        # check that encryoted and md5sum files do exist
                        originalMd5File = os.path.join(WorkingDir, fileName + '.md5')
                        encryptedMd5File = os.path.join(WorkingDir, fileName + '.gpg.md5')
                        encryptedFile = os.path.join(WorkingDir, fileName + '.gpg')
                        if os.path.isfile(originalMd5File) and os.path.isfile(encryptedMd5File) and os.path.isfile(encryptedFile):
                            # get the name of the encrypted file
                            encryptedName = fileName + '.gpg'
                            # get the md5sums
                            encryptedMd5 = subprocess.check_output('cat {0}'.format(encryptedMd5File), shell = True).decode('utf-8').rstrip()
                            originalMd5 = subprocess.check_output('cat {0}'.format(originalMd5File), shell = True).decode('utf-8').rstrip()
                            if encryptedMd5 != '' and originalMd5 != '':
                                # capture md5sums, build updated dict
                                Files[file] = {'filePath': file, 'unencryptedChecksum': originalMd5, 'encryptedName': encryptedName, 'checksum': encryptedMd5, 'fileTypeId': fileTypeId} 
                            else:
                                # update boolean
                                Encrypted = False
                        else:
                            # update boollean
                            Encrypted = False
                
                    # check if md5sums and encrypted files is available for all files
                    if Encrypted == True:
                        # update file info and status only if all files do exist and md5sums can be extracted
                        conn = EstablishConnection(CredentialFile, DataBase)
                        cur = conn.cursor()
                        cur.execute('UPDATE {0} SET {0}.files=\"{1}\", {0}.errorMessages=\"None\", {0}.Status=\"upload\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, str(Files), alias, Box))
                        conn.commit()
                        conn.close()
                    elif Encrypted == False:
                        # reset status encrypting -- > encrypt, record error message
                        Error = 'Encryption or md5sum did not complete'
                        conn = EstablishConnection(CredentialFile, DataBase)
                        cur = conn.cursor()
                        cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\", {0}.Status=\"encrypt\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, Error, alias, Box))
                        conn.commit()
                        conn.close()
                    

# use this script to launch qsubs to encrypt the files and do a checksum
def UploadAliasFiles(D, filePath, StagePath, FileDir, CredentialFile, Box, Queue, Mem, UploadMode):
    '''
    (dict, str, str, str, str, str, int, bool) -> (list, list)
    Take a dictionary with file information for a given alias, the file with 
    db credentials, the path to the original files (ie. the files used to generate
    encrypted and md5sums), the directory were the command scripts are saved,
    the queue name and memory to launch the jobs and return a tuple with exit 
    code and job name used for uploading he encrypted and md5 files corresponding
    to filepath. 
    '''
    
    # parse the crdential file, get username and password for given box
    UserName, MyPassword = ParseCredentials(CredentialFile, Box)
    
    # write shell scripts with command
    assert os.path.isdir (FileDir)
    # make a directory to save the scripts
    qsubdir = os.path.join(FileDir, 'qsubs')
    if os.path.isdir(qsubdir) == False:
        os.mkdir(qsubdir)
    # create a log dir
    logDir = os.path.join(qsubdir, 'log')
    if os.path.isdir(logDir) == False:
        os.mkdir(logDir)
    assert os.path.isdir(logDir)
    
    # create destination directory
    Cmd = "ssh xfer4.res.oicr.on.ca \"lftp -u {0},{1} -e \\\" set ftp:ssl-allow false; mkdir -p {2}; bye;\\\" ftp://ftp-private.ebi.ac.uk\""
    subprocess.call(Cmd.format(UserName, MyPassword, StagePath), shell=True)     
        
    # command to create destination directory and upload files    
    # aspera is installed on xfer4
    if UploadMode == 'lftp':
        UploadCmd = "ssh xfer4.res.oicr.on.ca \"lftp -u {0},{1} -e \\\" set ftp:ssl-allow false; mput {3} {4} {5} -O {2}  bye;\\\" ftp://ftp-private.ebi.ac.uk\""
    elif UploadMode == 'aspera':
        UploadCmd = "ssh xfer4.res.oicr.on.ca \"export ASPERA_SCP_PASS={0};ascp -P33001 -O33001 -QT -l300M {1} {2}@fasp.ega.ebi.ac.uk:{3};ascp -P33001 -O33001 -QT -l300M {4} {2}@fasp.ega.ebi.ac.uk:{3};ascp -P33001 -O33001 -QT -l300M {5} {2}@fasp.ega.ebi.ac.uk:{3};\""
        
    # get alias
    assert len(list(D.keys())) == 1
    alias = list(D.keys())[0]
        
    # get filename
    fileName = os.path.basename(filePath)
    encryptedFile = os.path.join(FileDir, D[alias]['files'][filePath]['encryptedName'])
    originalMd5 = os.path.join(FileDir, fileName + '.md5')
    encryptedMd5 = os.path.join(FileDir, fileName + '.gpg.md5')
    if os.path.isfile(encryptedFile) and os.path.isfile(originalMd5) and os.path.isfile(encryptedMd5):
        # upload files
        if UploadMode == 'lftp':
            MyCmd = UploadCmd.format(UserName, MyPassword, StagePath, encryptedFile, encryptedMd5, originalMd5)
        elif UploadMode == 'aspera':
            MyCmd = UploadCmd.format(MyPassword, encryptedMd5, UserName, StagePath, originalMd5, encryptedFile)
        # put command in a shell script    
        BashScript = os.path.join(qsubdir, alias + '_' + fileName + '_upload.sh')
        newfile = open(BashScript, 'w')
        newfile.write(MyCmd + '\n')
        newfile.close()
        # launch job directly
        JobName = 'Upload.{0}'.format(alias + '__' + fileName)
        QsubCmd = "qsub -b y -q {0} -l h_vmem={1}g -N {2} -e {3} -o {3} \"bash {4}\"".format(Queue, Mem, JobName, logDir, BashScript)    
        job = subprocess.call(QsubCmd, shell=True)
        return job, JobName
    else:
        return '', ''
    

# use this function to upload the files
def UploadAnalysesObjects(CredentialFile, DataBase, Table, AttributesTable, Box, Queue, Mem, UploadMode, Max):
    '''
    (file, str, str, str, str, int, int, str) -> None
    Take the file with credentials to connect to the database and to EGA,
    and upload files of aliases with upload status using specified
    Queue, Memory and UploadMode and update status to uploading. 
    '''
    
    # check that Analysis table exists
    Tables = ListTables(CredentialFile, DataBase)
    if Table in Tables:
        
        # parse the crdential file, get username and password for given box
        UserName, MyPassword = ParseCredentials(CredentialFile, Box)
                       
        # connect to database
        conn = EstablishConnection(CredentialFile, DataBase)
        cur = conn.cursor()
        # extract files for alias in upload mode for given box
        cur.execute('SELECT {0}.alias, {0}.files, {0}.WorkingDirectory, {1}.StagePath FROM {0} JOIN {1} WHERE {0}.Status=\"upload\" AND {0}.egaBox=\"{2}\" AND {0}.attributes = {1}.alias'.format(Table, AttributesTable, Box))
        
        # check that some alias are in upload mode
        Data = cur.fetchall()
        # close connection
        conn.close()
        
        # count the number of files being uploaded
        Uploading = int(subprocess.check_output('qstat | grep Upload | wc -l', shell=True).decode('utf-8').rstrip())        
        # upload new files up to Max
        Maximum = int(Max) - Uploading
        Data = Data[: Maximum]
        
        if len(Data) != 0:
            for i in Data:
                # create dict {alias: {'files':files, 'StagePath':stagepath, 'FileDirectory':filedirectory}}
                D = {}
                alias = i[0]
                assert alias not in D
                files = i[1].replace("'", "\"")
                # get the working directory for that alias
                WorkingDir = GetWorkingDirectory(i[2])
                assert '/scratch2/groups/gsi/bis/EGA_Submissions' in WorkingDir
                D[alias] = {'files': json.loads(files), 'StagePath': i[3], 'FileDirectory': WorkingDir}
                
                # check stage folder, file directory
                assert len(list(D.keys())) == 1 and alias == list(D.keys())[0]
                # get the source and destination directories
                StagePath = D[alias]['StagePath']
                FileDir = D[alias]['FileDirectory']
                assert StagePath != '/'
                # store the job names in a list
                JobCodes, JobNames = [], []
                # get the files, check that the files are in the directory,
                # create stage directory if doesn't exist and upload
                for filePath in D[alias]['files']:
                    j, k = UploadAliasFiles(D, filePath, StagePath, FileDir, CredentialFile, Box, Queue, Mem, UploadMode)
                    # store job names and exit code
                    JobNames.append(k)
                    JobCodes.append(j)
                # check if upload launched properly for all files under that alias, update status -> uploading
                if len(set(JobCodes)) == 1 and list(set(JobCodes))[0] == 0:
                    # store the job names in errorMessages
                    JobNames = ';'.join(JobNames)
                    conn = EstablishConnection(CredentialFile, DataBase)
                    cur = conn.cursor()
                    cur.execute('UPDATE {0} SET {0}.Status=\"uploading\", {0}.JobNames=\"{1}\", {0}.errorMessages=\"None\"  WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\";'.format(Table, JobNames, alias, Box))
                    conn.commit()
                    conn.close()
                else:
                    # record error message and job names, keep status same upload --> upload
                    JobNames = ';'.join(JobNames)
                    Error = 'Could not launch upload jobs'
                    conn = EstablishConnection(CredentialFile, DataBase)
                    cur = conn.cursor()
                    cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\", {0}.JobNames=\"{2}\"  WHERE {0}.alias=\"{3}\" AND {0}.egaBox=\"{4}\"'.format(Table, Error, JobNames, alias, Box))
                    conn.commit()
                    conn.close()
                    
                    
# use this function to print a dictionary of directory
def ListFilesStagingServer(CredentialFile, DataBase, Table, AttributesTable, Box):
    '''
    (str, str, str, str, str, bool) -> dict
    Return a dictionary of directory: files on the EGA staging server under the 
    given Box for alias in DataBase Table with uploading status
    '''
        
    # parse credential file to get EGA username and password
    UserName, MyPassword = ParseCredentials(CredentialFile, Box)
      
    # make a dict {directory: [files]}
    FilesBox = {}
        
    # check that Analysis table exists
    Tables = ListTables(CredentialFile, DataBase)
    if Table in Tables:
        # connect to database
        conn = EstablishConnection(CredentialFile, DataBase)
        cur = conn.cursor()
        # extract files for alias in upload mode for given box
        cur.execute('SELECT {0}.alias, {1}.StagePath FROM {0} JOIN {1} WHERE {0}.attributes = {1}.alias AND {0}.Status=\"uploading\" AND {0}.egaBox=\"{2}\"'.format(Table, AttributesTable, Box))
        # check that some alias are in upload mode
        Data = cur.fetchall()
        # close connection
        conn.close()
        
        if len(Data) != 0:
            # make a list of stagepath
            StagePaths = list(set([i[1] for i in Data]))
            for i in StagePaths:
                uploaded_files = subprocess.check_output("ssh xfer4.res.oicr.on.ca 'lftp -u {0},{1} -e \"set ftp:ssl-allow false; ls {2}; bye;\" ftp://ftp-private.ebi.ac.uk'".format(UserName, MyPassword, i), shell=True).decode('utf-8').rstrip().split('\n')
                # get the file paths
                for j in range(len(uploaded_files)):
                    uploaded_files[j] = uploaded_files[j].split()[-1]
                # populate dict
                FilesBox[i] = uploaded_files
    return FilesBox
    
  
# use this function to check usage of working directory
def GetWorkDirSpace():
    '''
    () -> list
    Return a list with total size, used space and available space (all in Tb)
    for the working directory /scratch2/groups/gsi/bis/EGA_Submissions/
    '''
    
    # get total, free, and used space in working directory
    Usage = subprocess.check_output('df -h /scratch2/groups/gsi/bis/EGA_Submissions/', shell=True).decode('utf-8').rstrip().split()
    total, used, available = Usage[8], Usage[9], Usage[10]
    L = [total, used, available]
    for i in range(len(L)):
        if 'T' in L[i]:
            L[i] = float(L[i].replace('T', ''))
        elif 'K' in L[i]:
            L[i] = float(L[i].replace('K', '')) / 1000000000
        elif 'M' in L[i]:
            L[i] = float(L[i].replace('M', '')) / 1000000
        elif 'G' in L[i]:
            L[i] = float(L[i].replace('G', '')) / 1000
    return L
    
# use this function to check usage of a single file
def GetFileSize(FilePath):
    '''
    (str) -> float
    Return the size in Tb of FilePath
    '''
        
    filesize = subprocess.check_output('du -sh {0}'.format(FilePath), shell=True).decode('utf-8').rstrip().split()
    assert FilePath == filesize[1]
    # convert file size to Tb
    if 'T' in filesize[0]:
        filesize = float(filesize[0].replace('T', ''))
    elif 'K' in filesize[0]:
        filesize = filesize[0].replace('K', '')
        filesize = float(filesize) / 1000000000
    elif 'M' in filesize[0]:
        filesize = filesize[0].replace('M', '')
        filesize = float(filesize) / 1000000
    elif 'G' in filesize[0]:
        filesize = filesize[0].replace('G', '')
        filesize = float(filesize) / 1000
    return filesize


# use this function to compute disk usage of files in encrypt status
def CountFileUsage(CredentialFile, DataBase, Table, Box, Status):
    '''
    (str, str, str, str, int) -> dict
    Return a dictionary with the size of all files for a given alias for alias
    with encrypting status
    '''
        
    # create a dict {alias : file size}
    D = {}
        
    # check if Table exist
    Tables = ListTables(CredentialFile, DataBase)
    if Table in Tables:
        # connect to database
        conn = EstablishConnection(CredentialFile, DataBase)
        cur = conn.cursor()
        # pull alias and files for status = encrypt
        cur.execute('SELECT {0}.alias, {0}.files FROM {0} WHERE {0}.Status=\"{1}\" AND {0}.egaBox=\"{2}\"'.format(Table, Status, Box))
        Data = cur.fetchall()
        conn.close()
        
        # check that some files are in encrypt mode
        if len(Data) != 0:
            for i in Data:
                assert i[0] not in D
                # convert single quotes to double quotes for str -> json conversion
                files = json.loads(i[1].replace("'", "\""))
                # make a list to record file sizes of all files under the given alias
                filesize = []
                # loop over filepath:
                for j in files:
                    # get the file size
                    filesize.append(GetFileSize(files[j]['filePath']))
                D[i[0]] = sum(filesize)
    return D            


# use this function to select alias to encrypt based on disk usage
def SelectAliasesForEncryption(CredentialFile, DataBase, Table, Box, DiskSpace):
    '''
    (str, str, str, str) -> list
    Connect to submission DataBase with file credentials, extract alias with encrypt
    status and return a list of aliases with files that can be encrypted while 
    keeping DiskSpace (in TB) of free space in scratch
    '''
        
    # get disk space of working directory
    total, used, available = GetWorkDirSpace()
        
    # get file size of all files under each alias with encrypt status
    Encrypt = CountFileUsage(CredentialFile, DataBase, Table, Box, 'encrypt')
    # get file size of all files under each alias with encrypting status
    Encrypting = CountFileUsage(CredentialFile, DataBase, Table, Box, 'encrypting')
    
    # set the file size at the current usage
    FileSize = used
    # add file size for all aliases with encrypting status
    for alias in Encrypting:
        FileSize += Encrypting[alias]
        
    # record aliases for encryption
    Aliases = []
    for alias in Encrypt:
        # do not encrypt if the new files result is < 15Tb of disk availability 
        if available - (FileSize + Encrypt[alias]) > DiskSpace:
            FileSize += Encrypt[alias]
            Aliases.append(alias)
    return Aliases


# use this function to check the success of the upload
def IsUploadSuccessfull(LogFile):
    '''
    (str) --> bool
    Read the log of the upload script and return True if 3 lines with 'Completed'
    are in the log (ie. successfull upload of 2 md5sums and 1 .gpg), and return
    False otherwise
    Pre-condition: this log output is for aspera upload    
    '''
    
    infile = open(LogFile)
    content = infile.read()
    infile.close()
    
    # 3 'Completed' if successful upload (2 md5sums and 1 encrypted are uploaded together) 
    if content.count('Completed') == 3:
        return True
    else:
        return False

# use this function to check the success of the upload
def CheckUploadSuccess(LogDir, alias, FileName):
    '''
    (str) --> bool
    Take the directory where logs of the upload script are saved, retrieve the
    most recent out log and return True if all files are uploaded (ie no error)
    or False if errors are found
    '''

    # sort the out log files from the most recent to the older ones
    logfiles = subprocess.check_output('ls -lt {0}'.format(os.path.join(LogDir, 'Upload.*.o*')), shell=True).decode('utf-8').rstrip().split('\n')
    # keep the log out file names
    for i in range(len(logfiles)):
        logfiles[i] = logfiles[i].strip().split()[-1]
    
    # set up a boolean to update if most recent out log is found for FileName
    Found = False
    
    # loop over the out log
    for filepath in logfiles:
        # extract logname and split to get alias and file name
        logname = os.path.basename(filepath).split('__') 
        if alias == logname[0].replace('Upload.', '') and FileName == logname[1][:logname[1].rfind('.o')]:
            # update boolean and exit
            Found = True
            break   
    
    # check if most recent out log is found
    if Found == True:
        # check that log file exists
        if os.path.isfile(filepath):
            # check if upload was successful
            return IsUploadSuccessfull(filepath)
        else:
            return False
    else:
        return False
    
    
# use this function to check that files were successfully uploaded and update status uploading -> uploaded
def CheckUploadFiles(CredentialFile, DataBase, Table, AttributesTable, Box):
    '''
    (str, str, str, str, str) -> None
    Take the file with db credentials, the table names and box for the Database
    and update status of all alias from uploading to uploaded if all the files
    for that alias were successfuly uploaded. 
    '''

    # parse credential file to get EGA username and password
    UserName, MyPassword = ParseCredentials(CredentialFile, Box)
        
    # check that Analysis table exists
    Tables = ListTables(CredentialFile, DataBase)
    if Table in Tables and AttributesTable in Tables:
        
        # make a dict {directory: [files]} for alias with uploading status 
        FilesBox = ListFilesStagingServer(CredentialFile, DataBase, Table, AttributesTable, Box)
        
        # connect to database
        conn = EstablishConnection(CredentialFile, DataBase)
        cur = conn.cursor()
        # extract files for alias in upload mode for given box
        cur.execute('SELECT {0}.alias, {0}.files, {0}.JobNames, {0}.WorkingDirectory, {1}.StagePath FROM {0} JOIN {1} WHERE {0}.attributes = {1}.alias AND {0}.Status=\"uploading\" AND {0}.egaBox=\"{2}\"'.format(Table, AttributesTable, Box))
        # check that some alias are in upload mode
        Data = cur.fetchall()
        # close connection
        conn.close()
        
        if len(Data) != 0:
            # check that some files are in uploading mode
            for i in Data:
                alias = i[0]
                # convert single quotes to double quotes for str -> json conversion
                files = json.loads(i[1].replace("'", "\""))
                WorkingDirectory = GetWorkingDirectory(i[3])
                StagePath = i[4]
                jobNames = i[2]
                # set up boolean to be updated if uploading is not complete
                Uploaded = True
                # Check that jobs are not running
                StillRunning = [CheckRunningJob(JobName) for JobName in jobNames.split(';')]
                if True not in StillRunning:
                    # get the log directory
                    LogDir = os.path.join(WorkingDirectory, 'qsubs/log')
                    # check the out logs for each file
                    for filePath in files:
                        # get filename
                        filename = os.path.basename(filePath)
                        # check if errors are found in log
                        if CheckUploadSuccess(LogDir, alias, filename) == False:
                            Uploaded = False
                
                    # check if files are uploaded on the server
                    for filePath in files:
                        # get filename
                        fileName = os.path.basename(filePath)
                        assert fileName + '.gpg' == files[filePath]['encryptedName']
                        encryptedFile = files[filePath]['encryptedName']
                        originalMd5, encryptedMd5 = fileName + '.md5', fileName + '.gpg.md5'                    
                        for j in [encryptedFile, encryptedMd5, originalMd5]:
                            if j not in FilesBox[StagePath]:
                                Uploaded = False
                    # check if all files for that alias have been uploaded
                    if Uploaded == True:
                        # connect to database, update status and close connection
                        conn = EstablishConnection(CredentialFile, DataBase)
                        cur = conn.cursor()
                        cur.execute('UPDATE {0} SET {0}.Status=\"uploaded\", {0}.errorMessages=\"None\" WHERE {0}.alias=\"{1}\" AND {0}.egaBox=\"{2}\"'.format(Table, alias, Box)) 
                        conn.commit()                                
                        conn.close()              
                    elif Uploaded == False:
                        # reset status uploading --> upload, record error message
                        Error = 'Upload failed'
                        conn = EstablishConnection(CredentialFile, DataBase)
                        cur = conn.cursor()
                        cur.execute('UPDATE {0} SET {0}.Status=\"upload\", {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, Error, alias, Box)) 
                        conn.commit()                                
                        conn.close()
                   
# use this function to format the error Messages prior saving into db table
def CleanUpError(errorMessages):
    '''
    (str or list or None) -> str
    Take the errorMessages from the api json and format it to be added as
    a string in the database table
    '''
    # check how error Messages is returned from the api 
    if type(errorMessages) == list:
        if len(errorMessages) == 1:
            # get the string message
            errorMessages = errorMessages[0]
        elif len(errorMessages) > 1:
            # combine the messages as single string
            errorMessages = ':'.join(errorMessages)
        elif len(errorMessages) == 0:
            errorMessages = 'None'
    else:
        errorMessages = str(errorMessages)
    # remove double quotes to save in table
    errorMessages = str(errorMessages).replace("\"", "")
    return errorMessages



# use this function to remove encrypted and md5 files
def RemoveFilesAfterSubmission(CredentialFile, Database, Table, Box, Remove):
    '''
    (str, str, str, str, str, str, bool) -> None
    Connect to Database using CredentialFile, extract path of the encrypted amd md5sum
    files corresponding to the given Alias and Box in Table and delete them
    '''
    
    if Remove == True:
        # connect to database
        conn = EstablishConnection(CredentialFile, Database)
        cur = conn.cursor()
        # get the directory, files for all alias with SUBMITTED status
        cur.execute('SELECT {0}.alias, {0}.files, {0}.WorkingDirectory FROM {0} WHERE {0}.status=\"uploaded\" AND {0}.egaBox=\"{1}\"'.format(Table, Box))
        Data = cur.fetchall()
        conn.close()
        if len(Data) != 0:
            for i in Data:
                alias, files = i[0], json.loads(str(i[1]).replace("'", "\""))
                # get the working directory for that alias
                WorkingDir = GetWorkingDirectory(i[2])
                files = [os.path.join(WorkingDir, files[i]['encryptedName']) for i in files]
                for i in files:
                    assert i[-4:] == '.gpg'
                    a, b = i + '.md5', i.replace('.gpg', '') + '.md5'
                    if os.path.isfile(i) and '/scratch2/groups/gsi/bis/EGA_Submissions' in i and '.gpg' in i:
                        # remove encrypted file
                        os.system('rm {0}'.format(i))
                    if os.path.isfile(a) and '/scratch2/groups/gsi/bis/EGA_Submissions' in a and '.md5' in a:
                        # remove md5sum
                        os.system('rm {0}'.format(a))
                    if os.path.isfile(b) and '/scratch2/groups/gsi/bis/EGA_Submissions' in b and '.md5' in b:
                        # remove md5sum
                        os.system('rm {0}'.format(b))

# use this function to register objects
def RegisterObjects(CredentialFile, DataBase, Table, Box, Object, Portal):
    '''
    (file, str, str, str, str, str) -> None
    Take the file with credentials to connect to the submission database, 
    extract the json for each Object in Table and register the objects
    in EGA BOX using the submission Portal. 
    '''
    
    # pull json for objects with ready Status for given box
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
    cur.execute('SELECT {0}.Json FROM {0} WHERE {0}.Status=\"submit\" AND {0}.egaBox=\"{1}\"'.format(Table, Box))
    conn.close()
    
    # extract all information 
    Data = cur.fetchall()
    # check that objects in submit mode do exist
    if len(Data) != 0:
        # make a list of jsons
        L = [json.loads(i[0].replace("'", "\"")) for i in Data]
        assert len(L) == len(Data)

        # connect to EGA and get a token
        # parse credentials to get userName and Password
        UserName, MyPassword = ParseCredentials(CredentialFile, Box)
                    
        # get the token
        data = {"username": UserName, "password": MyPassword, "loginType": "submitter"}
        # get the adress of the submission portal
        if Portal[-1] == '/':
            URL = Portal[:-1]
        else:
            URL = Portal
        Login = requests.post(URL + '/login', data=data)
        # check that response code is OK
        if Login.status_code == requests.codes.ok:
            # response is OK, get Token
            Token = Login.json()['response']['result'][0]['session']['sessionToken']
            
            # open a submission for each object
            for J in L:
                headers = {"Content-type": "application/json", "X-Token": Token}
                submissionJson = {"title": "{0} submission", "description": "opening a submission for {0} {1}".format(Object, J["alias"])}
                OpenSubmission = requests.post(URL + '/submissions', headers=headers, data=str(submissionJson).replace("'", "\""))
                # check if submission is successfully open
                if OpenSubmission.status_code == requests.codes.ok:
                    # get submission Id
                    submissionId = OpenSubmission.json()['response']['result'][0]['id']
                    # create object
                    ObjectCreation = requests.post(URL + '/submissions/{0}/{1}'.format(submissionId, Object), headers=headers, data=str(J).replace("'", "\""))
                    # check response code
                    if ObjectCreation.status_code == requests.codes.ok:
                        # validate, get status (VALIDATED or VALITED_WITH_ERRORS) 
                        ObjectId = ObjectCreation.json()['response']['result'][0]['id']
                        submissionStatus = ObjectCreation.json()['response']['result'][0]['status']
                        assert submissionStatus == 'DRAFT'
                        # store submission json and status in db table
                        conn = EstablishConnection(CredentialFile, DataBase)
                        cur = conn.cursor()
                        cur.execute('UPDATE {0} SET {0}.submissionStatus=\"{1}\" WHERE {0}.alias="\{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, submissionStatus, J["alias"], Box))
                        conn.commit()
                        conn.close()
                        # validate object
                        ObjectValidation = requests.put(URL + '/{0}/{1}?action=VALIDATE'.format(Object, ObjectId), headers=headers)
                        # check code and validation status
                        if ObjectValidation.status_code == requests.codes.ok:
                            # get object status
                            ObjectStatus=ObjectValidation.json()['response']['result'][0]['status']
                            # record error messages
                            errorMessages = CleanUpError(ObjectValidation.json()['response']['result'][0]['validationErrorMessages'])
                            # store error message and status in db table
                            conn = EstablishConnection(CredentialFile, DataBase)
                            cur = conn.cursor()
                            cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\", {0}.submissionStatus=\"{2}\" WHERE {0}.alias="\{3}\" AND {0}.egaBox=\"{4}\"'.format(Table, str(errorMessages), ObjectStatus, J["alias"], Box))
                            conn.commit()
                            conn.close()
                            
                            # check if object is validated
                            if ObjectStatus == 'VALIDATED':
                                # submit object
                                ObjectSubmission = requests.put(URL + '/{0}/{1}?action=SUBMIT'.format(Object, ObjectId), headers=headers)
                                # check if successfully submitted
                                if ObjectSubmission.status_code == requests.codes.ok:
                                    # record error messages
                                    errorMessages = CleanUpError(ObjectValidation.json()['response']['result'][0]['submissionErrorMessages'])
                                    # store submission json and status in db table
                                    conn = EstablishConnection(CredentialFile, DataBase)
                                    cur = conn.cursor()
                                    cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\" WHERE {0}.alias="\{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, errorMessages, J["alias"], Box))
                                    conn.commit()
                                    conn.close()
                                    
                                    # check status
                                    ObjectStatus = ObjectSubmission.json()['response']['result'][0]['status']
                                    if ObjectStatus == 'SUBMITTED':
                                        # get the receipt, and the accession id
                                        Receipt, egaAccessionId = str(ObjectSubmission.json()).replace("\"", ""), ObjectSubmission.json()['response']['result'][0]['egaAccessionId']
                                        # store the date it was submitted
                                        Time = time.strftime('%Y-%m-%d', time.localtime(time.time()))
                                        # add Receipt, accession and time to table and change status
                                        conn = EstablishConnection(CredentialFile, DataBase)
                                        cur = conn.cursor()
                                        cur.execute('UPDATE {0} SET {0}.Receipt=\"{1}\", {0}.egaAccessionId=\"{2}\", {0}.Status=\"{3}\", {0}.submissionStatus=\"{3}\", {0}.CreationTime=\"{4}\" WHERE {0}.alias=\"{5}\" AND {0}.egaBox=\"{6}\"'.format(Table, Receipt, egaAccessionId, ObjectStatus, Time, J["alias"], Box))
                                        conn.commit()
                                        conn.close()
                                    else:
                                        # delete sample
                                        ObjectDeletion = requests.delete(URL + '/{0}/{1}'.format(Object, ObjectId), headers=headers)
                            else:
                                #delete sample
                                ObjectDeletion = requests.delete(URL + '/{0}/{1}'.format(Object, ObjectId), headers=headers)
            # disconnect by removing token
            response = requests.delete(URL + '/logout', headers={"X-Token": Token})     


# use this function to check information in Tables    
def IsInfoValid(CredentialFile, DataBase, Table, AttributesTable, ProjectsTable, Box, datatype):
    '''
    (str, str, str, str. str, str) -> dict
    Extract information from DataBase Table, AttributesTable and ProjectsTable
    using credentials in file, check if information is valid and return a dict
    with error message for each alias in Table
    '''

    # create a dictionary {alias: error}
    D = {}

    # list tables 
    Tables = ListTables(CredentialFile, DataBase)

    # get the enumerations
    URLs =  ['https://ega-archive.org/submission-api/v1/enums/analysis_file_types',
             'https://ega-archive.org/submission-api/v1/enums/experiment_types',
             'https://ega-archive.org/submission-api/v1/enums/analysis_types']
    MyPython = '/.mounts/labs/PDE/Modules/sw/python/Python-3.6.4/bin/python3.6'
    MyScript = '/.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/dev/SubmissionDB/SubmitToEGA.py'
    
    Enums = [json.loads(subprocess.check_output('ssh xfer4 \"{0} {1} Enums --URL {2}\"'.format(MyPython, MyScript, URL), shell=True).decode('utf-8').rstrip().replace("'", "\"")) for URL in URLs]
        
    
    ##### continue here
    
    
    
    GrabEgaEnums('https://ega-archive.org/submission-api/v1/enums/analysis_file_types')
    ExperimentTypes = GrabEgaEnums('https://ega-archive.org/submission-api/v1/enums/experiment_types')
    AnalysisTypes =  GrabEgaEnums('https://ega-archive.org/submission-api/v1/enums/analysis_types')

    # connect to db
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()      
    # get required information
    if datatype == 'analyses':
        if Table in Tables:
            cur.execute('SELECT {0}.alias, {0}.sampleAlias, {0}.files, {0}.egaBox, \
                        {0}.attributes, {0}.projects FROM {0} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{1}\"'.format(Table, Box))
    elif datatype == 'attributes':
        if Table in Tables and AttributesTable in Tables:
            cur.execute('SELECT {0}.alias, {1}.title, {1}.description, {1}.attributes, {1}.genomeId, {1}.StagePath \
                        FROM {0} JOIN {1} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{2}\" AND {0}.attributes={1}.alias'.format(Table, AttributesTable, Box))
    elif datatype == 'projects':
        if Table in Tables and ProjectsTable in Tables:
            cur.execute('SELECT {0}.alias, {1}.studyId, {1}.analysisCenter, {1}.Broker, {1}.analysisTypeId, {1}.experimentTypeId \
                        FROM {0} JOIN {1} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{2}\" AND {0}.projects={1}.alias'.format(Table, ProjectsTable, Box))
    Data = cur.fetchall()
    conn.close()
    if len(Data) != 0:
        if datatype == 'analyses':
            Keys = ['alias', 'sampleAlias', 'files', 'egaBox', 'attributes', 'projects']
            Required = ['alias', 'sampleAlias', 'files', 'egaBox', 'attributes', 'projects']
        elif datatype == 'attributes':
            Keys = ['alias', 'title', 'description', 'attributes', 'genomeId', 'StagePath']        
            Required = ['title', 'description', 'genomeId', 'StagePath']
        elif datatype == 'projects':
            Keys = ['alias', 'studyId', 'analysisCenter', 'Broker', 'analysisTypeId', 'experimentTypeId']
            Required = ['studyId', 'analysisCenter', 'Broker', 'analysisTypeId', 'experimentTypeId']
            
        for i in range(len(Data)):
            # set up boolean. update if missing values
            Missing = False
            # create a dict with all information
            d = {Keys[j]: Data[i][j] for j in range(len(Keys))}
            # create an error message
            Error = []
            # check if information is valid
            for key in Keys:
                if key in Required:
                    if d[key] in ['', 'NULL']:
                        Missing = True
                        Error.append(key)
                # check valid boxes. currently only 2 valid boxes ega-box-12 and ega-box-137
                if key == 'egaBox':
                    if d['egaBox'] not in ['ega-box-12', 'ega-box-137']:
                        Missing = True
                        Error.append(key)
                # check files
                if key == 'files':
                    files = json.loads(d['files'].replace("'", "\""))
                    for filePath in files:
                        # check if file is valid
                        if os.path.isfile(filePath) == False:
                            Missing = True
                            Error.append('files')
                        # check validity of file type
                        if files[filePath]['fileTypeId'].lower() not in FileTypes:
                            Missing = True
                            Error.append('fileTypeId')
                # check study Id
                if key == 'studyId':
                    if 'EGAS' not in d[key]:
                        Missing = True
                        Error.append(key)
                # check enumerations
                if key == 'experimentTypeId':
                    if d['experimentTypeId'] not in ExperimentTypes:
                        Missing = True
                        Error.append(key)
                if key == 'analysisTypeId':
                    if d['analysisTypeId'] not in AnalysisTypes:
                        Missing = True
                        Error.append(key)
                # check attributes of attributes table
                if key == 'attributes' and datatype == 'attributes':
                    if d['attributes'] not in ['', 'NULL']:
                        # check format of attributes
                        attributes = [json.loads(j.replace("'", "\"")) for j in d['attributes'].split(';')]
                        for k in attributes:
                            # do not allow keys other than tag, unit and value
                            if set(k.keys()).union({'tag', 'value', 'unit'}) != {'tag', 'value', 'unit'}:
                                Missing = True
                                Error.append(key)
                            # tag and value are required keys
                            if 'tag' not in k.keys() and 'value' not in k.keys():
                                Missing = True
                                Error.append(key)

            # check if object has missing/non-valid information
            if Missing == True:
                 # record error message and update status ready --> dead
                 Error = 'In {0} table, '.format(Table) + 'invalid fields:' + ';'.join(list(set(Error)))
            elif Missing == False:
                Error = 'None'
            assert d['alias'] not in D
            D[d['alias']] = Error
    return D



# use this function to check that all information for Analyses objects is available before encrypting files     
def CheckTableInformation(CredentialFile, DataBase, Table, ProjectsTable, AttributesTable, Box):
    '''
    (str, str, str, str, str, str) -> None
    Extract information from DataBase Table, ProjectsTable and AttributesTable
    using credentials in file and update status to "valid" if all information is correct
    or keep status to "ready" if incorrect or missing
    '''

    # create dict {alias: [errors]}
    K = {}
    # connect to db
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()      
    cur.execute('SELECT {0}.alias FROM {0} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{1}\"'.format(Table, Box))
    Data = cur.fetchall()
    conn.close()
    if len(Data) != 0:
        for i in Data:
            K[i[0]] = []
    
        # get error messages for the different tables. create dicts {alias" error}
        D = IsInfoValid(CredentialFile, DataBase, Table, AttributesTable, ProjectsTable, Box, 'analyses')
        E = IsInfoValid(CredentialFile, DataBase, Table, AttributesTable, ProjectsTable, Box, 'attributes')
        F = IsInfoValid(CredentialFile, DataBase, Table, AttributesTable, ProjectsTable, Box, 'projects')

        # merge dicts
        for alias in K:
            if alias in D:
                K[alias].append(D[alias])
            else:
                K[alias].append('In {0} table, no information'.format(Table))
            if alias in E:
                K[alias].append(E[alias])
            else:
                K[alias].append('In {0} table, no information'.format(AttributesTable))
            if alias in F:
                K[alias].append(F[alias])
            else:
                K[alias].append('In {0} table, no information'.format(ProjectsTable))
            
        # connect to database
        conn = EstablishConnection(CredentialFile, DataBase)
        cur = conn.cursor()      
        # update status and record errorMessage
        if len(K) != 0:
            for alias in K:
                # check if error message
                if len(list(set(K[alias]))) == 1:
                    if list(set(K[alias]))[0] == 'None':
                        # record error message, update status ready --> valid
                        cur.execute('UPDATE {0} SET {0}.Status=\"valid\", {0}.errorMessages=\"None\" WHERE {0}.alias=\"{1}\" AND {0}.egaBox=\"{2}\"'.format(Table, alias, Box))
                        conn.commit()
                elif len(list(set(K[alias]))) == 0:
                    Error = ['In {0} table, no information'.format(Table), 'In {0} table, no information'.format(AttributesTable), 'In {0} table, no information'.format(ProjectsTable)]
                    # record error message, update status ready --> valid
                    cur.execute('UPDATE {0} SET {0}.Status=\"valid\", {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, '|'.join(Error), alias, Box))
                    conn.commit()
                else:
                    # record errorMessage and keep status ready --> ready
                    cur.execute('UPDATE {0} {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, '|'.join(K[alias]), alias, Box))
                    conn.commit()
        conn.close()

  
            
# use this function to add data to the sample table
def AddSampleInfo(args):
    '''
    (list) -> None
    Take a list of command line arguments and add sample information
    to the Sample Table of the EGAsub database if samples are not already registered
    '''
    
    # pull down sample alias and egaId from metadata db, alias should be unique
    # create a dict {alias: accession} 
    Registered = ExtractAccessions(args.credential, args.metadatadb, args.box, args.table)
            
    # parse input table [{sample: {key:value}}] 
    Data = ParseSampleInputTable(args.input)

    # create table if table doesn't exist
    Tables = ListTables(args.credential, args.subdb)

    # connect to submission database
    conn = EstablishConnection(args.credential, args.subdb)
    cur = conn.cursor()    

    if args.table not in Tables:
        Fields = ['alias', 'subjectId', 'title', 'description', 'caseOrControlId',  
                  'gender', 'organismPart', 'cellLine', 'region', 'phenotype',
                  'anonymizedName', 'biosampleId', 'sampleAge',
                  'sampleDetail', 'attributes', 'Species', 'Taxon',
                  'ScientificName', 'SampleTitle', 'Center', 'RunCenter',
                  'StudyId', 'ProjectId', 'StudyTitle', 'StudyDesign', 'Broker',
                  'Json', 'submissionJson', 'submissionStatus',
                  'Receipt', 'CreationTime', 'egaAccessionId', 'egaBox', 'Status']
        # format colums with datatype
        Columns = []
        for i in range(len(Fields)):
            if Fields[i] == 'Status':
                Columns.append(Fields[i] + ' TEXT NULL')    
            elif Fields[i] == 'Json' or Fields[i] == 'Receipt':
                Columns.append(Fields[i] + ' MEDIUMTEXT NULL,')
            else:
                Columns.append(Fields[i] + ' TEXT NULL,')
        # convert list to string    
        Columns = ' '.join(Columns)        
        # create table with column headers
        cur = conn.cursor()
        cur.execute('CREATE TABLE {0} ({1})'.format(args.table, Columns))
        conn.commit()
    else:
        # get the column headers from the table
        cur.execute("SELECT * FROM {0}".format(args.table))
        Fields = [i[0] for i in cur.description]
    
    # create a string with column headers
    ColumnNames = ', '.join(Fields)
        
    # pull down sample alias from submission db. alias may be recorded but not submitted yet. aliases must be unique and not already recorded in the same box
    # create a dict {alias: accession}
    cur.execute('SELECT {0}.alias from {0} WHERE {0}.egaBox=\"{1}\"'.format(args.table, args.box))
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

# use this function to add data to AnalysesAttributes or AnalysesProjects table
def AddAnalysesAttributesProjects(args):
    '''
    (list) -> None
    Take a list of command line arguments and add attributes information
    to the AnalysesAttributes or AnalysesProjects Table of the EGASUBsub database
    if alias not already present
    '''

    # parse attribues input table
    D = ParseAnalysesAccessoryTables(args.input, args.datatype)
    
    # create table if table doesn't exist
    Tables = ListTables(args.credential, args.subdb)

    # connect to submission database
    conn = EstablishConnection(args.credential, args.subdb)
    cur = conn.cursor()
    
    if args.table not in Tables:
        if args.datatype == 'Attributes':
            Fields = ["alias", "title", "description", "genomeId", "attributes", "StagePath", "platform", "chromosomeReferences"]
        elif args.datatype == 'Projects':
            Fields = ['alias', 'analysisCenter', 'studyId', 'Broker', 'analysisTypeId',
                    'experimentTypeId', 'ProjectId', 'StudyTitle', 'StudyDesign'] 
        # format colums with datatype
        Columns = []
        for i in range(len(Fields)):
            if Fields[i] == 'chromosomeReferences' or Fields[i] == 'StudyDesign':
                Columns.append(Fields[i] + ' MEDIUMTEXT NULL')
            elif Fields[i] == 'StagePath':
                Columns.append(Fields[i] + ' MEDIUMTEXT NOT NULL,')
            elif Fields[i] == "alias":
                Columns.append(Fields[i] + ' VARCHAR(100) PRIMARY KEY UNIQUE,')
            else:
                Columns.append(Fields[i] + ' TEXT NULL,')
        # convert list to string    
        Columns = ' '.join(Columns)       
        # create table with column headers
        cur = conn.cursor()
        cur.execute('CREATE TABLE {0} ({1})'.format(args.table, Columns))
        conn.commit()
    else:
        # get the column headers from the table
        cur.execute("SELECT * FROM {0}".format(args.table))
        Fields = [i[0] for i in cur.description]
    
    # create a string with column headers
    ColumnNames = ', '.join(Fields)
    
    # pull down alias from submission db. alias must be unique
    cur.execute('SELECT {0}.alias from {0}'.format(args.table))
    Recorded = [i[0] for i in cur]
    
    # record objects only if input table has been provided with required fields
    if args.datatype == 'Attributes':
        RequiredFields = {"alias", "title", "description", "genomeId", "StagePath"}
    elif args.datatype == 'Projects':
        RequiredFields = {'alias', 'analysisCenter', 'studyId', 'Broker', 'analysisTypeId', 'experimentTypeId'}
    if RequiredFields.intersection(set(D.keys())) == RequiredFields:
        # get alias
        if D['alias'] in Recorded:
            # skip analysis, already recorded in submission database
            print('{0} is already recorded for in {1}'.format(D['alias'], args.table))
        else:
            # format attributes if present
            if 'attributes' in D:
                # format attributes
                attributes = [D['attributes'][j] for j in D['attributes']]
                attributes = ';'.join(list(map(lambda x: str(x), attributes))).replace("'", "\"")
                D['attributes'] = attributes
            # list values according to the table column order, use empty string if not present
            L = [D[field] if field in D else '' for field in Fields]
            # convert data to strings, converting missing values to NULL                    L
            Values = FormatData(L)        
            cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(args.table, ColumnNames, Values))
            conn.commit()
    conn.close()            


# use this function to add data to the analysis table
def AddAnalysesInfo(args):
    '''
    (list) -> None
    Take a list of command line arguments and add analysis information
    to the Analysis Table of the EGAsub database if files are not already registered
    '''
    
    # pull down analysis alias and egaId from metadata db, alias should be unique
    # create a dict {alias: accessions}
    Registered = ExtractAccessions(args.credential, args.metadatadb, args.box, args.table)
            
    # parse input table [{alias: {'sampleAlias':[sampleAlias], 'files': {filePath: {'filePath': filePath, 'fileName': fileName}}}}]
    Data = ParseAnalysisInputTable(args.input)

    # create table if table doesn't exist
    Tables = ListTables(args.credential, args.subdb)
    
    # connect to submission database
    conn = EstablishConnection(args.credential, args.subdb)
    cur = conn.cursor()
    
    if args.table not in Tables:
        Fields = ["alias", "sampleAlias", "sampleEgaAccessionsId", "analysisDate",
                  "files", "WorkingDirectory", "Json", "submissionStatus", "errorMessages", "Receipt",
                  "CreationTime", "egaAccessionId", "egaBox", "projects",
                  "attributes", "Status"]
        # format colums with datatype
        Columns = []
        for i in range(len(Fields)):
            if Fields[i] == 'Status':
                Columns.append(Fields[i] + ' TEXT NULL')
            elif Fields[i] in ['Json', 'Receipt', 'files']:
                Columns.append(Fields[i] + ' MEDIUMTEXT NULL,')
            elif Fields[i] == 'alias':
                Columns.append(Fields[i] + ' VARCHAR(100) PRIMARY KEY UNIQUE,')
            else:
                Columns.append(Fields[i] + ' TEXT NULL,')
        # convert list to string    
        Columns = ' '.join(Columns)        
        # create table with column headers
        cur = conn.cursor()
        cur.execute('CREATE TABLE {0} ({1})'.format(args.table, Columns))
        conn.commit()
    else:
        # get the column headers from the table
        cur.execute("SELECT * FROM {0}".format(args.table))
        Fields = [i[0] for i in cur.description]
    
    # create a string with column headers
    ColumnNames = ', '.join(Fields)
    
    # pull down analysis alias from submission db. alias may be recorded but not submitted yet. aliases must be unique and not already recorded in the same box
    # create a dict {alias: accession}
    cur.execute('SELECT {0}.alias from {0} WHERE {0}.egaBox=\"{1}\"'.format(args.table, args.box))
    Recorded = [i[0] for i in cur]
    
    # record objects only if input table has been provided with required fields
    if len(Data) != 0:
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
                D[alias]['projects'], D[alias]['attributes'], D[alias]['egaBox'] = args.projects, args.attributes, args.box 
                # check if analysisDate is provided in input table
                if 'analysisDate' not in D[alias]:
                    D[alias]['analysisDate'] = ''
                # add fileTypeId to each file
                for filePath in D[alias]['files']:
                    fileTypeId = ''
                    fileTypeId = filePath[-3:]
                    assert fileTypeId in ['bam', 'bai', 'vcf'], 'valid file extensions are bam, vcf and bai'
                    # check that file type Id is also in the filename
                    assert D[alias]['files'][filePath]['fileName'][-3:] == fileTypeId, '{0} should be part of the file name'.format(fileTypeId)
                    # add fileTypeId to dict
                    assert 'fileTypeId' not in D[alias]['files'][filePath] 
                    D[alias]['files'][filePath]['fileTypeId'] = fileTypeId
                # check if multiple sample alias are used. store sampleAlias as string
                sampleAlias = list(set(D[alias]['sampleAlias']))
                if len(sampleAlias) == 1:
                    # only 1 sampleAlias is used
                    sampleAlias = sampleAlias[0]
                else:
                    # multiple sample aliases are used
                    sampleAlias = ':'.join(sampleAlias)
                D[alias]['sampleAlias'] = sampleAlias    
                # set Status to ready
                D[alias]["Status"] = "ready"
                # list values according to the table column order
                L = [D[alias][field] if field in D[alias] else '' for field in Fields]
                # convert data to strings, converting missing values to NULL                    L
                Values = FormatData(L)        
                cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(args.table, ColumnNames, Values))
                conn.commit()
    conn.close()            



# use this function to submit Sample objects
def SubmitSamples(args):
    
    '''
    (list) -> None
    Take a list of command line arguments and submit samples to EGA following
    sequential steps that depend on the sample status mode
    '''
   
    # workflow for submitting samples:
    # add sample info to sample table -> set status to ready
    # form json for samples in ready mode and store in table -> set status to submit
   
      
    # check if Sample table exists
    Tables = ListTables(args.credential, args.database)
    
    if args.table in Tables:
        
        ## form json for samples in ready mode, add to table and update status -> submit
        AddSampleJsonToTable(args.credential, args.database, args.table, args.box)
        ## submit samples with submit status                
        RegisterObjects(args.credential, args.database, args.table, args.box, 'samples', args.portal)


# use this function to submit Analyses objects
def SubmitAnalyses(args):
    '''
    (list) -> None
    Take a list of command line arguments and encrypt, upload and register analysis
    objects to EGA following sequential steps that depend on the analysis status mode
    '''

    # check if Analyses table exists
    Tables = ListTables(args.credential, args.subdb)
    if args.table in Tables:
        
        ## check if required information is present in tables.
        # change status ready --> valid if no error or keep status ready --> ready and record errorMessage
        #CheckTableInformation(args.credential, args.subdb, args.table, args.projects, args.attributes, args.box)
        
        ## set up working directory, add to analyses table and update status valid --> start
        #AddWorkingDirectory(args.credential, args.subdb, args.table, args.box)
        
        ## update Analysis table in submission database with sample accessions and change status start -> encrypt
        #AddSampleAccessions(args.credential, args.metadatadb, args.subdb, args.box, args.table)

        ## encrypt new files only if diskspace is available. update status encrypt --> encrypting
        #EncryptFiles(args.credential, args.subdb, args.table, args.box, args.keyring, args.queue, args.memory, args.diskspace)
        
        ## check that encryption is done, store md5sums and path to encrypted file in db, update status encrypting -> upload 
        #CheckEncryption(args.credential, args.subdb, args.table, args.box)
        
        ## upload files and change the status upload -> uploading 
        UploadAnalysesObjects(args.credential, args.subdb, args.table, args.attributes, args.box, args.queue, args.memory, args.uploadmode, args.max)
                
        ## check that files have been successfully uploaded, update status uploading -> uploaded
        #CheckUploadFiles(args.credential, args.subdb, args.table, args.attributes, args.box)
        
        ## remove files with uploaded status
        #RemoveFilesAfterSubmission(args.credential, args.subdb, args.table, args.box, args.remove)
               
        ## form json for analyses in uploaded mode, add to table and update status uploaded -> submit
        #AddAnalysisJsonToTable(args.credential, args.subdb, args.table, args.attributes, args.projects, args.box)
        
        ## submit analyses with submit status                
        #RegisterObjects(args.credential, args.subdb, args.table, args.box, 'analyses', args.portal)

        
    
if __name__ == '__main__':

    # create top-level parser
    parser = argparse.ArgumentParser(prog = 'SubmitToEGA.py', description='manages submission to EGA')
    subparsers = parser.add_subparsers(title='sub-commands', description='valid sub-commands', help = 'sub-commands help')

    # add samples to Samples Table
    AddSamples = subparsers.add_parser('AddSamples', help ='Add sample information')
    AddSamples.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    AddSamples.add_argument('-t', '--Table', dest='table', default='Samples', help='Samples table. Default is Samples')
    AddSamples.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
    AddSamples.add_argument('-m', '--MetadataDb', dest='metadatadb', default='EGA', help='Name of the database collection EGA metadata. Default is EGA')
    AddSamples.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to store object information for submission to EGA. Default is EGASUB')
    AddSamples.add_argument('-i', '--Input', dest='input', help='Input table with sample info to load to submission database', required=True)
    AddSamples.add_argument('--Species', dest='species', default='Human', help='common species name')
    AddSamples.add_argument('--Taxon', dest='taxon', default='9606', help='species ID')    
    AddSamples.add_argument('--Name', dest='name', default='Homo sapiens', help='Species scientific name')
    AddSamples.add_argument('--SampleTitle', dest='sampleTitle', help='Title associated with submission', required=True)
    AddSamples.add_argument('--Center', dest='center', default='OICR_ICGC', help='Center name. Default is OICR_ICGC')
    AddSamples.add_argument('--RunCenter', dest='run', default='OICR', help='Run center name. Default is OICR')
    AddSamples.add_argument('--Study', dest='study', default='EGAS00001000900', help='Study ID. default is  EGAS00001000900')
    AddSamples.add_argument('--StudyTitle', dest='studyTitle', help='Title associated with study', required=True)
    AddSamples.add_argument('--Design', dest='design', help='Study design')
    AddSamples.add_argument('--Broker', dest='broker', default='EGA', help='Broker name. Default is EGA')
    AddSamples.set_defaults(func=AddSampleInfo)

    # add analyses to Analyses Table
    AddAnalyses = subparsers.add_parser('AddAnalyses', help ='Add analysis information to Analyses Table')
    AddAnalyses.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    AddAnalyses.add_argument('-t', '--Table', dest='table', default='Analyses', help='Analyses table. Default is Analyses')
    AddAnalyses.add_argument('-m', '--MetadataDb', dest='metadatadb', default='EGA', help='Name of the database collection EGA metadata. Default is EGA')
    AddAnalyses.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
    AddAnalyses.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
    AddAnalyses.add_argument('-i', '--Input', dest='input', help='Input table with analysis info to load to submission database', required=True)
    AddAnalyses.add_argument('-p', '--Project', dest='projects', help='Primary key in the AnalysesProjects table', required=True)
    AddAnalyses.add_argument('-a', '--Attributes', dest='attributes', help='Primary key in the AnalysesAttributes table', required=True)
    AddAnalyses.set_defaults(func=AddAnalysesInfo)

    # add analyses to Analyses Table
    AddAttributesProjects = subparsers.add_parser('AddAttributesProjects', help ='Add information to AnalysesAttributes or AnalysesProjects Tables')
    AddAttributesProjects.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    AddAttributesProjects.add_argument('-t', '--Table', dest='table', choices = ['AnalysesAttributes', 'AnalysesProjects'], help='Database Tables AnalysesAttributes or AnalysesProjects', required=True)
    AddAttributesProjects.add_argument('-i', '--Input', dest='input', help='Input table with attributes or projects information to load to submission database', required=True)
    AddAttributesProjects.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
    AddAttributesProjects.add_argument('-d', '--DataType', dest='datatype', choices=['Projects', 'Attributes'], help='Add Projects or Attributes infor to db')
    AddAttributesProjects.set_defaults(func=AddAnalysesAttributesProjects)
    
    # submit samples to EGA
    SampleSubmission = subparsers.add_parser('SampleSubmission', help ='Submit samples to EGA')
    SampleSubmission.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    SampleSubmission.add_argument('-t', '--Table', dest='table', default='Analyses', help='Samples table. Default is Analyses')
    SampleSubmission.add_argument('-d', '--Database', dest='database', default='EGAsub', help='Name of the database used to store object information for submission to EGA. Default is EGASUB')
    SampleSubmission.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
    SampleSubmission.add_argument('-p', '--Portal', dest='portal', default='https://ega.crg.eu/submitterportal/v1', help='EGA submission portal. Default is https://ega.crg.eu/submitterportal/v1')
    SampleSubmission.set_defaults(func=SubmitSamples)

    # submit analyses to EGA       
    AnalysisSubmission = subparsers.add_parser('AnalysisSubmission', help ='Submit analyses to EGA')
    AnalysisSubmission.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    AnalysisSubmission.add_argument('-t', '--Table', dest='table', default='Analyses', help='Database table. Default is Analyses')
    AnalysisSubmission.add_argument('-m', '--MetadataDb', dest='metadatadb', default='EGA', help='Name of the database collection EGA metadata. Default is EGA')
    AnalysisSubmission.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
    AnalysisSubmission.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
    AnalysisSubmission.add_argument('-k', '--Keyring', dest='keyring', default='/.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/publickeys/public_keys.gpg', help='Path to the keys used for encryption. Default is /.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/publickeys/public_keys.gpg')
    AnalysisSubmission.add_argument('-p', '--Projects', dest='projects', default='AnalysesProjects', help='DataBase table. Default is AnalysesProjects')
    AnalysisSubmission.add_argument('-a', '--Attributes', dest='attributes', default='AnalysesAttributes', help='DataBase table. Default is AnalysesAttributes')
    AnalysisSubmission.add_argument('-q', '--Queue', dest='queue', default='production', help='Queue for encrypting files. Default is production')
    AnalysisSubmission.add_argument('-u', '--UploadMode', dest='uploadmode', default='aspera', choices=['lftp', 'aspera'], help='Use lftp of aspera for uploading files. Use aspera by default')
    AnalysisSubmission.add_argument('-d', '--DiskSpace', dest='diskspace', default=15, type=int, help='Free disk space (in Tb) after encyption of new files. Default is 15TB')
    AnalysisSubmission.add_argument('--Portal', dest='portal', default='https://ega.crg.eu/submitterportal/v1', help='EGA submission portal. Default is https://ega.crg.eu/submitterportal/v1')
    AnalysisSubmission.add_argument('--Mem', dest='memory', default='10', help='Memory allocated to encrypting files. Default is 10G')
    AnalysisSubmission.add_argument('--Max', dest='max', default=8, type=int, help='Maximum number of files to be uploaded at once. Default is 8')
    AnalysisSubmission.add_argument('--Remove', dest='remove', action='store_true', help='Delete encrypted and md5 files when analyses are successfully submitted. Do not delete by default')
    AnalysisSubmission.set_defaults(func=SubmitAnalyses)


   

    



#####################################


    # collect enumerations from EGA
    CollectEnumParser = subparsers.add_parser('Enums', help ='Collect enumerations from EGA')
    CollectEnumParser.add_argument('--URL', dest='url', choices = ['https://ega-archive.org/submission-api/v1/enums/analysis_file_types',
                                                                   'https://ega-archive.org/submission-api/v1/enums/experiment_types',
                                                                   'https://ega-archive.org/submission-api/v1/enums/analysis_types'], help='URL with enumerations', required=True)
    CollectEnumParser.set_defaults(func=GrabEgaEnums)

#    # check table information. change status ready --> valid if no error or keep status ready --> ready and record errorMessage
#    CheckInfoParser = subparsers.add_parser('CheckInfo', help ='Check Table information')
#    CheckInfoParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
#    CheckInfoParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
#    CheckInfoParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Database table. Default is Analyses')
#    CheckInfoParser.add_argument('-a', '--Attributes', dest='attributes', default='AnalysesAttributes', help='DataBase table. Default is AnalysesAttributes')
#    CheckInfoParser.add_argument('-p', '--Projects', dest='projects', default='AnalysesProjects', help='DataBase table. Default is AnalysesProjects')
#    CheckInfoParser.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
#    CheckInfoParser.set_defaults(func=CheckTableInformation)
#
#    # set up working directory, add to analyses table and update status valid --> start       
#    AddWorkingDirParser = subparsers.add_parser('AddWorkingDirectories', help ='Create working directories')
#    AddWorkingDirParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
#    AddWorkingDirParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
#    AddWorkingDirParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Database table. Default is Analyses')
#    AddWorkingDirParser.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
#    AddWorkingDirParser.set_defaults(func=AddWorkingDirectory)
#
#    # add sample accessions and change status start -> encrypt       
#    AddSampleIdsParser = subparsers.add_parser('AddSampleIds', help ='Add sample accessions to Analyses Table')
#    AddSampleIdsParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
#    AddSampleIdsParser.add_argument('-m', '--MetadataDb', dest='metadatadb', default='EGA', help='Name of the database collection EGA metadata. Default is EGA')
#    AddSampleIdsParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
#    AddSampleIdsParser.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
#    AddSampleIdsParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Database table. Default is Analyses')
#    AddSampleIdsParser.set_defaults(func=AddSampleAccessions)
#
#    # encrypt new files only if diskspace is available. update status encrypt --> encrypting       
#    EncryptionParser = subparsers.add_parser('Encryption', help ='Encrypt files and run md5sum')
#    EncryptionParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
#    EncryptionParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
#    EncryptionParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Database table. Default is Analyses')
#    EncryptionParser.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
#    EncryptionParser.add_argument('-k', '--Keyring', dest='keyring', default='/.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/publickeys/public_keys.gpg', help='Path to the keys used for encryption. Default is /.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/publickeys/public_keys.gpg')
#    EncryptionParser.add_argument('-q', '--Queue', dest='queue', default='production', help='Queue for encrypting files. Default is production')
#    EncryptionParser.add_argument('-m', '--Mem', dest='memory', default='10', help='Memory allocated to encrypting files. Default is 10G')
#    EncryptionParser.add_argument('-d', '--DiskSpace', dest='diskspace', default=15, type=int, help='Free disk space (in Tb) after encyption of new files. Default is 15TB')
#    EncryptionParser.set_defaults(func=EncryptFiles)
#
#    # check that encryption is done, store md5sums and path to encrypted file in db, update status encrypting -> upload       
#    CheckEncryptionParser = subparsers.add_parser('CheckEncryption', help ='Check if encryption is done')
#    CheckEncryptionParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
#    CheckEncryptionParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
#    CheckEncryptionParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Database table. Default is Analyses')
#    CheckEncryptionParser.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
#    CheckEncryptionParser.set_defaults(func=CheckEncryption)
#
#    # upload files and change the status upload -> uploading       
#    UploadParser = subparsers.add_parser('Upload', help ='Upload files to staging server')
#    UploadParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
#    UploadParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
#    UploadParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Database table. Default is Analyses')
#    UploadParser.add_argument('-a', '--Attributes', dest='attributes', default='AnalysesAttributes', help='DataBase table. Default is AnalysesAttributes')
#    UploadParser.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
#    UploadParser.add_argument('-q', '--Queue', dest='queue', default='production', help='Queue for encrypting files. Default is production')
#    UploadParser.add_argument('-m', '--Mem', dest='memory', default='10', help='Memory allocated to encrypting files. Default is 10G')
#    UploadParser.add_argument('-u', '--UploadMode', dest='uploadmode', default='aspera', choices=['lftp', 'aspera'], help='Use lftp of aspera for uploading files. Use aspera by default')
#    UploadParser.add_argument('--Max', dest='max', default=8, type=int, help='Maximum number of files to be uploaded at once. Default is 8')
#    UploadParser.set_defaults(func=UploadAnalysesObjects)
#
#    # check that files have been successfully uploaded, update status uploading -> uploaded       
#    CheckUploadParser = subparsers.add_parser('CheckUpload', help ='Check that files have been uploaded')
#    CheckUploadParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
#    CheckUploadParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
#    CheckUploadParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Database table. Default is Analyses')
#    CheckUploadParser.add_argument('-a', '--Attributes', dest='attributes', default='AnalysesAttributes', help='DataBase table. Default is AnalysesAttributes')
#    CheckUploadParser.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
#    CheckUploadParser.set_defaults(func=CheckUploadFiles)
#
#    # form json for analyses in uploaded mode, add to table and update status uploaded -> submit       
#    AddJsonParser = subparsers.add_parser('AddAnalysesJson', help ='Add json to Analyses table')
#    AddJsonParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
#    AddJsonParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
#    AddJsonParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Database table. Default is Analyses')
#    AddJsonParser.add_argument('-a', '--Attributes', dest='attributes', default='AnalysesAttributes', help='DataBase table. Default is AnalysesAttributes')
#    AddJsonParser.add_argument('-p', '--Projects', dest='projects', default='AnalysesProjects', help='DataBase table. Default is AnalysesProjects')
#    AddJsonParser.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
#    AddJsonParser.set_defaults(func=AddAnalysisJsonToTable)
#
#    # submit analyses to EGA and update status submit -> SUBMITTED      
#    RegisterParser = subparsers.add_parser('Register', help ='Register objects to EGA')
#    RegisterParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
#    RegisterParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
#    RegisterParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Database table. Default is Analyses')
#    RegisterParser.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
#    RegisterParser.add_argument('-o', '--Object', dest='object', choices=['samples', 'analyses'], help='Object to register', required=True)
#    RegisterParser.add_argument('-p', '--Portal', dest='portal', default='https://ega.crg.eu/submitterportal/v1', help='EGA submission portal. Default is https://ega.crg.eu/submitterportal/v1')
#    RegisterParser.set_defaults(func=RegisterObjects)
#
#    # remove files for aliases with SUBMITTED status       
#    RemoveFilesParser = subparsers.add_parser('RemoveFiles', help ='Remove files after submission')
#    RemoveFilesParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
#    RemoveFilesParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
#    RemoveFilesParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Database table. Default is Analyses')
#    RemoveFilesParser.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
#    RemoveFilesParser.add_argument('--Remove', dest='remove', action='store_true', help='Delete encrypted and md5 files when analyses are successfully submitted. Do not delete by default')
#    RemoveFilesParser.set_defaults(func=RemoveFilesAfterSubmission)

############################################

    # get arguments from the command line
    args = parser.parse_args()
    # pass the args to the default function
    args.func(args)
