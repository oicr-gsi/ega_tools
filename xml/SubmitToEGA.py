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
import xml.etree.ElementTree as ET



# resource for jaon formatting and api submission
#https://ega-archive.org/submission/programmatic_submissions/json-message-format
#https://ega-archive.org/submission/programmatic_submissions/submitting-metadata


## functions common to multiple objects

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


# use this function to list enumerations
def ListEnumerations(MyScript, MyPython):
    '''
    (str, str) -> list
    Take a the path to the python program, and the path to the python script and
    return a dictionary with enumeration as key and corresponding dictionary of metadata as value
    Precondition: the list of enumerations available from EGA is hard-coded
    '''
    
    # list all enumerations available from EGA
    url = 'https://ega-archive.org/submission-api/v1/enums/'
    L = ['analysis_file_types', 'analysis_types', 'case_control', 'dataset_types', 'experiment_types',
         'file_types', 'genders', 'instrument_models', 'library_selections', 'library_sources',
         'library_strategies', 'reference_chromosomes', 'reference_genomes', 'study_types']
    URLs = [os.path.join(url, i) for i in L]

    # create a dictionary to store each enumeration
    Enums = {}
    for URL in URLs:
        Enums[os.path.basename(URL).title().replace('_', '')] = json.loads(subprocess.check_output('ssh xfer4 \"{0} {1} Enums --URL {2}\"'.format(MyPython, MyScript, URL), shell=True).decode('utf-8').rstrip().replace("'", "\""))
    return Enums


# use this function to record an error message for a given alias
def RecordMessage(CredentialFile, DataBase, Table, Box, Alias, Message, Status):
    '''
    (str, str, str, str, str, str, str) -> None
    Connect to database using the credentials in file and update the error message
    of the status for a given Alias and Box in Table if Status is respectively Error or Status
    '''
    
    # pull json for objects with ready Status for given box
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
    if Status == 'Error':
        # record error message
        cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" and {0}.egaBox=\"{3}\"'.format(Table, Message, Alias, Box))
    elif Status == 'Status':
        # record submission status
        cur.execute('UPDATE {0} SET {0}.submissionStatus=\"{1}\" WHERE {0}.alias="\{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, Message, Alias, Box))
    conn.commit()
    conn.close()
 
    
# use this function to delete objects with VALIDATED_WITH_ERRORS status    
def DeleteValidatedObjectsWithErrors(CredentialFile, DataBase, Table, Box, Object, Portal):
    '''
    (str, str, str, str, str, str) - > None
    Connect to Database using CredentialFile, extract all aliases with submit status
    and Box from Table, connect to the API and delete the corresponding Object
    if submissionStatus is VALIDATED_WITH ERRORS 
    '''

    # grab all aliases with submit status
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
    try:
        cur.execute('SELECT {0}.alias FROM {0} WHERE {0}.Status=\"submit\" AND {0}.egaBox=\"{1}\"'.format(Table, Box))
        # extract all information 
        Data = cur.fetchall()
    except:
        Data = []
    conn.close()
    
    # check if alias with submit status
    if len(Data) != 0:
        # extract the aliases
        Aliases = [i[0] for i in Data]
        
        # connect to EGA and get a token. parse credentials to get userName and Password
        UserName, MyPassword = ParseCredentials(CredentialFile, Box)
        # create json with credentials
        data = {"username": UserName, "password": MyPassword, "loginType": "submitter"}
        # get the adress of the submission portal
        if Portal[-1] == '/':
            URL = Portal[:-1]
        else:
            URL = Portal

        # connect to the API
        Login = requests.post(URL + '/login', data=data)
        # get a token
        Token = Login.json()['response']['result'][0]['session']['sessionToken']
        headers = {"Content-type": "application/json", "X-Token": Token}
        # retrieve all objects with VALIDATED_WITH ERRORS status
        response = requests.get(URL + '/{0}?status=VALIDATED_WITH_ERRORS&skip=0&limit=0'.format(Object), headers=headers, data=data)
        
        # loop over aliases
        for i in range(len(Aliases)):
            ObjectId = ''
            for j in response.json()['response']['result']:
                # check if alias with validated_with_errors status
                if j["alias"] == Aliases[i]:
                    ObjectId = j['id']
                    if ObjectId != '':
                        print(Aliases[i], ObjectId)
                        # delete object
                        requests.delete(URL + '/{0}/{1}'.format(Object, ObjectId), headers=headers)
        # disconnect from api
        requests.delete(URL + '/logout', headers={"X-Token": Token})     

    
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
    try:
        cur.execute('SELECT {0}.Json FROM {0} WHERE {0}.Status=\"submit\" AND {0}.egaBox=\"{1}\"'.format(Table, Box))
        # extract all information 
        Data = cur.fetchall()
    except:
        # record error message
        Data = []
    conn.close()
        
    # check that objects in submit mode do exist
    if len(Data) != 0:
        # make a list of jsons
        L = [json.loads(i[0].replace("'", "\"")) for i in Data]
        assert len(L) == len(Data)

        # connect to EGA and get a token
        # parse credentials to get userName and Password
        UserName, MyPassword = ParseCredentials(CredentialFile, Box)
        
        # create json with credentials
        data = {"username": UserName, "password": MyPassword, "loginType": "submitter"}
        # get the adress of the submission portal
        if Portal[-1] == '/':
            URL = Portal[:-1]
        else:
            URL = Portal

        # connect to API and open a submission for each object
        for J in L:
            Login = requests.post(URL + '/login', data=data)
            # check that response code is OK
            if Login.status_code == requests.codes.ok:
                # response is OK, get Token
                try:
                    Token = Login.json()['response']['result'][0]['session']['sessionToken']
                except:
                    # record error message
                    RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], 'Cannot obtain a token', 'Error')                
                else:
                    # open a submission with token
                    headers = {"Content-type": "application/json", "X-Token": Token}
                    submissionJson = {"title": "{0} submission", "description": "opening a submission for {0} {1}".format(Object, J["alias"])}
                    OpenSubmission = requests.post(URL + '/submissions', headers=headers, data=str(submissionJson).replace("'", "\""))
                    # check if submission is successfully open
                    if OpenSubmission.status_code == requests.codes.ok:
                        try:
                            # get submission Id
                            submissionId = OpenSubmission.json()['response']['result'][0]['id']
                        except:
                            # record error message
                            RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], 'Cannot obtain a submissionId', 'Error') 
                        else:
                            # create object
                            ObjectCreation = requests.post(URL + '/submissions/{0}/{1}'.format(submissionId, Object), headers=headers, data=str(J).replace("'", "\""))
                            # check response code
                            if ObjectCreation.status_code == requests.codes.ok:
                                # validate, get status (VALIDATED or VALITED_WITH_ERRORS) 
                                try:
                                    ObjectId = ObjectCreation.json()['response']['result'][0]['id']
                                    submissionStatus = ObjectCreation.json()['response']['result'][0]['status']
                                except:
                                    # record error message
                                    RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], 'Cannot create an object', 'Error') 
                                else:
                                    # store submission json and status (DRAFT) in db table
                                    RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], submissionStatus, 'Status') 
                                    # validate object
                                    ObjectValidation = requests.put(URL + '/{0}/{1}?action=VALIDATE'.format(Object, ObjectId), headers=headers)
                                    # check code and validation status
                                    if ObjectValidation.status_code == requests.codes.ok:
                                        # get object status
                                        try:
                                            ObjectStatus = ObjectValidation.json()['response']['result'][0]['status']
                                            errorMessages = CleanUpError(ObjectValidation.json()['response']['result'][0]['validationErrorMessages'])
                                        except:
                                            RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], 'Cannot obtain validation status', 'Error')
                                        else:
                                            # record error messages
                                            RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], errorMessages, 'Error')
                                            # record object status
                                            RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], ObjectStatus, 'Status') 
                                            # check if object is validated
                                            if ObjectStatus == 'VALIDATED':
                                                # submit object
                                                ObjectSubmission = requests.put(URL + '/{0}/{1}?action=SUBMIT'.format(Object, ObjectId), headers=headers)
                                                # check if successfully submitted
                                                if ObjectSubmission.status_code == requests.codes.ok:
                                                    # get status 
                                                    try:
                                                        errorMessages = CleanUpError(ObjectSubmission.json()['response']['result'][0]['submissionErrorMessages'])
                                                        ObjectStatus = ObjectSubmission.json()['response']['result'][0]['status']                
                                                    except:
                                                        # record error message
                                                        RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], 'Cannot obtain submission status', 'Error')
                                                    else:
                                                        # record error messages and status
                                                        RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], errorMessages, 'Error')
                                                        # record object status
                                                        RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], ObjectStatus, 'Status') 
                                                        # check status
                                                        if ObjectStatus == 'SUBMITTED':
                                                            # get the receipt, and the accession id
                                                            try:
                                                                Receipt, egaAccessionId = str(ObjectSubmission.json()).replace("\"", ""), ObjectSubmission.json()['response']['result'][0]['egaAccessionId']
                                                            except:
                                                                # record error message
                                                                RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], 'Cannot obtain receipt and/or accession Id', 'Error')
                                                            else:
                                                                # store the date it was submitted
                                                                Time = time.strftime('%Y-%m-%d', time.localtime(time.time()))
                                                                # add Receipt, accession and time to table and change status
                                                                conn = EstablishConnection(CredentialFile, DataBase)
                                                                cur = conn.cursor()
                                                                cur.execute('UPDATE {0} SET {0}.Receipt=\"{1}\", {0}.egaAccessionId=\"{2}\", {0}.Status=\"{3}\", {0}.submissionStatus=\"{3}\", {0}.CreationTime=\"{4}\" WHERE {0}.alias=\"{5}\" AND {0}.egaBox=\"{6}\"'.format(Table, Receipt, egaAccessionId, ObjectStatus, Time, J["alias"], Box))
                                                                conn.commit()
                                                                conn.close()
                                                        else:
                                                            # delete object
                                                            requests.delete(URL + '/{0}/{1}'.format(Object, ObjectId), headers=headers)
                                            else:
                                                #delete object
                                                requests.delete(URL + '/{0}/{1}'.format(Object, ObjectId), headers=headers)
                    # disconnect by removing token
                    requests.delete(URL + '/logout', headers={"X-Token": Token})     



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


# use this function to check information in Tables    
def IsInfoValid(CredentialFile, SubDataBase, Table, Box, Object, MyScript, MyPython, **KeyWordParams):
    '''
    (str, str, str, str, str, str, str, str, dict) -> dict
    Extract information from DataBase Table, AttributesTable and also from ProjectsTable
    if Object is analyses using credentials in file, check if information is valid and return a dict
    with error message for each alias in Table
    '''

    # create a dictionary {alias: error}
    D = {}

    # get the enumerations
    Enums = ListEnumerations(MyScript, MyPython)
    
    # connect to db
    conn = EstablishConnection(CredentialFile, SubDataBase)
    cur = conn.cursor()      
    # get required information
    if Object == 'analyses':
        if 'attributes' in KeyWordParams:
            AttributesTable = KeyWordParams['attributes']
            Cmd = 'SELECT {0}.alias, {1}.title, {1}.description, {1}.attributes, {1}.genomeId, {1}.StagePath \
            FROM {0} JOIN {1} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{2}\" AND {0}.attributes={1}.alias'.format(Table, AttributesTable, Box)
        elif 'projects' in KeyWordParams:
            ProjectsTable = KeyWordParams['projects']
            Cmd = 'SELECT {0}.alias, {1}.studyId, {1}.analysisCenter, {1}.Broker, {1}.analysisTypeId, {1}.experimentTypeId \
            FROM {0} JOIN {1} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{2}\" AND {0}.projects={1}.alias'.format(Table, ProjectsTable, Box) 
        else:
            Cmd = 'SELECT {0}.alias, {0}.sampleAlias, {0}.files, {0}.egaBox, \
            {0}.attributes, {0}.projects FROM {0} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{1}\"'.format(Table, Box)
    elif Object == 'samples':
        if 'attributes' in KeyWordParams:
            AttributesTable = KeyWordParams['attributes']
            Cmd = 'Select {0}.alias, {1}.title, {1}.description, {1}.attributes FROM {0} JOIN {1} WHERE \
            {0}.Status=\"ready\" AND {0}.egaBox=\"{2}\" AND {0}.attributes={1}.alias'.format(Table, AttributesTable, Box)
        else:
            Cmd = 'Select {0}.alias, {0}.caseOrControlId, {0}.genderId, {0}.phenotype, {0}.egaBox, \
            {0}.attributes FROM {0} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{1}\"'.format(Table, Box)
    elif Object == 'datasets':
        Cmd = 'SELECT {0}.alias, {0}.datasetTypeIds, {0}.policyId, {0}.runsReferences, {0}.analysisReferences, \
        {0}.title, {0}.description, {0}.datasetLinks, {0}.attributes FROM {0} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{1)\"'.format(Table, Box)            
    
    # extract data 
    try:
        cur.execute(Cmd)
        Data = cur.fetchall()
    except:
        Data = []
    conn.close()
    
    # check info
    if len(Data) != 0:
        if Object == 'analyses':
            if 'attributes' in KeyWordParams:
                Keys = ['alias', 'title', 'description', 'attributes', 'genomeId', 'StagePath']        
                Required = ['title', 'description', 'genomeId', 'StagePath']
            elif 'projects' in KeyWordParams:
                Keys = ['alias', 'studyId', 'analysisCenter', 'Broker', 'analysisTypeId', 'experimentTypeId']
                Required = ['studyId', 'analysisCenter', 'Broker', 'analysisTypeId', 'experimentTypeId']
            else:
                Keys = ['alias', 'sampleAlias', 'files', 'egaBox', 'attributes', 'projects']
                Required = ['alias', 'sampleAlias', 'files', 'egaBox', 'attributes', 'projects']
        elif Object == 'samples':
            if 'attributes' in KeyWordParams:
                Keys = ['alias', 'title', 'description', 'attributes']
                Required = ['title', 'description']
            else:
                Keys = ['alias', 'caseOrControlId', 'genderId', 'phenotype', 'egaBox', 'attributes']
                Required = ['alias', 'caseOrControlId', 'genderId', 'phenotype', 'egaBox', 'attributes']
        elif Object == 'datasets':
            Keys = ['alias', 'datasetTypeIds', 'policyId', 'runsReferences', 'analysisReferences', 'title',
                    'description', 'datasetLinks', 'attributes', 'egaBox']     
            Required = ['alias', 'datasetTypeIds', 'policyId', 'title', 'description', 'egaBox']
    
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
                    if d[key] in ['', 'NULL', None]:
                        Missing = True
                        Error.append(key)
                # check that references are provided
                if 'runsReferences' in d and 'analysisReferences' in d:
                    # at least runsReferences or analysesReferences should include some accessions
                    if d['runsReferences'] in ['', 'NULL', None] and d['analysisReferences'] in ['', 'NULL', None]:
                        Missing = True
                        Error.append('References')
                    if d['runsReferences'] not in ['', 'NULL', None]:
                        if False in list(map(lambda x: x.startswith('EGAR'), d['runsReferences'].split(';'))):
                            Missing = True
                            Error.append('runsReferences')
                    if d['analysisReferences'] not in ['', 'NULL', None]:
                        if False in list(map(lambda x: x.startswith('EGAZ'), d['analysisReferences'].split(';'))):
                            Missing = True
                            Error.append('analysisReferences')
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
                        if files[filePath]['fileTypeId'].lower() not in Enums['FileTypes']:
                            Missing = True
                            Error.append('fileTypeId')
                # check study Id
                if key == 'studyId':
                    if 'EGAS' not in d[key]:
                        Missing = True
                        Error.append(key)
                # chyeck policy Id
                if key == 'policyId':
                    if 'EGAP' not in d[key]:
                        Missing = True
                        Error.append(key)
                # check enumerations
                if key == 'experimentTypeId':
                    if d['experimentTypeId'] not in Enums['ExperimentTypes']:
                        Missing = True
                        Error.append(key)
                if key == 'analysisTypeId':
                    if d['analysisTypeId'] not in Enums['AnalysisTypes']:
                        Missing = True
                        Error.append(key)
                if key == 'caseOrControlId':
                    if d['caseOrControlId'] not in Enums['CaseControl']:
                        Missing = True
                        Error.append(key)
                if key == 'genderId':
                    if d['genderId'] not in Enums['Genders']:
                        Missing = True
                        Error.append(key)
                if key == 'datasetTypeIds':
                    if d['datasetTypeIds'] not in Enums['DatasetTypes']:
                        Missing = True
                        Error.append(key)
                
                # check attributes of attributes table
                if key == 'attributes':
                    if (Object in ['analyses', 'samples'] and 'attributes' in KeyWordParams) or Object == 'datasets':
                        if d['attributes'] not in ['', 'NULL', None]:
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
                Error = 'NoError'
            assert d['alias'] not in D
            D[d['alias']] = Error
    return D


# use this function to check that all information for Analyses objects is available before encrypting files     
def CheckTableInformation(CredentialFile, DataBase, Table, Object, Box, MyScript, MyPython, **KeyWordParams):
    '''
    (str, str, str, str, str, str, str, dict) -> None
    Extract information from DataBase Tables using credentials in file and update errorMessages for each alias
    '''

    # create dict {alias: [errors]}
    K = {}
    # connect to db
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()      
    try:
        cur.execute('SELECT {0}.alias, {0}.errorMessages FROM {0} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{1}\"'.format(Table, Box))
        Data = cur.fetchall()
    except:
        Data = []
    conn.close()
    # record error messages
    if len(Data) != 0:
        for i in Data:
            K[i[0]] = i[1].split('|')
        # check Table
        D = IsInfoValid(CredentialFile, DataBase, Table, Box, Object, MyScript, MyPython, **KeyWordParams)
        # record error messages
        for alias in K:
            if alias in D:
                K[alias].append(D[alias])
            else:
                K[alias].append('In {0} table, no information'.format(Table))
                  
    # update status and record errorMessage
    if len(K) != 0:
        # connect to database
        conn = EstablishConnection(CredentialFile, DataBase)
        cur = conn.cursor()    
        for alias in K:
            Error = '|'.join(K[alias])
            # record error message
            cur.execute('UPDATE {0} {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, Error, alias, Box))
            conn.commit()
        conn.close()


# use this function to check that all information is required for a given object
def CheckObjectInformation(CredentialFile, DataBase, Table, Box):
    '''
    (str, str, str, str) -> None
    Extract information from DataBase Table using credentials in file and update
    status to "valid" if all information is correct or keep status to "ready" if incorrect or missing
    '''
    
    # connect to db
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()      
    try:
        cur.execute('SELECT {0}.alias, {0}.errorMessages FROM {0} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{1}\"'.format(Table, Box))
        Data = cur.fetchall()
    except:
        Data = []
    # record error messages
    if len(Data) != 0:
        for i in Data:
            alias, Error = i[0], i[1].split('|')
            # check error message and update status only if no error found
            if len(list(set(Error))) == 1:
                if list(set(Error)) == 'NoError':
                    # update status ready --> valid
                    cur.execute('UPDATE {0} SET {0}.Status=\"valid\" WHERE {0}.alias=\"{1}\" AND {0}.egaBox=\"{2}\"'.format(Table, alias, Box))
                    conn.commit()
    conn.close()        
    

# use this function to format the analysis json
def FormatJson(D, Object, MyScript, MyPython):
    '''
    (dict, str, str) -> dict
    Take a dictionary with information for an object, the path to the script to fetch 
    the EGA enumerations, and return a dictionary with the expected format or
    a dictionary with the alias only if required fields are missing
    Precondition: strings in D have double-quotes
    '''
    
    Enums = ListEnumerations(MyScript, MyPython)
        
    # create a dict to be strored as a json. note: strings should have double quotes
    J = {}
    
    if Object == 'analyses':
        JsonKeys = ["alias", "title", "description", "studyId", "sampleReferences",
                    "analysisCenter", "analysisDate", "analysisTypeId", "files",
                    "attributes", "genomeId", "chromosomeReferences", "experimentTypeId", "platform"]
        Required = ["alias", "title", "description", "studyId", "analysisCenter",
                    "analysisTypeId", "files", "genomeId", "experimentTypeId", "StagePath"]
    elif Object == 'samples':
        JsonKeys = ["alias", "title", "description", "caseOrControlId", "genderId",
                    "organismPart", "cellLine", "region", "phenotype", "subjectId",
                    "anonymizedName", "biosampleId", "sampleAge", "sampleDetail", "attributes"]
        Required = ["alias", "title", "description", "caseOrControlId", "genderId", "phenotype"] 
    elif Object == 'datasets':
        JsonKeys = ["alias", "datasetTypeIds", "policyId", "runsReferences", "analysisReferences",
                    "title", "description", "datasetLinks", "attributes"]
        Required = ['alias', 'datasetTypeIds', 'policyId', 'title', 'description', 'egaBox']
    
    # map typeId with enumerations
    MapEnum = {"experimentTypeId": "ExperimentTypes", "analysisTypeId": "AnalysisTypes",
               "caseOrControlId": "CaseControl", "genderId": 'Genders', 
               "datasetTypeIds": "DatasetTypes"}        

    # loop over required json keys
    for field in JsonKeys:
        if field in D:
            if D[field] in ['NULL', '', None]:
                # some fields are required, return empty dict if field is empty
                if field in Required:
                    # erase dict and add alias
                    J = {}
                    J["alias"] = D["alias"]
                    # return dict with alias only if required fields are missing
                    return J
                # other fields can be missing, either as empty list or string
                else:
                    # chromosomeReferences is hard-coded as empty list
                    if field in ["chromosomeReferences", "attributes", "datasetLinks", "runsReferences", "analysisReferences"]:
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
                        if files[filePath]["fileTypeId"].lower() not in Enums['FileTypes']:
                            # cannot obtain fileTypeId. erase dict and add alias
                            J = {}
                            J["alias"] = D["alias"]
                            # return dict with alias only if required fields are missing
                            return J
                        else:
                            fileTypeId = Enums['FileTypes'][files[filePath]["fileTypeId"].lower()]
                        # create dict with file info, add path to file names
                        d = {"fileName": os.path.join(D['StagePath'], files[filePath]['encryptedName']),
                             "checksum": files[filePath]['checksum'],
                             "unencryptedChecksum": files[filePath]['unencryptedChecksum'],
                             "fileTypeId": fileTypeId}
                        J[field].append(d)
                elif field in ['runsReferences', 'analysisReferences']:
                    J[field] = D[field].split(';')
                elif field in ['attributes', 'datasetLinks']:
                    # ensure strings are double-quoted
                    attributes = D[field].replace("'", "\"")
                    # convert string to dict
                    # loop over all attributes
                    attributes = attributes.split(';')
                    J[field] = [json.loads(attributes[i].strip().replace("'", "\"")) for i in range(len(attributes))]
                # check enumerations
                elif field in MapEnum:
                    # check that enumeration is valid
                    if D[field] not in Enums[MapEnum[field]]:
                        # cannot obtain enumeration. erase dict and add alias
                        J = {}
                        J["alias"] = D["alias"]
                        # return dict with alias only if required fields are missing
                        return J
                    else:
                        # check field to add enumeration to json
                        if field == "experimentTypeId":
                            J[field] = [Enums[MapEnum[field]][D[field]]]
                        else:
                            J[field] = Enums[MapEnum[field]][D[field]]
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



# use this function to form jsons and store to submission db
def AddJsonToTable(CredentialFile, DataBase, Table, Box, Object, MyScript, MyPython, **KeyWordParams):
    '''
    (str, str, str, str, str, str, str, dict) -> None
    Form a json for Objects in the given Box and add it to Table by
    quering required information from Table and optional tables using the file
    with credentials to connect to Database update the status if json is formed correctly
    '''
    
    # connect to the database
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
    
    # get optional tables
    if 'projects' in KeyWordParams:
        ProjectsTable = KeyWordParams['projects']
    else:
        ProjectsTable = 'empty'
    if 'attributes' in KeyWordParams:
        AttributesTable = KeyWordParams['attributes']
    else:
        AttributesTable = 'empty'
    
    # command depends on Object type    
    if Object == 'analyses':
        Cmd = 'SELECT {0}.alias, {0}.sampleEgaAccessionsId, {0}.analysisDate, {0}.files, \
        {1}.title, {1}.description, {1}.attributes, {1}.genomeId, {1}.chromosomeReferences, {1}.StagePath, {1}.platform, \
        {2}.studyId, {2}.analysisCenter, {2}.Broker, {2}.analysisTypeId, {2}.experimentTypeId \
        FROM {0} JOIN {1} JOIN {2} WHERE {0}.Status=\"uploaded\" AND {0}.egaBox=\"{3}\" AND {0}.attributes = {1}.alias \
        AND {0}.projects = {2}.alias'.format(Table, AttributesTable, ProjectsTable, Box)
    elif Object == 'samples':
        Cmd = 'SELECT {0}.alias, {0}.caseOrControlId, {0}.genderId, {0}.organismPart, \
        {0}.cellLine, {0}.region, {0}.phenotype, {0}.subjectId, {0}.anonymizedName, {0}.biosampleId, \
        {0}.sampleAge, {0}.sampleDetail, {1}.title, {1}.description, {1}.attributes FROM {0} JOIN {1} \
        WHERE {0}.Status=\"valid\" AND {0}.egaBox=\"{2}\" AND {0}.attributes = {1}.alias'.format(Table, AttributesTable, Box)
    elif Object == 'datasets':
        Cmd = 'SELECT {0}.alias, {0}.datasetTypeIds, {0}.policyId, {0}.runsReferences, \
        {0}.analysisReferences, {0}.title, {0}.description, {0}.datasetLinks, {0}.attributes FROM {0} \
        WHERE {0}.Status=\"valid\" AND {0}.egaBox=\"{1}\"'.format(Table, Box)
    
    # extract information to for json    
    try:
        cur.execute(Cmd)
        # get column headers
        Header = [i[0] for i in cur.description]
        # extract all information 
        Data = cur.fetchall()
    except:
        Data = []
        
    # check that object are with appropriate status and/or that information can be extracted
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
        Jsons = [FormatJson(D, Object, MyScript, MyPython) for D in L]
        # add json back to table and update status
        for D in Jsons:
            # check if json is correctly formed (ie. required fields are present)
            if len(D) == 1:
                Error = 'Cannot form json, required field(s) missing'
                # add error in table and keep status (uploaded --> uploaded for analyses and valid --> valid for samples)
                alias = D['alias']
                cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, Error, alias, Box))
                conn.commit()
            else:
                # add json back in table and update status
                alias = D['alias']
                cur.execute('UPDATE {0} SET {0}.Json=\"{1}\", {0}.errorMessages=\"None\", {0}.Status=\"submit\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\";'.format(Table, str(D), alias, Box))
                conn.commit()
    conn.close()


# use this function to check the job exit status
def GetJobExitStatus(JobName):
    '''
    (str) -> str
    Take a job name and return the exit code of that job after it finished running
    ('0' indicates a normal, error-free run and '1' or another value inicates an error)
    '''
    
    # make a sorted list of accounting files with job info archives
    Archives = subprocess.check_output('ls -lt /oicr/cluster/ogs2011.11/default/common/accounting*', shell=True).decode('utf-8').rstrip().split('\n')
    # keep accounting files for the current year
    Archives = [Archives[i].split()[-1] for i in range(len(Archives)) if ':' in Archives[i].split()[-2]]
    
    # loop over the most recent archives and stop when job is found    
    for AccountingFile in Archives:
        try:
            i = subprocess.check_output('qacct -j {0} -f {1}'.format(JobName, AccountingFile), shell=True).decode('utf-8').rstrip().split('\n')
        except:
            i = ''
        else:
            if i != '':
                break
            
    # create a dict with months
    Months = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
               'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
    
    # check if accounting file with job has been found
    if i == '':
        # return error
        return '1'        
    else:
        # record all exit status. the same job may have been run multiple times if re-encryption was needed
        d = {}
        for j in i:
            if 'end_time' in j:
                k = j.split()[2:]
                # convert date to epoch time
                date = '.'.join([k[1], Months[k[0]], k[-1]]) + ' ' + k[2] 
                p = '%d.%m.%Y %H:%M:%S'
                date = int(time.mktime(time.strptime(date, p)))
            elif 'exit_status' in j:
                d[date] = j.split()[1]
        
        # get the exit status of the most recent job    
        EndJobs = list(d.keys())
        EndJobs.sort()
        if len(d) != 0:
            # return exit code
            return d[EndJobs[-1]]
        else:
            # return error
            return '1'
    
# use this function to grab all sub-directories of a given directory on the staging server
def GetSubDirectories(UserName, PassWord, Directory):
    '''
    (str, str, str) -> list
    Connect to EGA staging server using the credentials UserName and PassWord
    for a given box and return a list of sub-directories in Directory
    '''
    
    # make a list of directories on the staging servers
    Cmd = "ssh xfer4.res.oicr.on.ca \"lftp -u {0},{1} -e \\\" set ftp:ssl-allow false; ls {2} ; bye;\\\" ftp://ftp-private.ebi.ac.uk\"".format(UserName, PassWord, Directory)
    a = subprocess.check_output(Cmd, shell=True).decode('utf-8').rstrip().split('\n')
    Content = [os.path.join(Directory, i.split()[-1]) for i in a if i.startswith('d')]
    return Content

# use this function to list all directories on the staging server    
def GrabAllDirectoriesStagingServer(CredentialFile, Box):
    '''
    (str, str) -> list
    Connect to EGA staging server using the credential file and return a list
    of all directories on the staging server for the given Box
    '''
    
    # extract the user name and password from the credential file
    UserName, PassWord = ParseCredentials(CredentialFile, Box)
    
    # exclude EGA-owned directories
    Exclude = ['MD5_daily_reports', 'metadata']
    
    # make a list of directories on the staging server 
    a = GetSubDirectories(UserName, PassWord, '')
    # dump all sub-directories into the collecting list
    # skip EGA-owned directories
    b = [i for i in a if i not in Exclude]
    # add home directory
    b.append('')
    # make a list of directories already traversed
    checked = ['']
        
    # initialize list with list length so that first interation always occurs
    L = [0, len(b)]
    while L[-1] != L[-2]:
        for i in b:
            # ignore if already checked and ignore EGA-owned directories
            if i not in checked and i not in Exclude:
                b.extend(GetSubDirectories(UserName, PassWord, i))
                checked.append(i)
        # update L
        L.append(len(b))
    return b


# use this function to retrieve the size of a file on the staging server
def ExtractFileSizeStagingServer(CredentialFile, Box, Directory):
    '''
    (str, str, str) -> dict
    Connect to EGA staging server using the credentials UserName and PassWord
    for a given box and return a dictionary with file size for all files under Directory
    '''
    
    # extract the user name and password from the credential file
    UserName, PassWord = ParseCredentials(CredentialFile, Box)
    
    # make a list of files under Directory on the staging server
    Cmd = "ssh xfer4.res.oicr.on.ca \"lftp -u {0},{1} -e \\\" set ftp:ssl-allow false; ls {2} ; bye;\\\" ftp://ftp-private.ebi.ac.uk\"".format(UserName, PassWord, Directory)
    a = subprocess.check_output(Cmd, shell=True).decode('utf-8').rstrip().split('\n')
    Content = [i for i in a if i.startswith('-')]
    # extract file size for all files {filepath: file_size}
    Size = {}
    for S in Content:
        S = S.rstrip().split()
        FileSize = int(S[4])
        filePath = os.path.join(Directory, S[-1])
        Size[filePath] = FileSize
    return Size


# use this function to match file md5sums with objoect alias
def LinkFilesWithAlias(CredentialFile, Database, Table, Box):
    '''    
    (str, str, str, str) -> dict
    Take the credentials to log in metadata Database and return a dictionary with file path
    as key and a list of md5sums and accession ID as value
    '''
   
    # connect to db
    conn = EstablishConnection(CredentialFile, Database)
    cur = conn.cursor()
    # extract alias, xml, accession number    
    try:
       cur.execute('SELECT {0}.alias, {0}.xml, {0}.egaAccessionId FROM {0} WHERE {0}.egaBox=\"{1}\"'.format(Table, Box))
       Data = cur.fetchall()
    except:
        Data = []
    conn.close()
   
    # create a dict {filepath: [[md5unc, md5enc, accession, alias]]}    
    Files = {}  
    if len(Data)!= 0:
        for i in Data:
            # parse the xml, extract filenames and md5sums
            alias = i[0]
            tree = ET.ElementTree(ET.fromstring(i[1]))
            accession = i[2]
            j = tree.findall('.//FILE')
            for i in range(len(j)):
                filename = j[i].attrib['filename']
                md5unc = j[i].attrib['unencrypted_checksum']
                md5enc = j[i].attrib['checksum']
                if filename in Files:
                    Files[filename].append([md5unc, md5enc, alias, accession])
                else:
                    Files[filename] = [[md5unc, md5enc, alias, accession]]
    return Files 


# use this function to get file size and metadata for all files on the staging server in a Specific box box
def MergeFileInfoStagingServer(FileSize, RegisteredFiles, Box):
    '''
    (dict, dict, str) - > dict
    Take a dictionary of with file size for all files on the staging server of a
    given Box, and a dictionary of registered files in that Box return a dictionary
    of file information that include file size and metadata for the files on the staging server 
    '''
    
    D = {}
    for filename in FileSize:
        # get the name of the file. check if file on the root on the staging server
        if os.path.basename(filename) == '':
            FileName = filename
        else:
            FileName = os.path.basename(filename)
        # check if file is md5
        if filename[-4:] == '.md5':
            name = filename[:-4]
        else:
            name = filename
        # add filename, size and initialize empty lists to store alias and accessions
        if filename not in D:
            D[filename] = [filename, FileName, str(FileSize[filename]), [], [], Box]
        # check if file is registered
        # file may or may not have .gpg extension in RegisteredFiles
        # .gpg present upon registration but subsenquently removed from file name
        # add aliases and acessions
        if name in RegisteredFiles:
            for i in range(len(RegisteredFiles[name])):
                D[filename][3].append(RegisteredFiles[name][i][-2])
                D[filename][4].append(RegisteredFiles[name][i][-1])
        elif name[-4:] == '.gpg' and name[:-4] in RegisteredFiles:
            for i in range(len(RegisteredFiles[name[:-4]])):
                D[filename][3].append(RegisteredFiles[name[:-4]][i][-2])
                D[filename][4].append(RegisteredFiles[name[:-4]][i][-1])
        elif name[-4:] != '.gpg' and name + '.gpg' in RegisteredFiles:
            for i in range(len(RegisteredFiles[name + '.gpg'])):
                D[filename][3].append(RegisteredFiles[name + '.gpg'][i][-2])
                D[filename][4].append(RegisteredFiles[name + '.gpg'][i][-1])
        else:
            D[filename][3].append('NULL')
            D[filename][4].append('NULL')
        # convert lists to strings
        # check if multiple aliases and accessions exist for that file
        if len(D[filename][3]) > 1:
            D[filename][3] = ';'.join(D[filename][3])
        else:
            D[filename][3] = D[filename][3][0]
        if len(D[filename][4]) > 1:
            D[filename][4]= ';'.join(D[filename][4])
        else:
            D[filename][4] = D[filename][4][0]
    return D


# use this function to add file from the staging server into database
def AddFileInfoStagingServer(CredentialFile, MetDataBase, SubDataBase, AnalysesTable, RunsTable, StagingServerTable, Box):
    '''
    (str, str, str, str, str, str, str) -> None
    Add file info including size and accession IDs for files on the staging server
    of Box in Table StagingServerTable of SubDataBase using credentials to connect to each database
    '''
    
    # list all directories on the staging server of box
    Directories = GrabAllDirectoriesStagingServer(CredentialFile, Box)
    # Extract file size for all files on the staging server
    FileSize = [ExtractFileSizeStagingServer(CredentialFile, Box, i) for i in Directories]
    # Extract md5sums and accessions from the metadata database
    RegisteredAnalyses = LinkFilesWithAlias(CredentialFile, MetDataBase, AnalysesTable, Box)
    RegisteredRuns = LinkFilesWithAlias(CredentialFile, MetDataBase, RunsTable, Box)
        
    # merge registered files for each box
    Registered = {}
    for filename in RegisteredAnalyses:
        Registered[filename] = RegisteredAnalyses[filename]
        if filename in RegisteredRuns:
            Registered[filename].append(RegisteredRuns[filename])
    for filename in RegisteredRuns:
        if filename not in Registered:
            Registered[filename] = RegisteredRuns[filename]
                    
    # cross-reference dictionaries and get aliases and accessions for files on staging servers if registered
    Data = [MergeFileInfoStagingServer(D, Registered, Box) for D in FileSize]
                
    # create table if table doesn't exist
    Tables = ListTables(CredentialFile, SubDataBase)
    if StagingServerTable not in Tables:
        # connect to submission database
        conn = EstablishConnection(CredentialFile, SubDataBase)
        cur = conn.cursor()
        # format colums with datatype and convert to string
        Fields = ["file", "filename", "fileSize", "alias", "egaAccessionId", "egaBox"]
        Columns = ' '.join([Fields[i] + ' TEXT NULL,' if i != len(Fields) -1 else Fields[i] + ' TEXT NULL' for i in range(len(Fields))])
        cur.execute('CREATE TABLE {0} ({1})'.format(StagingServerTable, Columns))
        conn.commit()
        conn.close()
    else:
        # connect to submission database
        conn = EstablishConnection(CredentialFile, SubDataBase)
        cur = conn.cursor()
        # get the column headers from the table
        cur.execute("SELECT * FROM {0}".format(StagingServerTable))
        Fields = [i[0] for i in cur.description]
        conn.close()

    # connect to submission database
    conn = EstablishConnection(CredentialFile, SubDataBase)
    cur = conn.cursor()
    # format colums with datatype and convert to string
    Fields = ["file", "filename", "fileSize", "alias", "egaAccessionId", "egaBox"]
    Columns = ' '.join([Fields[i] + ' TEXT NULL,' if i != len(Fields) -1 else Fields[i] + ' TEXT NULL' for i in range(len(Fields))])
    # create a string with column headers
    ColumnNames = ', '.join(Fields)

    # drop all entries for that Box
    cur.execute('DELETE FROM {0} WHERE {0}.egaBox=\"{1}\"'.format(StagingServerTable, Box))
    conn.commit()
        
    # loop over dicts
    for i in range(len(Data)):
        # list values according to the table column order
        for filename in Data[i]:
            # convert data to strings, converting missing values to NULL
            Values =  FormatData(Data[i][filename])
            cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(StagingServerTable, ColumnNames, Values))
            conn.commit()
    conn.close()            


# use this function to add information to Footprint table
def AddFootprintData(CredentialFile, SubDataBase, StagingServerTable, FootPrintTable):
    '''
    (str, str, str, str) -> None
    Use credentials to connect to SubDatabase, extract file information from StagingServerTable
    and collapse it per directory in each staging server in FootPrintTable
    '''
    
    # connect to submission database
    conn = EstablishConnection(CredentialFile, SubDataBase)
    cur = conn.cursor()
    try:
        cur.execute('SELECT * FROM {0}'.format(StagingServerTable))
        Data = cur.fetchall()
    except:
        Data = []
    conn.close()
        
    Size = {}
    if len(Data) != 0:
        for i in Data:
            filesize = int(i[2])
            filename = i[0]
            directory = os.path.dirname(filename)
            box = i[-1]
            if directory == '':
                directory = '/'
            if i[4] not in ['NULL', '', None]:
                registered = True
            else:
                registered = False
            if box not in Size:
                Size[box] = {}
            if directory not in Size[box]:
                Size[box][directory] = [directory, 1, 0, 0, filesize, 0, 0]
            else:
                Size[box][directory][4] += filesize
                Size[box][directory][1] += 1
            if registered  == True:
                Size[box][directory][2] += 1
                Size[box][directory][5] += filesize
            else:
                 Size[box][directory][3] += 1
                 Size[box][directory][6] += filesize
        
        # compute size for all files per box
        for i in Data:
            box = i[-1]
            Size[box]['All'] = ['All', 0, 0, 0, 0, 0, 0]
        for box in Size:
            for directory in Size[box]:
                if directory != 'All':
                    cumulative = [x + y for x, y in list(zip(Size[box]['All'], Size[box][directory]))]
                    Size[box]['All'] = cumulative
            # correct directory value
            Size[box]['All'][0] = 'All'
        
        # connect to submission database
        conn = EstablishConnection(CredentialFile, SubDataBase)
        cur = conn.cursor()
  
        Fields = ["egaBox", "location", "AllFiles", "Registered", "NotRegistered", "Size", "SizeRegistered", "SizeNotRegistered"]
        # format colums with datatype - convert to string
        Columns = ' '.join([Fields[i] + ' TEXT NULL,' if i != len(Fields) -1 else Fields[i] + ' TEXT NULL' for i in range(len(Fields))])
        # create a string with column headers
        ColumnNames = ', '.join(Fields)

        SqlCommand = ['DROP TABLE IF EXISTS {0}'.format(FootPrintTable), 'CREATE TABLE {0} ({1})'.format(FootPrintTable, Columns)]
        for i in SqlCommand:
            cur.execute(i)
            conn.commit()
               
        # loop over data in boxes
        for box in Size:
            # loop over directory in each box
            for directory in Size[box]:
                # add box to list of data
                L = [box]
                L.extend(Size[box][directory])
                # list values according to the table column order
                # convert data to strings, converting missing values to NULL
                Values =  FormatData(list(map(lambda x: str(x), L)))
                cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(FootPrintTable, ColumnNames, Values))
                conn.commit()
    conn.close()            
                

# use this function to get the available disk space on the staging server
def GetDiskSpaceStagingServer(CredentialFile, DataBase, FootPrint, Box):
    '''
    (str, str, str, str) -> float
    Take a file with credentials to connect to EGA submission database and 
    extract the footprint of non-registered files in that Box (in Tb)
    '''
    
    # connect to database
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
    try:
        # extract files for alias in upload mode for given box
        cur.execute('SELECT {0}.SizeNotRegistered from {0} WHERE {0}.location=\"All\" AND {0}.egaBox=\"{1}\"'.format(FootPrint, Box))
        # check that some alias are in upload mode
        Data = int(cur.fetchall()[0][0]) / (10**12)
    except:
        Data = -1
    # close connection
    conn.close()
    return Data


## functions specific to Analyses objects
    
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
def EncryptAndChecksum(CredentialFile, DataBase, Table, Box, alias, filePaths, fileNames, KeyRing, OutDir, Queue, Mem, MyScript):
    '''
    (file, str, str, str, str, list, list, str, str, str, int, str) -> list
    Take the file with credential to connect to db, a given alias for Box in Table,
    lists with file paths and names, the path to the encryption keys, the directory
    where encrypted and cheksums are saved, the queue and memory allocated to run
    the jobs and return a list of exit codes specifying if the jobs were launched
    successfully or not
    '''

    MyCmd1 = 'md5sum {0} | cut -f1 -d \' \' > {1}.md5'
    MyCmd2 = 'gpg --no-default-keyring --keyring {2} -r EGA_Public_key -r SeqProdBio --trust-model always -o {1}.gpg -e {0}'
    MyCmd3 = 'md5sum {0}.gpg | cut -f1 -d \' \' > {0}.gpg.md5'
    
    # check that lists of file paths and names have the same number of entries
    if len(filePaths) != len(fileNames):
        return [-1]
    else:
        # make a list to store the job names and job exit codes
        JobExits, JobNames = [], []
        # loop over files for that alias      
        for i in range(len(filePaths)):
            # check that FileName is valid
            if os.path.isfile(filePaths[i]) == False:
                # return error that will be caught if file doesn't exist
                return [-1] 
            else:
                # check if OutDir exist
                if os.path.isdir(OutDir) == False:
                    return [-1] 
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
                    OutFile = os.path.join(OutDir, fileNames[i])
                    # put commands in shell script
                    BashScript1 = os.path.join(qsubdir, alias + '_' + fileNames[i] + '_md5sum_original.sh')
                    BashScript2 = os.path.join(qsubdir, alias + '_' + fileNames[i] + '_encrypt.sh')
                    BashScript3 = os.path.join(qsubdir, alias + '_' + fileNames[i] + '_md5sum_encrypted.sh')
            
                    with open(BashScript1, 'w') as newfile:
                        newfile.write(MyCmd1.format(filePaths[i], OutFile) + '\n')
                    with open(BashScript2, 'w') as newfile:
                        newfile.write(MyCmd2.format(filePaths[i], OutFile, KeyRing) + '\n')
                    with open(BashScript3, 'w') as newfile:
                        newfile.write(MyCmd3.format(OutFile) + '\n')
        
                    # launch qsub directly, collect job names and exit codes
                    JobName1 = 'Md5sum.original.{0}'.format(alias + '__' + fileNames[i])
                    # check if 1st file in list
                    if i == 0:
                        QsubCmd1 = "qsub -b y -q {0} -l h_vmem={1}g -N {2} -e {3} -o {3} \"bash {4}\"".format(Queue, Mem, JobName1, logDir, BashScript1)
                    else:
                        # launch job when previous job is done
                        QsubCmd1 = "qsub -b y -q {0} -hold_jid {1} -l h_vmem={2}g -N {3} -e {4} -o {4} \"bash {5}\"".format(Queue, JobNames[-1], Mem, JobName1, logDir, BashScript1)
                    job1 = subprocess.call(QsubCmd1, shell=True)
                                   
                    JobName2 = 'Encrypt.{0}'.format(alias + '__' + fileNames[i])
                    QsubCmd2 = "qsub -b y -q {0} -hold_jid {1} -l h_vmem={2}g -N {3} -e {4} -o {4} \"bash {5}\"".format(Queue, JobName1, Mem, JobName2, logDir, BashScript2)
                    job2 = subprocess.call(QsubCmd2, shell=True)
                            
                    JobName3 = 'Md5sum.encrypted.{0}'.format(alias + '__' + fileNames[i])
                    QsubCmd3 = "qsub -b y -q {0} -hold_jid {1} -l h_vmem={2}g -N {3} -e {4} -o {4} \"bash {5}\"".format(Queue, JobName2, Mem, JobName3, logDir, BashScript3)
                    job3 = subprocess.call(QsubCmd3, shell=True)
                            
                    # store job names and exit codes
                    JobExits.extend([job1, job2, job3])
                    JobNames.extend([JobName1, JobName2, JobName3])
        
        # launch check encryption job
        MyCmd = 'sleep 300; module load python-gsi/3.6.4; python3.6 {0} CheckEncryption -c {1} -s {2} -t {3} -b {4} -a {5} -j {6}'
        # put commands in shell script
        BashScript = os.path.join(qsubdir, alias + '_check_encryption.sh')
        with open(BashScript, 'w') as newfile:
            newfile.write(MyCmd.format(MyScript, CredentialFile, DataBase, Table, Box, alias, ':'.join(JobNames)) + '\n')
                
        # launch qsub directly, collect job names and exit codes
        JobName = 'CheckEncryption.{0}'.format(alias)
        # launch job when previous job is done
        QsubCmd = "qsub -b y -q {0} -hold_jid {1} -l h_vmem={2}g -N {3} -e {4} -o {4} \"bash {5}\"".format(Queue, JobNames[-1], Mem, JobName, logDir, BashScript)
        job = subprocess.call(QsubCmd, shell=True)
        # store the exit code (but not the job name)
        JobExits.append(job)          
        
        return JobExits



# use this function to encrypt files and update status to encrypting
def EncryptFiles(CredentialFile, DataBase, Table, Box, KeyRing, Queue, Mem, DiskSpace, MyScript):
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
                    # create parallel lists of file paths and names
                    filePaths, fileNames = [] , [] 
                    # loop over files for that alias
                    for file in files:
                        # get the filePath and fileName
                        filePaths.append(files[file]['filePath'])
                        fileNames.append(files[file]['fileName'])

                    # update status -> encrypting
                    conn = EstablishConnection(CredentialFile, DataBase)
                    cur = conn.cursor()
                    cur.execute('UPDATE {0} SET {0}.Status=\"encrypting\", {0}.errorMessages=\"None\" WHERE {0}.alias=\"{1}\" AND {0}.egaBox=\"{2}\"'.format(Table, alias, Box))
                    conn.commit()
                    conn.close()

                    # encrypt and run md5sums on original and encrypted files and check encryption status
                    JobCodes = EncryptAndChecksum(CredentialFile, DataBase, Table, Box, alias, filePaths, fileNames, KeyRing, WorkingDir, Queue, Mem, MyScript)
                    # check if encription was launched successfully
                    if not (len(set(JobCodes)) == 1 and list(set(JobCodes))[0] == 0):
                        # store error message, reset status encrypting --> encrypt
                        Error = 'Could not launch encryption jobs'
                        conn = EstablishConnection(CredentialFile, DataBase)
                        cur = conn.cursor()
                        cur.execute('UPDATE {0} SET {0}.Status=\"encrypt\", {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, Error, alias, Box))
                        conn.commit()
                        conn.close()
 
        
# use this function to check that encryption is done for a given alias
def CheckEncryption(CredentialFile, DataBase, Table, Box, Alias, JobNames):
    '''
    (file, str, str, str, str, str) -> None
    Take the file with DataBase credentials, a semicolon-seprated string of job
    names used for encryption and md5sum of all files under Alias, extract information
    from Table regarding Alias with encrypting Status and update status to upload and
    files with md5sums when encrypting is done
    '''        
        
    # make a list of job names
    JobNames = JobNames.split(':')
    
    # connect to database
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
    # pull alias files and working directory for Alias with status = encrypting
    try:
        cur.execute('SELECT {0}.alias, {0}.files, {0}.WorkingDirectory FROM {0} WHERE {0}.Status=\"encrypting\" AND {0}.egaBox=\"{1}\" AND {0}.alias=\"{2}\"'.format(Table, Box, Alias))
        Data = cur.fetchall()
    except:
        Data = []
    conn.close()
    # check that files are in encrypting mode for this Alias
    if len(Data) != 0:
        Data = Data[0]
        alias = Data[0]
        # get the working directory for that alias
        WorkingDir = GetWorkingDirectory(Data[2])
        # convert single quotes to double quotes for str -> json conversion
        files = json.loads(Data[1].replace("'", "\""))
        # create a dict to store the updated file info
        Files = {}
                
        # create boolean, update when md5sums and encrypted file not found or if jobs didn't exit properly 
        Encrypted = True
        
        # check the exit status of each encryption and md5sum jobs for that alis
        for jobName in JobNames:
            if GetJobExitStatus(jobName) != '0':
                Encrypted = False
        
        # check that files were encrypted and that md5sums were generated
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
    else:
        # couldn't evaluate encryption, record error and reset to encrypt
        # reset status encrypting -- > encrypt, record error message
        Error = 'Could not check encryption'
        conn = EstablishConnection(CredentialFile, DataBase)
        cur = conn.cursor()
        cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\", {0}.Status=\"encrypt\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, Error, Alias, Box))
        conn.commit()
        conn.close()

# use this script to launch qsubs to encrypt the files and do a checksum
def UploadAliasFiles(alias, files, StagePath, FileDir, CredentialFile, DataBase, Table, AttributesTable, Box, Queue, Mem, UploadMode, MyScript):
    '''
    (str, dict, str, str, str, str, str, str, str, str, str, str, str) -> list
    Take a files dictionary with file information for a given alias in Box, the file with 
    DataBase credentials, the Table names, the directory StagePath where to upload
    the files in UploadMode, the directory FileDir where the command scripts are saved, the queue
    name, memory and path to script to launch the jobs and return a list of
    exit codes used for uploading the encrypted and md5 files 
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
    
    # command to upload files. aspera is installed on xfer4
    if UploadMode == 'lftp':
        UploadCmd = "ssh xfer4.res.oicr.on.ca \"lftp -u {0},{1} -e \\\" set ftp:ssl-allow false; mput {3} {4} {5} -O {2}  bye;\\\" ftp://ftp-private.ebi.ac.uk\""
    elif UploadMode == 'aspera':
        UploadCmd = "ssh xfer4.res.oicr.on.ca \"export ASPERA_SCP_PASS={0};ascp -P33001 -O33001 -QT -l300M {1} {2}@fasp.ega.ebi.ac.uk:{3};ascp -P33001 -O33001 -QT -l300M {4} {2}@fasp.ega.ebi.ac.uk:{3};ascp -P33001 -O33001 -QT -l300M {5} {2}@fasp.ega.ebi.ac.uk:{3};\""
      
    # create parallel lists to store the job names and exit codes
    JobExits, JobNames = [], []
    # make a list of file paths
    filePaths = list(files.keys())
    # loop over filepaths
    for i in range(len(filePaths)):
        # create destination directory
        Cmd = "ssh xfer4.res.oicr.on.ca \"lftp -u {0},{1} -e \\\" set ftp:ssl-allow false; mkdir -p {2}; bye;\\\" ftp://ftp-private.ebi.ac.uk\""
        # put commands in shell script
        BashScript = os.path.join(qsubdir, alias + '_make_destination_directory.sh')
        with open(BashScript, 'w') as newfile:
            newfile.write(Cmd.format(UserName, MyPassword, StagePath))    
        # launch job directly for the 1st file only
        JobName = 'MakeDestinationDir.{0}'.format(alias)
        QsubCmd = "qsub -b y -q {0} -N {1} -e {2} -o {2} \"bash {3}\"".format(Queue, JobName, logDir, BashScript)
        if JobName not in JobNames:
            job = subprocess.call(QsubCmd, shell=True)
            # record job name but not exit code.
            # may produce an error message if directory already exists. do not evaluate command during CheckUpload
            JobNames.append(JobName)
            
        # get filename
        fileName = os.path.basename(filePaths[i])
        encryptedFile = os.path.join(FileDir, files[filePaths[i]]['encryptedName'])
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
            # hold until previous job is done
            QsubCmd = "qsub -b y -q {0} -hold_jid {1} -l h_vmem={2}g -N {3} -e {4} -o {4} \"bash {5}\"".format(Queue, JobNames[-1], Mem, JobName, logDir, BashScript)
            job = subprocess.call(QsubCmd, shell=True)
            # store job exit code and name
            JobExits.append(job)
            JobNames.append(JobName)
        else:
            return [-1]
    
    # launch check upload job
    CheckCmd = 'sleep 600; module load python-gsi/3.6.4; python3.6 {0} CheckUpload -c {1} -s {2} -t {3} -b {4} -a {5} --Attributes {6} -j {7}'
    # do not check job used to make destination directory
    JobNames = JobNames[1:]
    # put commands in shell script
    BashScript = os.path.join(qsubdir, alias + '_check_upload.sh')
    with open(BashScript, 'w') as newfile:
        newfile.write(CheckCmd.format(MyScript, CredentialFile, DataBase, Table, Box, alias, AttributesTable, ':'.join(JobNames)) + '\n')
                
    # launch qsub directly, collect job names and exit codes
    JobName = 'CheckUpload.{0}'.format(alias)
    # launch job when previous job is done
    QsubCmd = "qsub -b y -q {0} -hold_jid {1} -l h_vmem={2}g -N {3} -e {4} -o {4} \"bash {5}\"".format(Queue, JobNames[-1], Mem, JobName, logDir, BashScript)
    job = subprocess.call(QsubCmd, shell=True)
    # store the exit code (but not the job name)
    JobExits.append(job)          
        
    return JobExits

# use this function to upload the files
def UploadAnalysesObjects(CredentialFile, DataBase, Table, AttributesTable, FootPrintTable, Box, Queue, Mem, UploadMode, Max, MaxFootPrint, MyScript):
    '''
    (file, str, str, str, str, int, int, str) -> None
    Take the file with credentials to connect to the database and to EGA,
    and upload files of aliases with upload status using specified
    Queue, Memory and UploadMode and update status to uploading. 
    '''
    
    # connect to database
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
    
    try:
        # extract files for alias in upload mode for given box
        cur.execute('SELECT {0}.alias, {0}.files, {0}.WorkingDirectory, {1}.StagePath FROM {0} JOIN {1} WHERE {0}.Status=\"upload\" AND {0}.egaBox=\"{2}\" AND {0}.attributes = {1}.alias'.format(Table, AttributesTable, Box))
        # check that some alias are in upload mode
        Data = cur.fetchall()
    except:
        Data = []
    # close connection
    conn.close()
        
    # get the footprint of non-registered files on the Box's staging server
    NotRegistered = GetDiskSpaceStagingServer(CredentialFile, DataBase, FootPrintTable, Box)
    
    # check that alias are ready for uploading and that staging server's limit is not reached 
    if len(Data) != 0 and 0 < NotRegistered < MaxFootPrint:
        # count the number of files being uploaded
        Uploading = int(subprocess.check_output('qstat | grep Upload | wc -l', shell=True).decode('utf-8').rstrip())        
        # upload new files up to Max
        Maximum = int(Max) - Uploading
        if Maximum < 0:
            Maximum = 0
        Data = Data[: Maximum]
        
        for i in Data:
            alias = i[0]
            # get the file information, working directory and stagepath for that alias
            files = json.loads(i[1].replace("'", "\""))
            WorkingDir = GetWorkingDirectory(i[2])
            assert '/scratch2/groups/gsi/bis/EGA_Submissions' in WorkingDir
            StagePath  = i[3]
                
            # update status -> uploading
            conn = EstablishConnection(CredentialFile, DataBase)
            cur = conn.cursor()
            cur.execute('UPDATE {0} SET {0}.Status=\"uploading\", {0}.errorMessages=\"None\"  WHERE {0}.alias=\"{1}\" AND {0}.egaBox=\"{2}\";'.format(Table, alias, Box))
            conn.commit()
            conn.close()
            
            # upload files
            JobCodes = UploadAliasFiles(alias, files, StagePath, WorkingDir, CredentialFile, DataBase, Table, AttributesTable, Box, Queue, Mem, UploadMode, MyScript)
            # check if upload launched properly for all files under that alias
            if not (len(set(JobCodes)) == 1 and list(set(JobCodes))[0] == 0):
                # record error message, reset status same uploading --> upload
                Error = 'Could not launch upload jobs'
                conn = EstablishConnection(CredentialFile, DataBase)
                cur = conn.cursor()
                cur.execute('UPDATE {0} SET {0}.Status=\"upload\", {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, Error, alias, Box))
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
    (str, str, str, str, int) -> list
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
    
    # substract file size for all aliases with encrypting status from available size
    for alias in Encrypting:
        available -= Encrypting[alias]
        
    # record aliases for encryption
    Aliases = []
    for alias in Encrypt:
        # do not encrypt if the new files result is < DiskSpace of disk availability 
        if available - Encrypt[alias] > DiskSpace:
            available -= Encrypt[alias]
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
    (str, str, str) --> bool
    Take the directory where logs of the upload script are saved, an alias and 
    the file name and retrieve the most recent out log and return True if all
    files are uploaded (ie no error) or False if errors are found
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
    
    
# use this function to check that files were successfully uploaded for a given alias and update status uploading -> uploaded
def CheckUploadFiles(CredentialFile, DataBase, Table, AttributesTable, Box, Alias, JobNames):
    '''
    (str, str, str, str, str, str, str) -> None
    Take the file with db credentials, a semicolon-separated string of job names
    used for uploading files under Alias, the table names and box for the Database
    and update status of Alias from uploading to uploaded if all the files
    for that alias were successfuly uploaded. 
    '''

    # parse credential file to get EGA username and password
    UserName, MyPassword = ParseCredentials(CredentialFile, Box)
        
    # make a dict {directory: [files]} for alias with uploading status 
    FilesBox = ListFilesStagingServer(CredentialFile, DataBase, Table, AttributesTable, Box)
        
    # connect to database
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
    try:
        # extract files for alias in uploading mode for given box
        cur.execute('SELECT {0}.alias, {0}.files, {0}.WorkingDirectory, {1}.StagePath FROM {0} JOIN {1} WHERE {0}.attributes = {1}.alias AND {0}.Status=\"uploading\" AND {0}.egaBox=\"{2}\" AND {0}.alias=\"{3}\"'.format(Table, AttributesTable, Box, Alias))
        Data = cur.fetchall()
    except:
        Data= []
    conn.close()
        
    if len(Data) != 0:
        # check that some files are in uploading mode
        for i in Data:
            alias = i[0]
            # convert single quotes to double quotes for str -> json conversion
            files = json.loads(i[1].replace("'", "\""))
            WorkingDirectory = GetWorkingDirectory(i[2])
            StagePath = i[3]
            # set up boolean to be updated if uploading is not complete
            Uploaded = True
            
            # get the log directory
            LogDir = os.path.join(WorkingDirectory, 'qsubs/log')
            # check the out logs for each file
            for filePath in files:
                # get filename
                filename = os.path.basename(filePath)
                # check if errors are found in log
                if CheckUploadSuccess(LogDir, alias, filename) == False:
                    Uploaded = False
            
            # check the exit status of the jobs uploading files
            for jobName in JobNames.split(':'):
                if GetJobExitStatus(jobName) != '0':
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
    else:
        # reset status uploading --> upload, record error message
        Error = 'Could not check uploaded files'
        conn = EstablishConnection(CredentialFile, DataBase)
        cur = conn.cursor()
        cur.execute('UPDATE {0} SET {0}.Status=\"upload\", {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, Error, Alias, Box)) 
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

#################

## functions specific to datasets

















########################



## functions specific to Samples objects

# use this function to parse the input sample table
def ParseSampleInputTable(Table):
    '''
    (file) -> list
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
    L = ["alias", "caseOrControlId", "genderId", "organismPart", "cellLine",
         "region", "phenotype", "subjectId", "anonymizedName", "biosampleId",
         "sampleAge", "sampleDetail"]
    Missing = [i for i in L if i not in Header]
    
    if len(Missing) != 0:
        print('These required fields are missing: {0}'.format(', '.join(Missing)))
    else:
        # required fields are present, read the content of the file
        Content = infile.read().rstrip().split('\n')
        for S in Content:
            S = list(map(lambda x: x.strip(), S.split('\t')))
            # missing values are not permitted
            if len(Header) != len(S):
                print('missing values are not permitted. Empty strings and NA are allowed')
            else:
                # create a dict to store the key: value pairs
                D = {}
                # get the alias name
                alias = S[Header.index('alias')]
                D[alias] = {}
                for i in range(len(S)):
                    assert Header[i] not in D[alias]
                    D[alias][Header[i]] = S[i]    
                L.append(D)    
    infile.close()
    return L        


# use this function to parse the attributes sample table
def ParseSampleAttributesTable(Table):
    '''
    (file) -> dict
    Take a tab-delimited file and return a dictionary storing sample attributes
    Preconditions: Required fields must be present or returned list is empty,
    and missing entries are not permitted (e.g. can be '', NA)
    '''
    
    infile = open(Table)
    Content = infile.read().rstrip().split('\n')
    infile.close()
    # create a dict {key: value}
    D = {}
    # check that required fields are present
    Expected = ['alias', 'title', 'description']
    Fields = [S.split(':')[0].strip() for S in Content if ':' in S]
    Missing = [i for i in Expected if i not in Fields]
    if len(Missing) != 0:
        print('These required fields are missing: {0}'.format(', '.join(Missing)))
    else:
        for S in Content:
            S = list(map(lambda x: x.strip(), S.split(':')))
            if S[0] != 'attributes':
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
    infile.close()
    return D
 


## functions to run script    
   
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
    

# use this function to add data to the dataset table
def AddDatasetInfo(args):
    '''
    (list) -> None
    Take a list of command line arguments and add dataset information
    to the Dataset Table of the EGAsub database if samples are not already registered
    Precondition: can only add infor for a single dataset at one time
    '''
    
    # check if accessions are valid
    # check if accessions are provided as file or list in the command
    if args.accessionfile:
        # check that path is valid
        if os.path.isfile(args.accessionfile):
            # accessions are provided by input file
            infile = open(args.accessionfile)
            AccessionIds = infile.read().rstrip().split('\n')
            infile.close()
        else:
            print('Provide a valid file with accessions')
    elif args.accessions:
        # accessions are provided from the command
        AccessionIds = args.accessions
    else:
        # provide empty list
        AccessionIds = []
    
    # check if accessions contains information
    if len(AccessionIds) == 0:
        print('Accessions are required')
    else:
        # record dataset information only if Runs and/or Analyses accessions have been provided
        if False in list(map(lambda x: x.startswith('EGAZ') or x.startswith('EGAR'), AccessionIds)):
            print('Accessions should start with EGAR or EGAZ')
    
    # check if dataset links are provided
    datasetslinks = []
    ValidURLs = True
    if args.datasetslinks:
        # check if valid file
        if os.path.isfile(args.datasetslinks):
            infile = open(args.datasetslinks)
            for line in infile:
                if 'https' in line:
                    line = line.rstrip().split()
                    datasetslinks.append({"label": line[0], "url": line[1]})
                else:
                    ValidURLs = False
            infile.close()
        else:
            print('Provide valid file with URLs')
    
    # check if attributes are provided
    attributes = []
    ValidAttributes = True
    if args.attributes:
        # check if valid file
        if os.path.isfile(args.attributes):
            infile = open(args.attributes)
            for line in infile:
                line = line.rstrip()
                if line != '':
                    line = line.split('\t')
                    if len(line) == 2:
                        attributes.append({"tag": line[0], "value": line[1]})
                    else:
                        ValidAttributes = False
            infile.close()
        else:
            print('Provide valid attributes file')
    
    # check if provided data is valid
    if ValidAttributes and ValidURLs and len(AccessionIds) != 0 and False not in list(map(lambda x: x.startswith('EGAZ') or x.startswith('EGAR'), AccessionIds)):
        # create table if table doesn't exist
        Tables = ListTables(args.credential, args.subdb)
        # connect to submission database
        conn = EstablishConnection(args.credential, args.subdb)
        cur = conn.cursor()
        if args.table not in Tables:
            Fields = ["alias", "datasetTypeIds", "policyId", "runsReferences",
                      "analysisReferences", "title", "description", "datasetLinks",
                      "attributes", "Json", "submissionStatus", "errorMessages", "Receipt",
                      "CreationTime", "egaAccessionId", "egaBox", "Status"]
            # format colums with datatype
            Columns = []
            for i in range(len(Fields)):
                if Fields[i] == 'Status':
                    Columns.append(Fields[i] + ' TEXT NULL')
                elif Fields[i] in ['Json', 'Receipt']:
                    Columns.append(Fields[i] + ' MEDIUMTEXT NULL,')
                elif Fields[i] in ['runsReferences', 'analysisReferences']:
                    Columns.append(Fields[i] + ' LONGTEXT NULL,')
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
    
        # pull down alias from submission db. alias may be recorded but not submitted yet. aliases must be unique and not already recorded in the same box
        # create a dict {alias: accession}
        cur.execute('SELECT {0}.alias from {0} WHERE {0}.egaBox=\"{1}\"'.format(args.table, args.box))
        Recorded = [i[0] for i in cur]
        # pull down dataset alias and egaId from metadata db, alias should be unique
        # create a dict {alias: accession} 
        Registered = ExtractAccessions(args.credential, args.metadatadb, args.box, args.table)
    
        # check if alias is recorded or registered
        if args.alias in Recorded:
            # skip, already recorded in submission database
            print('{0} is already recorded for box {1} in the submission database'.format(args.alias, args.box))
        elif args.alias in Registered:
            # skip, already registered in EGA
            print('{0} is already registered in box {1} under accession {2}'.format(args.alias, args.box, Registered[args.alias]))
        else:
            # sort Runs and Analyses Id
            runsReferences = [i for i in AccessionIds if i.startswith('EGAR')]
            analysisReferences = [i for i in AccessionIds if i.startswith('EGAZ')]
            
            # make a list of data ordered according to columns
            D = {"alias": args.alias, "datasetTypeIds": ';'.join(args.datasetTypeIds),
                    "policyId": args.policy, "runsReferences": ';'.join(runsReferences),
                    "analysisReferences": ';'.join(analysisReferences), "title": args.title,
                    "description": args.description, "datasetLinks": ';'.join(datasetslinks),
                    "attributes": ';'.join(attributes)}            
            # list values according to the table column order
            L = [D[field] if field in D else '' for field in Fields]
            # convert data to strings, converting missing values to NULL
            Values = FormatData(L)        
            cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(args.table, ColumnNames, Values))
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

    # parse input table
    Data = ParseSampleInputTable(args.input)
    
    # create table if table doesn't exist
    Tables = ListTables(args.credential, args.subdb)
    
    # connect to submission database
    conn = EstablishConnection(args.credential, args.subdb)
    cur = conn.cursor()
    
    if args.table not in Tables:
        Fields = ["alias", "caseOrControlId", "genderId", "organismPart", "cellLine",
                  "region", "phenotype", "subjectId", "anonymizedName", "biosampleId",
                  "sampleAge", "sampleDetail", "Json", "submissionStatus", "errorMessages", "Receipt",
                  "CreationTime", "egaAccessionId", "egaBox", "attributes", "Status"]
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
    
    # pull down alias from submission db. alias may be recorded but not submitted yet. aliases must be unique and not already recorded in the same box
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
                D[alias]['attributes'], D[alias]['egaBox'] = args.attributes, args.box 
                # add alias
                D[alias]['sampleAlias'] = alias    
                # set Status to ready
                D[alias]["Status"] = "ready"
                # list values according to the table column order
                L = [D[alias][field] if field in D[alias] else '' for field in Fields]
                # convert data to strings, converting missing values to NULL                    L
                Values = FormatData(L)        
                cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(args.table, ColumnNames, Values))
                conn.commit()
    conn.close()            


# use this function to add data to SampleAttributes table
def AddSampleAttributes(args):
    '''
    Take a list of command line arguments and add attributes information
    to the SamplesAttributes Table of the EGASUBsub database if alias not already present
    '''
    
    # parse attribues table
    D = ParseSampleAttributesTable(args.input)
    
    # create a list of tables
    Tables = ListTables(args.credential, args.subdb)

    # connect to submission database
    conn = EstablishConnection(args.credential, args.subdb)
    cur = conn.cursor()
    
    if args.table not in Tables:
        Fields = ["alias", "title", "description", "attributes"]
        
        # format colums with datatype
        Columns = []
        for i in range(len(Fields)):
            if Fields[i] == 'attributes':
                Columns.append(Fields[i] + ' MEDIUMTEXT NULL')
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
    RequiredFields = {"alias", "title", "description"}
    if RequiredFields.intersection(set(D.keys())) == RequiredFields:
        # get alias
        if D['alias'] in Recorded:
            # skip sample, already recorded in submission database
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
                    fileTypeId = filePath[-3:].lower()
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


# use this function to check encryption
def IsEncryptionDone(args):
    '''
    (list) -> None
    Take a list of command line arguments and update status to upload if encryption
    is done for a given alias or reset status to encrypt
    '''
    # check that encryption is done, store md5sums and path to encrypted file in db, update status encrypting -> upload 
    CheckEncryption(args.credential, args.subdb, args.table, args.box, args.alias, args.jobnames)
  
    
# use this function to check upload    
def IsUploadDone(args):
    '''    
    (list) -> None
    Take a list of command line arguments and update status to uploaded if upload
    is done for a given alias or reset status to upload
    '''
    # check that files have been successfully uploaded, update status uploading -> uploaded or rest status uploading -> upload
    CheckUploadFiles(args.credential, args.subdb, args.table, args.attributes, args.box, args.alias, args.jobnames)
    

# use this function to form json for Analyses objects
def FormAnalysesJson(args):
    '''
    (list) -> None
    Take a list of command line arguments and encrypt, upload and register analysis
    objects to EGA following sequential steps that depend on the analysis status mode
    '''

    # check if Analyses table exists
    Tables = ListTables(args.credential, args.subdb)
    if args.table in Tables:
        
        ## check if required information is present in table
        CheckTableInformation(args.credential, args.subdb, args.table, 'analyses', args.box, args.myscript, args.mypython)
        ## check if required information is present in attributes table
        CheckTableInformation(args.credential, args.subdb, args.table, 'analyses', args.box, args.myscript, args.mypython, attributes = args.attributes)
        ## check if required information is present in attributes table
        CheckTableInformation(args.credential, args.subdb, args.table, 'analyses', args.box, args.myscript, args.mypython, projects = args.projects)
        # change status ready --> valid if no error or keep status ready --> ready
        CheckObjectInformation(args.credential, args.subdb, args.table, args.box)
                
        ## set up working directory, add to analyses table and update status valid --> start
        AddWorkingDirectory(args.credential, args.subdb, args.table, args.box)
        
        ## update Analysis table in submission database with sample accessions and change status start -> encrypt
        AddSampleAccessions(args.credential, args.metadatadb, args.subdb, args.box, args.table)

        ## encrypt new files only if diskspace is available. update status encrypt --> encrypting
        ## check that encryption is done, store md5sums and path to encrypted file in db, update status encrypting -> upload or reset encrypting -> encrypt
        EncryptFiles(args.credential, args.subdb, args.table, args.box, args.keyring, args.queue, args.memory, args.diskspace, args.myscript)
        
        ## upload files and change the status upload -> uploading 
        ## check that files have been successfully uploaded, update status uploading -> uploaded or rest status uploading -> upload
        UploadAnalysesObjects(args.credential, args.subdb, args.table, args.attributes, args.footprint, args.box, args.queue, args.memory, args.uploadmode, args.max, args.maxfootprint, args.myscript)
        
        ## remove files with uploaded status
        RemoveFilesAfterSubmission(args.credential, args.subdb, args.table, args.box, args.remove)
               
        ## form json for analyses in uploaded mode, add to table and update status uploaded -> submit
        AddJsonToTable(args.credential, args.subdb, args.table, args.box, 'analyses', args.myscript, args.mypython, project = args.projects, attributes = args.attributes)

# use this function to form json for Samples objects
def FormSamplesJson(args):
    '''
    (list) -> None
    Take a list of command line arguments and form json with metadata for sample registration
    '''

    # check if Analyses table exists
    Tables = ListTables(args.credential, args.subdb)
    if args.table in Tables and args.attributes in Tables:
        
        ## check if required information is present in table.
        CheckTableInformation(args.credential, args.subdb, args.table, 'samples', args.box, args.myscript, args.mypython)
        ## check if required information is present in attributes table.
        CheckTableInformation(args.credential, args.subdb, args.table, 'samples', args.box, args.myscript, args.mypython, attributes = args.attributes)
        # change status ready --> valid if no error or keep status ready --> ready
        CheckObjectInformation(args.credential, args.subdb, args.table, args.box)
        
        ## form json for samples in valid status add to table
        # update status valid -> submit if no error of keep status --> valid and record errorMessage
        AddJsonToTable(args.credential, args.subdb, args.table, args.box, 'samples', args.myscript, args.mypython, attributes = args.attributes)

       
# use this function to submit object metadata 
def SubmitMetadata(args):
    '''
    (list) -> None
    Take a list of command line arguments and submit json(s) to register a given object
    '''
    
    # check if Analyses table exists
    Tables = ListTables(args.credential, args.subdb)
    if args.table in Tables:
        # clean up objects with VALIDATED_WITH_ERRORS submission status
        DeleteValidatedObjectsWithErrors(args.credential, args.subdb, args.table, args.box, args.object, args.portal)
        # submit analyses with submit status                
        RegisterObjects(args.credential, args.subdb, args.table, args.box, args.object, args.portal)

    
# use this function to list files on the staging servers
def FileInfoStagingServer(args):
    '''
    (list) -> None
    Take a list of command line arguments and populate tables with file info
    including size and accessions Ids of files on the staging servers of available boxes
    '''

    # add file info from each box
    for i in args.box:
        AddFileInfoStagingServer(args.credential, args.metadatadb, args.subdb, args.analysestable, args.runstable, args.stagingtable, i)
    # add data into footprint table
    AddFootprintData(args.credential, args.subdb, args.stagingtable, args.footprinttable)
    
    
    
if __name__ == '__main__':

    # create top-level parser
    parser = argparse.ArgumentParser(prog = 'SubmitToEGA.py', description='manages submission to EGA')
    subparsers = parser.add_subparsers(title='sub-commands', description='valid sub-commands', help = 'sub-commands help')

    # add samples to Samples Table
    AddSamplesParser = subparsers.add_parser('AddSamples', help ='Add sample information to Samples Table')
    AddSamplesParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    AddSamplesParser.add_argument('-m', '--MetadataDb', dest='metadatadb', default='EGA', help='Name of the database collection EGA metadata. Default is EGA')
    AddSamplesParser.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
    AddSamplesParser.add_argument('-t', '--Table', dest='table', default='Samples', help='Samples table. Default is Samples')
    AddSamplesParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
    AddSamplesParser.add_argument('-a', '--Attributes', dest='attributes', help='Primary key in the SamplesAttributes table', required=True)
    AddSamplesParser.add_argument('-i', '--Input', dest='input', help='Input table with sample info to load to submission database', required=True)
    AddSamplesParser.set_defaults(func=AddSampleInfo)

    # add sample attributes to SamplesAttributes Table
    AddSamplesAttributesParser = subparsers.add_parser('AddSamplesAttributes', help ='Add sample attributes information to SamplesAttributes Table')
    AddSamplesAttributesParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    AddSamplesAttributesParser.add_argument('-t', '--Table', dest='table', default='SamplesAttributes', help='SamplesAttributes table. Default is SamplesAttributes')
    AddSamplesAttributesParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
    AddSamplesAttributesParser.add_argument('-i', '--Input', dest='input', help='Input table with sample attributes info', required=True)
    AddSamplesAttributesParser.set_defaults(func=AddSampleAttributes)

    # add analyses to Analyses Table
    AddAnalysesParser = subparsers.add_parser('AddAnalyses', help ='Add analysis information to Analyses Table')
    AddAnalysesParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    AddAnalysesParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Analyses table. Default is Analyses')
    AddAnalysesParser.add_argument('-m', '--MetadataDb', dest='metadatadb', default='EGA', help='Name of the database collection EGA metadata. Default is EGA')
    AddAnalysesParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
    AddAnalysesParser.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
    AddAnalysesParser.add_argument('-i', '--Input', dest='input', help='Input table with analysis info to load to submission database', required=True)
    AddAnalysesParser.add_argument('-p', '--Project', dest='projects', help='Primary key in the AnalysesProjects table', required=True)
    AddAnalysesParser.add_argument('-a', '--Attributes', dest='attributes', help='Primary key in the AnalysesAttributes table', required=True)
    AddAnalysesParser.set_defaults(func=AddAnalysesInfo)

    # add analyses to Analyses Table
    AddAttributesProjectsParser = subparsers.add_parser('AddAttributesProjects', help ='Add information to AnalysesAttributes or AnalysesProjects Tables')
    AddAttributesProjectsParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    AddAttributesProjectsParser.add_argument('-t', '--Table', dest='table', choices = ['AnalysesAttributes', 'AnalysesProjects'], help='Database Tables AnalysesAttributes or AnalysesProjects', required=True)
    AddAttributesProjectsParser.add_argument('-i', '--Input', dest='input', help='Input table with attributes or projects information to load to submission database', required=True)
    AddAttributesProjectsParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
    AddAttributesProjectsParser.add_argument('-d', '--DataType', dest='datatype', choices=['Projects', 'Attributes'], help='Add Projects or Attributes infor to db')
    AddAttributesProjectsParser.set_defaults(func=AddAnalysesAttributesProjects)
    
    # add datasets to Datasets Table
    AddDatasetsParser = subparsers.add_parser('AddDatasets', help ='Add datasets information to Datasets Table')
    AddDatasetsParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    AddDatasetsParser.add_argument('-t', '--Table', dest='table', default='Datasets', help='Datasets table. Default is Datasets')
    AddDatasetsParser.add_argument('-m', '--MetadataDb', dest='metadatadb', default='EGA', help='Name of the database collection EGA metadata. Default is EGA')
    AddDatasetsParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
    AddDatasetsParser.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
    AddDatasetsParser.add_argument('-a', '--Alias', dest='alias', help='Alias for the dataset', required=True)
    AddDatasetsParser.add_argument('-p', '--Policy', dest='policy', help='Policy Id. Must start with EGAP', required=True)
    AddDatasetsParser.add_argument('--Description', dest='description', help='Description. Will be published on the EGA website', required=True)
    AddDatasetsParser.add_argument('--Title', dest='title', help='Short title. Will be published on the EGA website', required=True)
    AddDatasetsParser.add_argument('--DatasetId', dest='datasetTypeIds', nargs='*', help='Dataset Id. A single string or a list. Controlled vocabulary available from EGA enumerations https://ega-archive.org/submission-api/v1/enums/dataset_types', required=True)
    AddDatasetsParser.add_argument('--Accessions', dest='accessions', nargs='*', help='Analyses accession Ids. A single string or a list of EGAR and/or EGAZ accessions. Can also be provided as a list using args.accessionfile')
    AddDatasetsParser.add_argument('--AccessionFile', dest='accessionfile', help='File with analyses accession Ids. Must contains EGAR and/or EGAZ accessions. Can also be provided as a command parameter but accessions passed in a file take precedence')
    AddDatasetsParser.add_argument('--DatasetsLinks', dest='datasetslinks', help='Optional file with dataset URLs')
    AddDatasetsParser.add_argument('--Attributes', dest='attributes', help='Optional file with attributes')
    AddDatasetsParser.set_defaults(func=AddDatasetInfo)
    
    # collect enumerations from EGA
    CollectEnumParser = subparsers.add_parser('Enums', help ='Collect enumerations from EGA')
    CollectEnumParser.add_argument('--URL', dest='url', choices=['https://ega-archive.org/submission-api/v1/enums/path/enums/analysis_file_types',
                                                                 'https://ega-archive.org/submission-api/v1/enums/analysis_types',
                                                                 'https://ega-archive.org/submission-api/v1/enums/case_control',
                                                                 'https://ega-archive.org/submission-api/v1/enums/dataset_types',
                                                                 'https://ega-archive.org/submission-api/v1/enums/experiment_types',
                                                                 'https://ega-archive.org/submission-api/v1/enums/file_types',
                                                                 'https://ega-archive.org/submission-api/v1/enums/genders',
                                                                 'https://ega-archive.org/submission-api/v1/enums/instrument_models',
                                                                 'https://ega-archive.org/submission-api/v1/enums/library_selections',
                                                                 'https://ega-archive.org/submission-api/v1/enums/library_sources',
                                                                 'https://ega-archive.org/submission-api/v1/enums/library_strategies',
                                                                 'https://ega-archive.org/submission-api/v1/enums/reference_chromosomes',
                                                                 'https://ega-archive.org/submission-api/v1/enums/reference_genomes',
                                                                 'https://ega-archive.org/submission-api/v1/enums/study_types'], help='URL with enumerations', required=True)
    CollectEnumParser.set_defaults(func=GrabEgaEnums)

    # form analyses to EGA       
    FormAnalysesJsonParser = subparsers.add_parser('FormAnalysesJson', help ='Form Analyses json for submission to EGA')
    FormAnalysesJsonParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    FormAnalysesJsonParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Database table. Default is Analyses')
    FormAnalysesJsonParser.add_argument('-m', '--MetadataDb', dest='metadatadb', default='EGA', help='Name of the database collection EGA metadata. Default is EGA')
    FormAnalysesJsonParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
    FormAnalysesJsonParser.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
    FormAnalysesJsonParser.add_argument('-p', '--Projects', dest='projects', default='AnalysesProjects', help='DataBase table. Default is AnalysesProjects')
    FormAnalysesJsonParser.add_argument('-a', '--Attributes', dest='attributes', default='AnalysesAttributes', help='DataBase table. Default is AnalysesAttributes')
    FormAnalysesJsonParser.add_argument('-k', '--Keyring', dest='keyring', default='/.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/publickeys/public_keys.gpg', help='Path to the keys used for encryption. Default is /.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/publickeys/public_keys.gpg')
    FormAnalysesJsonParser.add_argument('-q', '--Queue', dest='queue', default='production', help='Queue for encrypting files. Default is production')
    FormAnalysesJsonParser.add_argument('-u', '--UploadMode', dest='uploadmode', default='aspera', choices=['lftp', 'aspera'], help='Use lftp of aspera for uploading files. Use aspera by default')
    FormAnalysesJsonParser.add_argument('-d', '--DiskSpace', dest='diskspace', default=15, type=int, help='Free disk space (in Tb) after encyption of new files. Default is 15TB')
    FormAnalysesJsonParser.add_argument('-f', '--FootPrint', dest='footprint', default='FootPrint', help='Database Table with footprint of registered and non-registered files. Default is Footprint')
    FormAnalysesJsonParser.add_argument('--MyScript', dest='myscript', default= '/.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/dev/SubmissionDB/SubmitToEGA.py', help='Path the EGA submission script. Default is /.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/dev/SubmissionDB/SubmitToEGA.py')
    FormAnalysesJsonParser.add_argument('--MyPython', dest='mypython', default='/.mounts/labs/PDE/Modules/sw/python/Python-3.6.4/bin/python3.6', help='Path the python version. Default is /.mounts/labs/PDE/Modules/sw/python/Python-3.6.4/bin/python3.6')
    FormAnalysesJsonParser.add_argument('--Mem', dest='memory', default='10', help='Memory allocated to encrypting files. Default is 10G')
    FormAnalysesJsonParser.add_argument('--Max', dest='max', default=8, type=int, help='Maximum number of files to be uploaded at once. Default is 8')
    FormAnalysesJsonParser.add_argument('--MaxFootPrint', dest='maxfootprint', default=15, type=int, help='Maximum footprint of non-registered files on the box\'s staging sever. Default is 15Tb')
    FormAnalysesJsonParser.add_argument('--Remove', dest='remove', action='store_true', help='Delete encrypted and md5 files when analyses are successfully submitted. Do not delete by default')
    FormAnalysesJsonParser.set_defaults(func=FormAnalysesJson)

    # check encryption
    CheckEncryptionParser = subparsers.add_parser('CheckEncryption', help='Check that encryption is done for a given alias')
    CheckEncryptionParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    CheckEncryptionParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Database table. Default is Analyses')
    CheckEncryptionParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
    CheckEncryptionParser.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
    CheckEncryptionParser.add_argument('-a', '--Alias', dest='alias', help='Object alias', required=True)
    CheckEncryptionParser.add_argument('-j', '--Jobs', dest='jobnames', help='Colon-separated string of job names used for encryption and md5sums of all files under a given alias', required=True)
    CheckEncryptionParser.set_defaults(func=IsEncryptionDone)
    
    # check upload
    CheckUploadParser = subparsers.add_parser('CheckUpload', help='Check that upload is done for a given alias')
    CheckUploadParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    CheckUploadParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Database table. Default is Analyses')
    CheckUploadParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
    CheckUploadParser.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
    CheckUploadParser.add_argument('-a', '--Alias', dest='alias', help='Object alias', required=True)
    CheckUploadParser.add_argument('-j', '--Jobs', dest='jobnames', help='Colon-separated string of job names used for uploading all files under a given alias', required=True)
    CheckUploadParser.add_argument('--Attributes', dest='attributes', default='AnalysesAttributes', help='DataBase table. Default is AnalysesAttributes')
    CheckUploadParser.set_defaults(func=IsUploadDone)
    
    # register analyses to EGA       
    RegisterAnalysesParser = subparsers.add_parser('RegisterAnalyses', help ='Submit Analyses json to EGA')
    RegisterAnalysesParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    RegisterAnalysesParser.add_argument('-t', '--Table', dest='table', help='Submission database table', required=True)
    RegisterAnalysesParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
    RegisterAnalysesParser.add_argument('-b', '--Box', dest='box', default='ega-box-12', help='Box where samples will be registered. Default is ega-box-12')
    RegisterAnalysesParser.add_argument('-o', '--Object', dest='object', choices=['samples', 'analyses'], help='EGA object to register', required=True)
    RegisterAnalysesParser.add_argument('--Portal', dest='portal', default='https://ega.crg.eu/submitterportal/v1', help='EGA submission portal. Default is https://ega.crg.eu/submitterportal/v1')
    RegisterAnalysesParser.set_defaults(func=SubmitMetadata)

    # list files on the staging servers
    StagingServerParser = subparsers.add_parser('StagingServer', help ='List file info on the staging servers')
    StagingServerParser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    StagingServerParser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the submission database. Default is EGASUB')
    StagingServerParser.add_argument('-m', '--MetadataDb', dest='metadatadb', default='EGA', help='Name of the database used to collect EGA metadata. Default is EGA')
    StagingServerParser.add_argument('-b', '--Box', dest='box', nargs='*', help='Boxes where samples will be registered. One more boxes are required', required=True)
    StagingServerParser.add_argument('--RunsTable', dest='runstable', default='Runs', help='Submission database table. Default is Runs')
    StagingServerParser.add_argument('--AnalysesTable', dest='analysestable', default='Analyses', help='Submission database table. Default is Analyses')
    StagingServerParser.add_argument('--StagingTable', dest='stagingtable', default='StagingServer', help='Submission database table. Default is StagingServer')
    StagingServerParser.add_argument('--FootprintTable', dest='footprinttable', default='FootPrint', help='Submission database table. Default is FootPrint')
    StagingServerParser.set_defaults(func=FileInfoStagingServer)
   
    # get arguments from the command line
    args = parser.parse_args()
    # pass the args to the default function
    args.func(args)
