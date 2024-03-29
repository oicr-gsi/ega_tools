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
import gzip


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
    
    # box name is ega-box-xxxx
    boxname = Box[Box.index('b'):].title().replace('-', '')
    UserKey, PassWordKey = 'UserName' +  boxname, 'MyPassWord' +  boxname
    UserName, MyPassword = Credentials[UserKey], Credentials[PassWordKey]
       
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
    in file system for each alias in Table and given Box and record working directory in Table
    '''
    
    # check if table exists
    Tables = ListTables(CredentialFile, DataBase)
    
    if Table in Tables:
        # connect to db
        conn = EstablishConnection(CredentialFile, DataBase)
        cur = conn.cursor()
        # get the alias with valid status
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
        # get the alias and working directory with valid status
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
                    cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, ';'.join(Error), alias, Box))  
                    conn.commit()
                else:
                    # no error, update Status valid --> start
                    cur.execute('UPDATE {0} SET {0}.Status=\"encrypt\", {0}.errorMessages=\"None\" WHERE {0}.alias=\"{1}\" AND {0}.egaBox=\"{2}\"'.format(Table, alias, Box))  
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
def DeleteValidatedObjectsWithErrors(CredentialFile, DataBase, Table, Box, Object, Portal, SubmissionStatus):
    '''
    (str, str, str, str, str, str) - > None
    Connect to Database using CredentialFile, extract all aliases with submit status
    and Box from Table, connect to the API and delete the corresponding Object
    with submissionStatus (VALIDATED_WITH_ERRORS, VALIDATED, DRAFT) 
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
        response = requests.get(URL + '/{0}?status={1}&skip=0&limit=0'.format(Object, SubmissionStatus), headers=headers, data=data)
        
        # loop over aliases
        for i in range(len(Aliases)):
            ObjectId = ''
            for j in response.json()['response']['result']:
                # check if alias with validated_with_errors status
                if j["alias"] == Aliases[i]:
                    ObjectId = j['id']
                    if ObjectId != '':
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
    
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
    try:
        cur.execute('SELECT {0}.Json, {0}.egaAccessionId FROM {0} WHERE {0}.Status=\"submit\" AND {0}.egaBox=\"{1}\"'.format(Table, Box))
        # extract all information 
        Data = cur.fetchall()
    except:
        # record error message
        Data = []
    conn.close()
        
    # check that objects in submit mode do exist
    if len(Data) != 0:
        # make a list of jsons. filter out filesobjects already registered that have been re-uploaded because not archived
        L = [json.loads(i[0].replace("'", "\""), strict=False) for i in Data if not i[1].startswith('EGA')]
        
        for i in range(len(L)):
            if 'chromosomeReferences' in L[i]:
                if L[i]['chromosomeReferences'] != []:
                    for j in range(len(L[i]['chromosomeReferences'])):
                        if L[i]['chromosomeReferences'][j]['label'] == 'None':
                            L[i]['chromosomeReferences'][j]['label'] = None
        
        
        
        
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
            # record error message if no token or open submission if token is obtained
            try:
                Token = Login.json()['response']['result'][0]['session']['sessionToken']
            except:
                # record error message
                RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], 'Cannot obtain a token', 'Error')                
            else:
                # open a submission with token
                headers = {"Content-type": "application/json", "X-Token": Token}
                submissionJson = {"title": "{0} submission".format(Object), "description": "opening a submission for {0} {1}".format(Object, J["alias"])}
                OpenSubmission = requests.post(URL + '/submissions', headers=headers, data=str(submissionJson).replace("'", "\""))
                # record error if submission Id is not retrieved or create object if submission successfully open
                try:
                    # get submission Id
                    submissionId = OpenSubmission.json()['response']['result'][0]['id']
                except:
                    # record error message
                    RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], 'Cannot obtain a submissionId', 'Error') 
                else:
                    # create object
                    ObjectCreation = requests.post(URL + '/submissions/{0}/{1}'.format(submissionId, Object), headers=headers, data=str(J).replace("'", "\""))
                    # record status (DRAFT) and validate object if created or record error message. status --> VALIDATED or VALITED_WITH_ERRORS 
                    try:
                        ObjectId = ObjectCreation.json()['response']['result'][0]['id']
                        submissionStatus = ObjectCreation.json()['response']['result'][0]['status']
                    except:
                        # record error message
                        error = ObjectCreation.json()['header']['userMessage']
                        RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], 'Cannot create an object: {0}'.format(error), 'Error') 
                    else:
                        # store submission json and status (DRAFT) in db table
                        RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], submissionStatus, 'Status') 
                        # validate object
                        ObjectValidation = requests.put(URL + '/{0}/{1}?action=VALIDATE'.format(Object, ObjectId), headers=headers)
                        # record status and submit object or record error message
                        try:
                            ObjectStatus = ObjectValidation.json()['response']['result'][0]['status']
                            errorMessages = CleanUpError(ObjectValidation.json()['response']['result'][0]['validationErrorMessages'])
                        except:
                            error = ObjectValidation.json()['header']['userMessage'] + ';' + ObjectValidation.json()['header']['developerMessage']
                            RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], 'Cannot obtain validation status: {0}'.format(error), 'Error')
                        else:
                            # record error messages
                            RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], errorMessages, 'Error')
                            # record object status
                            RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], ObjectStatus, 'Status') 
                            # check if object is validated
                            if ObjectStatus == 'VALIDATED':
                                # submit object
                                ObjectSubmission = requests.put(URL + '/{0}/{1}?action=SUBMIT'.format(Object, ObjectId), headers=headers)
                                # update error, record status or record error if submission cannot be done
                                try:
                                    errorMessages = CleanUpError(ObjectSubmission.json()['response']['result'][0]['submissionErrorMessages'])
                                    ObjectStatus = ObjectSubmission.json()['response']['result'][0]['status']                
                                except:
                                    # record error message
                                    RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], 'Cannot obtain submission status', 'Error')
                                else:
                                    # update error, record status
                                    RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], errorMessages, 'Error')
                                    RecordMessage(CredentialFile, DataBase, Table, Box, J["alias"], ObjectStatus, 'Status') 
                                    # check status
                                    if ObjectStatus == 'SUBMITTED':
                                        # get the receipt, and the accession id
                                        try:
                                            Receipt = str(ObjectSubmission.json()).replace("\"", "")
                                            # egaAccessionId is None for experiments, but can be obtained from the list of egaAccessionIds
                                            if Object == 'experiments':
                                                egaAccessionId = ObjectSubmission.json()['response']['result'][0]['egaAccessionIds'][0]
                                            else:
                                                egaAccessionId = ObjectSubmission.json()['response']['result'][0]['egaAccessionId']
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
def IsInfoValid(CredentialFile, MetadataDataBase, SubDataBase, Table, Box, Object, MyScript, MyPython, **KeyWordParams):
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
    
    # get optional tables
    if 'attributes' in KeyWordParams:
        AttributesTable = KeyWordParams['attributes']
    if 'projects' in KeyWordParams:
        ProjectsTable = KeyWordParams['projects']
    
    # get required information
    if Object == 'analyses':
        # extract information from Analyses, atrtibutes and projects tables
        Cmd = 'SELECT {0}.alias, {0}.sampleReferences, {0}.files, {0}.egaBox, \
            {0}.AttributesKey, {0}.ProjectKey, {1}.title, {1}.description, {1}.attributes, \
            {1}.genomeId, {1}.StagePath, {2}.studyId, {2}.analysisCenter, {2}.Broker, \
            {2}.analysisTypeId, {2}.experimentTypeId FROM {0} JOIN {1} JOIN {2} \
            WHERE {0}.Status=\"start\" AND {0}.egaBox=\"{3}\" AND {0}.AttributesKey={1}.alias \
            AND {0}.ProjectKey={2}.alias'.format(Table, AttributesTable, ProjectsTable, Box)
        Keys = ['alias', 'sampleReferences', 'files', 'egaBox', 'AttributesKey', 
                'ProjectKey', 'title', 'description', 'attributes', 'genomeId', 
                'StagePath', 'studyId', 'analysisCenter', 'Broker', 'analysisTypeId', 'experimentTypeId']
        Required = ['alias', 'sampleReferences', 'files', 'egaBox', 'AttributesKey',
                    'ProjectKey', 'title', 'description', 'genomeId', 'StagePath',
                    'studyId', 'analysisCenter', 'Broker', 'analysisTypeId', 'experimentTypeId']              
    elif Object == 'samples':
        Cmd = 'Select {0}.alias, {0}.caseOrControlId, {0}.genderId, {0}.phenotype, {0}.egaBox, \
              {0}.AttributesKey, {1}.title, {1}.description, {1}.attributes FROM {0} JOIN {1} WHERE \
              {0}.Status=\"start\" AND {0}.egaBox=\"{2}\" AND {0}.AttributesKey={1}.alias'.format(Table, AttributesTable, Box)
        Keys = ['alias', 'caseOrControlId', 'genderId', 'phenotype', 'egaBox', 'AttributesKey',
                'title', 'description', 'attributes']
        Required = ['alias', 'caseOrControlId', 'genderId', 'phenotype', 'egaBox', 'AttributesKey',
                    'title', 'description']
    elif Object == 'datasets':
        Cmd = 'SELECT {0}.alias, {0}.datasetTypeIds, {0}.policyId, {0}.runsReferences, {0}.analysisReferences, \
        {0}.title, {0}.description, {0}.datasetLinks, {0}.attributes , {0}.egaBox FROM {0} WHERE {0}.Status=\"start\" AND {0}.egaBox=\"{1}\"'.format(Table, Box)            
        Keys = ['alias', 'datasetTypeIds', 'policyId', 'runsReferences', 'analysisReferences', 'title',
                'description', 'datasetLinks', 'attributes', 'egaBox']     
        Required = ['alias', 'datasetTypeIds', 'policyId', 'title', 'description', 'egaBox']
    elif Object == 'experiments':
        Cmd  = 'SELECT {0}.alias, {0}.title, {0}.instrumentModelId, {0}.librarySourceId, \
        {0}.librarySelectionId, {0}.libraryStrategyId, {0}.designDescription, {0}.libraryName, \
        {0}.libraryConstructionProtocol, {0}.libraryLayoutId, {0}.pairedNominalLength, \
        {0}.pairedNominalSdev, {0}.sampleId, {0}.studyId, {0}.egaBox FROM {0} WHERE {0}.Status=\"start\" AND {0}.egaBox=\"{1}\"'.format(Table, Box)
        Keys = ["alias", "title", "instrumentModelId", "librarySourceId", "librarySelectionId",
                "libraryStrategyId", "designDescription", "libraryName", "libraryConstructionProtocol",
                "libraryLayoutId", "pairedNominalLength", "pairedNominalSdev", "sampleId", "studyId", "egaBox"]
        Required = ["alias", "title", "instrumentModelId", "librarySourceId", "librarySelectionId",
                    "libraryStrategyId", "designDescription", "libraryName", "libraryLayoutId",
                    "pairedNominalLength", "pairedNominalSdev", "sampleId", "studyId", "egaBox"]
    elif Object == 'studies':
        Cmd = 'SELECT {0}.alias, {0}.studyTypeId, {0}.shortName, {0}.title, \
        {0}.studyAbstract, {0}.ownTerm, {0}.pubMedIds, {0}.customTags, {0}.egaBox FROM {0} \
        WHERE {0}.Status=\"start\" AND {0}.egaBox=\"{1}\"'.format(Table, Box)
        Keys = ["alias", "studyTypeId", "shortName", "title", "studyAbstract",
                "ownTerm", "pubMedIds", "customTags", "egaBox"]
        Required = ["alias", "studyTypeId", "title", "studyAbstract", "egaBox"]
    elif Object == 'policies':
        Cmd = 'SELECT {0}.alias, {0}.dacId, {0}.title, {0}.policyText, {0}.url, {0}.egaBox FROM {0} \
        WHERE {0}.Status=\"start\" AND {0}.egaBox=\"{1}\"'.format(Table, Box)
        Keys = ["alias", "dacId", "title", "policyText", "url", "egaBox"]
        Required = ["alias", "dacId", "title", "policyText", "egaBox"]
    elif Object == 'dacs':
        Cmd = 'SELECT {0}.alias, {0}.title, {0}.contacts, {0}.egaBox FROM {0} WHERE {0}.status=\"start\" AND {0}.egaBox="\{1}\"'.format(Table, Box)
        Keys = ["alias", "title", "contacts", "egaBox"]
        Required = ["alias", "title", "contacts", "egaBox"]        
    elif Object == 'runs':
        Cmd = 'SELECT {0}.alias, {0}.sampleId, {0}.runFileTypeId, {0}.experimentId, \
        {0}.files, {0}.egaBox FROM {0} WHERE {0}.status=\"start\" AND {0}.egaBox=\"{1}\"'.format(Table, Box)
        Keys = ["alias", "sampleId", "runFileTypeId", "experimentId", "files", "egaBox"]
        Required = ["alias", "sampleId", "runFileTypeId", "experimentId", "files", "egaBox"]

      
    # extract data 
    try:
        cur.execute(Cmd)
        Data = cur.fetchall()
    except:
        Data = []
    conn.close()
    
    # map typeId with enumerations
    MapEnum = {"experimentTypeId": "ExperimentTypes", "analysisTypeId": "AnalysisTypes",
               "caseOrControlId": "CaseControl", "genderId": "Genders", "datasetTypeIds": "DatasetTypes",
               "instrumentModelId": "InstrumentModels", "librarySourceId": "LibrarySources",
               "librarySelectionId": "LibrarySelections",  "libraryStrategyId": "LibraryStrategies",
               "studyTypeId": "StudyTypes", "chromosomeReferences": "ReferenceChromosomes",
               "genomeId": "ReferenceGenomes", "fileTypeId": "AnalysisFileTypes", "runFileTypeId": "FileTypes"}

    # check info
    if len(Data) != 0:
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
                        Error.append('Missing required key {0}'.format(key))
                # check that alias is not already used
                if key == 'alias':
                    # extract alias and accessions from table
                    Registered = ExtractAccessions(CredentialFile, MetadataDataBase, Box, Table)
                    if d[key] in Registered:
                        # alias already used for the same table and box
                        Missing = True
                        Error.append('Alias already registered')
                    if Object in ['runs', 'analyses'] and '__' in d[key]:
                        # double underscore is not allowed in runs and analyses alias
                        # because alias and file name are retrieved from job name
                        # split on double underscore for checking upload and encryption
                        Missing = True
                        Error.append('Double underscore not allowed in runs and analyses alias')
                # check that references are provided for datasets
                if 'runsReferences' in d and 'analysisReferences' in d:
                    # at least runsReferences or analysesReferences should include some accessions
                    if d['runsReferences'] in ['', 'NULL', None] and d['analysisReferences'] in ['', 'NULL', None]:
                        Missing = True
                        Error.append('Missing runsReferences and analysisReferences')
                    if d['runsReferences'] not in ['', 'NULL', None]:
                        if False in list(map(lambda x: x.startswith('EGAR'), d['runsReferences'].split(';'))):
                            Missing = True
                            Error.append('Missing runsReferences')
                    if d['analysisReferences'] not in ['', 'NULL', None]:
                        if False in list(map(lambda x: x.startswith('EGAZ'), d['analysisReferences'].split(';'))):
                            Missing = True
                            Error.append('Missing analysisReferences')
                # check that accessions or aliases are provided
                if key in ['sampleId', 'sampleReferences', 'dacId', 'studyId']:
                    if d[key] in ['', 'None', None, 'NULL']:
                        Missing = True
                        Error.append('Missing alias and or accession for {0}'.format(key))
                # check files
                if key == 'files':
                    files = json.loads(d['files'].replace("'", "\""))
                    for filePath in files:
                        # check if file is valid
                        if os.path.isfile(filePath) == False:
                            Missing = True
                            Error.append('Invalid file paths')
                        # check validity of file type for Analyses objects only. doesn't exist for Runs
                        if Object == 'Analyses':
                            if files[filePath]['fileTypeId'].lower() not in Enums['FileTypes']:
                                Missing = True
                                Error.append('Invalid fileTypeId')
                # check policy Id
                if key == 'policyId':
                    if 'EGAP' not in d[key]:
                        Missing = True
                        Error.append('Invalid policyId, should start with EGAP')
                # check library layout
                if key == "libraryLayoutId":
                    if str(d[key]) not in ['0', '1']:
                        Missing = True
                        Error.append('Invalid {0}: should be 0 or 1'.format(key))
                if key in ['pairedNominalLength', 'pairedNominalSdev']:
                    try:
                        float(d[key])
                    except:
                        Missing = True
                        Error.append('Invalid type for {0}, should be a number'.format(key))
                # check enumerations
                if key in MapEnum:
                    # datasetTypeIds can be a list of multiple Ids
                    if key == 'datasetTypeIds':
                        for k in d[key].split(';'):
                            if k not in Enums[MapEnum[key]]:
                                Missing = True
                                Error.append('Invalid enumeration for {0}'.format(key))
                    # check that enumeration is valid
                    if d[key] not in Enums[MapEnum[key]]:
                        Missing = True
                        Error.append('Invalid enumeration for {0}'.format(key))
                # check custom attributes
                if key == 'attributes':
                    if d['attributes'] not in ['', 'NULL', None]:
                        # check format of attributes
                        attributes = [json.loads(j.replace("'", "\"")) for j in d['attributes'].split(';')]
                        for k in attributes:
                            # do not allow keys other than tag, unit and value
                            if set(k.keys()).union({'tag', 'value', 'unit'}) != {'tag', 'value', 'unit'}:
                                Missing = True
                                Error.append('Invalif {0} format'.format(key))
                            # tag and value are required keys
                            if 'tag' not in k.keys() and 'value' not in k.keys():
                                Missing = True
                                Error.append('Missing tag and value from {0}'.format(key))

            # check if object has missing/non-valid information
            if Missing == True:
                 Error = ';'.join(list(set(Error)))
            elif Missing == False:
                Error = 'NoError'
            assert d['alias'] not in D
            D[d['alias']] = Error
    
    return D



# use this function to check that all information for Analyses objects is available before encrypting files     
def CheckTableInformation(CredentialFile, MetadataDataBase, SubDataBase, Table, Object, Box, MyScript, MyPython, **KeyWordParams):
    '''
    (str, str, str, str, str, str, str, str, dict) -> None
    Extract information from Submission Database Tables using credentials in file and update errorMessages for each alias
    '''

    # check Table information
    D = IsInfoValid(CredentialFile, MetadataDataBase, SubDataBase, Table, Box, Object, MyScript, MyPython, **KeyWordParams)
      
    # extract all aliases with start status
    conn = EstablishConnection(CredentialFile, SubDataBase)
    cur = conn.cursor()      
    try:
        cur.execute('SELECT {0}.alias FROM {0} WHERE {0}.Status=\"start\" AND {0}.egaBox=\"{1}\"'.format(Table, Box))
        Data = [i[0] for i in cur]
    except:
        Data = []
    conn.close()
    
    # create dict {alias: errors}
    K = {}
    # record error messages
    for alias in Data:
        if alias in D:
            K[alias] = D[alias]
        else:
            K[alias] = 'No information. Possible issues with table keys or database connection'
                  
    # update status and record errorMessage
    conn = EstablishConnection(CredentialFile, SubDataBase)
    cur = conn.cursor()    
    for alias in K:
        # record error message and/or update status
        if K[alias] == 'NoError':
            # update status
            cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\", {0}.Status=\"clean\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, K[alias], alias, Box))
            conn.commit()
        else:
            # record error
            cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, K[alias], alias, Box))
            conn.commit()
    conn.close()


# use this function to extract the chromosome names from the vcf
def GetContigNamesFromVcfHeader(file):
    '''
    (str) -> list
    
    Take the path to a vcf file (compressed or not) and return a list of contigs
    specified in the vcf header
    '''
    
    # get the chromosomes
    contigs = []
    if file[-4:] == '.vcf':
        infile = open(file)
    elif file[-7:] == '.vcf.gz':
        infile = gzip.open(file, 'rt')
    # read vcf header  
    for line in infile:
        if line.startswith('##'):
            contigs.append(line)
        else:
            # finished reading header
            break
    infile.close()
    
    chromos = [i for i in contigs if i.startswith('##contig')]
    for i in range(len(chromos)):
        contig = chromos[i].split(',')[0].split('=')[-1]
        if '_' in contig:
            contig = contig[:contig.index('_')]
        if not contig.lower().startswith('chr'):
            contig = 'chr' + contig
        chromos[i] = contig
    # remove duplicate names
    chromos = list(set(chromos))
           
    return chromos        


def ExtractContigNamesFromVcf(file):
    '''
    (str) -> list
    
    Take the path to a vcf file (compressed or not) and return a list of contigs
    specified in the vcf body
    '''
    
    # get the chromosomes
    contigs = set()
    if file[-4:] == '.vcf':
        infile = open(file)
    elif file[-7:] == '.vcf.gz':
        infile = gzip.open(file, 'rt')
    # read vcf file  
    for line in infile:
        # skip header
        if not line.startswith('#'):
            line = line.rstrip().split('\t')
            contigs.add(line[0])
    infile.close()
    # make a list of contigs
    chromos = list(contigs)
    for i in range(len(chromos)):
        if '_' in chromos[i]:
            chromos[i] = chromos[i][:chromos[i].index('_')]
        if not chromos[i].lower().startswith('chr'):
            chromos[i] = 'chr' + chromos[i]
    # remove duplicate names
    chromos = list(set(chromos))
    return chromos        


def ExtractContigNamesFromTSV(file):
    '''
    (str) -> list
    
    Take the path to a tsv file (compressed or not) and return a list of contigs
    '''
    
    # get the chromosomes
    chromos = []
    if file[-4:] == '.tsv':    
        infile = open(file)    
    elif file[-7:] == '.tsv.gz':
        infile = gzip.open(file, 'rt')
    # read file
    for line in infile:
        line = line.rstrip()
        if line != '':
            line = line.split('\t')
            line = list(map(lambda x: x.strip(), line))
            contig = line[0]
            if 'chr' not in contig.lower():
                contig = 'chr' + contig
            else:
                contig = contig.lower()
            chromos.append(contig)
    infile.close()
    chromos = list(set(chromos))
    return chromos
    
    
# use this function to format the analysis json
def FormatJson(D, Object, MyScript, MyPython):
    '''
    (dict, str, str, str) -> dict
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
        Required = ["alias", "title", "description", "studyId", "sampleReferences", "analysisCenter",
                    "analysisTypeId", "files", "genomeId", "experimentTypeId", "StagePath"]
    elif Object == 'samples':
        JsonKeys = ["alias", "title", "description", "caseOrControlId", "genderId",
                    "organismPart", "cellLine", "region", "phenotype", "subjectId",
                    "anonymizedName", "bioSampleId", "sampleAge", "sampleDetail", "attributes"]
        Required = ["alias", "title", "description", "caseOrControlId", "genderId", "phenotype"] 
    elif Object == 'datasets':
        JsonKeys = ["alias", "datasetTypeIds", "policyId", "runsReferences", "analysisReferences",
                    "title", "description", "datasetLinks", "attributes"]
        Required = ['alias', 'datasetTypeIds', 'policyId', 'title', 'description', 'egaBox']
    elif Object == 'experiments':
        JsonKeys = ["alias", "title", "instrumentModelId", "librarySourceId", "librarySelectionId",
                    "libraryStrategyId", "designDescription", "libraryName", "libraryConstructionProtocol",
                    "libraryLayoutId", "pairedNominalLength", "pairedNominalSdev", "sampleId", "studyId", "egaBox"]
        Required = ["alias", "title", "instrumentModelId", "librarySourceId", "librarySelectionId",
                    "libraryStrategyId", "designDescription", "libraryName", "libraryLayoutId",
                    "pairedNominalLength", "pairedNominalSdev", "sampleId", "studyId", "egaBox"]
    elif Object == 'studies':
        JsonKeys = ["alias", "studyTypeId", "shortName", "title", "studyAbstract", "ownTerm", "pubMedIds", "customTags", "egaBox"]
        Required = ["alias", "studyTypeId", "title", "studyAbstract", "egaBox"]
    elif Object == 'policies':
        JsonKeys = ["alias", "dacId", "title", "policyText", "url", "egaBox"]
        Required = ["alias", "dacId", "title", "policyText", "egaBox"]
    elif Object == 'dacs':
        JsonKeys = ["alias", "title", "contacts", "egaBox"]
        Required = ["alias", "title", "contacts", "egaBox"]    
    elif Object == 'runs':
        JsonKeys = ["alias", "sampleId", "runFileTypeId", "experimentId", "files", "egaBox"]
        Required = ["alias", "sampleId", "runFileTypeId", "experimentId", "files", "egaBox"]

       
    # map typeId with enumerations
    MapEnum = {"experimentTypeId": "ExperimentTypes", "analysisTypeId": "AnalysisTypes",
               "caseOrControlId": "CaseControl", "genderId": "Genders", "datasetTypeIds": "DatasetTypes",
               "instrumentModelId": "InstrumentModels", "librarySourceId": "LibrarySources",
               "librarySelectionId": "LibrarySelections",  "libraryStrategyId": "LibraryStrategies",
               "studyTypeId": "StudyTypes", "chromosomeReferences": "ReferenceChromosomes",
               "genomeId": "ReferenceGenomes", "fileTypeId": "AnalysisFileTypes", "runFileTypeId": "FileTypes"}

    # map chromosome names for vcf
    chromoTonames = {'chr1': 'CM000663', 'chr2': 'CM000664', 'chr3': 'CM000665',
                   'chr4': 'CM000666', 'chr5': 'CM000667', 'chr6': 'CM000668',
                   'chr7': 'CM000669', 'chr8': 'CM000670', 'chr9': 'CM000671',
                   'chr10': 'CM000672', 'chr11': 'CM000673', 'chr12': 'CM000674',
                   'chr13': 'CM000675', 'chr14': 'CM000676', 'chr15': 'CM000677',
                   'chr16': 'CM000678', 'chr17': 'CM000679', 'chr18': 'CM000680',
                   'chr19': 'CM000681', 'chr20': 'CM000682', 'chr21': 'CM000683',
                   'chr22': 'CM000684', 'chrX': 'CM000685', 'chrY': 'CM000686'}
                      
    # reverse dictionary
    namesTochromo = {}
    for i in chromoTonames:
        namesTochromo[chromoTonames[i]] = i

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
                    # check if field is already recorded. eg: chromosomeReferences may be set already for vcf
                    if field not in J:
                        # some non-required fields need to be lists
                        if field in ["chromosomeReferences", "attributes", "datasetLinks", "runsReferences",
                                     "analysisReferences", "pubMedIds", "customTags"]:
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
                    # file format is different for analyses and runs
                    if Object == 'analyses':
                        # make a list of contigs used. required for vcf, optional for bam
                        contigs = []
                        # loop over file name
                        for filePath in files:
                            # create a dict to store file info
                            # check that fileTypeId is valid
                            if files[filePath]["fileTypeId"].lower() not in Enums[MapEnum['fileTypeId']]:
                                # cannot obtain fileTypeId. erase dict and add alias
                                J = {}
                                J["alias"] = D["alias"]
                                # return dict with alias only if required fields are missing
                                return J
                            else:
                                fileTypeId = Enums[MapEnum['fileTypeId']][files[filePath]["fileTypeId"].lower()]
                            # check if analysis object is bam or vcf
                            # chromosomeReferences is optional for bam but required for vcf and tab
                            if files[filePath]["fileTypeId"].lower() == 'vcf':
                                 # make a list of contigs from the vcf header
                                 contigs.extend(GetContigNamesFromVcfHeader(filePath))
                                 if len(contigs) == 0:
                                     # if contig names not in VCF header, extract contigs from VCF file
                                     contigs.extend(ExtractContigNamesFromVcf(filePath))
                            elif files[filePath]["fileTypeId"].lower() == 'tab':
                                # make a list of contigs 
                                contigs.extend(ExtractContigNamesFromTSV(filePath))
                            # create dict with file info, add path to file names
                            d = {"fileName": os.path.join(D['StagePath'], files[filePath]['encryptedName']),
                                 "checksum": files[filePath]['checksum'],
                                 "unencryptedChecksum": files[filePath]['unencryptedChecksum'],
                                 "fileTypeId": fileTypeId}
                            J[field].append(d)
                    
                        # check if chromosomes were recorded
                        if len(contigs) != 0:
                            # remove duplicate names
                            contigs = list(set(contigs))
                            # map chromosome names 
                            if 'genomeId' not in D:
                                # erase dict and add alias
                                J = {}
                                J["alias"] = D["alias"]
                                return J
                            else:
                                # only GRCh37 and GRch38 are supported
                                if D['genomeId'].lower() not in ['grch37', 'grch38']:
                                    # erase dict and add alias
                                    J = {}
                                    J["alias"] = D["alias"]
                                    return J
                                else:
                                    if D['genomeId'].lower() == 'grch37':
                                        suffix = '.1'
                                    elif D['genomeId'].lower() == 'grch38':
                                        suffix = '.2'
                                    values = [chromoTonames[i] + suffix for i in contigs if i in chromoTonames]
                                    # add chromosome reference info
                                    J['chromosomeReferences'] = [{"value": Enums['ReferenceChromosomes'][i], "label": namesTochromo[i.replace(suffix, '')]} for i in values if i in Enums['ReferenceChromosomes']]  
                    elif Object == 'runs':
                        # loop over file name
                        for filePath in files:
                            # create a dict with file info, add stagepath to file name
                            d = {"fileName": os.path.join(D['StagePath'], files[filePath]['encryptedName']),
                                 "checksum": files[filePath]['checksum'], "unencryptedChecksum": files[filePath]['unencryptedChecksum'],
                                 "checksumMethod": 'md5'}
                            J[field].append(d)
                elif field in ['runsReferences', 'analysisReferences', 'pubMedIds']:
                    J[field] = D[field].split(';')
                elif field in ['attributes', 'datasetLinks', 'customTags']:
                    # ensure strings are double-quoted
                    attributes = D[field].replace("'", "\"")
                    # convert string to dict
                    # loop over all attributes
                    attributes = attributes.split(';')
                    J[field] = [json.loads(attributes[i].strip().replace("'", "\"")) for i in range(len(attributes))]
                elif field == 'libraryLayoutId':
                    try:
                        int(D[field]) in [0, 1]
                        J[field] = int(D[field])
                    except:
                        # must be coded 0 for paired end or 1 for single end
                        J = {}
                        # return dict with alias if required field is missing
                        J["alias"] = D["alias"]
                        return J
                elif field  in ['pairedNominalLength', 'pairedNominalSdev']:
                    try:
                        float(D[field])
                        J[field] = float(D[field])
                    except:
                        # must be coded 0 for paired end or 1 for single end
                        J = {}
                        # return dict with alias if required field is missing
                        J["alias"] = D["alias"]
                        return J        
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
                        elif field == "datasetTypeIds":
                            # multiple Ids can be stored
                            J[field] = [Enums[MapEnum[field]][k] for k in D[field].split(';')]
                        else:
                            J[field] = Enums[MapEnum[field]][D[field]]
                elif field == 'sampleReferences':
                    # populate with sample accessions
                    J[field] = [{"value": accession.strip(), "label":""} for accession in D[field].split(';')]
                elif field == 'contacts':
                    J[field] = [json.loads(contact.replace("'", "\"")) for contact in D[field].split(';')]
                
                # fields added as aliases must be replaced with accessions
                elif field in ['studyId', 'policyId', 'dacId', 'experimentId']:
                    a = ['studyId', 'policyId', 'dacId', 'experimentId']
                    b = ['EGAS', 'EGAP', 'EGAC', 'EGAX']
                    for i in range(len(a)):
                        if field == a[i]:
                            if D[field].startswith(b[i]):
                                J[field] = D[field]
                            else:
                                # erase dict and add alias
                                J = {}
                                J["alias"] = D["alias"]
                                return J
                else:
                    J[field] = D[field]
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
        Cmd = 'SELECT {0}.alias, {0}.sampleReferences, {0}.analysisDate, {0}.files, \
        {1}.title, {1}.description, {1}.attributes, {1}.genomeId, {1}.chromosomeReferences, {1}.StagePath, {1}.platform, \
        {2}.studyId, {2}.analysisCenter, {2}.Broker, {2}.analysisTypeId, {2}.experimentTypeId \
        FROM {0} JOIN {1} JOIN {2} WHERE {0}.Status=\"uploaded\" AND {0}.egaBox=\"{3}\" AND {0}.AttributesKey = {1}.alias \
        AND {0}.ProjectKey = {2}.alias'.format(Table, AttributesTable, ProjectsTable, Box)
    elif Object == 'samples':
        Cmd = 'SELECT {0}.alias, {0}.caseOrControlId, {0}.genderId, {0}.organismPart, \
        {0}.cellLine, {0}.region, {0}.phenotype, {0}.subjectId, {0}.anonymizedName, {0}.bioSampleId, \
        {0}.sampleAge, {0}.sampleDetail, {1}.title, {1}.description, {1}.attributes FROM {0} JOIN {1} \
        WHERE {0}.Status=\"clean\" AND {0}.egaBox=\"{2}\" AND {0}.AttributesKey = {1}.alias'.format(Table, AttributesTable, Box)
    elif Object == 'datasets':
        Cmd = 'SELECT {0}.alias, {0}.datasetTypeIds, {0}.policyId, {0}.runsReferences, \
        {0}.analysisReferences, {0}.title, {0}.description, {0}.datasetLinks, {0}.attributes FROM {0} \
        WHERE {0}.Status=\"valid\" AND {0}.egaBox=\"{1}\"'.format(Table, Box)
    elif Object == 'experiments':
        Cmd  = 'SELECT {0}.alias, {0}.title, {0}.instrumentModelId, {0}.librarySourceId, \
        {0}.librarySelectionId, {0}.libraryStrategyId, {0}.designDescription, {0}.libraryName, \
        {0}.libraryConstructionProtocol, {0}.libraryLayoutId, {0}.pairedNominalLength, \
        {0}.pairedNominalSdev, {0}.sampleId, {0}.studyId FROM {0} WHERE {0}.Status=\"valid\" AND {0}.egaBox=\"{1}\"'.format(Table, Box)
    elif Object == 'studies':
        Cmd = 'SELECT {0}.alias, {0}.studyTypeId, {0}.shortName, {0}.title, {0}.studyAbstract, \
        {0}.ownTerm, {0}.pubMedIds, {0}.customTags FROM {0} WHERE {0}.Status=\"clean\" AND {0}.egaBox=\"{1}\"'.format(Table, Box) 
    elif Object == 'policies':
        Cmd = 'SELECT {0}.alias, {0}.dacId, {0}.title, {0}.policyText, {0}.url FROM {0} \
        WHERE {0}.Status=\"valid\" AND {0}.egaBox=\"{1}\"'.format(Table, Box)
    elif Object == 'dacs':
        Cmd = 'SELECT {0}.alias, {0}.title, {0}.contacts FROM {0} WHERE {0}.status=\"clean\" AND {0}.egaBox="\{1}\"'.format(Table, Box)
    elif Object == 'runs':
        Cmd = 'SELECT {0}.alias, {0}.sampleId, {0}.runFileTypeId, {0}.experimentId, {0}.files, \
        {0}.WorkingDirectory, {0}.StagePath FROM {0} WHERE {0}.Status=\"uploaded\" AND {0}.egaBox=\"{1}\"'.format(Table, Box)
    
          
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
                if len(k) != 0:
                    # convert date to epoch time
                    date = '.'.join([k[1], Months[k[0]], k[-1]]) + ' ' + k[2] 
                    p = '%d.%m.%Y %H:%M:%S'
                    date = int(time.mktime(time.strptime(date, p)))
                else:
                    date = 0
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
def AddFootprintData(CredentialFile, SubDataBase, StagingServerTable, FootPrintTable, Box):
    '''
    (str, str, str, str, str) -> None
    Use credentials to connect to SubDatabase, extract file information from
    StagingServerTable for given Box and collapse it per directory in FootPrintTable
    '''
    
    # connect to submission database
    conn = EstablishConnection(CredentialFile, SubDataBase)
    cur = conn.cursor()
    try:
        cur.execute('SELECT * FROM {0} WHERE {0}.egaBox=\"{1}\"'.format(StagingServerTable, Box))
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
        
        # list all tables in EGA metadata db
        Tables = ListTables(CredentialFile, SubDataBase)

        # connect to submission database
        conn = EstablishConnection(CredentialFile, SubDataBase)
        cur = conn.cursor()
  
        # create table if doesn't exist        
        if FootPrintTable not in Tables:
            Fields = ["egaBox", "location", "AllFiles", "Registered", "NotRegistered", "Size", "SizeRegistered", "SizeNotRegistered"]
            # format colums with datatype - convert to string
            Columns = ' '.join([Fields[i] + ' TEXT NULL,' if i != len(Fields) -1 else Fields[i] + ' TEXT NULL' for i in range(len(Fields))])
            # create table with column headers
            cur.execute('CREATE TABLE {0} ({1})'.format(FootPrintTable, Columns))
            conn.commit()
        else:
            # get the column headers from the table
            cur.execute("SELECT * FROM {0}".format(FootPrintTable))
            Fields = [i[0] for i in cur.description]
            
        # create a string with column headers
        ColumnNames = ', '.join(Fields)
        # drop all entries for that Box
        cur.execute('DELETE FROM {0} WHERE {0}.egaBox=\"{1}\"'.format(FootPrintTable, Box))
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
    
    # make a list of boxes in foorptint table
    cur.execute('SELECT {0}.egaBox FROM {0}'.format(FootPrint))
    Boxes = list(set([i[0] for i in cur]))
    # if Box not in Boxes, footprint is 0
    if Box not in Boxes:
        return 0
      
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


# use this function to get the column header of a given table in database
def RetrieveColumnHeader(CredentialFile, DataBase, Table):
    '''
    (str, str, str) -> list
    Connect to Database using credentials and return a list of column names
    in Table from Database
    '''
    
    # connect to database
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
    cur.execute('SHOW COLUMNS FROM {0}'.format(Table))
    Header = list(map(lambda x: x[0], cur.fetchall()))
    conn.close()
    return Header


## functions specific to Analyses objects =====================================
    
# use this function to add sample accessions to Analysis Table in the submission database
def AddAccessions(CredentialFile, MetadataDataBase, SubDataBase, Table, AssociatedTable, ColumnName, prefix, UpdateStatus, Box):
    '''
    (str, str, str, str, str, str, str, bool, str) -> None
    
    Take a file with credentials to connect to metadata and submission databases
    and update Table  in the submission database with the accessions of dependent 
    objects in AssociatedTable using prefix and ColumnName. Update status --> ready
    if no error and UpdateStatus is True
    '''
    
    # grab EGA accessions from metadata database, create a dict {alias: accession}
    Registered = ExtractAccessions(CredentialFile, MetadataDataBase, Box, AssociatedTable)
           
    # connect to the submission database
    conn = EstablishConnection(CredentialFile, SubDataBase)
    cur = conn.cursor()
    # pull alias, dependent Ids for given box
    Cmd = 'SELECT {0}.alias, {0}.{1} FROM {0} WHERE {0}.Status=\"clean\" AND {0}.egaBox=\"{2}\"'.format(Table, ColumnName, Box)
    
    try:
        cur.execute(Cmd)
        Data = cur.fetchall()
    except:
        Data = []
    
    # create a dict {alias: [accessions, ErrorMessage]}
    Dependent = {}
    # check if alias are in start status
    if len(Data) != 0:
        for i in Data:
            # make a list of dependent Alias
            Alias = i[1].split(';')
            # make a list of dependent accessions
            Accessions = []
            for j in Alias:
                if j.startswith(prefix):
                    Accessions.append(j)
                elif j in Registered:
                    Accessions.append(Registered[j])
            # record error if aliases have missing accessions
            # alias may be in medata table but accession may be NULL if EGA Id is not yet available
            if len(Alias) != len(Accessions) or 'NULL' in Accessions:
                Error = 'Accessions not available'
            else:
                Error = ''
            Dependent[i[0]] =  [';'.join(Accessions), Error]
            
        if len(Dependent) != 0:
            for alias in Dependent:
                Error = Dependent[alias][-1]
                if Error == '':
                    # update accessions
                    cur.execute('UPDATE {0} SET {0}.{1}=\"{2}\", {0}.errorMessages=\"None\" WHERE {0}.alias=\"{3}\" AND {0}.egaBox=\"{4}\"'.format(Table, ColumnName, Dependent[alias][0], alias, Box)) 
                else:
                    # record error message
                    cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, Error, alias, Box)) 
                conn.commit()
                
                # check if status can be updated
                if UpdateStatus == True and Error == '':
                    # update runs and experiments status only if studyId and sampleId are present
                    if Table in ['Experiments', 'Runs']:
                        # make a list of sampleId
                        try:
                            cur.execute('SELECT {0}.sampleId FROM {0} WHERE {0}.egaBox=\"{1}\" AND {0}.alias=\"{2}\"'.format(Table, Box, alias))
                            samples = cur.fetchall()[0][0].split(';')
                        except:
                            samples = []
                        if len(samples) != 0:
                            # check if all samples have accession Ids 
                            HasAccessions = list(set(list(map(lambda x:x.startswith("EGAN"), samples))))
                            if len(HasAccessions) == 1 and HasAccessions[0] == True:
                                # update satus start --> ready
                                cur.execute('UPDATE {0} SET {0}.Status=\"ready\" WHERE {0}.alias=\"{1}\" AND {0}.egaBox=\"{2}\"'.format(Table, alias, Box)) 
                                conn.commit()
                    else:
                        # update satus start --> ready
                        cur.execute('UPDATE {0} SET {0}.Status=\"ready\" WHERE {0}.alias=\"{1}\" AND {0}.egaBox=\"{2}\"'.format(Table, alias, Box)) 
                        conn.commit()
    conn.close()    

    
# use this function to add studyId accession to Analyses Project Table in the submission database
def AddStudyIdAnalysesProject(CredentialFile, MetadataDataBase, SubDataBase, AnalysisTable, ProjectTable, StudiesTable, Box):
    '''
    (str, str, str, str, str, str, str, str) -> None
    
    Take a file with credentials to connect to metadata and submission databases
    and update column studyId in AnalysesProject Table with the study EGA accession if alias is present
    '''
    
    # grab EGA accessions from metadata database, create a dict {alias: accession}
    Registered = ExtractAccessions(CredentialFile, MetadataDataBase, Box, StudiesTable)
           
    # connect to the submission database
    conn = EstablishConnection(CredentialFile, SubDataBase)
    cur = conn.cursor()
    # pull alias, dependent Ids for given box
    Cmd = 'SELECT {0}.alias, {0}.studyId FROM {0} JOIN {1} WHERE {1}.ProjectKey={0}.alias AND {1}.egaBox=\"{2}\"'.format(ProjectTable, AnalysisTable, Box)
        
    try:
        cur.execute(Cmd)
        Data = cur.fetchall()
    except:
        Data = []
    
    # update studyId in project 
    Data = list(set(Data))
    # make a new list with [(alias: studyId)]
    accessions = []
    for i in Data:
        if not i[1].startswith('EGAS'):
            if i[1] in Registered:
                accessions.append([i[0], Registered[i[1]]])
    if len(accessions) != 0:
        for i in accessions:
            with conn.cursor() as cur:
                cur.execute('UPDATE {0} SET {0}.studyId=\"{1}\" WHERE {0}.alias=\"{2}\"'.format(ProjectTable, i[1], i[0])) 
                conn.commit()
    conn.close()    
    

# use this function to check availability of Object egaAccessionId
def CheckEgaAccessionId(CredentialFile, SubDataBase, MetDataBase, Object, Table, Box):
    '''
    (file, str, str, str, str, str) -> None
    Check that all EGA accessions a given Object depends on for registration are available 
    metadata in MetDataBase and update status of aliases in Table for Box or keep the same status 
    '''
    
    # collect all egaAccessionIds for all tables in EGA metadata db
    # accessions may be egaAccessionIds or may be accessions of dependencies
    # eg. dac EGAC00001000010 is not in any egaAccessionId because it was registered in a different box
    # but policy EGAP00001000077 depends on this dac. it can be retrieved in dacId of the policy table
    
    EgaAccessions = []
    # list all tables in EGA metadata db
    Tables = ListTables(CredentialFile, MetDataBase)
    # extract accessions for each table
    for i in Tables:
        # connect to metadata database
        conn = EstablishConnection(CredentialFile, MetDataBase)
        cur = conn.cursor()
        # extract egaAccessions and Ids of dependencies
        for j in ['egaAccessionId', 'dacId', 'policyId']:
            try:
                cur.execute('SELECT {0}.{1} from {0} WHERE {0}.egaBox=\"{2}\"'.format(i, j, Box)) 
                Data = [i[0] for i in cur]   
            except:
                Data = []
            # dump egaAccessions for that table to master list
            EgaAccessions.extend(Data)
        conn.close()
        
   
    # connect to the submission database
    conn = EstablishConnection(CredentialFile, SubDataBase)
    cur = conn.cursor()
    # pull alias and egaAccessionIds to be verified
    if Object == 'analyses':
        Cmd = 'SELECT {0}.alias, {0}.sampleReferences FROM {0} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{1}\"'.format(Table, Box)
    elif Object == 'experiments':
        Cmd = 'SELECT {0}.alias, {0}.sampleId, {0}.studyId FROM {0} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{1}\"'.format(Table, Box)
    elif Object == 'datasets':
        Cmd = 'SELECT {0}.alias, {0}.runsReferences, {0}.analysisReferences, {0}.policyId FROM {0} WHERE {0}.Status=\"clean\" AND {0}.egaBox=\"{1}\"'.format(Table, Box)
    elif Object == 'policies':
        Cmd = 'SELECT {0}.alias, {0}.dacId FROM {0} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{1}\"'.format(Table, Box)
    elif Object == 'runs':
        Cmd = 'SELECT {0}.alias, {0}.sampleId, {0}.experimentId FROM {0} WHERE {0}.Status=\"ready\" AND {0}.egaBox=\"{1}\"'.format(Table, Box)
    
    try:
        cur.execute(Cmd)
        Data = cur.fetchall()
    except:
        Data = []
       
    # create a dict to collect all accessions to be verified for a given alias
    Verify = {}
    # check if alias are in start status
    if len(Data) != 0:
        for i in Data:
            # get alias
            alias = i[0]
            # make a list with all other accessions
            accessions = []
            for j in range(1, len(i)):
                accessions.extend(list(map(lambda x: x.strip(), i[j].split(';'))))
            # remove NULL from list when only analysis or runs are included in the dataset 
            while 'NULL' in accessions:
                accessions.remove('NULL')
            Verify[alias] = accessions
        
        if len(Verify) != 0:
            # check if all accessions are readily available from metadata db
            for alias in Verify:
                # make a list with accession membership
                if False in [i in EgaAccessions for i in Verify[alias]]:
                    Error = 'EGA accession(s) not available as metadata' 
                    # record error and keep status unchanged
                    cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, Error, alias, Box)) 
                    conn.commit()
                else:
                    Error = 'NoError'
                    # set error to NoError and update status 
                    cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\", {0}.Status=\"valid\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, Error, alias, Box)) 
                    conn.commit()
    conn.close()    


# use this script to launch qsubs to encrypt the files and do a checksum
def EncryptAndChecksum(CredentialFile, DataBase, Table, Box, alias, Object, filePaths, fileNames, KeyRing, OutDir, Mem, MyScript):
    '''
    (file, str, str, str, str, str, list, list, str, str, str, int, str) -> list
    Take the file with Credential to connect to db, a given alias for Object
    for Box in Table, lists with file paths and names, the path to the encryption
    keys, the directory where encrypted and cheksums are saved, and 
    memory allocated to run the jobs and return a list of exit codes specifying
    if the jobs were launched successfully or not
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
                        QsubCmd1 = "qsub -b y -P gsi -l h_vmem={0}g -N {1} -e {2} -o {2} \"bash {3}\"".format(Mem, JobName1, logDir, BashScript1)
                    else:
                        # launch job when previous job is done
                        QsubCmd1 = "qsub -b y -P gsi -hold_jid {0} -l h_vmem={1}g -N {2} -e {3} -o {3} \"bash {4}\"".format(JobNames[-1], Mem, JobName1, logDir, BashScript1)
                    job1 = subprocess.call(QsubCmd1, shell=True)
                                   
                    JobName2 = 'Encrypt.{0}'.format(alias + '__' + fileNames[i])
                    QsubCmd2 = "qsub -b y -P gsi -hold_jid {0} -l h_vmem={1}g -N {2} -e {3} -o {3} \"bash {4}\"".format(JobName1, Mem, JobName2, logDir, BashScript2)
                    job2 = subprocess.call(QsubCmd2, shell=True)
                            
                    JobName3 = 'Md5sum.encrypted.{0}'.format(alias + '__' + fileNames[i])
                    QsubCmd3 = "qsub -b y -P gsi -hold_jid {0} -l h_vmem={1}g -N {2} -e {3} -o {3} \"bash {4}\"".format(JobName2, Mem, JobName3, logDir, BashScript3)
                    job3 = subprocess.call(QsubCmd3, shell=True)
                            
                    # store job names and exit codes
                    JobExits.extend([job1, job2, job3])
                    JobNames.extend([JobName1, JobName2, JobName3])
        
        # launch check encryption job
        MyCmd = 'sleep 300; module load python-gsi/3.6.4; python3.6 {0} CheckEncryption -c {1} -s {2} -t {3} -b {4} -a {5} -o {6} -j \"{7}\"'
        # put commands in shell script
        BashScript = os.path.join(qsubdir, alias + '_check_encryption.sh')
        with open(BashScript, 'w') as newfile:
            newfile.write(MyCmd.format(MyScript, CredentialFile, DataBase, Table, Box, alias, Object, ';'.join(JobNames)) + '\n')
                
        # launch qsub directly, collect job names and exit codes
        JobName = 'CheckEncryption.{0}'.format(alias)
        # launch job when previous job is done
        QsubCmd = "qsub -b y -P gsi -hold_jid {0} -l h_vmem={1}g -N {2} -e {3} -o {3} \"bash {4}\"".format(JobNames[-1], Mem, JobName, logDir, BashScript)
        job = subprocess.call(QsubCmd, shell=True)
        # store the exit code (but not the job name)
        JobExits.append(job)          
        
        return JobExits



# use this function to encrypt files and update status to encrypting
def EncryptFiles(CredentialFile, DataBase, Table, Object, Box, KeyRing, Mem, DiskSpace, MyScript):
    '''
    (file, str, str, str, str, str, str, int, str, str) -> None
    Take a file with credentials to connect to Database, encrypt files of aliases
    of Object (analyses or runs) only if DiskSpace (in TB) is available in scratch
    after encryption and update file status to encrypting if encryption and md5sum
    jobs are successfully launched using required memory 
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

                    # remove encrypted files if already exist in working directory
                    # it generates an error if encrypted files are present and encryption starts again
                    # make a list of files in working directory
                    CurrentEncrypted = [os.path.join(WorkingDir, j) for j in os.listdir(WorkingDir) if j[-4:] == '.gpg' in j] 
                    for j in CurrentEncrypted:
                        os.remove(j)
                    
                    # update status -> encrypting
                    conn = EstablishConnection(CredentialFile, DataBase)
                    cur = conn.cursor()
                    cur.execute('UPDATE {0} SET {0}.Status=\"encrypting\", {0}.errorMessages=\"None\" WHERE {0}.alias=\"{1}\" AND {0}.egaBox=\"{2}\"'.format(Table, alias, Box))
                    conn.commit()
                    conn.close()

                    # encrypt and run md5sums on original and encrypted files and check encryption status
                    JobCodes = EncryptAndChecksum(CredentialFile, DataBase, Table, Box, alias, Object, filePaths, fileNames, KeyRing, WorkingDir, Mem, MyScript)
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
def CheckEncryption(CredentialFile, DataBase, Table, Box, Alias, Object, JobNames):
    '''
    (file, str, str, str, str, str, str) -> None
    Take the file with DataBase credentials, a semicolon-seprated string of job
    names used for encryption and md5sum of all files under the Alias of Object,
    extract information from Table regarding Alias with encrypting Status and update
    status to upload and files with md5sums when encrypting is done
    '''        
        
    # make a list of job names
    JobNames = JobNames.split(';')
    
    # connect to database
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
    
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
            if Object == 'analyses':
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
                    if Object == 'analyses':
                        Files[file] = {'filePath': file, 'unencryptedChecksum': originalMd5, 'encryptedName': encryptedName, 'checksum': encryptedMd5, 'fileTypeId': fileTypeId} 
                    elif Object == 'runs':
                        Files[file] = {'filePath': file, 'unencryptedChecksum': originalMd5, 'encryptedName': encryptedName, 'checksum': encryptedMd5}
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
def UploadAliasFiles(alias, files, StagePath, FileDir, CredentialFile, DataBase, Table, Object, Box, Mem, UploadMode, MyScript, **KeyWordParams):
    '''
    (str, dict, str, str, str, str, str, str, str, str, str, str, str, dict) -> list
    Take a files dictionary with file information for a given alias in Box, the file with 
    DataBase credentials, the Table names, the directory StagePath where to upload
    the files in UploadMode, the directory FileDir where the command scripts are saved, name, memory and path to script to launch the jobs and return a list of
    exit codes used for uploading the encrypted and md5 files 
    '''
    
    # parse the crdential file, get username and password for given box
    UserName, MyPassword = ParseCredentials(CredentialFile, Box)
    
    # write shell scripts with command
    assert os.path.isdir(FileDir)
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
        QsubCmd = "qsub -b y -P gsi -N {0} -e {1} -o {1} \"bash {2}\"".format(JobName, logDir, BashScript)
        if JobName not in JobNames:
            job = subprocess.call(QsubCmd, shell=True)
            # record job name but not exit code.
            # may produce an error message if directory already exists. do not evaluate command during CheckUpload
            JobNames.append(JobName)
            
        # get filename and encryptedname
        fileName = os.path.basename(filePaths[i])
        encryptedName = files[filePaths[i]]['encryptedName']
        encryptedFile = os.path.join(FileDir, encryptedName)
        originalMd5 = os.path.join(FileDir, encryptedName[:-4]  + '.md5')
        encryptedMd5 = os.path.join(FileDir, encryptedName + '.md5')
        if os.path.isfile(encryptedFile) and os.path.isfile(originalMd5) and os.path.isfile(encryptedMd5):
            # upload files
            if UploadMode == 'lftp':
                MyCmd = UploadCmd.format(UserName, MyPassword, StagePath, encryptedFile, encryptedMd5, originalMd5)
            elif UploadMode == 'aspera':
                MyCmd = UploadCmd.format(MyPassword, encryptedMd5, UserName, StagePath, originalMd5, encryptedFile)
            # put command in a shell script    
            BashScript = os.path.join(qsubdir, alias + '_' + encryptedName[:-4] + '_upload.sh')
            newfile = open(BashScript, 'w')
            newfile.write(MyCmd + '\n')
            newfile.close()
            # launch job directly
            JobName = 'Upload.{0}'.format(alias + '__' + fileName)
            # hold until previous job is done
            QsubCmd = "qsub -b y -P gsi -hold_jid {0} -l h_vmem={1}g -N {2} -e {3} -o {3} \"bash {4}\"".format(JobNames[-1], Mem, JobName, logDir, BashScript)
            job = subprocess.call(QsubCmd, shell=True)
            # store job exit code and name
            JobExits.append(job)
            JobNames.append(JobName)
        else:
            return [-1]
    
    # launch check upload job
    if Object == 'analyses':
        if 'attributes' in KeyWordParams:
            AttributesTable = KeyWordParams['attributes']
        CheckCmd = 'sleep 600; module load python-gsi/3.6.4; python3.6 {0} CheckUpload -c {1} -s {2} -t {3} -b {4} -a {5} -j \"{6}\" -o {7} --Attributes {8}'
    elif Object == 'runs':
        CheckCmd = 'sleep 600; module load python-gsi/3.6.4; python3.6 {0} CheckUpload -c {1} -s {2} -t {3} -b {4} -a {5} -j \"{6}\" -o {7}' 
    
    # do not check job used to make destination directory
    JobNames = JobNames[1:]
    # put commands in shell script
    BashScript = os.path.join(qsubdir, alias + '_check_upload.sh')
    with open(BashScript, 'w') as newfile:
        if Object == 'analyses':
            newfile.write(CheckCmd.format(MyScript, CredentialFile, DataBase, Table, Box, alias, ';'.join(JobNames), Object, AttributesTable) + '\n')
        elif Object == 'runs':
            newfile.write(CheckCmd.format(MyScript, CredentialFile, DataBase, Table, Box, alias, ';'.join(JobNames), Object) + '\n')
            
    # launch qsub directly, collect job names and exit codes
    JobName = 'CheckUpload.{0}'.format(alias)
    # launch job when previous job is done
    QsubCmd = "qsub -b y -P gsi -hold_jid {0} -l h_vmem={1}g -N {2} -e {3} -o {3} \"bash {4}\"".format(JobNames[-1], Mem, JobName, logDir, BashScript)
    job = subprocess.call(QsubCmd, shell=True)
    # store the exit code (but not the job name)
    JobExits.append(job)          
        
    return JobExits

# use this function to upload the files
def UploadObjectFiles(CredentialFile, DataBase, Table, Object, FootPrintTable, Box, Mem, UploadMode, Max, MaxFootPrint, MyScript, **KeyWordParams):
    '''
    (file, str, str, str, str, int, int, str) -> None
    Take the file with credentials to connect to the database and to EGA,
    and upload files of aliases with upload status using specified Memory and 
    UploadMode and update status to uploading. 
    '''
    
    
    # connect to database
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
   
    # check Object
    if Object == 'analyses':
        # retreieve attributes table
        if 'attributes' in KeyWordParams:
            AttributesTable = KeyWordParams['attributes']
        else:
            AttributesTable = 'empty'
    
        # extract files
        try:
            # extract files for alias in upload mode for given box
            cur.execute('SELECT {0}.alias, {0}.files, {0}.WorkingDirectory, {1}.StagePath FROM {0} JOIN {1} WHERE {0}.Status=\"upload\" AND {0}.egaBox=\"{2}\" AND {0}.AttributesKey = {1}.alias'.format(Table, AttributesTable, Box))
            # check that some alias are in upload mode
            Data = cur.fetchall()
        except:
            Data = []
    elif Object == 'runs':
        # extract files
        try:
            # extract files for alias in upload mode for given box
            cur.execute('SELECT {0}.alias, {0}.files, {0}.WorkingDirectory, {0}.StagePath FROM {0} WHERE {0}.Status=\"upload\" AND {0}.egaBox=\"{1}\"'.format(Table, Box))
            # check that some alias are in upload mode
            Data = cur.fetchall()
        except:
            Data = []
    # close connection
    conn.close()
        
    # get the footprint of non-registered files on the Box's staging server
    NotRegistered = GetDiskSpaceStagingServer(CredentialFile, DataBase, FootPrintTable, Box)
    
    # check that alias are ready for uploading and that staging server's limit is not reached 
    if len(Data) != 0 and 0 <= NotRegistered < MaxFootPrint:
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
            cur.execute('UPDATE {0} SET {0}.Status=\"uploading\", {0}.errorMessages=\"None\" WHERE {0}.alias=\"{1}\" AND {0}.egaBox=\"{2}\";'.format(Table, alias, Box))
            conn.commit()
            conn.close()
            
            # upload files
            JobCodes = UploadAliasFiles(alias, files, StagePath, WorkingDir, CredentialFile, DataBase, Table, Object, Box, Mem, UploadMode, MyScript, **KeyWordParams)
                        
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
def ListFilesStagingServer(CredentialFile, DataBase, Table, Box, Object, **KeyWordParams):
    '''
    (str, str, str, str, str, dict) -> dict
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
        if Object == 'analyses':
            if 'attributes' in KeyWordParams:
                AttributesTable = KeyWordParams['attributes']
            else:
                AttributesTable = 'empty'
            Cmd = 'SELECT {0}.alias, {1}.StagePath FROM {0} JOIN {1} WHERE {0}.AttributesKey = {1}.alias AND {0}.Status=\"uploading\" AND {0}.egaBox=\"{2}\"'.format(Table, AttributesTable, Box)
        elif Object == 'runs':
            Cmd = 'SELECT {0}.alias, {0}.StagePath FROM {0} WHERE {0}.Status=\"uploading\" AND {0}.egaBox=\"{1}\"'.format(Table, Box)
        
        try:
            cur.execute(Cmd)
            Data = cur.fetchall()
        except:
            Data = []
        conn.close()
        
        # check that some aliases have the proper status
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
        # '__' may be present in file name, '__' is not allowed in alias
        logname = os.path.basename(filepath).split('__')
        i, j = logname[0], '__'.join(logname[1:])
        if alias == i.replace('Upload.', '') and FileName == j[:j.rfind('.o')]:
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
def CheckUploadFiles(CredentialFile, DataBase, Table, Box, Object, Alias, JobNames, **KeyWordParams):
    '''
    (str, str, str, str, str, str, str, dict) -> None
    Take the file with db credentials, a semicolon-separated string of job names
    used for uploading files under Alias, the Table and box for the Database
    and update status of Alias from uploading to uploaded if all the files for
    that alias were successfuly uploaded.  
    '''

    # parse credential file to get EGA username and password
    UserName, MyPassword = ParseCredentials(CredentialFile, Box)
        
    # make a dict {directory: [files]} for alias with uploading status 
    FilesBox = ListFilesStagingServer(CredentialFile, DataBase, Table, Box, Object, **KeyWordParams)
        
    # connect to database
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
    
    if Object == 'analyses':
        if 'attributes' in KeyWordParams:
            AttributesTable = KeyWordParams['attributes']
        else:
            AttributesTable = 'empty'
        Cmd = 'SELECT {0}.alias, {0}.files, {0}.WorkingDirectory, {1}.StagePath FROM {0} JOIN {1} WHERE {0}.AttributesKey = {1}.alias AND {0}.Status=\"uploading\" AND {0}.egaBox=\"{2}\" AND {0}.alias=\"{3}\"'.format(Table, AttributesTable, Box, Alias)
    elif Object == 'runs':
        Cmd = 'SELECT {0}.alias, {0}.files, {0}.WorkingDirectory, {0}.StagePath FROM {0} WHERE {0}.Status=\"uploading\" AND {0}.egaBox=\"{1}\" AND {0}.alias=\"{2}\"'.format(Table, Box, Alias)

    try:
        # extract files for alias in uploading mode for given box
        cur.execute(Cmd)
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
            for jobName in JobNames.split(';'):
                if GetJobExitStatus(jobName) != '0':
                    Uploaded = False
            
            # check if files are uploaded on the server
            for filePath in files:
                # get filename
                fileName = os.path.basename(filePath)
                #assert fileName + '.gpg' == files[filePath]['encryptedName']
                encryptedFile = files[filePath]['encryptedName']
                
                #originalMd5, encryptedMd5 = fileName + '.md5', fileName + '.gpg.md5'                    
                
                originalMd5, encryptedMd5 = encryptedFile[:-4] + '.md5', encryptedFile + '.md5'                    
                
                
                
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
            errorMessages = ';'.join(errorMessages)
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
    files corresponding to the given Alias and Box in Table and delete them if status
    is uploaded
    '''
    
    if Remove == True:
        # connect to database
        conn = EstablishConnection(CredentialFile, Database)
        cur = conn.cursor()
        
        try:
            # get the directory, files for all alias with SUBMITTED status
            cur.execute('SELECT {0}.alias, {0}.files, {0}.WorkingDirectory FROM {0} WHERE {0}.status=\"uploaded\" AND {0}.egaBox=\"{1}\"'.format(Table, Box))
            Data = cur.fetchall()
        except:
            Data = []
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


## functions specific to experiments ==========================================

           
    

 


## functions to run script ====================================================    
   
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
            if 'instrument_models' in args.url:
                if i['value'] == 'unspecified':
                    # grab label instead of value
                    assert i['label'] not in Enum
                    Enum[i['label']] = i['tag']
                else:
                    assert i['value'] not in Enum
                    Enum[i['value']] = i['tag']
            elif 'reference_chromosomes' in args.url:
                # grab value : tag
                # group corresponds to tag in reference_genomes. currently, does not suppot patches
                # group = 15 --> tag = 15 in reference_genomes = GRCH37
                # group = 1 --> tag = 1 in reference_genomes = GRCH38
                if i['group'] in ['1', '15']:
                    assert i['value'] not in Enum
                    Enum[i['value']] = i['tag']
            else:
                assert i['value'] not in Enum
                Enum[i['value']] = i['tag']
    print(Enum)    
    

# use this function to check encryption
def IsEncryptionDone(args):
    '''
    (list) -> None
    Take a list of command line arguments and update status to upload if encryption
    is done for a given alias or reset status to encrypt
    '''
    # check that encryption is done, store md5sums and path to encrypted file in db
    # update status encrypting -> upload
    CheckEncryption(args.credential, args.subdb, args.table, args.box, args.alias, args.object, args.jobnames)
  
        
# use this function to check upload    
def IsUploadDone(args):
    '''    
    (list) -> None
    Take a list of command line arguments and update status to uploaded if upload
    is done for a given alias or reset status to upload
    '''
    
    if args.object == 'analyses':
        # check that files have been successfully uploaded, update status uploading -> uploaded or rest status uploading -> upload
        CheckUploadFiles(args.credential, args.subdb, args.table, args.box, args.object, args.alias, args.jobnames, attributes = args.attributes)
    elif args.object == 'runs':
        CheckUploadFiles(args.credential, args.subdb, args.table, args.box, args.object, args.alias, args.jobnames)
    
# use this function to form json for a given object
def CreateJson(args):
    '''
    (list) -> None
    Take a list of command line arguments and form the submission json for a given Object
    The specific steps involved vary based on the Object
    '''

    # check if Analyses table exists
    Tables = ListTables(args.credential, args.subdb)
        
    if args.table in Tables:
        ## grab aliases with start status and check if required information is present in table(s) 
        if args.object == 'analyses':
            # check if required information is present in analyses, attributes and projects tables
            CheckTableInformation(args.credential, args.metadatadb, args.subdb, args.table, args.object, args.box, args.myscript, args.mypython, attributes = args.attributes, projects = args.projects) 
        elif args.object == 'samples':
            # check if required information is present in samples and attributes tables
            CheckTableInformation(args.credential, args.metadatadb, args.subdb, args.table, args.object, args.box, args.myscript, args.mypython, attributes = args.attributes)
        else:
            # check if required information is present in object table
            CheckTableInformation(args.credential, args.metadatadb, args.subdb, args.table, args.object, args.box, args.myscript, args.mypython)
        
        ## replace aliases with accessions and change status clean --> ready or keep clean --> clean
        if args.object == 'analyses':
            # replace studyId in Analyses project table if study alias is present
            AddStudyIdAnalysesProject(args.credential, args.metadatadb, args.subdb, args.table, 'AnalysesProjects', 'Studies', args.box)
            # replace sample aliases for analyses objects and update status ir no error
            AddAccessions(args.credential, args.metadatadb, args.subdb, args.table, 'Samples', 'sampleReferences', 'EGAN', True, args.box)
        elif args.object == 'experiments':
            # replace sample aliases for experiments objects
            AddAccessions(args.credential, args.metadatadb, args.subdb, args.table, 'Samples', 'sampleId', 'EGAN', False, args.box)
            # replace study aliases for experiments objects and update status if no error
            AddAccessions(args.credential, args.metadatadb, args.subdb, args.table, 'Studies', 'studyId', 'EGAS', True, args.box)
        elif args.object == 'runs':
            # replace sample aliases for runs objects
            AddAccessions(args.credential, args.metadatadb, args.subdb, args.table, 'Samples', 'sampleId', 'EGAN', False, args.box)
            # replace experiment aliases for runs objects and update status if no error
            AddAccessions(args.credential, args.metadatadb, args.subdb, args.table, 'Experiments', 'experimentId', 'EGAX', True, args.box)
        elif args.object == 'policies':
            # replace DAC aliases for policies objects and update status if no error
            AddAccessions(args.credential, args.metadatadb, args.subdb, args.table, 'Dacs', 'DacId', 'EGAC', True, args.box)
        
        ## check that EGA accessions that object depends on are available metadata and change status --> valid or keep clean --> clean
        if args.object in ['analyses', 'datasets', 'experiments', 'policies', 'runs']:
            CheckEgaAccessionId(args.credential, args.subdb, args.metadatadb, args.object, args.table, args.box)
                
        ## encrypt and upload files for analyses and runs 
        if args.object in ['analyses', 'runs']:
            ## set up working directory, add to analyses table and update status valid --> encrypt
            AddWorkingDirectory(args.credential, args.subdb, args.table, args.box)
                   
            ## encrypt new files only if diskspace is available. update status encrypt --> encrypting
            ## check that encryption is done, store md5sums and path to encrypted file in db, update status encrypting -> upload or reset encrypting -> encrypt
            EncryptFiles(args.credential, args.subdb, args.table, args.object, args.box, args.keyring, args.memory, args.diskspace, args.myscript)
        
            ## upload files and change the status upload -> uploading 
            ## check that files have been successfully uploaded, update status uploading -> uploaded or rest status uploading -> upload
            if args.object == 'analyses':
                UploadObjectFiles(args.credential, args.subdb, args.table, args.object, args.footprint, args.box, args.memory, args.uploadmode, args.max, args.maxfootprint, args.myscript, attributes = args.attributes)
            elif args.object == 'runs':
                UploadObjectFiles(args.credential, args.subdb, args.table, args.object, args.footprint, args.box, args.memory, args.uploadmode, args.max, args.maxfootprint, args.myscript)
            
            ## remove files with uploaded status. does not change status. keep status uploaded --> uploaded
            RemoveFilesAfterSubmission(args.credential, args.subdb, args.table, args.box, args.remove)

        ## form json and add to table and update status --> submit or keep current status
        if args.object == 'analyses':
            ## form json for analyses in uploaded mode, add to table and update status uploaded -> submit
            AddJsonToTable(args.credential, args.subdb, args.table, args.box, args.object, args.myscript, args.mypython, projects = args.projects, attributes = args.attributes)
        elif args.object == 'samples':
             # update status valid -> submit if no error of keep status --> valid and record errorMessage
             AddJsonToTable(args.credential, args.subdb, args.table, args.box, args.object, args.myscript, args.mypython, attributes = args.attributes)
        else:
            ## form json for all other objects in valid status and add to table
            # update status valid -> submit if no error or leep status --> and record errorMessage
            AddJsonToTable(args.credential, args.subdb, args.table, args.box, args.object, args.myscript, args.mypython)


# use this function to submit object metadata 
def SubmitMetadata(args):
    '''
    (list) -> None
    Take a list of command line arguments and submit json(s) to register a given object
    '''
    
    # check if Analyses table exists
    Tables = ListTables(args.credential, args.subdb)
    if args.table in Tables:
        # clean up objects with VALIDATED_WITH_ERRORS, VALIDATED and DRAFT submission status
        for i in ['VALIDATED_WITH_ERRORS', 'VALIDATED', 'DRAFT']:
            DeleteValidatedObjectsWithErrors(args.credential, args.subdb, args.table, args.box, args.object, args.portal, i)
        # submit analyses with submit status                
        RegisterObjects(args.credential, args.subdb, args.table, args.box, args.object, args.portal)


def FindFileTypeId(d, L, analysis_enums):
    '''
    (dict, list, dict) -> dict

    Take the dictionary with analysis file info, the list of dictionaries
    with submitted file info and a dictionary of analysis_file_types enumerations
    and return a dictionary mapping files to fileTypeId value
    '''

        
    # reverse dict {tag: value}
    tags = {}
    for i in analysis_enums:
        tags[analysis_enums[i]] = i
    
    # create a dict o map files to fileTypeId
    file_type = {}
    
    # loop over file paths in d
    for i in d:
        # loop over list of dicts with submitted file info
        for j in L:
            # map file type Id to file path by comapring checksums
            if d[i]['checksum'] == j['checksum']:
                fileTypeId = j['fileTypeId']
                file_type[i] = tags[fileTypeId]
    return file_type
    

def AddMissingWorkingDir(CredentialFile, DataBase, Table, Alias, Box):
    '''
    (file, str, str, str, str, str, str) -> None
    Create working directory in file system if doesn't already exist for a given alias
    with SUBMITTED status and record in submission database
    '''
    
    # create working directory if doesn't exist and record in submission database
    
    # connect to db
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
    
    try:
        cur.execute('SELECT {0}.alias, {0}.WorkingDirectory FROM {0} WHERE {0}.alias=\"{1}\" AND {0}.Status=\"SUBMITTED\" AND {0}.egaBox=\"{2}\"'.format(Table, Alias, Box))
        Data = cur.fetchall()[0]
    except:
        Data = []
    if len(Data) != 0:
        # check working directory
        alias, WorkingDir = Data[0], Data[1]
        if WorkingDir in ['', None, 'None', 'NULL']:
            # create working directory
            UID = str(uuid.uuid4())             
            # record identifier in table, create working directory in file system
            cur.execute('UPDATE {0} SET {0}.WorkingDirectory=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, UID, alias, Box))  
            conn.commit()
            # create working directories
            WorkingDir = GetWorkingDirectory(UID, WorkingDir = '/scratch2/groups/gsi/bis/EGA_Submissions')
            os.makedirs(WorkingDir)
    
  
# use this function to edit status to ReEncrypt
def EditSubmittedStatus(CredentialFile, DataBase, Table, Alias, Box, analysis_enums):
    '''
    (file, str, str, str, str, str, dict) -> None
    Connect to Database using the credential file and edit Alias status in Table
    for Box from SUBMITTED to encrypt, create a working directory if it doesn't
    already exist and reformat the file json
    '''
    
    # connect to db
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
        
    # 1. create working directories if they don't exist
    AddMissingWorkingDir(CredentialFile, DataBase, Table, Alias, Box)

    # 2. check that working directory exist. update Status --> encrypt and reformat file json if no error or keep status and record message
    try:
        cur.execute('SELECT {0}.alias, {0}.egaAccessionId, {0}.files, {0}.json, {0}.WorkingDirectory FROM {0} WHERE {0}.alias=\"{1}\" AND {0}.Status=\"SUBMITTED\" AND {0}.egaBox=\"{2}\"'.format(Table, Alias, Box))
        Data = cur.fetchall()[0]
    except:
        Data = []
    if len(Data) != 0:
        Error = []
        alias, egaAccession, files, submission_json, WorkingDir = Data[0], Data[1], json.loads(Data[2].replace("'", "\"")), json.loads(Data[3].replace("'", "\"")), GetWorkingDirectory(Data[4])
        # check if analysis of runs objects
        if egaAccession.startswith('EGAZ'):
            # analysis object, find file type for all files
            file_types = FindFileTypeId(files, submission_json['files'], analysis_enums)
            # reformat file json
            NewFiles = {}
            for file in files:
                fileTypeId, fileName, filePath = file_types[file], files[file]['encryptedName'], files[file]['filePath']
                assert fileName[-4:] == '.gpg'
                fileName = fileName[:-4]
                NewFiles[file] = {'filePath': filePath, 'fileName': fileName, 'fileTypeId': fileTypeId}
        elif egaAccession.startswith('EGAR'):
            # run object
            # reformat file json
            NewFiles = {}
            for file in files:
                fileName, filePath = files[file]['encryptedName'], files[file]['filePath']
                assert fileName[-4:] == '.gpg'
                fileName = fileName[:-4]
                NewFiles[file] = {'filePath': filePath, 'fileName': fileName}
        if WorkingDir in ['', 'NULL', None, 'None']:
            Error.append('Working directory does not have a valid Id')
        if os.path.isdir(WorkingDir) == False:
            Error.append('Working directory not generated')
        # check if error message
        if len(Error) != 0:
            # error is found, record error message
            cur.execute('UPDATE {0} SET {0}.errorMessages=\"{1}\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\"'.format(Table, ';'.join(Error), alias, Box))  
            conn.commit()
        else:
            # no error, update Status SUBMITTED --> encrypt and file json
            cur.execute('UPDATE {0} SET {0}.files=\"{1}\", {0}.Status=\"encrypt\", {0}.errorMessages=\"None\" WHERE {0}.alias=\"{2}\" AND {0}.egaBox=\"{3}\" AND {0}.Status=\"SUBMITTED\"'.format(Table, str(NewFiles), alias, Box))  
            conn.commit()
    conn.close()            



def UpdateSubmittedStatus(CredentialFile, DataBase, Table, Box):
    '''
    (str, str, str, str) -> None
    
    Connect to submission Database using CredentialFile and update Status to SUBMITTED
    for all aliases in Table that are already registered in Box and have an egaAccessionId
    but for which files needed to be re-uploaded. Status is updated from submit to SUBMITTED 
    '''
    
    # connect to submission database
    conn = EstablishConnection(CredentialFile, DataBase)
    cur = conn.cursor()
        
    # update table        
    try:
        cur.execute('SELECT {0}.alias, {0}.egaAccessionId FROM {0} WHERE {0}.Status=\"submit\" AND {0}.submissionStatus=\"SUBMITTED\" AND {0}.egaBox=\"{1}\"'.format(Table, Box))
        # extract all information 
        Data = cur.fetchall()
    except:
        # record error message
        Data = []
    
    if len(Data) != 0:
        for i in Data:
            alias, accession = i[0], i[1]
            # check that accession exists and object is already registered
            if accession.startswith('EGA'):
                # object already registered update status submit --> SUBMITTED 
                cur.execute('UPDATE {0} SET {0}.Status=\"SUBMITTED\" WHERE {0}.alias=\"{1}\" AND {0}.egaAccessionId=\"{2}\" AND {0}.Status=\"submit\" AND {0}.submissionStatus=\"SUBMITTED\" AND {0}.egaBox=\"{3}\"'.format(Table, alias, accession, Box))  
                conn.commit()
    conn.close()

# use this function to re-uploaded registered files
def ReUploadRegisteredFiles(args):
    '''
    (list) -> None
    Take a list of command-line arguments and change the status of registered
    file objects to encrypt for re-encryption and re-uploading (by the main tool)
    if action is reupload or change the status from submit to SUBMITTED
    when re-upload is done and json is updated if action is update
    '''
    
    # check action to be performed
    if args.action == 'reupload':
        # get analysis_file_types enumerations
        Enums = ListEnumerations(args.myscript, args.mypython)
        analysis_enums = Enums['AnalysisFileTypes']
        
        # grab alias and EGA accessions from metadata database, create a dict {alias: accession} for analysis objects
        AnalysesRegistered = ExtractAccessions(args.credential, args.metadatadb, args.box, args.analysistable)
        # grab alias and EGA accessions from metadata database, create a dict {alias: accession} for run objects
        RunsRegistered = ExtractAccessions(args.credential, args.metadatadb, args.box, args.runstable)
        
        # get the list of aliases, egaAccessionId from file
        if args.aliasfile:
            try:
                infile = open(args.aliasfile)
                Aliases = list(map(lambda x: x.strip(), infile.read().rstrip().split('\n')))
                infile.close()
            except:
                Aliases = []
        else:
            Aliases = []
    
        # change status of registered aliases to encrypt
        if len(Aliases) != 0:
            for i in Aliases:
                i = list(map(lambda x: x.strip(), i.split()))
                alias, egaAccession = i[0], i[1]
                if egaAccession.startswith('EGAZ'):
                    # analysis object
                    # make sure the object is already registered
                    if alias in AnalysesRegistered:
                        # change status SUBMITTED --> encrypt and create working directory if doesn't exist    
                        EditSubmittedStatus(args.credential, args.subdb, args.analysistable, alias, args.box, analysis_enums)
                elif egaAccession.startswith('EGAR'):
                    # runs object
                    # make sure the object is already registered
                    if alias in RunsRegistered:
                        # change status SUBMITTED --> encrypt and create working directory if doesn't exist    
                        EditSubmittedStatus(args.credential, args.subdb, args.runstable, alias, args.box, analysis_enums)
            
            # the following is run through the main tool
            # 1. encrypt new files only if diskspace is available. update status encrypt --> encrypting
            # check that encryption is done, store md5sums and path to encrypted file in db, update status encrypting -> upload or reset encrypting -> encrypt
        
            # 2. upload files and change the status upload -> uploading 
            # check that files have been successfully uploaded, update status uploading -> uploaded or rest status uploading -> upload
                
            # 3. remove files with uploaded status. does not change status. keep status uploaded --> uploaded
        
            # 4. form json and store in db and update status --> submit
            # file objects with submit status already registered are filtered out and not submitted
    elif args.action == 'update':
        # change status submit --> SUBMITTED when re-upload of files for analysis objects is done
        UpdateSubmittedStatus(args.credential, args.subdb, args.analysistable, args.box)
        # change status submit --> SUBMITTED when re-upload of files for runs objects is done
        UpdateSubmittedStatus(args.credential, args.subdb, args.runstable, args.box)

# use this function to list files on the staging servers
def FileInfoStagingServer(args):
    '''
    (list) -> None
    Take a list of command line arguments and populate tables with file info
    including size and accessions Ids of files on the staging servers for given box
    '''

    # add info for all files on staging server for given box
    AddFileInfoStagingServer(args.credential, args.metadatadb, args.subdb, args.analysestable, args.runstable, args.stagingtable, args.box)
    # summarize data into footprint table
    AddFootprintData(args.credential, args.subdb, args.stagingtable, args.footprinttable, args.box)
    

if __name__ == '__main__':

    # create top-level parser
    parent_parser = argparse.ArgumentParser(prog = 'Gaea.py', description='manages submission to EGA', add_help=False)
    parent_parser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    parent_parser.add_argument('-m', '--MetadataDb', dest='metadatadb', default='EGA', help='Name of the database collection EGA metadata. Default is EGA')
    parent_parser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
    parent_parser.add_argument('-b', '--Box', dest='box', choices=['ega-box-12', 'ega-box-137', 'ega-box-1269'], help='Box where objects will be registered', required=True)
    
    # create main parser
    main_parser = argparse.ArgumentParser(prog = 'Gaea.py', description='manages EGA submissions')
    subparsers = main_parser.add_subparsers(title='sub-commands', description='valid sub-commands', help = 'sub-commands help')

    # collect enumerations from EGA
    CollectEnumParser = subparsers.add_parser('Enums', help ='Collect enumerations from EGA')
    CollectEnumParser.add_argument('--URL', dest='url', choices=['https://ega-archive.org/submission-api/v1/enums/analysis_file_types',
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
    FormJsonParser = subparsers.add_parser('FormJson', help ='Form Analyses json for submission to EGA', parents = [parent_parser])
    FormJsonParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Database table. Default is Analyses')
    FormJsonParser.add_argument('-p', '--Projects', dest='projects', default='AnalysesProjects', help='DataBase table. Default is AnalysesProjects')
    FormJsonParser.add_argument('-a', '--Attributes', dest='attributes', default='AnalysesAttributes', help='DataBase table. Default is AnalysesAttributes')
    FormJsonParser.add_argument('-k', '--Keyring', dest='keyring', default='/.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/publickeys/public_keys.gpg', help='Path to the keys used for encryption. Default is /.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/publickeys/public_keys.gpg')
    FormJsonParser.add_argument('-u', '--UploadMode', dest='uploadmode', default='aspera', choices=['lftp', 'aspera'], help='Use lftp of aspera for uploading files. Use aspera by default')
    FormJsonParser.add_argument('-d', '--DiskSpace', dest='diskspace', default=15, type=int, help='Free disk space (in Tb) after encyption of new files. Default is 15TB')
    FormJsonParser.add_argument('-f', '--FootPrint', dest='footprint', default='FootPrint', help='Database Table with footprint of registered and non-registered files. Default is Footprint')
    FormJsonParser.add_argument('-o', '--Object', dest='object', choices=['samples', 'analyses', 'experiments', 'datasets', 'policies', 'studies', 'dacs', 'runs'], help='Object to register', required=True)
    FormJsonParser.add_argument('--MyScript', dest='myscript', default= '/.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/Submission_Tools/Gaea.py', help='Path the EGA submission script. Default is /.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/Submission_Tools/Gaea.py')
    FormJsonParser.add_argument('--MyPython', dest='mypython', default='/.mounts/labs/PDE/Modules/sw/python/Python-3.6.4/bin/python3.6', help='Path the python version. Default is /.mounts/labs/PDE/Modules/sw/python/Python-3.6.4/bin/python3.6')
    FormJsonParser.add_argument('--Mem', dest='memory', default='10', help='Memory allocated to encrypting files. Default is 10G')
    FormJsonParser.add_argument('--Max', dest='max', default=8, type=int, help='Maximum number of files to be uploaded at once. Default is 8')
    FormJsonParser.add_argument('--MaxFootPrint', dest='maxfootprint', default=15, type=int, help='Maximum footprint of non-registered files on the box\'s staging sever. Default is 15Tb')
    FormJsonParser.add_argument('--Remove', dest='remove', action='store_true', help='Delete encrypted and md5 files when analyses are successfully submitted. Do not delete by default')
    FormJsonParser.set_defaults(func=CreateJson)

    # check encryption
    CheckEncryptionParser = subparsers.add_parser('CheckEncryption', help='Check that encryption is done for a given alias', parents = [parent_parser])
    CheckEncryptionParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Database table. Default is Analyses')
    CheckEncryptionParser.add_argument('-a', '--Alias', dest='alias', help='Object alias', required=True)
    CheckEncryptionParser.add_argument('-o', '--Object', dest='object', choices=['analyses', 'runs'], help='Object files to encrypt', required=True)
    CheckEncryptionParser.add_argument('-j', '--Jobs', dest='jobnames', help='Colon-separated string of job names used for encryption and md5sums of all files under a given alias', required=True)
    CheckEncryptionParser.set_defaults(func=IsEncryptionDone)
    
    # check upload
    CheckUploadParser = subparsers.add_parser('CheckUpload', help='Check that upload is done for a given alias', parents = [parent_parser])
    CheckUploadParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Database table. Default is Analyses')
    CheckUploadParser.add_argument('-a', '--Alias', dest='alias', help='Object alias', required=True)
    CheckUploadParser.add_argument('-j', '--Jobs', dest='jobnames', help='Colon-separated string of job names used for uploading all files under a given alias', required=True)
    CheckUploadParser.add_argument('-o', '--Object', dest='object', choices=['analyses', 'runs'], help='EGA object to register (runs or analyses', required=True)
    CheckUploadParser.add_argument('--Attributes', dest='attributes', default='AnalysesAttributes', help='DataBase table. Default is AnalysesAttributes')
    CheckUploadParser.set_defaults(func=IsUploadDone)
    
    # register analyses to EGA       
    RegisterObjectParser = subparsers.add_parser('RegisterObject', help ='Submit Analyses json to EGA', parents = [parent_parser])
    RegisterObjectParser.add_argument('-t', '--Table', dest='table', help='Submission database table', required=True)
    RegisterObjectParser.add_argument('-o', '--Object', dest='object', choices=['samples', 'analyses', 'experiments', 'datasets', 'policies', 'studies', 'dacs', 'runs'], help='EGA object to register', required=True)
    RegisterObjectParser.add_argument('--Portal', dest='portal', default='https://ega.crg.eu/submitterportal/v1', help='EGA submission portal. Default is https://ega.crg.eu/submitterportal/v1')
    RegisterObjectParser.set_defaults(func=SubmitMetadata)

    # list files on the staging servers
    StagingServerParser = subparsers.add_parser('StagingServer', help ='List file info on the staging servers', parents = [parent_parser])
    StagingServerParser.add_argument('-rt', '--RunsTable', dest='runstable', default='Runs', help='Submission database table. Default is Runs')
    StagingServerParser.add_argument('-at', '--AnalysesTable', dest='analysestable', default='Analyses', help='Submission database table. Default is Analyses')
    StagingServerParser.add_argument('-st', '--StagingTable', dest='stagingtable', default='StagingServer', help='Submission database table. Default is StagingServer')
    StagingServerParser.add_argument('-ft', '--FootprintTable', dest='footprinttable', default='FootPrint', help='Submission database table. Default is FootPrint')
    StagingServerParser.set_defaults(func=FileInfoStagingServer)
   
    # re-upload registered files that cannot be archived       
    ReUploadParser = subparsers.add_parser('ReUploadFiles', help ='Encrypt and re-upload files that are registered but cannot be archived', parents = [parent_parser])
    ReUploadParser.add_argument('-a', '--Action', dest='action', choices=['reupload', 'update'], help='Action to be performed: set status to encrypt if reupload or set status to SUBMITTED if update', required=True)
    ReUploadParser.add_argument('-at', '--AnalysisTable', dest='analysistable', help='Analysis Database table', default='Analyses')
    ReUploadParser.add_argument('-rt', '--RunsTable', dest='runstable', help='Runs Database table', default='Runs')
    ReUploadParser.add_argument('--Alias', dest='aliasfile', help='Two-column tab-delimited file with aliases and egaAccessionId of files that need to be re-uploaded')
    ReUploadParser.add_argument('--MyScript', dest='myscript', default= '/.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/Submission_Tools/Gaea.py', help='Path the EGA submission script. Default is /.mounts/labs/gsiprojects/gsi/Data_Transfer/Release/EGA/Submission_Tools/Gaea.py')
    ReUploadParser.add_argument('--MyPython', dest='mypython', default='/.mounts/labs/PDE/Modules/sw/python/Python-3.6.4/bin/python3.6', help='Path the python version. Default is /.mounts/labs/PDE/Modules/sw/python/Python-3.6.4/bin/python3.6')
    ReUploadParser.set_defaults(func=ReUploadRegisteredFiles)

    # get arguments from the command line
    args = main_parser.parse_args()
    # pass the args to the default function
    args.func(args)