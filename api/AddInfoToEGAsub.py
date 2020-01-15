# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 18:51:04 2019

@author: rjovelin
"""

# import modules
import os
import argparse
# import functions 
from Gaea import *

# resource for json formatting and api submission
#https://ega-archive.org/submission/programmatic_submissions/json-message-format
#https://ega-archive.org/submission/programmatic_submissions/submitting-metadata


## functions specific to Analyses objects =====================================
    
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
    Missing =  [i for i in ['alias', 'sampleReferences', 'filePath'] if i not in Header]
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
                    L = ['alias', 'sampleReferences', 'filePath', 'analysisDate']
                    alias, sampleAlias, filePath, analysisDate = [S[Header.index(L[i])] for i in range(len(L))]
                else:
                    L = ['alias', 'sampleReferences', 'filePath']
                    alias, sampleAlias, filePath = [S[Header.index(L[i])] for i in range(len(L))]
                    analysisDate = ''
                # file name is not supplied, use filename in filepath             
                assert filePath != '/' and filePath[-1] != '/'
                fileName = os.path.basename(filePath)                
            else:
                # file name is supplied, use filename
                if 'analysisDate' in Header:
                    L = ['alias', 'sampleReferences', 'filePath', 'fileName', 'analysisDate']
                    alias, sampleAlias, filePath, fileName, analysisDate = [S[Header.index(L[i])] for i in range(len(L))]
                else:
                    L = ['alias', 'sampleReferences', 'filePath', 'fileName']
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
                D[alias]['sampleReferences'] = [sampleAlias]
                D[alias]['files'] = {}
                D[alias]['files'][filePath] = {'filePath': filePath, 'fileName': fileName}
            else:
                assert D[alias]['alias'] == alias
                # record sampleAlias
                D[alias]['sampleReferences'].append(sampleAlias)
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


## functions specific to experiments ==========================================

#use this function to parse the input experiment table
def ParseExperimentInputTable(Table):
    '''
    (file) -> list 
    Take a tab-delimited file and return a list of dictionaries, each dictionary
    storing the information for a unique experiment object
    Preconditions: Required fields must be present or returned list is empty,
    and missing entries are not permitted (e.g. can be '', NA)
    '''
    
    # create a dict to store information about the experiments
    D = {}
    
    infile = open(Table)
    # get file header
    Header = infile.readline().rstrip().split('\t')
    # check that required fields are present
    Missing =  [i for i in  ["sampleId", "alias", "libraryName"] if i not in Header]
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
            if "pairedNominalLength" not in Header:
                Length = 0
            else:
                Length = S[Header.index("pairedNominalLength")]
            if "pairedNominalSdev" not in Header:
                Sdev = 0
            else:
                Sdev = S[Header.index("pairedNominalSdev")]
            L = ["sampleId", "alias", "libraryName"]
            sample, alias, library  = [S[Header.index(L[i])] for i in range(len(L))]
            
            assert alias not in D
            # create inner dict
            D[alias] = {'alias': alias, 'libraryName': library, 'sampleId': sample,
             'pairedNominalLength': Length, 'pairedNominalSdev': Sdev}
    infile.close()

    # create list of dicts to store the info under a same alias
    L = [{alias: D[alias]} for alias in D]             
    return L            
    

## functions specific to Samples objects ======================================

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
    Required = ["alias", "caseOrControlId", "genderId", "phenotype", "subjectId"]
    Missing = [i for i in Required if i not in Header]
    
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
 

## functions to run script ====================================================    
   
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
            runsReferences = [i.strip() for i in AccessionIds if i.startswith('EGAR')]
            analysisReferences = [i.strip() for i in AccessionIds if i.startswith('EGAZ')]
            
            # make a list of data ordered according to columns
            D = {"alias": args.alias, "datasetTypeIds": ';'.join(args.datasetTypeIds),
                    "policyId": args.policy, "runsReferences": ';'.join(runsReferences),
                    "analysisReferences": ';'.join(analysisReferences), "title": args.title,
                    "description": args.description, "datasetLinks": ';'.join(datasetslinks),
                    "attributes": ';'.join(attributes), 'Status': 'start', 'egaBox': args.box}            
            # list values according to the table column order
            L = [D[field] if field in D else '' for field in Fields]
            # convert data to strings, converting missing values to NULL
            Values = FormatData(L)        
            cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(args.table, ColumnNames, Values))
            conn.commit()
        conn.close()            

 
# use this function to add data to the experiment table
def AddExperimentInfo(args):
    '''
    (list) -> None
    Take a list of command line arguments and add experiment information
    to the Experiment Table of the EGAsub database 
    '''
    
    # create table if table doesn't exist
    Tables = ListTables(args.credential, args.subdb)
    
    # connect to submission database
    conn = EstablishConnection(args.credential, args.subdb)
    cur = conn.cursor()
    if args.table not in Tables:
        Fields = ["alias", "title", "instrumentModelId", "librarySourceId",
                  "librarySelectionId", "libraryStrategyId", "designDescription",
                  "libraryName", "libraryConstructionProtocol", "libraryLayoutId",
                  "pairedNominalLength", "pairedNominalSdev", "sampleId", "studyId",
                  "Json", "submissionStatus", "errorMessages", "Receipt", "CreationTime",
                  "egaAccessionId", "egaBox", "Status"]
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
    
    # pull down alias and egaId from metadata db, alias should be unique
    # create a dict {alias: accessions}
    Registered = ExtractAccessions(args.credential, args.metadatadb, args.box, args.table)
    
    # pull down alias from submission db. alias may be recorded but not submitted yet. aliases must be unique and not already recorded in the same box
    cur.execute('SELECT {0}.alias from {0} WHERE {0}.egaBox=\"{1}\"'.format(args.table, args.box))
    Recorded = [i[0] for i in cur]
    
    # parse data from the input table
    Data = ParseExperimentInputTable(args.input)
    
    # record objects only if input table has been provided with required fields
    if len(Data) != 0:
        # check that experiments are not already in the database for that box
        for D in Data:
            # get experiment alias
            alias = list(D.keys())[0]
            if alias in Registered:
                # skip already registered in EGA
                print('{0} is already registered in box {1} under accession {2}'.format(alias, args.box, Registered[alias]))
            elif alias in Recorded:
                # skip already recorded in submission database
                print('{0} is already recorded for box {1} in the submission database'.format(alias, args.box))
            else:
                # add fields from the command
                D[alias]['title'], D[alias]['studyId']  = args.title, args.study              
                D[alias]['designDescription'], D[alias]["instrumentModelId"] = args.description, args.instrument
                D[alias]["librarySourceId"], D[alias]["librarySelectionId"] = args.source, args.selection
                D[alias]["libraryStrategyId"], D[alias]["libraryConstructionProtocol"] = args.strategy, args.protocol
                D[alias]["libraryLayoutId"], D[alias]['egaBox'] = args.library, args.box
                # set Status to start
                D[alias]["Status"] = "start"
                # list values according to the table column order
                L = [D[alias][field] if field in D[alias] else '' for field in Fields]
                # convert data to strings, converting missing values to NULL                    L
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
                  "region", "phenotype", "subjectId", "anonymizedName", "bioSampleId",
                  "sampleAge", "sampleDetail", "Json", "submissionStatus", "errorMessages", "Receipt",
                  "CreationTime", "egaAccessionId", "egaBox", "AttributesKey", "Status"]
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
                D[alias]['AttributesKey'] = args.attributes
                D[alias]['egaBox'] = args.box 
                # add alias
                D[alias]['sampleAlias'] = alias    
                # set Status to start
                D[alias]["Status"] = "start"
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
        Fields = ["alias", "sampleReferences", "analysisDate",
                  "files", "WorkingDirectory", "Json", "submissionStatus", "errorMessages", "Receipt",
                  "CreationTime", "egaAccessionId", "egaBox", "ProjectKey",
                  "AttributesKey", "Status"]
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
            # double undersocre is not allowed because alias and file names are
            # retrieved from job name split on double underscore for checking upload and encryption
            if '__' in alias:
                print('double underscore is not allowed in alias bame')
            else:               
                if alias in Registered:
                    # skip analysis, already registered in EGA
                    print('{0} is already registered in box {1} under accession {2}'.format(alias, args.box, Registered[alias]))
                elif alias in Recorded:
                    # skip analysis, already recorded in submission database
                    print('{0} is already recorded for box {1} in the submission database'.format(alias, args.box))
                else:
                    # add fields from the command
                    D[alias]['ProjectKey'], D[alias]['AttributesKey'], D[alias]['egaBox'] = args.projects, args.attributes, args.box 
                    # check if analysisDate is provided in input table
                    if 'analysisDate' not in D[alias]:
                        D[alias]['analysisDate'] = ''
                    # add fileTypeId to each file
                    for filePath in D[alias]['files']:
                        extension, fileTypeId = '', ''
                        extension = filePath[filePath.rfind('.'):].lower()
                        if extension == '.gz':
                            fileTypeId = filePath[-6:].replace('.gz', '')
                        else:
                            fileTypeId = extension.replace('.', '')
                        assert fileTypeId in ['bam', 'bai', 'vcf', 'tab'], 'valid file extensions are bam, vcf, bai and tab'
                        # check that file type Id is also in the filename
                        if 'gz' in extension:
                            assert D[alias]['files'][filePath]['fileName'][-6:].replace('.gz', '') == fileTypeId, '{0} should be part of the file name'.format(fileTypeId)
                        else:
                            assert D[alias]['files'][filePath]['fileName'][-3:] == fileTypeId, '{0} should be part of the file name'.format(fileTypeId)
                        # add fileTypeId to dict
                        assert 'fileTypeId' not in D[alias]['files'][filePath] 
                        D[alias]['files'][filePath]['fileTypeId'] = fileTypeId
                    # check if multiple sample alias/Ids are used. store sample aliases/Ids as string
                    sampleIds = ';'.join(list(set(D[alias]['sampleReferences'])))
                    D[alias]["sampleReferences"] = sampleIds    
                    # set Status to start
                    D[alias]["Status"] = "start"
                    # list values according to the table column order
                    L = [D[alias][field] if field in D[alias] else '' for field in Fields]
                    # convert data to strings, converting missing values to NULL                    L
                    Values = FormatData(L)        
                    cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(args.table, ColumnNames, Values))
                    conn.commit()
    conn.close()            


# use this function to parse study input table
def ParseStudyInputTable(Table):
    '''
    (file) -> dict
    Read Table and returns of key: value pairs 
    '''
    
    infile = open(Table)
    Content = infile.read().rstrip().split('\n')
    infile.close()
    # create a dict {key: value}
    D = {}
    # check that required fields are present
    Expected = ["alias", "studyTypeId", "title", "studyAbstract"]
    
    Fields = [S.split(':')[0].strip() for S in Content if ':' in S]
    Missing = [i for i in Expected if i not in Fields]
    if len(Missing) != 0:
        print('These required fields are missing: {0}'.format(', '.join(Missing)))
    else:
        for S in Content:
            S = list(map(lambda x: x.strip(), S.split(':')))
            # non-attributes may contain multiple colons. need to put them back together
            if S[0] == 'attributes':
                if 'customTags' not in D:
                    D['customTags'] = []
                D['customTags'].append({'tag': str(S[1]), 'value': ':'.join([str(S[i]) for i in range(2, len(S))])})
            elif S[0] == 'pubMedIds':
                D[S[0]] = ';'.join([str(S[i]) for i in range(1, len(S))])
            else:
                D[S[0]] = ':'.join([str(S[i]) for i in range(1, len(S))])
    infile.close()
    return D

# use this function to add data to the study table
def AddStudyInfo(args):
    '''
    (list) -> None
    Take a list of command line arguments and add study information into the Study
    Table of the EGAsub database
    '''
    
    # create table if table doesn't exist
    Tables = ListTables(args.credential, args.subdb)
    
    # connect to submission database
    conn = EstablishConnection(args.credential, args.subdb)
    cur = conn.cursor()
    
    if args.table not in Tables:
        Fields = ["alias", "studyTypeId", "shortName", "title", "studyAbstract",
                  "ownTerm", "pubMedIds", "customTags", "Json", "submissionStatus",
                  "errorMessages", "Receipt", "CreationTime", "egaAccessionId", "egaBox",  "Status"]
        # format colums with datatype
        Columns = []
        for i in range(len(Fields)):
            if Fields[i] == 'Status':
                Columns.append(Fields[i] + ' TEXT NULL')
            elif Fields[i] in ['Json', 'Receipt', 'files', 'pubMedIds', 'studyAbstract']:
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
    
    # parse input file
    Data = ParseStudyInputTable(args.input)
    
    # pull down alias and egaId from metadata db, alias should be unique
    # create a dict {alias: accessions}
    Registered = ExtractAccessions(args.credential, args.metadatadb, args.box, args.table)
            
    # pull down alias from submission db. alias may be recorded but not submitted yet. aliases must be unique and not already recorded in the same box
    # create a dict {alias: accession}
    cur.execute('SELECT {0}.alias from {0} WHERE {0}.egaBox=\"{1}\"'.format(args.table, args.box))
    Recorded = [i[0] for i in cur]
    
    # record objects only if input table has been provided with required fields
    if len(Data) != 0:
        # get alias
        alias = Data['alias']
        if alias in Registered:
            # skip analysis, already registered in EGA
            print('{0} is already registered in box {1} under accession {2}'.format(alias, args.box, Registered[alias]))
        elif alias in Recorded:
            # skip analysis, already recorded in submission database
            print('{0} is already recorded for box {1} in the submission database'.format(alias, args.box))
        else:
            Data["Status"] = "start"
            Data["egaBox"] = args.box
            # list values according to the table column order
            L = [str(Data[field]) if field in Data else '' for field in Fields]
            # convert data to strings, converting missing values to NULL                    L
            Values = FormatData(L)        
            cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(args.table, ColumnNames, Values))
            conn.commit()
    conn.close()            



# use this function to parse the DAC input file
def ParseDACInputTable(Table):
    '''
    (file) -> list
    Return a list of dictionaries with key:values pairs with keys in header
    of Table file
    '''
    
    infile = open(Table)
    Header = infile.readline().rstrip().split('\t')
    Content = infile.read().rstrip().split('\n')
    infile.close()
    # create a list [{key: value}]
    L = []
    # check that required fields are present
    Expected = ["contactName", "email", "organisation", "phoneNumber", "mainContact"]
    Missing = [i for i in Expected if i not in Header]    
    if len(Missing) != 0:
        print('These required fields are missing: {0}'.format(', '.join(Missing)))
    else:
        for S in Content:
            D = {}
            S = list(map(lambda x: x.strip(), S.split('\t')))
            for i in range(len(Header)):
                D[Header[i]] = S[i]
            L.append(D)
    return L        
            
    
# use this function to add DAC info
def AddDACInfo(args):
    '''
    (list) -> None
    Take a list of command line arguments and add DAC information into the DAC 
    Table of the EGAsub database
    '''
    
    # create table if table doesn't exist
    Tables = ListTables(args.credential, args.subdb)
    
    # connect to submission database
    conn = EstablishConnection(args.credential, args.subdb)
    cur = conn.cursor()
    
    if args.table not in Tables:
        Fields = ["alias", "title", "contacts", "Json", "submissionStatus", "errorMessages",
                  "Receipt", "CreationTime", "egaAccessionId", "egaBox",  "Status"]
        # format colums with datatype
        Columns = []
        for i in range(len(Fields)):
            if Fields[i] == 'Status':
                Columns.append(Fields[i] + ' TEXT NULL')
            elif Fields[i] in ['Json', 'Receipt', 'title', 'contacts']:
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
    
    # parse input file
    Data = list(map(lambda x: str(x), ParseDACInputTable(args.input)))    
    
    # pull down alias and egaId from metadata db, alias should be unique
    # create a dict {alias: accessions}
    Registered = ExtractAccessions(args.credential, args.metadatadb, args.box, args.table)
            
    # pull down alias from submission db. alias may be recorded but not submitted yet. aliases must be unique and not already recorded in the same box
    # create a dict {alias: accession}
    cur.execute('SELECT {0}.alias from {0} WHERE {0}.egaBox=\"{1}\"'.format(args.table, args.box))
    Recorded = [i[0] for i in cur]
    
    # record objects only if input table has been provided with required fields
    if len(Data) != 0:
        # check if alias is unique
        if args.alias in Registered:
            # skip, already registered in EGA
            print('{0} is already registered in box {1} under accession {2}'.format(args.alias, args.box, Registered[args.alias]))
        elif args.alias in Recorded:
            # skip, already recorded in submission database
            print('{0} is already recorded for box {1} in the submission database'.format(args.alias, args.box))
        else:
            # create dict and add command line arguments
            D = {'alias': args.alias, 'title': args.title, 'contacts': ';'.join(Data), 'egaBox': args.box, 'Status': 'start'}
            # list values according to the table column order
            L = [str(D[field]) if field in D else '' for field in Fields]
            # convert data to strings, converting missing values to NULL                    L
            Values = FormatData(L)        
            cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(args.table, ColumnNames, Values))
            conn.commit()
    conn.close()            


# use this function to add DAC info
def AddPolicyInfo(args):
    '''
    (list) -> None
    Take a list of command line arguments and add Policy information into the Policy 
    Table of the EGAsub database
    '''
    
    # create table if table doesn't exist
    Tables = ListTables(args.credential, args.subdb)
    
    # connect to submission database
    conn = EstablishConnection(args.credential, args.subdb)
    cur = conn.cursor()
    
    if args.table not in Tables:
        Fields = ["alias", "dacId", "title", "policyText", "url", "Json",
                  "submissionStatus", "errorMessages", "Receipt", "CreationTime",
                  "egaAccessionId", "egaBox",  "Status"]
        # format colums with datatype
        Columns = []
        for i in range(len(Fields)):
            if Fields[i] == 'Status':
                Columns.append(Fields[i] + ' TEXT NULL')
            elif Fields[i] in ['Json', 'Receipt', 'title']:
                Columns.append(Fields[i] + ' MEDIUMTEXT NULL,')
            elif Fields[i] == 'policyText':
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
    
    # pull down alias and egaId from metadata db, alias should be unique
    # create a dict {alias: accessions}
    Registered = ExtractAccessions(args.credential, args.metadatadb, args.box, args.table)
            
    # pull down alias from submission db. alias may be recorded but not submitted yet. aliases must be unique and not already recorded in the same box
    # create a dict {alias: accession}
    cur.execute('SELECT {0}.alias from {0} WHERE {0}.egaBox=\"{1}\"'.format(args.table, args.box))
    Recorded = [i[0] for i in cur]
    
    # check if alias is unique
    if args.alias in Registered:
        # skip, already registered in EGA
        print('{0} is already registered in box {1} under accession {2}'.format(args.alias, args.box, Registered[args.alias]))
    elif args.alias in Recorded:
        # skip, already recorded in submission database
        print('{0} is already recorded for box {1} in the submission database'.format(args.alias, args.box))
    else:
        
        # create a dict to store fields
        Data = {}
                
        # add fields from the command
        # create dict and add command line arguments
        # get policy text from command or file
        if args.policyfile:
            infile = open(args.policyfile)
            policyText = infile.read().rstrip()
            infile.close()
        elif args.policytext:
            policyText = args.policytext
        else:
            raise ValueError('Missing policy text')
            
        if args.url:
            Data['url'] = args.url
            
        Data['alias'], Data['dacId'], Data['egaBox'] = args.alias, args.dacid, args.box
        Data['title'], Data['policyText'] = args.title, policyText
            
        # set status --> start
        Data['Status'] = 'start'
        # list values according to the table column order
        L = [str(Data[field]) if field in Data else '' for field in Fields]
        # convert data to strings, converting missing values to NULL
        Values = FormatData(L)        
        cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(args.table, ColumnNames, Values))
        conn.commit()
    conn.close()            


# use this function to parse run table info
def ParseRunInfo(Table):
    '''
    (file) -> dict
    Return a dictionary with run info from the Table file
    '''
    
    # create a dict to store the information about the files
    D = {}
    
    infile = open(Table)
    # get file header
    Header = infile.readline().rstrip().split('\t')
    # check that required fields are present
    Expected = ['alias', 'sampleId', 'experimentId', 'filePath']
    Missing =  [i for i in Expected if i not in Header]
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
                # upload file under the same name
                L = ['alias', 'sampleId', 'experimentId', 'filePath']
                alias, sampleAlias, experimentId, filePath = [S[Header.index(L[i])] for i in range(len(L))]
                assert filePath != '/' and filePath[-1] != '/'
                fileName = os.path.basename(filePath)                
            else:
                # file name is supplied at least for some runs, upload as fileName if provided
                L = ['alias', 'sampleId', 'experimentId', 'filePath', 'fileName']
                alias, sampleAlias, experimentId, filePath, fileName = [S[Header.index(L[i])] for i in range(len(L))]
                # get fileName from path if fileName not provided for that alias
                if fileName in ['', 'NULL', 'NA']:
                    fileName = os.path.basename(filePath)
            # check if alias already recorded ( > 1 files for this alias)
            if alias not in D:
                # create inner dict, record sampleAlias and create files dict
                D[alias] = {}
                D[alias]['alias'] = alias
                D[alias]['sampleId'] = sampleAlias
                D[alias]['experimentId'] = experimentId
                D[alias]['files'] = {}
                D[alias]['files'][filePath] = {'filePath': filePath, 'fileName': fileName}
            else:
                assert D[alias]['alias'] == alias
                # check that aliass is the same
                assert D[alias]['sampleId'] == sampleAlias
                # record file info, filepath shouldn't be recorded already 
                assert filePath not in D[alias]['files']
                D[alias]['files'][filePath] = {'filePath': filePath, 'fileName': fileName}
    infile.close()
    return D
    
# use this function to add data to the runs table
def AddRunsInfo(args):
    '''
    (list) -> None
    Take a list of command line arguments and add runs information to the Runs
    Table of the EGAsub database if files are not already registered
    '''
    
    
    # create table if table doesn't exist
    Tables = ListTables(args.credential, args.subdb)
    
    # connect to submission database
    conn = EstablishConnection(args.credential, args.subdb)
    cur = conn.cursor()
    
    # create table if it doesn't exist
    if args.table not in Tables:
        Fields = ["alias", "sampleId", "runFileTypeId", "experimentId", "files",
                  "WorkingDirectory", "StagePath", "Json", "submissionStatus", "errorMessages", "Receipt",
                  "CreationTime", "egaAccessionId", "egaBox", "Status"]
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
    
    # pull down alias and egaId from metadata db, alias should be unique
    # create a dict {alias: accessions}
    Registered = ExtractAccessions(args.credential, args.metadatadb, args.box, args.table)
    
    
    # pull down alias from submission db. alias may be recorded but not submitted yet.
    # aliases must be unique and not already recorded in the same box
    # create a dict {alias: accession}
    cur.execute('SELECT {0}.alias from {0} WHERE {0}.egaBox=\"{1}\"'.format(args.table, args.box))
    Recorded = [i[0] for i in cur]
    
    # parse input table [{alias: {'sampleAlias':[sampleAlias], 'files': {filePath: {'filePath': filePath, 'fileName': fileName}}}}]
    try:
        Data = ParseRunInfo(args.input)
        # make a list of dictionary holding info for a single alias
        Data = [{alias: Data[alias]} for alias in Data]
    except:
        Data = []
        
    # record objects only if input table has been provided with required fields
    if len(Data) != 0:
        # check that runs are not already in the database for that box
        for D in Data:
            # get run alias
            alias = list(D.keys())[0]
            # double underscore is not allowed in alias name because alias and file name
            # are retrieved from job name split on double underscore for verificatrion of upload and encryption 
            if '__' in alias:
                print('double underscore is not allowed in alias name')
            else:
                if alias in Registered:
                    # skip, already registered in EGA
                    print('{0} is already registered in box {1} under accession {2}'.format(alias, args.box, Registered[alias]))
                elif alias in Recorded:
                    # skip, already recorded in submission database
                    print('{0} is already recorded for box {1} in the submission database'.format(alias, args.box))
                else:
                    # add fields from the command
                    D[alias]['runFileTypeId'], D[alias]['egaBox'], D[alias]['StagePath'] = args.filetype, args.box, args.stagepath
                    # set Status to start
                    D[alias]["Status"] = "start"
                    # list values according to the table column order
                    L = [D[alias][field] if field in D[alias] else '' for field in Fields]
                    # convert data to strings, converting missing values to NULL                    L
                    Values = FormatData(L)        
                    cur.execute('INSERT INTO {0} ({1}) VALUES {2}'.format(args.table, ColumnNames, Values))
                    conn.commit()
    conn.close()            


if __name__ == '__main__':

    # create top-level parser
    parent_parser = argparse.ArgumentParser(prog = 'AddInfoToEGAsub.py', description='Add information to the EGAsub tables', add_help=False)
    parent_parser.add_argument('-c', '--Credentials', dest='credential', help='file with database credentials', required=True)
    parent_parser.add_argument('-m', '--MetadataDb', dest='metadatadb', default='EGA', help='Name of the database collection EGA metadata. Default is EGA')
    parent_parser.add_argument('-s', '--SubDb', dest='subdb', default='EGASUB', help='Name of the database used to object information for submission to EGA. Default is EGASUB')
    parent_parser.add_argument('-b', '--Box', dest='box', choices=['ega-box-12', 'ega-box-137', 'ega-box-1269'], help='Box where samples will be registered')
    
    # create main parser
    main_parser = argparse.ArgumentParser(prog = 'AddInfoToEGAsub.py', description='Add information to the EFAsub tables')
    subparsers = main_parser.add_subparsers(title='sub-commands', description='valid sub-commands', help = 'sub-commands help')

    # add samples to Samples Table
    AddSamplesParser = subparsers.add_parser('AddSamples', help ='Add sample information to Samples Table', parents=[parent_parser])
    AddSamplesParser.add_argument('-t', '--Table', dest='table', default='Samples', help='Samples table. Default is Samples')
    AddSamplesParser.add_argument('-a', '--Attributes', dest='attributes', help='Primary key in the SamplesAttributes table', required=True)
    AddSamplesParser.add_argument('-i', '--Input', dest='input', help='Input table with sample info to load to submission database', required=True)
    AddSamplesParser.set_defaults(func=AddSampleInfo)

    # add sample attributes to SamplesAttributes Table
    AddSamplesAttributesParser = subparsers.add_parser('AddSamplesAttributes', help ='Add sample attributes information to SamplesAttributes Table', parents=[parent_parser])
    AddSamplesAttributesParser.add_argument('-t', '--Table', dest='table', default='SamplesAttributes', help='SamplesAttributes table. Default is SamplesAttributes')
    AddSamplesAttributesParser.add_argument('-i', '--Input', dest='input', help='Input table with sample attributes info', required=True)
    AddSamplesAttributesParser.set_defaults(func=AddSampleAttributes)

    # add analyses to Analyses Table
    AddAnalysesParser = subparsers.add_parser('AddAnalyses', help ='Add analysis information to Analyses Table', parents = [parent_parser])
    AddAnalysesParser.add_argument('-t', '--Table', dest='table', default='Analyses', help='Analyses table. Default is Analyses')
    AddAnalysesParser.add_argument('-i', '--Input', dest='input', help='Input table with analysis info to load to submission database', required=True)
    AddAnalysesParser.add_argument('-p', '--Project', dest='projects', help='Primary key in the AnalysesProjects table', required=True)
    AddAnalysesParser.add_argument('-a', '--Attributes', dest='attributes', help='Primary key in the AnalysesAttributes table', required=True)
    AddAnalysesParser.set_defaults(func=AddAnalysesInfo)

    # add analyses attributes or projects to corresponding Table
    AddAttributesProjectsParser = subparsers.add_parser('AddAttributesProjects', help ='Add information to AnalysesAttributes or AnalysesProjects Tables', parents = [parent_parser])
    AddAttributesProjectsParser.add_argument('-t', '--Table', dest='table', choices = ['AnalysesAttributes', 'AnalysesProjects'], help='Database Tables AnalysesAttributes or AnalysesProjects', required=True)
    AddAttributesProjectsParser.add_argument('-i', '--Input', dest='input', help='Input table with attributes or projects information to load to submission database', required=True)
    AddAttributesProjectsParser.add_argument('-d', '--DataType', dest='datatype', choices=['Projects', 'Attributes'], help='Add Projects or Attributes infor to db')
    AddAttributesProjectsParser.set_defaults(func=AddAnalysesAttributesProjects)
    
    # add datasets to Datasets Table
    AddDatasetsParser = subparsers.add_parser('AddDatasets', help ='Add datasets information to Datasets Table', parents = [parent_parser])
    AddDatasetsParser.add_argument('-t', '--Table', dest='table', default='Datasets', help='Datasets table. Default is Datasets')
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
    
    # add experiments to Experiments Table
    AddExperimentParser = subparsers.add_parser('AddExperiments', help ='Add experiments information to Experiments Table', parents = [parent_parser])
    AddExperimentParser.add_argument('-t', '--Table', dest='table', default='Experiments', help='Experiments table. Default is Experiments')
    AddExperimentParser.add_argument('-i', '--Input', dest='input', help='Input table with library and sample information', required=True)
    AddExperimentParser.add_argument('--Title', dest='title', help='Short title', required=True)
    AddExperimentParser.add_argument('--StudyId', dest='study', help='Study alias or EGA accession Id', required=True)
    AddExperimentParser.add_argument('--Description', dest='description', help='Library description', required=True)
    AddExperimentParser.add_argument('--Instrument', dest='instrument', help='Instrument model. Controlled vocabulary from EGA', required=True)
    AddExperimentParser.add_argument('--Selection', dest='selection', help='Library selection. Controlled vocabulary from EGA', required=True)
    AddExperimentParser.add_argument('--Source', dest='source', help='Library source. Controlled vocabulary from EGA', required=True)
    AddExperimentParser.add_argument('--Strategy', dest='strategy', help='Library strategy. Controlled vocabulary from EGA', required=True)
    AddExperimentParser.add_argument('--Protocol', dest='protocol', help='Library construction protocol.', required=True)
    AddExperimentParser.add_argument('--Layout', dest='library', help='0 for aired and 1 for single end sequencing', required=True)
    AddExperimentParser.set_defaults(func=AddExperimentInfo)
    
    # add DAC info to DACs Table
    AddDACsParser = subparsers.add_parser('AddDAC', help ='Add DAC information to DACs Table', parents = [parent_parser])
    AddDACsParser.add_argument('-t', '--Table', dest='table', default='Dacs', help='DACs table. Default is Dacs')
    AddDACsParser.add_argument('-i', '--Input', dest='input', help='Input table with contact information', required=True)
    AddDACsParser.add_argument('-a', '--Alias', dest='alias', help='Alias for the DAC', required=True)
    AddDACsParser.add_argument('--Title', dest='title', help='Short title for the DAC', required=True)
    AddDACsParser.set_defaults(func=AddDACInfo)
    
    # add Policy info to Policy Table
    AddPolicyParser = subparsers.add_parser('AddPolicy', help ='Add Policy information to Policy Table', parents = [parent_parser])
    AddPolicyParser.add_argument('-t', '--Table', dest='table', default='Policies', help='Policy table. Default is Policies')
    AddPolicyParser.add_argument('-a', '--Alias', dest='alias', help='Alias for the Policy', required=True)
    AddPolicyParser.add_argument('-d', '--DacId', dest='dacid', help='DAC Id or DAC alias', required=True)
    AddPolicyParser.add_argument('-tl', '--Title', dest='title', help='Policy title', required=True)
    AddPolicyParser.add_argument('-pf', '--PolicyFile', dest='policyfile', help='File with policy text')
    AddPolicyParser.add_argument('-pt', '--PolicyText', dest='policytext', help='Policy text')
    AddPolicyParser.add_argument('-u', '--Url', dest='url', help='Url')
    AddPolicyParser.set_defaults(func=AddPolicyInfo)
    
    # add Run info to Runs Table
    AddRunsParser = subparsers.add_parser('AddRuns', help ='Add Policy information to Policy Table', parents = [parent_parser])
    AddRunsParser.add_argument('-t', '--Table', dest='table', default='Runs', help='Run table. Default is Runs')
    AddRunsParser.add_argument('-i', '--Input', dest='input', help='Input table with required information', required=True)
    AddRunsParser.add_argument('-f', '--FileTypeId', dest='filetype', help='Controlled vocabulary decribing the file type. Example: "One Fastq file (Single)" or "Two Fastq files (Paired)"', required=True)
    AddRunsParser.add_argument('--StagePath', dest='stagepath', help='Directory on the staging server where files are uploaded', required=True)
    AddRunsParser.set_defaults(func=AddRunsInfo)
                      
    # add Study in to Studies table
    AddStudyParser = subparsers.add_parser('AddStudy', help ='Add Study information to Studies Table', parents = [parent_parser])
    AddStudyParser.add_argument('-t', '--Table', dest='table', default='Studies', help='Studies table. Default is Studies')
    AddStudyParser.add_argument('-i', '--Input', dest='input', help='Input table with required information', required=True)
    AddStudyParser.set_defaults(func=AddStudyInfo)
        
    # get arguments from the command line
    args = main_parser.parse_args()
    # pass the args to the default function
    args.func(args)