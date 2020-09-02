#!/usr/bin/env python

"""
Usage:
  py.test build_cdm_object.py -s -rs --spec --tb=short --zk_nodes=<servers> --zk_path=<path> --customer=<customer> --ccdm_path=<path> --path=<path> --build_type=<type> --html=<path> --self-contained-html --junitxml=<path> -d -n <num_processes>
  build_cdm_object.py (-h | --help)
  build_cdm_object.py --version
Description:
    Helper script called from build_cdm_driver.py for parallel test execution. This script not intended to be called directly.
"""

# CDM Build Script
# Called by build_cdm_driver.py to execute subtests in parallel
#
# Created By: Adam Kaus
# Created On: 13-Sep-2016
# Notes:
#
# Revision History: 13-Sep-2016 ACK Initial version
#                   22-Sep-2016 ACK DB connection close moved to conftest.py
#                   05-Oct-2016 ACK Added commit/rollback statements to test__build_object; Changed logic for directory search
#                   12-Dec-2016 ACK Modified logic for python import to force use of specific version and to recompile; Enhanced error handling for PLO post-processing
#                   19-Dec-2016 ACK Added logic to execute the data anomaly handler
#                   05-Jan-2017 ACK Modified to use zookeeper instead of redis for obtaining db connection details
#                   08-Mar-2017 ACK Revision History will be maintained via git history from this point forward 
#

import sys, os
from os import path
import glob
import pytest
import re
import codecs
from docopt import docopt
import imp
import py_compile
import multiprocessing

class Test_Build():

    # used to parse the name from SQL statment for use as a test name (ex. get the constraint name)
    # pSQL  -> command being parsed
    # pLeft -> left anchor
    # pRight -> right anchor
    # pRFind -> use rfind() instead of find 
    def parse_name(self, pSQL, pLeft, pRight, pRFind=False):
        lName = ''
        if pRFind:
            lLeftChar = pSQL.upper().rfind(pLeft) + len(pLeft)
            lRightChar = pSQL.upper().rfind(pRight)
        else:
            lLeftChar = pSQL.upper().find(pLeft) + len(pLeft)
            lRightChar = pSQL.upper().find(pRight)  
        lName = pSQL[lLeftChar:lRightChar].strip()

        return lName # if this returns an empty string it will be caught below and a default test name applied

    def file_has_string(self, input_file, s):
        return s in open(input_file).read()

    # generate individual tests as applicable for the path input
    def pytest_generate_tests(self, metafunc):
        lPath = metafunc.config.option.path # get value of --path
        lBuildType = metafunc.config.option.build_type # get value of --build_type
        lStudyName = metafunc.config.option.study_name # get value of --study_name
        if lPath is None:
            pytest.fail('Path fixture is empty')
        
        if lPath.endswith('.sql') and '*' not in lPath:
            if lBuildType != 'CONSTRAINT' or self.file_has_string(lPath, 'parallel_test=0'):
                # just generate 1 test for the whole file for non-constraint sql files (ex. cleanup)
                lFile = lPath[lPath.rfind('/')+1:-4] #substring path to get file name
                if lStudyName != 'combined_studies':
                    metafunc.parametrize("file_name,sql,object_name", [(lPath, None, lFile)], ids=['{study_name}_{filename}'.format(study_name=lStudyName, filename=lFile)])
                else:
                    metafunc.parametrize("file_name,sql,object_name", [(lPath, None, lFile)], ids=[lFile])

            else:
                # if it is a SQL constraint file then we need to separate each statement by semi-colon and 
                # generate separete tests that are run in parallel (ex. create constraints in parallel)
                paramList = []
                idList = []
                lSQLCmd = ''
                cnt = 0

                f = open(lPath,'r')
                lAddLine = True
                for i, line in enumerate(f):
                    # read each line of the SQL file and remove commented lines
                    #if i == 0:
                    # this section commented out to force failure if file encoded in UTF8
                    # line = line.lstrip(codecs.BOM_UTF8) # strip UTF8 BOM if exists
                    if lAddLine and line[:2] == '/*':
                        lAddLine = False
                    if lAddLine and line[:2] != '--':
                        lSQLCmd += line
                    if not lAddLine and '*/' in line:
                        lAddLine = True
                lSQLCmd = lSQLCmd.split(';') # split on semi-colon

                for i in (lSQLCmd):
                    lObject = None
                    lSQL = i.strip()
                    cnt += 1
                    if len(lSQL) > 0: 
                        lSQL += ';' # add the semi-colon back for execution

                        # this block uses key phrases to the left and right of the object name to identify the test name
                        # ex. return table_name.column.name for a column comment statement
                        if 'COMMENT ON TABLE' in (lSQL.upper()):
                            lObject = self.parse_name(lSQL, 'COMMENT ON TABLE', ' IS ')
                        elif 'COMMENT ON COLUMN' in (lSQL.upper()):
                            lObject = self.parse_name(lSQL, 'COMMENT ON COLUMN', ' IS ')
                        elif 'SET NOT NULL' in (lSQL.upper()):
                            # need additional commands to concatenate the table name and column name
                            lObject = self.parse_name(lSQL, 'ALTER TABLE ', ' ALTER ', True)
                            lObject1 = self.parse_name(lSQL, ' ALTER ', 'SET NOT NULL', True)
                            lObject += '.{column}'.format(column=lObject1)
                        elif 'CREATE INDEX' in (lSQL.upper()):
                            lObject = self.parse_name(lSQL, 'CREATE INDEX', 'ON ')
                        elif 'CREATE UNIQUE INDEX' in (lSQL.upper()):
                            lObject = self.parse_name(lSQL, 'CREATE UNIQUE INDEX', 'ON ')
                        elif 'PRIMARY KEY' in (lSQL.upper()):
                            lObject = self.parse_name(lSQL, 'ADD CONSTRAINT', 'PRIMARY KEY')
                        elif 'FOREIGN KEY' in (lSQL.upper()):
                            lObject = self.parse_name(lSQL, 'ADD CONSTRAINT', 'FOREIGN KEY')

                        paramList.append((None, lSQL, lPath))
                        if lObject != None and len(lObject) > 0 and lObject not in idList:
                            # this block will generate a test with the meaningful ID parsed above 
                            # if the same ID has not already been used (to avoid a uniqueness issue)
                            if lStudyName != 'combined_studies':
                                idList.append('''{study_name}_{object}'''.format( study_name=lStudyName, object=lObject))
                            else:
                                idList.append('''{object}'''.format(object=lObject))
                        else:
                            # if for some reason we cannot assign a meaningful test ID then generate it 
                            # with a numeric ID 
                            idList.append('''{object}_{int}'''.format( object=lObject, int=cnt))
                metafunc.parametrize("file_name,sql,object_name", paramList, ids=idList)
                f.close()
        
        elif lPath.endswith('.py'):
            # for a python file simply generate a single test and pass the file path
            lFile = lPath[lPath.rfind('/')+1:-3] #substring path to get file name 
            if lStudyName != 'combined_studies':
                    metafunc.parametrize("file_name,sql,object_name", [(lPath, None, lFile)], ids=['{study_name}_{filename}'.format(study_name=lStudyName, filename=lFile)])
            else:
                metafunc.parametrize("file_name,sql,object_name", [(lPath, None, lFile)], ids=[lFile])
        
        else:
            # here we assume this is a dir/search path so generate a separate test per file
            lFiles = sorted(glob.glob('{path}'.format(path=lPath)))
            paramList = []
            idList = []
            if not lFiles:
                pytest.fail('No Files found in {path}'.format(path=lPath))
            for i in lFiles:
                lTable = i[i.rfind('/')+1:-4] #substring path to get table name 
                paramList.append((i, None, lTable))
                if lStudyName != 'combined_studies':
                    idList.append('''{study_name}_{table}'''.format(study_name=lStudyName, table=lTable))
                else:
                    idList.append('''{table}'''.format(table=lTable))
            metafunc.parametrize("file_name,sql,object_name", paramList, ids=idList)

    # build tests generated depending on specifications from pytest_generate_tests
    def test__build_object(self, connect_database, schema, build_type, object_name, file_name, sql, zk_nodes, zk_path, customer, ccdm_path, num_processes, sql_limit, days_limit, scorecard_incremental):
        try:
            lFailure = 0
            lErrMsg = 'Unknown Error'
            lCur = connect_database.cursor()

            if build_type == 'PYTHON':
                # currently designed to handle the plo_post_proccessing_script.py script and test__6_5_cdm_data_anomaly_handler 
                # which are imported and executed as a module 
                if object_name in ('plo_post_proccessing_script', 'data_anomaly_handler', 'scorecard_post_processing', 'oversight_scorecard_post_processing'):
                    lPYFolder = file_name[ : file_name.rfind('/') ] # get the folder path containing this file
                    lPYFileName = '{file}.py'.format(file=object_name) # add extension to the object_name

                    sys.path.append(lPYFolder) # append the sys path
                    py_compile.compile( os.path.join(lPYFolder, lPYFileName) ) # force re-compile the file
                    f, filename, description = imp.find_module(object_name, [lPYFolder]) # find the module specifically in lPYFolder
                    lPYModule = imp.load_module(object_name, f, filename, description) # import the module
                    f.close() # close the file object returned by imp.find_module
                    
                    if object_name == 'plo_post_proccessing_script':
                        lExit = lPYModule.main(zk_nodes, zk_path, customer, schema) # execute the main() function of the module; returns 0 on success or error message on failure
                    elif object_name == 'scorecard_post_processing':
                        if days_limit == 'None':
                            # build_cdm_object is executed via cmd so days_limit is passed as cmd parameter (string)
                            # convert string representation of "None" to actual None 
                            days_limit = None

                        if scorecard_incremental == True : 
                            lBackfill=False
                        else:
                            lBackfill=True

                        lExit = lPYModule.main(schema, zk_nodes, zk_path, customer, backfill=lBackfill, dayslimit=days_limit) # execute the main() function of the module; returns 0 on success or error message on failure
                    elif object_name == 'oversight_scorecard_post_processing':
                        jobs = num_processes
                        if jobs == 'auto':
                            jobs = multiprocessing.cpu_count() # default to # of cores like xdist      
                        lExit = lPYModule.main(schema, zk_nodes, zk_path, customer, jobs, None) # execute the main() function of the module; returns 0 on succes or error message on failure
                    else:
                        lExit = lPYModule.main(zk_nodes, zk_path, customer, schema, ccdm_path) # execute the main() function of the module; returns 0 on succes or error message on failure

                    if lExit != 0:
                        lFailure = 1
                        lErrMsg = lExit

                else:
                    lFailure = 1
                    lErrMsg = 'Unknown Python file: {object}'.format(object=object_name)                    
        
            elif file_name != None or sql != None:
                # either a sql file to read and execute or a single SQL command to execute
                lSQL = 'set search_path to \'' + schema + '\';' + "\n"
                lSQL += 'BEGIN;' + "\n"

                if build_type in ('CDM', 'PLO'):
                    lSQL += 'drop table if exists "' + object_name + '" cascade;' + "\n\n"

                if build_type == 'CDM':
                    lSQL += 'create table "' + object_name + '" as ' + "\n" 
                
                if file_name != None:
                    # read file into memory
                    f = open(file_name,'r')
                    for i, ml in enumerate(f):
                        newSQL = ml
                        #if i == 0:
                            # this section commented out to force failure if file encoded in UTF8
                            #newSQL = newSQL.lstrip(codecs.BOM_UTF8) # strip UTF8 BOM if exists
                        newSQL = newSQL.replace('KEY*/','').replace('/*KEY','') # remove KEY identifiers for columns inherently handled by the adapters including comprehendid, objectuniquekey, etc.

                        if sql_limit:
                            newSQL = newSQL.replace('LIMIT*/','').replace('/*LIMIT','') # if enabled remove LIMIT identifiers used to enable LIMIT command (used to enhance processing speed in certain use cases)

                        lSQL += newSQL # append modified line

                    f.close()
        
                elif sql != None:
                    # not a file so simply add the SQL command
                    lSQL += sql

                lSQL += 'END;'

                if build_type in ('CDM', 'PLO'):
                    # execute on each newly created table for performance
                    lSQL += 'ANALYZE "' + object_name + '";';  

                try:
                    lCur.execute(lSQL)
                except:
                    connect_database.rollback()
                    lFailure = 1
                    lErrMsg = sys.exc_info()[1]
 
            else:
                # catch-all 
                lFailure = 1
                lErrMsg = 'Unknown File Type'

        except:
            lFailure = 1
            lErrMsg = sys.exc_info()[1]
 
        finally:
            connect_database.commit()
            lCur.close()    
            assert lFailure == 0, "Build Failed for {0}: {1}".format(object_name, lErrMsg)

if __name__ == '__main__':
    args = docopt(__doc__, version='build_cdm_object 1.0')
