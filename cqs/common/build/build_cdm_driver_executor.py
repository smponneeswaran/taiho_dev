#!/usr/bin/env python

"""
build_cdm_driver_executor.py
Usage:
  py.test build_cdm_driver_executor.py -s -rs --spec --tb=short --zk_nodes=<servers> --zk_path=<path> --customer=<customer> --schema=<schema> --din_customer_path=<path> --ccdm_path=<path> 
                                        [--html=<path> --self-contained-html --junitxml=<path> --ccdm_integration_test=<y | n> --drop_schema=<y | n> --rebuild_schema=<y | n> 
                                        --clone_schema=<schema> --drop_constraints=<y | n> --drop_constraints_exclusions=<table_list> --num_processes=<num | auto> --start=<test_num> --stop=<test_num> 
                                        --tests=<test_num - test_num> --generate_report_pdf=<y> --sql_limit=<y | n> --days_limit=<num> --scorecard_incremental=<y | n> --deploy_to_app=<y | n> --study_name=<study_name>]
  build_cdm_driver_executor.py (-h | --help)
  build_cdm_driver_executor.py --version
Description:
    Executes full build of CDM and PLO objects using pytest to collect and report results
    Test Numbers/Descriptions (for use with --start and --stop parameters):
    -------------------
    2 = test__2_check_sql_syntax
    3 = test__3_verify_schema
    4 = test__4_build_functions
    5 = test__5_build_cdm
    6 = test__6_cdm_cleanup
    6.5 = test__6_5_cdm_data_anomaly_handler
    7 = test__7_build_cdm_constraints
    8 = test__8_build_plo
    9 = test__9_plo_post_processing
    10 = test__10_plo_cleanup
    11 = test__11_build_plo_constraints
    12 = test__12_run_tests
    12.5 = test__12_5_merge_schema
Options:
-h --help                                   # show this screen
--version                                   # show version
-s                                          # disable capturing
-rs                                         # report skipped reasons
--spec                                      # include specification output
--tb=<short>                                # enable short traceback. Always set to "short"
--zk_nodes=<servers>                        # comma separated list of zookeeper hosts
--zk_path=<path>                            # zookeeper path for client folders (ex. /com/comprehend/panacea) 
--customer=<customer>                       # customer name; must match name in zookeeper path
--schema=<schema>                           # schema name
--din_customer_path=<path>                  # path for location of mapping files (directory containing the "layerX" folders)
--html=<path>                               # (OPTIONAL) report path/filename for main report in HTML format
--self-contained-html                       # (OPTIONAL) if HTML parameter included creates a self contained HTML report (more convenient for sharing)
--junitxml=<path>                           # (OPTIONAL) report path/filename for main report in XML format (used for automated build integration)
--ccdm_integration_test=<y>                 # (OPTIONAL) if set to y will execute the ccdm integration tests[default: n]
--clone_schema=<schema>                     # (OPTIONAL) if provided will use schema as a starting point for build by cloning
--drop_schema=<y>                           # (OPTIONAL) if set to y will drop schema only after successful build [default: n]
--handle_anomalies=<y>                      # (OPTIONAL) if set to y will execute data anomaly handler [default: n]
--drop_constraints=<y>                      # (OPTIONAL) if set to y will drop all CDM constraints and PLO objects before starting build
--drop_constraints_exclusions=<table_list>  # (OPTIONAL) comma separated list of PLO table names to exclude when drop_constraints=y (intended for use with scorecard PLOs)
--rebuild_schema=<y>                        # (OPTIONAL) if set to y will drop and recreate existing schema before build [default: n]
--num_processes=<num>                       # (OPTIONAL) number of simultaneous processors [default: auto]
--start=<num>                               # (OPTIONAL) start at test number (skip previous tests)
--stop=<num>                                # (OPTIONAL) end at test number (skip remaining tests)
--tests=<a,b,c-d>                           # (OPTIONAL) comma separated list of test numbers and/or range of test numbers (using "-") to execute
--generate_report_pdf=<y>                   # (OPTIONAL) if set y will generate html output into a single pdf
--sql_limit=<num>                           # (OPTIONAL) if set y will enable commented out /*LIMIT lines in SQL before execution
--days_limit=<num>                          # (OPTIONAL) if set will limit scorecard post-processing backfill date range to last <num> day(s) 
--scorecard_incremental=<y>                 # (OPTIONAL) if set will impose scorecard post-processing to run with incremental mode 
--deploy_to_app=<y>                         # (OPTIONAL) if set y will deploy successful build to cqs schema with proper permissions
--study_name=<study_name>                   # (OPTIONAL) name of the study that is being processed
"""

# Created By: Adam Kaus
# Created On: 13-Sep-2016
# Revision History: 09-Sep-2016 ACK Initial version
#                   23-Sep-2016 ACK Added create/drop schema enhancement
#                   26-Sep-2016 ACK Added html_report_path and xml_report_path fixtures for report output destination
#                   05-Oct-2016 ACK Added test__3_build_functions and test__9_plo_cleanup
#                   18-Oct-2016 ACK Added start and stop parameters and docopt integration
#                   28-Oct-2016 ACK minor adjustment to schema_exists function; introduced connect_database_func fixture
#                   01-Nov-2016 DPR Added parameter for ccdm integration test and docopt (ccdm_integration_test)
#                   03-Nov-2016 ACK Moved all CDM integration test logic to conftest.py file
#                   11-Nov-2016 ACK Added new parameters to support CCDM repo build: clone_schema, drop_constraints, tests 
#                   22-Nov-2016 ACK Updated location of plo_cleanup.sql and changed wording of report cleanup warning message
#                   05-Dec-2016 ACK Bug fix to git details message
#                   09-Dec-2016 MDE Add functionality for output to generate pdf
#                   12-Dec-2016 ACK Added logic to remove python cache files prior to import/execution to ensure use of latest versions
#                   19-Dec-2016 ACK Added test__6_5_cdm_data_anomaly_handler
#                   05-Jan-2017 ACK Modified to use zookeeper instead of redis for obtaining db connection details
#                   08-Mar-2017 ACK Revision History will be maintained via git history from this point forward 
#
# Notes:
#       - Executes tests in serial order which call build_cdm_object.py to execute subtests in parallel
#           NOTE: Multiprocessing can be disabled for a file by adding "serial_proc=1" to the header comments of the file
#       - Can determine start/stop point for build by specifying test numbers using --start and --stop parameters or --tests parameters
#           test__1_verify_build_setup is always executed, however
#       - Required non-standard packages/components:
#           - postgresql ("ex. brew install postgresql" - required for libecpg referenced by pgsanity_cust)
#           - kazoo
#           - psycopg2
#           - pytest
#           - pytest-xdist
#           - pytest-html
#           - pytest-spec
#       - Must Executed via pytest (cannot be executed stand alone). Example:
#           - py.test build_cdm_driver.py -s -rs --spec --tb=short --zk_nodes=10.131.0.112 --zkpath=dev/com/comprehend/panacea --customer=jazz --schema=cqs --din_customer_path=/Users/akaus/din-customer/jazz/ccdm --ccdm_path=/Users/akaus/ccdm --html=/Users/akaus/ccdm/resources/mappings/cqs/common/build/report/main_report.html --self-contained-html --junitxml=/Users/akaus/ccdm/resources/mappings/cqs/common/build/report/main_report.xml
#       - pkg/pgsanity_cust is a customized python package re-factored for this build program
#

import sys, os
from os import path
from pathlib import Path
sys.path.append('{filepath}/pkg'.format(filepath = path.dirname( path.abspath(__file__) ) ) ) # pkg subdirectory resides within same directory as this file
sys.path.append(str(Path(path.abspath(__file__)).parents[4] /'validation'/'cqs'/'global_cqs')) # /ccdm/resources/valiation/cqs/global_cqs
sys.path.append(str(Path(path.abspath(__file__)).parents[4] /'utils')) # /ccdm/resources/utils
from cqs_dictionary import * # fetching cqs_dictionary to get USDM tables
import gen_usdm_schema # schema generator in /utils
import pgsanity_cust # custom sql syntax checker package in /pkg dir
import glob
import pytest
import shutil
import re
import time
import select
from docopt import docopt
import git
from git import Repo
import pdfkit
import time

class Test_Build_Executor():

######################################################
######## Start global variables and functions ########
######################################################

    gSkipRemaining = True # Used to enable/disable tests based on previous test results. In some steps we want to skip the remaining tests on failure.
                          # In other tests we want to proceed despite failure. This is a test-by-test setting that must be set to False to proceed.

    gSkipFinalSteps = False # Used to control final processing steps such as drop_schema and deploy_schema. Set to true on failure.
                            # If gSkipRemaining = True than by default that will also disable the final steps as well.


    # decode error messages returned by pytest.main
    def get_error_msg(self, pErr):
        '''
        Standard Exit Codes:
        -------------
        EXIT_OK = 0
        EXIT_TESTSFAILED = 1
        EXIT_INTERRUPTED = 2
        EXIT_INTERNALERROR = 3
        EXIT_USAGEERROR = 4
        EXIT_NOTESTSCOLLECTED = 5
        '''
        lErrMsg = 'Unknown'
        if pErr == 0:
            lErrMsg = 'Success'
        elif pErr == 1:
            lErrMsg = 'Tests Failed (see individual reports for details)'
        elif pErr == 2:
            lErrMsg == 'Execution Interrupted'
        elif pErr == 3:
            lErrMsg = 'Internal Error'
        elif pErr == 4:
            lErrMsg = 'Usage Error'
        elif pErr == 5:
            lErrMsg = 'No Tests Collected'
        elif pErr == 98: #custom error messgae
            lErrMsg = 'Object exists in multiple layers'
        elif pErr == 99: #custom error messgae
            lErrMsg = 'No objects found'
        return lErrMsg

    # the standard parameters passed to pytest.main() in methods below come from the build_cmd fixture in conftest.py
    # ret_cmd is used to extend those parameters based on additional settings per test
    def ret_cmd(self, pCmd, pRepName, pPath, pBuildType, pHTMLReportPath, pXMLReportPath, pStudyName):
        lCmd = pCmd[:]
        lCmd.extend(['--path={path}'.format(path=pPath), '--build_type={build_type}'.format(build_type=pBuildType), '--study_name={study_name}'.format(study_name=pStudyName)])
        if pHTMLReportPath:
            lCmd.extend(['--html={path}/{name}.html'.format(path=pHTMLReportPath, name=pRepName), '--self-contained-html'])
        if pXMLReportPath:
            lCmd.extend(['--junitxml={path}/{name}.xml'.format(path=pXMLReportPath, name=pRepName)])

        # the following opens an individual file to see if the "serial" switch is enabled in the header comments
        # if yes we infiltrate the parameters coming from build_cmd in conftest.py to disable multi-processing
        if path.isfile(pPath):
            f = open(pPath)
            for line in f:
                if 'serial_proc=1' in line.lower():
                    for i, item in enumerate(lCmd):
                        if item.lower() == '-n':
                            # the next parameter after "-n" will be the number of processes
                            lCmd[i+1] = 1
                            break

            f.close()        
        return lCmd

    # run a single test step (i.e. execution of one file or directory)
    # pCmd -> commands passed to pytest.main
    # pPath -> absolute path to search
    # pTestName -> name of test being executed
    # pReportSuffix -> date/time used in report name
    # pBuildType -> type passed to build_cdm_object.py for use in build logic
    def run_step(self, pCmd, pPath, pTestName, pHTMLReportPath, pXMLReportPath, pReportSuffix, pBuildType, pStudyName):
        lExitCode = -1
        lPath = pPath.lower()
        print '\n**** BUILDING: {path} ****'.format(path=lPath)
        
        lRepName = '{test}_{study_name}_{suffix}'.format(test=pTestName, study_name=pStudyName, suffix=pReportSuffix)
        lCmd = self.ret_cmd(pCmd, lRepName,  pPath, pBuildType, pHTMLReportPath, pXMLReportPath, pStudyName)
        lExitCode = pytest.main(lCmd)
        
        if lExitCode != 0:
            Test_Build_Executor.gSkipFinalSteps = True # test failed so skip final steps
        assert lExitCode == 0, 'Build Failed for {object}: {err}'.format(object=lPath, err=self.get_error_msg(lExitCode))

    # similar to run_step but loop through a directory and execute separate tests for each file or subdirectory
    def run_step_loop(self, pCmd, pPath, pTestName, pHTMLReportPath, pXMLReportPath, pReportSuffix, pBuildType, pStudyName):
        lExitCode = -1
        lObjName = ''
        lFailures = ''
        lCount = 0
        lObjList = []
        lErrObjectName = ''

        lDir = sorted(glob.glob(pPath))
        # Start of the patch, this will rearrange the order of post processing scripts by move oversight_scorecard_post_processing to the last task
        if (pTestName is "test__9_plo_post_processing"):
            for p in lDir:
                if "oversight_scorecard_post_processing" in p:
                    lDir.append(p)
                    lDir.remove(p)
                    break
        # End of the patch for rearranging order of post processing scripts
        for i in lDir:
            # loop through directory
            lSubDir = i
            lCount += 1
            print '\n**** BUILDING: {path} ****'.format(path=lSubDir)
            
            if lSubDir.endswith('.sql') or lSubDir.endswith('.py'):
                # return sql file name
                lObjName = lSubDir[lSubDir.rfind('/')+1:-4]
                if lObjName.lower() not in lObjList:
                    lObjList.append(lObjName.lower())
                else : 
                    lErrObjectName += lObjName.lower() + ', '
                    lExitCode = 98

            else:
                # return directory name
                lObjName = lSubDir[lSubDir.rfind('/')+1:] # this is the directory name
                lSubDir = lSubDir + '/*.sql' # This tells it to run all sql files within the directory
                lFilelist = sorted(glob.glob(lSubDir)) # getting list of sql files inside the directory
                for lFileName in lFilelist : # looping through sql files to detect if any of them are repeated
                    lFileName = lFileName[lFileName.rfind('/')+1:-4]

                    if lFileName.lower() not in lObjList :
                        lObjList.append(lFileName.lower())
                    else : 
                        lErrObjectName += lFileName.lower()+ ', '
                        lExitCode = 98
        
            lRepName = '{test}_{study_name}_{object}_{suffix}'.format(test=pTestName, study_name=pStudyName, object=lObjName, suffix=pReportSuffix)
            lCmd = self.ret_cmd(pCmd, lRepName,  lSubDir, pBuildType, pHTMLReportPath, pXMLReportPath, pStudyName)
            lExit = pytest.main(lCmd)

            if lExit > lExitCode:
                lExitCode = lExit # if success this gets set to 0 since lExitCode = -1. on error this will be the highest error code
            if lExit > 0:
                lFailures += '{object}, '.format(object=lObjName)

        if len(lErrObjectName) > 0 :
            lErrObjectName = lErrObjectName[:-2] #remove last comma
            lFailures += '{object}, '.format(object=lErrObjectName)

        lFailures = lFailures[:-2] #remove last comma
        if lCount == 0:
            # path search did not return results so fail
            lExitCode = 99
            lFailures = pPath
        if lExitCode != 0:
            Test_Build_Executor.gSkipFinalSteps = True # test failed so skip final steps
        assert lExitCode == 0, 'Build Failed for {object}: {err}'.format(object=lFailures, err=self.get_error_msg(lExitCode))

    # returns string of repository details for output to user
    def get_repo_details(self, pRepoPath):
        lReturn = ''

        # determines if folder within a git repository
        # traverses up the directory structure and checks all folders
        def in_git_repo(pPath):
            lPath = pPath
            lIsGit = False
            while not lIsGit:
                if git.repo.fun.is_git_dir(lPath + '/.git'):
                    lIsGit = True
                    return lPath
                if lPath == '/':
                    break # at root level so stop
                lPath = path.dirname(lPath) # move up one directory
            return lIsGit

        # format commit message
        # remove blank lines and add tabs to new lines
        def clean_lines(pTxt):
            lTxt = re.sub("\n\s*\n*", "\n", pTxt)
            lTxt = '# \t'.join(lTxt.splitlines(True))
            return lTxt

        lRepoPath = in_git_repo(pRepoPath)
        if lRepoPath:
            lRepo = Repo(lRepoPath)
            if not lRepo.head.is_detached:
                lBranch = lRepo.active_branch
                lBranchName = lBranch.name
            else:
                # not on a branch but a commit
                lBranch = '(Detached HEAD)'

            lCommit = lRepo.head.commit
            lCommitMessage = lRepo.head.commit.message
            lCommitMessage = clean_lines(lCommitMessage)
            
            lTag = 'Not Tagged'
            for tag in lRepo.tags: 
                if tag.commit == lCommit:
                    lTag = tag

            lReturn = u'''# Branch: {branch}
# Tag: {tag}
# Latest Commit SHA: {sha}
# Latest Commit Message:
#       {commit}#'''.format(dir=pRepoPath, branch=lBranch, tag=lTag, sha=lCommit, commit=lCommitMessage)

        else:
            lReturn = '''# ** Not a GitHub Repository **
#'''
        return lReturn

    # check syntax of all sql files in or under pPath
    def check_sql_syntax(self, pPath):
        lFailures = ''
        # recursively search directories
        for dir, sub, f in os.walk(pPath):
                for filename in sorted(glob.iglob('{path}/*.sql'.format(path=dir))):
                    lExitMsg = pgsanity_cust.main([filename])
                    if len(lExitMsg) > 0:
                        lFailures += '\n{err}'.format(err=lExitMsg[0])
        return lFailures

    # check if pSchema exists in DB (return boolean)
    def schema_exists(self, pSchema, pConn):
        try:
            lCur = pConn.cursor()
            lVal = 0
            lSQL = 'SELECT 1 FROM information_schema.schemata WHERE lower(schema_name) = lower(\'{schema}\')'.format(schema=pSchema)
            lCur.execute(lSQL)
            if lCur.rowcount > 0:
                lVal = lCur.fetchone()[0]
            if lVal == 1:
                return True
            else:
                return False

        finally:
            pConn.commit()
            lCur.close()

    # execute CREATE SCHEMA and alter owner to <database_name>-master-write
    def create_schema(self, pSchema, pConn):
        try:
            lCur = pConn.cursor()
            if self.schema_exists(pSchema, pConn):
                pytest.fail('Schema "{schema}" already exists cannot create'.format(schema=pSchema))
            else:
                # pg_advisory_xact_lock avoids conflicts with concurrent DDL statements caused by multithreading
                lSQL = '''SELECT pg_advisory_xact_lock(850347456);
                            CREATE SCHEMA "{schema}";'''.format(schema=pSchema)
                lCur.execute(lSQL)

                # get the dbname and master-write role name
                lDBName = pConn.get_dsn_parameters()['dbname']
                lRoleName = '{dbname}-master-write'.format(dbname=lDBName)

                # check if role exists in this DB and assign as owner
                lSQL = '''SELECT 1 FROM pg_roles where rolname='{rolename}' '''.format(rolename=lRoleName)
                lCur.execute(lSQL)
                if lCur.rowcount > 0:
                    lSQL = 'ALTER SCHEMA "{schema}" OWNER TO "{rolename}"'.format(schema=pSchema, rolename=lRoleName)
                    lCur.execute(lSQL)
                else:
                    print '\n**** Role name "{rolename}" does not exist in this database ****'.format(rolename=lRoleName) 

        finally:
            pConn.commit()
            lCur.close()

    def drop_schema(self, pSchema, pConn):
        try:
            lCur = pConn.cursor()
            # fetching USDM tables from cqs_dictionary file
            lTables = cdmPropertiesDict.keys() + ploPropertiesDict.keys()
            vLastTable = False

            if lTables is not None and len(lTables) > 0:
                for table in lTables:
                    if table == lTables[-1] :
                        vLastTable = True

                    lSQL = '''SELECT pg_advisory_xact_lock(850347456);
                            BEGIN;
                            SELECT public.drop_schema_tables('{schema}', '{table}', {LastTable}::boolean);
                            END; '''.format(schema=pSchema, table=table, LastTable=vLastTable)
                    lCur.execute(lSQL)
            
        finally:
            pConn.commit()
            lCur.close()

    # function to clone schema pClone into pTarget; will drop if already exists
    def clone_schema(self, pClone, pTarget, pConn):
        try:
            lCur = pConn.cursor()

            if self.schema_exists(pTarget, pConn):
                print '\n**** Target schema "{schema}" exists, dropping first ****'.format(schema=pTarget)
                self.drop_schema(pTarget, pConn)

            self.create_schema(pTarget, pConn)
            lSQL = '''DO
                      $$DECLARE 
                        objeto text;
                        buffer text;
                      BEGIN                         
                          FOR objeto IN
                              SELECT TABLE_NAME::text FROM information_schema.TABLES WHERE table_schema = '{source}'
                          LOOP
                              buffer := '"{target}"."' || objeto || '"';
                              EXECUTE 'CREATE TABLE ' || buffer || ' (LIKE "{source}"."' || objeto || '" INCLUDING CONSTRAINTS INCLUDING INDEXES INCLUDING DEFAULTS)';
                              EXECUTE 'INSERT INTO ' || buffer || ' (SELECT * FROM "{source}"."' || objeto || '")';
                              EXECUTE 'ANALYZE ' || buffer;
                          END LOOP;
                      END$$;'''.format(source=pClone, target=pTarget)
            lCur.execute(lSQL)
        
        finally:
            pConn.commit()
            lCur.close()


    # primarily used if starting after test__5_build_cdm
    # pExclusions = optional comma separated string of PLO tables which should not be dropped
    def drop_all_constraints(self, pSchema, pConn, pExclusions=''):
        try:
            lCur = pConn.cursor()

            if self.schema_exists(pSchema, pConn):
                # build exclusion string
                exclusionStr = ''
                if len(pExclusions) > 0:
                    for obj in pExclusions.split(','):
                        if len(exclusionStr) > 0:
                            exclusionStr += ', \'{obj}\''.format(obj=obj.lower().strip())
                        else:
                            exclusionStr += '\'{obj}\''.format(obj=obj.lower().strip())
                else:
                    exclusionStr = '\'\''

                lSQL = '''DO 
                          $$DECLARE 
                                r record; 
                                pTableName varchar(1000); 
                                pColumnName varchar(1000); 
                                pIndex varchar(1000);
                          BEGIN
                                --drop PLOs
                                FOR r IN SELECT table_name 
                                            FROM information_schema.tables 
                                            WHERE (lower(table_name) like 'rpt%' or lower(table_name) like 'dim%' or lower(table_name) like 'fact%' or lower(table_name) like 'kpisummary%') and lower(table_schema) = lower('{schema}') and lower(table_name) not in ({exclusions})
                                LOOP
                                   EXECUTE 'DROP TABLE "{schema}"."' || quote_ident(r.table_name)|| '" CASCADE';
                                END LOOP;

                                --drop CDM FKs
                                FOR r IN SELECT table_name, constraint_name FROM information_schema.table_constraints where lower(table_schema) = lower('{schema}') and constraint_type = 'FOREIGN KEY'
                                LOOP
                                   EXECUTE 'ALTER TABLE "{schema}"."' || quote_ident(r.table_name)|| '" DROP CONSTRAINT "'|| quote_ident(r.constraint_name) || '" CASCADE';
                                END LOOP;

                                --drop CDM PKs
                                FOR r IN SELECT table_name, constraint_name, constraint_type FROM information_schema.table_constraints where lower(table_schema) = lower('{schema}') and constraint_type = 'PRIMARY KEY'
                                LOOP
                                   EXECUTE 'ALTER TABLE "{schema}"."' || quote_ident(r.table_name)|| '" DROP CONSTRAINT "'|| quote_ident(r.constraint_name) || '" CASCADE';
                                END LOOP;

                                --drop CDM column not nulls
                                FOR pTableName in select table_name from information_schema.tables where lower(table_schema) = lower('{schema}')
                                LOOP
                                   FOR pColumnName in select a.attname
                                                        from pg_catalog.pg_attribute a
                                                        where attrelid = ('{schema}.' || pTableName)::regclass
                                                            and a.attnum > 0
                                                            and not a.attisdropped
                                                            and a.attnotnull
                                      LOOP
                                         EXECUTE 'ALTER TABLE "{schema}"."' || pTableName ||'" ALTER COLUMN "'||pColumnName||'" DROP NOT NULL';    
                                      END LOOP;
                                END LOOP;

                                --drop indexes
                                FOR pIndex in SELECT cast(c.oid::regclass as TEXT)
                                                FROM pg_class  c, pg_namespace n 
                                                WHERE 
                                                lower(n.nspname) = lower('{schema}') 
                                                and n.oid = c.relnamespace
                                                and c.relkind = 'i'
                                                ORDER BY c.relpages DESC
                                LOOP
                                    EXECUTE 'DROP INDEX IF EXISTS ' || pIndex || ' CASCADE';
                                END LOOP;

                          END$$;'''.format (schema=pSchema, exclusions=exclusionStr)

                lCur.execute(lSQL)

        finally:
            pConn.commit()
            lCur.close()  

    # remove python cache files prior to import/execution to ensure latest versions are compiled
    # pDirList is a list of directory names to check
    def remove_py_cache_files(self, pDirList):
        for d in pDirList:
            if os.path.exists( os.path.join(d, '__pycache__') ):
                shutil.rmtree( os.path.join(d, '__pycache__') )
            for f in glob.glob('{path}/*.pyc'.format(path=d)):
                os.remove(f)  
            for f in glob.glob('{path}/*.pyc'.format(path=d)):
                os.remove(f)  

    # function to merge the data in to master schema
    def run_merge_schema(self, pTestName, pHTMLReportPath, pXMLReportPath, pReportSuffix, pSchema, pMasterSchema, pStudyid, pConn, pDebugging, partition_exclusion):
        lCur = pConn.cursor()
        vStudyName_mod = re.sub('[^a-zA-Z0-9\n\.]', '_',pStudyid).lower()
        partExclList = partition_exclusion.split(',')

        try :
            vSQL = '''SELECT table_name from information_schema.tables where table_schema='{schema}' '''.format(schema=pSchema)
            lCur.execute(vSQL)
            lTables = lCur.fetchall()

            if lTables is not None and len(lTables) > 0:
                for table in lTables:
                    if table[0] in partExclList:
                        # if table is not partitioned  then just move the first instance into the master schema
                        tSQL = '''SELECT pg_advisory_xact_lock(439423900);'''
                        lCur.execute(tSQL)
                        tSQL = '''SELECT 1 FROM information_schema.tables WHERE table_schema='{master_schema}' AND table_name='{table_name}';'''.format(table_name=table[0], master_schema=pMasterSchema)
                        lCur.execute(tSQL)
                        if lCur.rowcount == 0:
                            tSQL = '''ALTER TABLE {schema}.{table_name} SET SCHEMA {master_schema}'''.format(schema=pSchema, table_name=table[0], master_schema=pMasterSchema)
                            lCur.execute(tSQL)
                    else:
                        # queries to rename constraints
                        cSQL = '''SELECT constraint_name FROM information_schema.table_constraints tb 
                                  WHERE  table_schema = '{schema}' AND tb.table_name = '{table_name}' AND constraint_type IN ('FOREIGN KEY','PRIMARY KEY')'''.format(schema=pSchema, table_name=table[0])
                        lCur.execute(cSQL)
                        lCons = lCur.fetchall()

                        for constraints in lCons:
                            cSQL = ''' ALTER TABLE {schema}.{table_name} RENAME CONSTRAINT {constraint_name} TO "{timestamp}_{constraint_name}_{study}" '''.format(schema=pSchema, table_name=table[0], constraint_name=constraints[0],study=vStudyName_mod, timestamp=int(round(time.time() * 1000)))
                            lCur.execute(cSQL)

                        # queries to rename indexes
                        iSQL = '''SELECT indexname FROM pg_indexes  
                                  WHERE  schemaname = '{schema}' 
                                  AND tablename = '{table_name}' 
                                  AND indexname NOT IN (SELECT constraint_name
                                                         FROM   information_schema.table_constraints tb
                                                         WHERE  table_schema = '{schema}' AND tb.table_name = '{table_name}' AND constraint_type IN ('FOREIGN KEY','PRIMARY KEY'))'''.format(schema=pSchema, table_name=table[0])
                        lCur.execute(iSQL)
                        lIndxs = lCur.fetchall()

                        for indexes in lIndxs:
                            iSQL = ''' ALTER INDEX {schema}.{index_name} RENAME TO "{timestamp}_{index_name}_{study}" '''.format(schema=pSchema, table_name=table[0], index_name=indexes[0], study=vStudyName_mod, timestamp=int(round(time.time() * 1000)))
                            lCur.execute(iSQL)

                        vSQL = '''ALTER TABLE {schema}.{table_name} rename to {table_name}_{study};
                                  DROP TABLE IF EXISTS {master_schema}.{table_name}_{study} CASCADE;
                                  ALTER TABLE {schema}.{table_name}_{study} SET SCHEMA {master_schema};'''.format(schema=pSchema, table_name=table[0], study=vStudyName_mod, master_schema=pMasterSchema)
                        lCur.execute(vSQL)

                        # pg_advisory_xact_lock avoids conflicts with concurrent DDL statements caused by multi-threading; magic number randomly generated for this logic
                        vSQL = '''SELECT pg_advisory_xact_lock(439423900); 
                                    CREATE TABLE IF NOT EXISTS {master_schema}.{table_name} (LIKE {master_schema}.{table_name}_{study});
                                    ALTER TABLE {master_schema}.{table_name}_{study} INHERIT {master_schema}.{table_name};'''.format(schema=pSchema, table_name=table[0], study=vStudyName_mod, master_schema=pMasterSchema)
                        lCur.execute(vSQL)
                    pConn.commit()
                   
            # Dropping schema here as all tables from all study level schema's has been moved to master schema and can be referred there for debugging purpose
            if not pDebugging:
                self.drop_schema(pSchema, pConn)
            
        finally:
            pConn.commit()
            lCur.close()

    # build schema based on cqs_dictionary.py
    # primarily used for creation of gold schema for USDM schema handler
    def build_usdm_schema(self, pSchema, pConn):
        gen_usdm_schema.main(pSchema, 
                                runsql=True, 
                                buildindexes=True, 
                                disablepartitions=True, # partitions are only on the master schema 
                                dropschema=True, # drop if already exists
                                enableschemahandler=True, # gold schema does not need plos
                                pgconn=pConn)
        pConn.commit()

######################################################
######### End global variables and functions #########
######################################################


######################################################
################ Start test functions ################
######################################################

    # 2 - SQL syntax check
    def test__2_check_sql_syntax(self, start, stop, tests, din_customer_path, ccdm_path):
        if start > 2 or stop < 2 or 2 not in tests:
            pytest.skip("Test skipped by user")

        lExitCode = 0
        lFailures = ''
        try:
            # For builds in internal environments sometimes these parameters match. In this case skip the din-customer syntax checking as it 
            # will recursively search all ccdm folders and fail on some of the demo sql. Just the following check will suffice in this case.
            if din_customer_path != ccdm_path: 
                lFailures += self.check_sql_syntax(din_customer_path) # check mappings
            lFailures += self.check_sql_syntax('{path}/resources/mappings/cqs'.format(path=ccdm_path)) # check all sql in common subdir of ccdm
            if len(lFailures) > 0:
                lExitCode = 1

            # if this test fails then skip remaining tests
            if lExitCode == 0:
                Test_Build_Executor.gSkipRemaining = False
            else:
                Test_Build_Executor.gSkipRemaining = True

            assert lExitCode == 0, 'SQL Syntax Check Failed:{err}'.format(err=lFailures)

        except:
            Test_Build_Executor.gSkipRemaining = True
            pytest.fail(sys.exc_info()[1])

    # 3 - verify schema exists. if not, create schema
    @pytest.mark.skipif("Test_Build_Executor.gSkipRemaining", reason="Test skipped due to previous failure")
    def test__3_verify_schema(self, start, stop, tests, schema, rebuild_schema, clone_schema, drop_constraints, drop_constraints_exclusions, study_name, connect_database_func):
        if start > 3 or stop < 3 or 3 not in tests:
            pytest.skip("Test skipped by user")
        if Test_Build_Executor.gSkipRemaining:
            pytest.skip("Test skipped due to previous failure")

        try:
            lExitCode = 1
            lErrorMsg = ''

            # clone schema
            if clone_schema:
                if self.schema_exists(clone_schema, connect_database_func): # clone schema exists
                    print '\n**** Cloning schema "{clone}" into "{schema}" ****'.format(clone=clone_schema, schema=schema)
                    self.clone_schema(clone_schema, schema, connect_database_func)

                else: # schema to clone does not exist. Throw error
                    lExitCode = 2
                    lErrorMsg = 'Schema "{schema}" does not exist. Cannot clone schema.'.format(schema=clone_schema)

            # schema does not yet exist so create
            elif self.schema_exists(schema, connect_database_func) is False:
                print '\n**** Schema "{schema}" does not exist. Creating schema. ****'.format(schema=schema)
                self.create_schema(schema, connect_database_func)

            # rebuild schema
            elif rebuild_schema:
                print '\n**** Rebuilding schema "{schema}" ****'.format(schema=schema)
                self.drop_schema(schema, connect_database_func)
                self.create_schema(schema, connect_database_func)

            # schema exists so do nothing    
            else:
                print '\n**** Schema "{schema}" already exists. Proceeding with build. ****'.format(schema=schema)

            # drop cdm constraints based on parameter
            if drop_constraints:
                print '\n**** Dropping constraints and PLOs in schema "{schema}" ****'.format(schema=schema)
                self.drop_all_constraints(schema, connect_database_func, drop_constraints_exclusions)

            # now check again and pass if schema exists
            if self.schema_exists(schema, connect_database_func) and lExitCode <= 1: # do not check if already failed above
                lExitCode = 0 

            # build gold schema for usdm handler
            self.build_usdm_schema('{schema}_gold'.format(schema=schema), connect_database_func) 

            # if this test fails then skip remaining tests
            if lExitCode == 0:
                Test_Build_Executor.gSkipRemaining = False
            else:
                Test_Build_Executor.gSkipRemaining = True

            if sys.exc_info()[1]:
                lErrorMsg += sys.exc_info()[1]
            assert lExitCode == 0, 'Schema verification failed: {err}'.format(err=lErrorMsg)
        
        except:
            Test_Build_Executor.gSkipRemaining = True
            pytest.fail(sys.exc_info()[1])

    # 4 - compile functions
    @pytest.mark.skipif("Test_Build_Executor.gSkipRemaining", reason="Test skipped due to previous failure")
    def test__4_build_functions(self, start, stop, tests, build_cmd, din_customer_path, html_report_path, xml_report_path, report_suffix, study_name):
        if start > 4 or stop < 4 or 4 not in tests:
            pytest.skip("Test skipped by user")
        if Test_Build_Executor.gSkipRemaining:
            pytest.skip("Test skipped due to previous failure")

        lPath = '{path}/functions/*.sql'.format(path=din_customer_path)
        if not os.path.exists('{path}/functions'.format(path=din_customer_path)):
            pytest.skip("No functions found")
        lTestName = 'test__4_build_functions'
        self.run_step(build_cmd, lPath, lTestName, html_report_path, xml_report_path, report_suffix, 'FUNCTIONS', study_name)

    # 5 - build cdm - loop through layer folders
    @pytest.mark.skipif("Test_Build_Executor.gSkipRemaining", reason="Test skipped due to previous failure")
    def test__5_build_cdm(self, start, stop, tests, build_cmd, din_customer_path, html_report_path, xml_report_path, report_suffix, study_name):
        if start > 5 or stop < 5 or 5 not in tests:
            pytest.skip("Test skipped by user")
        if Test_Build_Executor.gSkipRemaining:
            pytest.skip("Test skipped due to previous failure")

        lPath = '{path}/layer*'.format(path=din_customer_path) #search for folders named "layerX"
        lTestName = 'test__5_build_cdm'
        self.run_step_loop(build_cmd, lPath, lTestName, html_report_path, xml_report_path, report_suffix, 'CDM', study_name)

    # 6 - execute cleanup
    @pytest.mark.skipif("Test_Build_Executor.gSkipRemaining", reason="Test skipped due to previous failure")
    def test__6_cdm_cleanup(self, start, stop, tests, build_cmd, din_customer_path, html_report_path, xml_report_path, report_suffix, study_name):
        if start > 6 or stop < 6 or 6 not in tests:
            pytest.skip("Test skipped by user")
        if Test_Build_Executor.gSkipRemaining:
            pytest.skip("Test skipped due to previous failure")

        lPath = '{path}/cleanup/*.sql'.format(path=din_customer_path)
        if not os.path.exists('{path}/cleanup'.format(path=din_customer_path)):
            pytest.skip("No CDM cleanup scripts found")
        lTestName = 'test__6_cdm_cleanup'
        self.run_step(build_cmd, lPath, lTestName, html_report_path, xml_report_path, report_suffix, 'CLEANUP', study_name)

    # 6.5 - execute data anomaly handler
    @pytest.mark.skipif("Test_Build_Executor.gSkipRemaining", reason="Test skipped due to previous failure")
    def test__6_5_cdm_data_anomaly_handler(self, start, tests, stop, build_cmd, ccdm_path, html_report_path, xml_report_path, report_suffix, handle_anomalies, study_name):
        if start > 6.5 or stop < 6.5 or 6.5 not in tests or handle_anomalies is False:
            pytest.skip("Test skipped by user")
        if Test_Build_Executor.gSkipRemaining:
            pytest.skip("Test skipped due to previous failure")

        lPath = '{path}/resources/mappings/cqs/common/build/data_anomaly_handler/data_anomaly_handler.py'.format(path=ccdm_path)
        if not os.path.exists(lPath):
            pytest.skip("No Data Anomaly Handler script found")
        lTestName = 'test__6_5_cdm_data_anomaly_handler'
        self.run_step(build_cmd, lPath, lTestName, html_report_path, xml_report_path, report_suffix, 'PYTHON', study_name)

    # 7 - build cdm constraints
    @pytest.mark.skipif("Test_Build_Executor.gSkipRemaining", reason="Test skipped due to previous failure")
    def test__7_build_cdm_constraints(self, start, tests, stop, build_cmd, ccdm_path, html_report_path, xml_report_path, report_suffix, study_name):
        if start > 7 or stop < 7 or 7 not in tests:
            pytest.skip("Test skipped by user")
        if Test_Build_Executor.gSkipRemaining:
            pytest.skip("Test skipped due to previous failure")

        lPath = '{path}/resources/mappings/cqs/common/ccdm_constraints/*.sql'.format(path=ccdm_path)
        lTestName = 'test__7_build_cdm_constraints'
        self.run_step_loop(build_cmd, lPath, lTestName, html_report_path, xml_report_path, report_suffix, 'CONSTRAINT', study_name)

    # 8 - build plos
    @pytest.mark.skipif("Test_Build_Executor.gSkipRemaining", reason="Test skipped due to previous failure")
    def test__8_build_plo(self, start, stop, tests, build_cmd, ccdm_path, html_report_path, xml_report_path, report_suffix, study_name):
        if start > 8 or stop < 8 or 8 not in tests:
            pytest.skip("Test skipped by user")
        if Test_Build_Executor.gSkipRemaining:
            pytest.skip("Test skipped due to previous failure")

        lPath = '{path}/resources/mappings/cqs/common/plo/layer*'.format(path=ccdm_path)
        lTestName = 'test__8_build_plo'
        self.run_step_loop(build_cmd, lPath, lTestName, html_report_path, xml_report_path, report_suffix, 'PLO', study_name)

    # 9 - plo post-processing
    @pytest.mark.skipif("Test_Build_Executor.gSkipRemaining", reason="Test skipped due to previous failure")
    def test__9_plo_post_processing(self, start, tests, stop, build_cmd, ccdm_path, html_report_path, xml_report_path, report_suffix, study_name):
        if start > 9 or stop < 9 or 9 not in tests:
            pytest.skip("Test skipped by user")
        if Test_Build_Executor.gSkipRemaining:
            pytest.skip("Test skipped due to previous failure")

        # execute any .py scripts with "post_proc" in the filename but exclude those named "_wrapper"
        lPath = '{path}/resources/mappings/cqs/common/plo/*post_proc*[!_wrapper].py'.format(path=ccdm_path)
        #if not os.path.exists(lPath):
        if len(glob.glob(lPath)) == 0:
            pytest.skip("No PLO post-processing scripts found")
        lTestName = 'test__9_plo_post_processing'
        self.run_step_loop(build_cmd, lPath, lTestName, html_report_path, xml_report_path, report_suffix, 'PYTHON', study_name)

    # 10 - plo cleanup
    @pytest.mark.skipif("Test_Build_Executor.gSkipRemaining", reason="Test skipped due to previous failure")
    def test__10_plo_cleanup(self, start, stop, tests, build_cmd, ccdm_path, html_report_path, xml_report_path, report_suffix, study_name):
        if start > 10 or stop < 10 or 10 not in tests:
            pytest.skip("Test skipped by user")
        if Test_Build_Executor.gSkipRemaining:
            pytest.skip("Test skipped due to previous failure")

        lPath = '{path}/resources/mappings/cqs/common/plo_cleanup/plo_cleanup.sql'.format(path=ccdm_path)
        if not os.path.exists(lPath):
            pytest.skip("No PLO cleanup scripts found")
        lTestName = 'test__10_plo_cleanup'
        self.run_step(build_cmd, lPath, lTestName, html_report_path, xml_report_path, report_suffix, 'CLEANUP', study_name)

    # 11 - build plo constraints
    @pytest.mark.skipif("Test_Build_Executor.gSkipRemaining", reason="Test skipped due to previous failure")
    def test__11_build_plo_constraints(self, start, tests, stop, build_cmd, ccdm_path, html_report_path, xml_report_path, report_suffix, study_name):
        if start > 11 or stop < 11 or 11 not in tests:
            pytest.skip("Test skipped by user")
        if Test_Build_Executor.gSkipRemaining:
            pytest.skip("Test skipped due to previous failure")

        lPath = '{path}/resources/mappings/cqs/common/plo_constraints/*.sql'.format(path=ccdm_path)
        lTestName = 'test__11_build_plo_constraints'
        self.run_step_loop(build_cmd, lPath, lTestName, html_report_path, xml_report_path, report_suffix, 'CONSTRAINT', study_name)

    # 12 - run tests
    @pytest.mark.skipif("Test_Build_Executor.gSkipRemaining", reason="Test skipped due to previous failure")
    def test__12_run_tests(self, start, stop, tests, cdm_test_cmd, plo_test_cmd, html_report_path, xml_report_path, report_suffix, study_name):
        if start > 12 or stop < 12 or 12 not in tests:
            pytest.skip("Test skipped by user")
        if Test_Build_Executor.gSkipRemaining:
            pytest.skip("Test skipped due to previous failure")

        lExit = 0
        lRepName = 'test__12_run_cdm_tests_{study_name}_{suffix}'.format(suffix=report_suffix, study_name=study_name)
        lCmd = cdm_test_cmd
        if html_report_path:
            lCmd.extend(['--html={path}/{name}.html'.format(path=html_report_path,name=lRepName), '--self-contained-html'])
        if xml_report_path:
            lCmd.extend(['--junitxml={path}/{name}.xml'.format(path=xml_report_path,name=lRepName)])
        lExitCode = pytest.main(lCmd)
        if lExitCode > lExit:
            lExit = lExitCode

        lRepName = 'test__12_run_plo_tests_{study_name}_{suffix}'.format(suffix=report_suffix, study_name=study_name)
        lCmd = plo_test_cmd
        if html_report_path:
            lCmd.extend(['--html={path}/{name}.html'.format(path=html_report_path,name=lRepName), '--self-contained-html'])
        if xml_report_path:
            lCmd.extend(['--junitxml={path}/{name}.xml'.format(path=xml_report_path,name=lRepName)])
        lExitCode = pytest.main(lCmd)
        if lExitCode > lExit:
            lExit = lExitCode
        
        if lExit != 0:
            Test_Build_Executor.gSkipRemaining = True # test failed so skip remaining
        assert lExit == 0, 'Testing Failed: {err}'.format(err=self.get_error_msg(lExit))

    # 11.5 - finally merge date from differenr schema's
    def test__12_5_merge_schema (self, start, tests, stop, html_report_path, xml_report_path, report_suffix, schema, connect_database_func, study_name, debugging, partition_exclusion):
        if start > 12.5 or stop < 12.5 or 12.5 not in tests:
            pytest.skip("Test skipped by user")
        
        if not debugging:
            self.drop_schema('{schema}_gold'.format(schema=schema), connect_database_func) # clean up the gold schema

        if study_name != 'combined_studies':
            # merge into the master schema for study-specific mappings
            lMaster_schema=schema.replace(('_{study_name}_new'.format(study_name=re.sub('[^a-zA-Z0-9\n\.]', '_',study_name).lower())),'')
            lTestName = 'test__12_5_merging_data_{study_name}'.format(study_name=study_name)
            self.run_merge_schema( lTestName, html_report_path, xml_report_path, report_suffix, schema, lMaster_schema, study_name, connect_database_func, debugging, partition_exclusion)

######################################################
################# End test functions #################
######################################################

if __name__ == '__main__':
    args = docopt(__doc__, version='build_cdm_driver_executor 1.0')
