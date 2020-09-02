#!/usr/bin/env python

"""
build_cdm_driver.py
Usage:
  py.test build_cdm_driver.py -s -rs --spec --tb=short --zk_nodes=<servers> --zk_path=<path> --customer=<customer> --schema=<schema> --din_customer_path=<path> --ccdm_path=<path> 
                                [--html=<path> --self-contained-html --junitxml=<path> --ccdm_integration_test=<y | n> --drop_schema=<y | n> --rebuild_schema=<y | n> 
                                --clone_schema=<schema> --drop_constraints=<y | n> --drop_constraints_exclusions=<table_list> --num_processes=<num | auto> --start=<test_num> --stop=<test_num> 
                                --tests=<test_num - test_num> --generate_report_pdf=<y> --sql_limit=<y | n> --days_limit=<num> --scorecard_incremental=<y | n> --deploy_to_app=<y | n> --study_list=<list of studies>]
  build_cdm_driver.py (-h | --help)
  build_cdm_driver.py --version
Description:
    Executes following steps of build process and issues command to execute build_cdm_driver_interpreter.py which runs each study in parallel fashion based on the num_processes value
    Test Numbers/Descriptions (for use with --start and --stop parameters):
    -------------------
    1 = test__1_verify_build_setup
    13 = test__13_drop_schema
    14 = test__14_deploy_schema
    15 = test__15_build_final_steps
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
--deploy_to_app=<y>                  	    # (OPTIONAL) if set y will deploy successful build to cqs schema with proper permissions
--study_list                                # (OPTIONAL) comma separated list of studies
"""

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
sys.path.append('{filepath}/pkg'.format(filepath = path.dirname( path.abspath(__file__) ) ) ) # pkg subdirectory resides within same directory as this file
sys.path.append(path.dirname( path.dirname( path.dirname( path.dirname( path.dirname( path.abspath(__file__) ) ) ))) + '/validation/cqs/global_cqs' ) # fetching the directory path for global_cqs
from cqs_dictionary import * # fetching cqs_dictionary to get USDM tables
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
import shutil


class Test_Build_Driver():

######################################################
######## Start global variables and functions ########
######################################################

    gSkipRemaining = False # Used to enable/disable tests based on previous test results. In some steps we want to skip the remaining tests on failure.
                          # In other tests we want to proceed despite failure. This is a test-by-test setting that must be set to False to proceed.

    gSkipFinalSteps = False # Used to control final processing steps such as drop_schema and deploy_schema. Set to true on failure.
                            # If gSkipRemaining = True than by default that will also disable the final steps as well.


    # the standard parameters passed to pytest.main() in methods below come from the build_cmd fixture in conftest.py
    # ret_cmd is used to extend those parameters based on additional settings per test
    def ret_cmd(self, pCmd, pRepName, pDin_customer_path, pHTMLReportPath, pXMLReportPath):
        lCmd = pCmd[:]
        lCmd.extend(['--din_customer_path={path}'.format(path=pDin_customer_path)])
        if pHTMLReportPath:
            lCmd.extend(['--html={path}/{name}.html'.format(path=pHTMLReportPath, name=pRepName), '--self-contained-html'])
        if pXMLReportPath:
            lCmd.extend(['--junitxml={path}/{name}.xml'.format(path=pXMLReportPath, name=pRepName)])
        return lCmd

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
            lTxt = lTxt.encode('ascii', 'ignore')
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


    # checking the study folders present under din_customer_path
    def get_study_list(self, din_customer_path, ccdm_path, study_list):

        lStudyDirList=[]

        if study_list:
            if study_list is not None and len(study_list) > 0:
                for study in study_list:
                    if os.path.isdir(os.path.join(din_customer_path,study)):
                        lStudyDirList.append(study)
                    else:
                        pytest.fail('**** Study {study} mentioned in --study_list parameter do not have study level folder under din_customer_path. Please check ***'.format(study=study))
                        
        elif din_customer_path.split('_temp_',1)[0] != '/tmp'+ccdm_path:
            # list comprehension to fetch the list of folders present under the din-customer path 
            lStudyDirList = [study for study in os.listdir(din_customer_path) if os.path.isdir(os.path.join(din_customer_path,study)) and 'global' not in study]

            # to check if layer* folders are present under ccdm folder and if it is present then populate the lStudyDirList with value 'combined_studies_build' to identify legacy all study approach has be followed
            if len([folder for folder in lStudyDirList if 'layer' in folder ]) >0 : 
                lStudyDirList=['combined_studies']
        else:
            lStudyDirList=['combined_studies'] 
        return lStudyDirList

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

    # execute drop_schema_tables on pSchema
    def drop_schema(self, pSchema, pConn, pTableLevel=True):
        try:
            lCur = pConn.cursor()
            if pTableLevel:
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
            else:
                lSQL = '''DROP SCHEMA IF EXISTS "{schema}" CASCADE;'''.format(schema=pSchema)
            lCur.execute(lSQL)
        finally:
            pConn.commit()
            lCur.close()

    # iterate over list of pFiles sql files and execute SQL
    def run_sql_file(self, pSchema, pConn, pFiles):
        lSQL = None
        try:
            lCur = pConn.cursor()
            for lItem in pFiles:
                lFile = open(lItem, 'r')
                lSQL = ''.join(line for line in lFile)
                lCur.execute(lSQL)
        finally:
            pConn.commit()
            lCur.close()
    
######################################################
######### End global variables and functions #########
######################################################


######################################################
################ Start test functions ################
######################################################
        

    # 1 - Verify build setup
    # if test does not pass then mark remaining tests as "skip"
    def test__1_run_build(self, driver_prepare_cmd, din_customer_path, ccdm_path, 
                            schema, html_report_path, xml_report_path, report_suffix, debugging, 
                            study_list, rebuild_schema, app_schema, app_scripts, rebuild_app_schema, connect_database_func):

        # sleep for pSec seconds and display message to user
        # break if [Enter] or [Ctrl + c] keys are pressed
        def sleep(pSec): # in seconds
            try:
                for i in range(pSec,-1,-1):
                    print 'Build will automatically continue in {start_bold} {seconds} {end_bold} seconds...\r'.format(start_bold='\033[1m', seconds=i, end_bold='\033[0m'),
                    sys.stdout.flush()
                    time.sleep(1)
                    i,o,e = select.select([sys.stdin],[],[],0.0001)
                    for s in i:
                        if s == sys.stdin:
                            input = sys.stdin.readline()
                            return None
            except KeyboardInterrupt:
                Test_Build_Driver.gSkipRemaining = True
                pytest.fail('Build cancelled by user')

        try:
            lExitCode = 0
            lErrMsg = 'Unknown Error'

            # verify ccdm and din-customer paths exist
            if not os.path.exists(din_customer_path):
                lExitCode = 1
                lErrMsg = 'Invalid din_customer_path: {path}'.format(path=din_customer_path)
                pytest.fail(lErrMsg)

            if not os.path.exists(ccdm_path):
                lExitCode = 1
                lErrMsg = 'Invalid ccdm_path: {path}'.format(path=ccdm_path)
                pytest.fail(lErrMsg)

            # confirm and display github repo details
            print '''\n######################## GitHub Repo Details ########################'''
            print '''# DIN-Customer Repo:\n# ------------------\n# Directory: {dir}'''.format(dir=din_customer_path)
            print self.get_repo_details(din_customer_path)
            print '''# CCDM Repo:\n# --------------------\n# Directory: {dir}'''.format(dir=ccdm_path)
            print self.get_repo_details(ccdm_path)
            print '''#####################################################################\n'''
            print 'Press [Ctrl + c] to Cancel, [Enter] to Continue'
            sleep(10) # pause for 10 seconds (enter or ctrl +c to override)

            lTestName = 'studies_report_{suffix}'.format(suffix=report_suffix)

            lCur = connect_database_func.cursor()
            # postgres function to delete tables one by one in the given schema.
            lSQL = '''CREATE OR REPLACE FUNCTION public.drop_schema_tables (pSchema TEXT = NULL, pTable TEXT = NULL, pLastTable BOOLEAN = FALSE)
                            RETURNS VOID AS
                        $$
                        BEGIN
                            
                            IF  pSchema IS NULL THEN
                              RAISE EXCEPTION 'schema parameter is NULL!';
                            END IF;

                            EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(pSchema)||'.'||quote_ident(pTable) || ' CASCADE';

                            IF pLastTable THEN
                                EXECUTE 'DROP SCHEMA IF EXISTS ' || quote_ident(pSchema)||' CASCADE';
                            END IF;
                        END;
                        $$
                        LANGUAGE PLPGSQL;'''
            lCur.execute(lSQL)
            connect_database_func.commit()
            lCur.close()

            # create temp directories where code can be modified without impacting local copies
            if din_customer_path != ccdm_path :
                temp_din_customer_path = '/tmp' + din_customer_path + '_temp_' + str(int(round(time.time() * 1000)))
                if os.path.isdir(temp_din_customer_path):
                    shutil.rmtree(temp_din_customer_path)
                shutil.copytree(din_customer_path, temp_din_customer_path)
            else :
                temp_din_customer_path = din_customer_path

            if study_list is not None and len(study_list) > 0:
                study_list=study_list.split(',')

            # handle master schema
            slist = self.get_study_list(din_customer_path, ccdm_path, study_list)
            if slist is not None and len(slist) > 0 and slist[0] != 'combined_studies':
                # create master schema
                if self.schema_exists(schema, connect_database_func) is False:
                    print '\n**** Master Schema "{master_schema}" does not exist. Creating schema. ****'.format(master_schema=schema)
                    self.create_schema(schema, connect_database_func)
                # rebuilding master schema in case of split study approach
                elif rebuild_schema:
                    if self.schema_exists(schema, connect_database_func): # clone schema exists     
                        print '\n**** Rebuilding master schema "{master_schema}" ****'.format(master_schema=schema)
                        self.drop_schema(schema, connect_database_func)
                        self.create_schema(schema, connect_database_func) 
                # schema exists so do nothing    
                else:
                    print '\n**** Master Schema "{master_schema}" already exists. Proceeding with build. ****'.format(master_schema=schema)
            
            # handle app schema
            if rebuild_app_schema is True and self.schema_exists(app_schema, connect_database_func) is True:
                self.drop_schema(app_schema, connect_database_func, False)
            if rebuild_app_schema is True or self.schema_exists(app_schema, connect_database_func) is False:
                self.run_sql_file(schema, connect_database_func, app_scripts) # run scripts to build the app schema       

            # preparing command to execute build_cdm_driver_interpreter.py script 
            driver_prepare_cmd = self.ret_cmd(driver_prepare_cmd, lTestName, temp_din_customer_path, html_report_path, xml_report_path)
            lExitCode = pytest.main(driver_prepare_cmd)

            # removing the temp din-customer folder when debugging flag is not set
            if not debugging and din_customer_path != ccdm_path:
                shutil.rmtree(temp_din_customer_path)

            # if this test fails then skip remaining tests
            if lExitCode == 0:
                Test_Build_Driver.gSkipRemaining = False
            else:
                Test_Build_Driver.gSkipRemaining = True

            assert lExitCode == 0, '**** Build Verification Failed **** '
        except:
            Test_Build_Driver.gSkipRemaining = True
            pytest.fail(sys.exc_info()[1])

    # 13 - drop temporary schema if necessary
    @pytest.mark.skipif("Test_Build_Driver.gSkipRemaining", reason="Test skipped due to previous failure")
    def test__13_drop_schema(self, start, stop, tests, schema, drop_schema, deploy_to_app, connect_database_func):
        if start > 13 or stop < 13 or 13 not in tests or deploy_to_app or not drop_schema: # disabled by parameters
            pytest.skip("Test skipped by user")
        if Test_Build_Driver.gSkipRemaining or Test_Build_Driver.gSkipFinalSteps:
            pytest.skip("Test skipped due to previous failure")

        lExitCode = 1
        print '\n**** Dropping schema "{schema}"'.format(schema=schema)
        self.drop_schema(schema, connect_database_func)

        if self.schema_exists(schema, connect_database_func) is False:
            lExitCode = 0

        assert lExitCode == 0, 'Schema "{schema}" not successfully dropped'.format(schema=schema)

    # 14 - deploy schema if necessary
    @pytest.mark.skipif("Test_Build_Driver.gSkipRemaining", reason="Test skipped due to previous failure")
    def test__14_deploy_schema(self, start, stop, tests, schema, deploy_to_app, connect_database_func):
        if start > 14 or stop < 14 or 14 not in tests or not deploy_to_app: # disabled by parameters
            pytest.skip("Test skipped by user")
        if Test_Build_Driver.gSkipRemaining or Test_Build_Driver.gSkipFinalSteps:
            pytest.skip("Test skipped due to previous failure")
 
        try:
            lExitCode = 1
            print '\n**** Deploying schema "{schema}" to app as "cqs" schema with proper permissions'.format(schema=schema)

            # execute RENAME cqs SCHEMA to backup, RENAME SCHEMA to cqs, alter owner to app user
            lCur = connect_database_func.cursor()

            # Change owner of dev schema from the master write role to the application role in order 
            # to ensure the application architecture mode can access the schema properly
            lDBName = connect_database_func.get_dsn_parameters()['dbname']
            lAppRoleName = lDBName.replace('app-clinical','app') # Requires proper convention followed on app role name
            lSQL = 'ALTER SCHEMA "{schema}" OWNER TO "{rolename}"'.format(schema=schema, rolename=lAppRoleName)
            lCur.execute(lSQL)

            # check if master-write role exists in this DB and grant access
            lMasterWriteRoleName = '{dbname}-master-write'.format(dbname=lDBName)
            lSQL = '''SELECT 1 FROM pg_roles where rolname='{rolename}' '''.format(rolename=lMasterWriteRoleName)
            lCur.execute(lSQL)
            if lCur.rowcount > 0:
                lSQL = 'GRANT USAGE ON  SCHEMA "{schema}" to "{rolename}";'.format(schema=schema, rolename=lMasterWriteRoleName)
                lCur.execute(lSQL)
                lSQL = 'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" to "{rolename}";'.format(schema=schema, rolename=lMasterWriteRoleName)
                lCur.execute(lSQL)
            else:
                print '\n**** Role name "{rolename}" does not exist in this database ****'.format(rolename=lMasterWriteRoleName)

            if self.schema_exists('cqs', connect_database_func): # Backup existing cqs schema
                # Move cqs schema to backup schema including yyyymmddhh24miss
                timestr = time.strftime("%Y%m%d_%H%M%S")

                lSQL = '''ALTER SCHEMA cqs RENAME TO cqs_bkup_{cdatestamp}'''.format(cdatestamp=timestr)
                lCur.execute(lSQL)

            # Rename the development schema to cqs
            lSQL = '''ALTER SCHEMA "{schema}" rename to cqs'''.format(schema=schema)
            lCur.execute(lSQL)

            # Any error in the execution of the above SQL will result in an error
            assert lExitCode == 1, 'Schema "{schema}" not successfully deployed'.format(schema=schema)

        finally:
            connect_database_func.commit()
            lCur.close()

    # 15 - build final steps
    def test__15_build_final_steps(self, start, stop, tests, html_report_path, generate_report_pdf):
        try:
            lExitCode = 1
            if not html_report_path or not generate_report_pdf:
                pytest.skip("Test skipped by user")
            else:
                mydirectory = os.path.realpath(html_report_path)
                myoutfilename = mydirectory + '/build_report.pdf'

                files = [f for f in os.listdir(mydirectory + '/') if re.match(r'test.*\.html',f)]
                newfiles = []
                for f in files:
                    vnum = re.search(r'\d+', f).group(0)
                    newfiles.append('{:03d}'.format(int(vnum)) + '~' + f)

                sorted_files = sorted(newfiles,key=str.lower)

                groomed_files = []

                # If main html file was created, add it here
                if os.path.exists(mydirectory + '/main_report.html'):
                    groomed_files.append(mydirectory + '/main_report.html')

                for f in sorted_files:
                    newfile = mydirectory + '/' + re.search(r'.*\~(test.*\.html)', f).group(1)
                    groomed_files.append(newfile)

                options = {
                    'orientation': 'Landscape',
                    'page-size': 'Letter',
                    'zoom':.95,
                    'footer-right': '[page]/[topage]',
                    'header-center': '[webpage]'
                }

                if len(groomed_files) > 0:
                        pdfkit.from_file(groomed_files,myoutfilename,options=options)
                      
                lExitCode = 0

                assert lExitCode == 0, 'PDF File not successfully generated'
        finally:
            ltemp = 'dummy'

######################################################
################# End test functions #################
######################################################

if __name__ == '__main__':
    args = docopt(__doc__, version='build_cdm_driver 1.0')


