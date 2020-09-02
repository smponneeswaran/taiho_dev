#!/usr/bin/env python

"""
build_cdm_driver_interpreter.py
Usage:
  py.test build_cdm_driver_interpretor.py -s -rs --spec --tb=short --zk_nodes=<servers> --zk_path=<path> --customer=<customer> --schema=<schema> --din_customer_path=<path> --ccdm_path=<path> 
                                            [--html=<path> --self-contained-html --junitxml=<path> --ccdm_integration_test=<y | n> --drop_schema=<y | n> --rebuild_schema=<y | n> 
                                            --clone_schema=<schema> --drop_constraints=<y | n> --drop_constraints_exclusions=<table_list> --num_processes=<num | auto> --start=<test_num> --stop=<test_num> 
                                            --tests=<test_num - test_num> --generate_report_pdf=<y> --sql_limit=<y | n> --days_limit=<num> --scorecard_incremental=<y | n> --deploy_to_app=<y | n> --study_list=<study_list>]
  build_cdm_driver_interpretor.py (-h | --help)
  build_cdm_driver_interpretor.py --version
Description:
    Executes the following functions to generate separate command to call build_cdm_driver_executor.py for each study. Also, the study level commands can be executed in parallel based on the value provided in num_processes argument
    Test Descriptions:
    -------------------
    1. pytest_generate_tests    = To generate test for each study and call the build_cdm_driver_executor.py for each study
    2. test__prepare_cmd        = Function to prepare command to execute build_cdm_driver_executor.py script  

Options:
-h --help                                   # show this screen
--version                                   # show version
"""

import sys, os
from os import path
sys.path.append('{filepath}/pkg'.format(filepath = path.dirname( path.abspath(__file__) ) ) ) # pkg subdirectory resides within same directory as this file
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

class Test_Build_Interpretor():

######################################################
######## Start global variables and functions ########
######################################################

    # the standard parameters passed to pytest.main() in methods below come from the build_cmd fixture in conftest.py
    # ret_cmd is used to extend those parameters based on additional settings per test
    def ret_cmd(self, pCmd, pStudy_name, pRepName, pPath, pHTMLReportPath, pXMLReportPath):
        lCmd = pCmd[:]
        lCmd.extend(['--din_customer_path={path}'.format(path=pPath)])
        if pStudy_name:
            lCmd.extend(['--study_name={study_name}'.format(study_name=pStudy_name)])
        if pHTMLReportPath:
            lCmd.extend(['--html={path}/{name}.html'.format(path=pHTMLReportPath, name=pRepName), '--self-contained-html'])
        if pXMLReportPath:
            lCmd.extend(['--junitxml={path}/{name}.xml'.format(path=pXMLReportPath, name=pRepName)])
       
        return lCmd

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
                        
        elif din_customer_path.split('_temp_',1)[0] != ccdm_path:
            # list comprehension to fetch the list of folders present under the din-customer path 
            lStudyDirList = [study for study in os.listdir(din_customer_path) if os.path.isdir(os.path.join(din_customer_path,study)) and 'global' not in study]

            # to check if layer* folders are present under ccdm folder and if it is present then populate the lStudyDirList with value 'combined_studies_build' to identify legacy all study approach has be followed
            if len([folder for folder in lStudyDirList if 'layer' in folder ]) >0 : 
                lStudyDirList=['combined_studies']
        else:
            lStudyDirList=['combined_studies'] 
        return lStudyDirList

    # function to derive the study level schema
    def get_study_schema(self, schema, studyid):
        if studyid == 'combined_studies':
            study_schema = schema
        else :
            study_schema = schema+'_{study}_new'.format(study=re.sub('[^a-zA-Z0-9\n\.]', '_',studyid).lower())
        return study_schema

 
######################################################
######### End global variables and functions #########
######################################################


######################################################
################ Start test functions ################
#####################################################
        
    # function to generate tests for each study
    def pytest_generate_tests(self, metafunc):

        try:
            lExitCode = 0

            din_customer_path = metafunc.config.option.din_customer_path
            ccdm_path = metafunc.config.option.ccdm_path
            study_list = metafunc.config.option.study_list
            if study_list:
                study_list = study_list.split(',')
            lStudyList = self.get_study_list(din_customer_path, ccdm_path, study_list) 

            if lStudyList is not None and len(lStudyList) > 0:
                if 'study_name' in metafunc.fixturenames:
                    metafunc.parametrize("study_name", lStudyList, ids=lStudyList, scope="class")
            else:
                lExitCode = 1
                pytest.fail('**** No mapping folders present in the given din_customer_path. Please check ***')
        except:
            pytest.fail(sys.exc_info()[1])

    # function to prepare command to execute build_cdm_driver_executor.py script   
    def test__build_study(self, study_name, driver_executor_cmd, din_customer_path, schema, html_report_path, xml_report_path, report_suffix):
        try:
            if study_name != 'combined_studies':
                print '''\n######################## Copying files from Global folder to Study Level Folders for study {study_name}########################'''.format(study_name=study_name)
                global_path = '{din_customer_path}/global'.format(din_customer_path=din_customer_path)
                lglobal_folders = [subdir for subdir in os.listdir(global_path) if os.path.isdir(os.path.join(global_path, subdir)) ]

                for folder in lglobal_folders:
                    global_files = os.listdir(os.path.join(global_path, folder)) 
                    for filename in global_files:
                        if not os.path.isdir(os.path.join(din_customer_path, study_name, folder)):
                            os.mkdir(os.path.join(din_customer_path, study_name, folder))
                        study_files = os.listdir(os.path.join(din_customer_path, study_name, folder)) 
                        
                        if (os.path.isfile(os.path.join(global_path, folder, filename))) and study_name !='global' and filename not in study_files:
                            shutil.copy(os.path.join(global_path, folder, filename), os.path.join(din_customer_path, study_name, folder, filename))
                
                din_customer_path = os.path.join(din_customer_path, study_name)
                study_schema = self.get_study_schema(schema, study_name)
                driver_executor_cmd = ['--schema={study_schema}'.format(study_schema=study_schema) if val=='--schema={schema}'.format(schema=schema) else val for val in driver_executor_cmd]

            if study_name != '':
                lTestName = 'main_report_{study}_{suffix}'.format(study=study_name, suffix=report_suffix)
            else:
                lTestName = report_suffix
            driver_executor_cmd = self.ret_cmd(driver_executor_cmd, study_name, lTestName, din_customer_path, html_report_path, xml_report_path)
            lExitCode = pytest.main(driver_executor_cmd)
            assert lExitCode == 0, 'Build failed for study :{study_name}'.format(study_name=study_name) 

        except:
            pytest.fail(sys.exc_info()[1])

######################################################
################# End test functions #################
######################################################

if __name__ == '__main__':
    args = docopt(__doc__, version='build_cdm_driver_interpretor 1.0')


