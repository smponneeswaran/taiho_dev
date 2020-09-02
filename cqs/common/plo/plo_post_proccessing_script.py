#!/usr/bin/env python

"""
Usage:
  plo_post_proccessing_script.py  --zk_nodes=<nodes> --zk_path=<path> --customer=<customer> --schema=<schema-name> [--logdir=<dir>] [--debug] [--starttimelog]
  plo_post_proccessing_script.py (-h | --help)
  plo_post_proccessing_script.py --version
Description:
    This script is to perfrom the operation on the plo's created using sql's in the first pass to
    facilitate the complex functional enhancement of plo's
"""

#
# Revision History: 13-Sep-2016 ACK Converted to main() funciton to support import as module
#                   17-Nov-2016 DPR Fixing the study risk level to set to null instead of High when the expected valuesa are null as per tp18947
#                   06-Dec-2016 DPR Adding code to populate rpt_pivotal_study_analytics_datapoints plo with forecast datapoints
#                   12-Dec-2016 ACK Moved exception handling to main() function
#                   14-Dec-2016 DPR Fixing the bug as per tp 20051
#                   17-Dec-2016 ACK Added pivotal_study_analytics_datapoints cleanup logic
#                   04-Jan-2017 DPR Updating the tests as per the changes made to logistic regression as part of tp 20593
#                   04-Jan-2017 ACK Modified to use zookeeper instead of redis for obtaining db connection details
#                   06-Feb-2017 ACK Revision History will be maintained via GitHub History from this point forward
#

import sys, os
from os import path
from pathlib import Path
sys.path.append( str(Path(path.abspath(__file__)).parents[4] / 'utils') ) # /ccdm/resources/utils
sys.path.append( str(Path(path.abspath(__file__)).parents[5] / 'analytics/notebooks/enrollment_forecast') ) # /ccdm/analytics
import json
import re
import logging
import traceback
import psycopg2
import ConfigParser
import shutil
import errno
import random
import math
import cmath
import numpy
import time
from datetime import datetime, timedelta
import scipy.optimize
from docopt import docopt
from numpy.polynomial import Polynomial as P
from scipy.optimize import curve_fit
from scipy import stats
from logging.handlers import TimedRotatingFileHandler
import db_connect, zk_connect
import subprocess
import pandas as pd
from forecastsim import simulate_study
import calendar

numpy.seterr(all='ignore')

# zk_connect class
zk = zk_connect.zk_class()

# Zookeeper variables for successful execution time logging functionality
zkLogPath = '/CQS/cqs/plo_post_processing'
zkSuccessTimeNode = 'PloPostProcessingRunTime'

# initialize logger
def initialize_logger(customer, schema, debug=False, logdir=None):
    global logger
    logger = logging.getLogger(__name__)
    logger.propagate = False
    if debug:
        logLevel = logging.DEBUG
    else:
        logLevel = logging.INFO

    logger.setLevel(logLevel)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logLevel)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if logdir is not None:
        filename = (os.path.split(__file__)[1]).split('.',1)[0]
        log_filename = "{path}/{filename}_{customer}_{schema}.log".format(path=logdir, filename=filename, customer=customer, schema=schema)
        file_handler = TimedRotatingFileHandler(log_filename, when="D", interval=1, backupCount=10, delay=False, utc=True)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)


# initialize logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# class for defining return values
class ReturnCodes():
    exitCode = None


# function performing the 2nd degree polynomial regression
def quadratic_polynomial_reggression (x, y, target):
    result = None
    try:
        coeffs = numpy.polyfit(x,y,deg=2)
        yf = numpy.polyval(numpy.poly1d(coeffs), x)
        equation = numpy.poly1d(numpy.polyfit(x, y, deg=2), variable='N') # Polynomial Equation with coeffiecients a, b, c

        if target is None:
            target = 0

        d = ((coeffs[1])**2) - (4*(coeffs[0])*(coeffs[2] - target))
        res1 = (-(coeffs[1])-cmath.sqrt(d))/(2*(coeffs[0]))
        res2 = (-(coeffs[1])+cmath.sqrt(d))/(2*(coeffs[0]))

        result = ''
        if res1.real > res2.real :
            result = res1.real
        else:
            result = res2.real

        epoch_now = datetime.fromtimestamp(int(time.time())) # epoch format of current date.
        epoch_after_tenyrs = epoch_now + timedelta(days=5475) ## adding 15 years to the current date

        # checking if the predicted date is with in 15 years from now. this check is required to make sure the function not entering a infinite loop
        if int(float(result)) > int((epoch_after_tenyrs.strftime('%s'))) : 
            result = None

    except Exception, e:
        logger.exception(e)
        ReturnCodes.exitCode = os.EX_SOFTWARE
    finally:
        return result

#function to derive the sigmoid curve values
def sigmoid(p,x):
    y = None
    try:
        x0,y0,c,k=p
        y = c / (1 + numpy.exp(-k*(x-x0))) + y0
    except Exception, e:
        logger.exception(e)
        ReturnCodes.exitCode = os.EX_SOFTWARE
    finally:
        return y

#function to calculate the residuals to feed the leastsq function
def residuals(p,x,y):
    result = None
    try:
        result = y - sigmoid(p,x)
    except Exception, e:
        logger.exception(e)
        ReturnCodes.exitCode = os.EX_SOFTWARE
    finally:
        return result

#function to calculate monthly forecast for the last 4 months using polynomial regression
def polynomial_monthly_forecast_calculator (conn, series_name, studyid, target):
    result_val = None
    cur = None
    try:
        cur = conn.cursor()
        x, y, z =[], [], []
        for i, valseries in enumerate(series_name):
            if valseries[1] == studyid:
                x.append(valseries[3])
                y.append(valseries[7])
                z.append(valseries[4])

    
        x_orig_length = len(x)
        y_orig_length = len(y)


        x = x[-4:] # limiting to last 4 x values
        y = y[-4:] # limiting to last 4 y values
        z = z[0:] # Planned start date

        x_fin, y_fin =[], []
        for i, val in enumerate(x):
            if z[0] is None: # bug fix to handle null planned start date
                z[0] = 0
            x_new = -x[i] + (2* z[0]) # Logic to mirror the date to produce the parabola for polynomial coeffs
            x_fin.append(x_new)
            y_fin.append(y[i])

        for j, val in enumerate(x):
            x_fin.append(x[j])
            y_fin.append(y[j])

        x = numpy.array(x_fin, dtype='float')
        y = numpy.array(y_fin, dtype='float')

        coeffs = numpy.polyfit(x,y,deg=2)
        if target is None:
            target = 0

        d = ((coeffs[1])**2) - (4*(coeffs[0])*(coeffs[2] - target))
        res1 = (-(coeffs[1])-cmath.sqrt(d))/(2*(coeffs[0]))
        res2 = (-(coeffs[1])+cmath.sqrt(d))/(2*(coeffs[0]))

        result = ''
        if res1.real > res2.real :
            result = res1.real
        else:
            result = res2.real

        epoch_now = datetime.fromtimestamp(int(time.time())) # epoch format of current date
        epoch_after_tenyrs = epoch_now + timedelta(days=5475) ## adding 15 years to the current date

        # checking if the predicted date is with in 15 years from now. this check is required to make sure the function not entering a infinite loop
        if int(float(result)) <= int((epoch_after_tenyrs.strftime('%s'))) : 

            x_predict=[]
            y_predict=[]
            target_len = len(x)-4 # to populate the forecast date series starting from 4 months prior to current month
            targetX = x[target_len] 

            while targetX <= result: # projecting the values till enrollment target is met

                vSql = '''select extract(epoch from(select to_timestamp({targetX}))  + Interval '1 Month')'''.format(targetX = targetX)
                cur.execute(vSql)
                vInc_Month_ts = cur.fetchone()

                vMonthly_target = (coeffs[0]*(vInc_Month_ts[0]**2))+(coeffs[1]*(vInc_Month_ts[0]))+coeffs[2] # 2nd degree ploynomial equation (a * x power 2) + (b * x) + c

                if vInc_Month_ts[0] < result:
                    x_predict.append(vInc_Month_ts[0])
                    y_predict.append(vMonthly_target)

                else :
                     x_predict.append(result)
                     y_predict.append(target)

                targetX = vInc_Month_ts[0]

            x_final_length = len(x_predict)
            y_final_length = len(y_predict)

            if x_final_length > 100:
                x_predict = x_predict[:100]

            if y_final_length > 100:
                y_predict = y_predict[:100]

            if len(y) > 0 and target > max(y):
                result_val = x_predict, y_predict
        else:
            logger.info('Skipping polynomial forecast calculation because calculated target date is greater than 15 years due to insufficient data : {study}'.format(study=studyid))
            result_val = '','' 
    except Exception, e:
        logger.exception(e)
        ReturnCodes.exitCode = os.EX_SOFTWARE
    finally:
        if cur is not None:
            cur.close()
        return result_val

# function to calculate the forecast values
def polynomial_forecast_calculator (series_name, studyid, target):
    result_equation = None
    x, y, z =[], [], []
    try:
        for i, valseries in enumerate(series_name):
            if valseries[1] == studyid:
                x.append(valseries[3])
                y.append(valseries[7])
                z.append(valseries[4])

        if len(x) >=2 :  # applying this condtiton since the numpy polyfit calcutaion requires minimum of two data points in order to provide acceptable caluclated values
            x = x[-4:] # limiting to last 4 x values
            y = y[-4:] # limiting to last 4 y values
            z = z[0:] # Planned start date

        
            x_fin, y_fin =[], []
            for i, val in enumerate(x):
                if z[0] is None:
                    z[0] = 0 # bug fix to handle null planned start date
                x_new = -x[i] + (2* z[0]) # Logic to mirror the date to produce the parabola for polynomial coeffs
                x_fin.append(x_new)
                y_fin.append(y[i])

            for j, val in enumerate(x):
                x_fin.append(x[j])
                y_fin.append(y[j])

            x = numpy.array(x_fin, dtype='float')
            y = numpy.array(y_fin, dtype='float')

            # condition to check that target was not already met.
            if max(y) < target:
                result_equation = quadratic_polynomial_reggression (x, y, target)

        else:
            logger.info('Skipping polynomial forecast calculation because non availabilty of >= 2 datapoints for study : {study}'.format(study=studyid))
            result_equation = ''
    except Exception, e:
        logger.exception(e)
        ReturnCodes.exitCode = os.EX_SOFTWARE
    finally:
        return result_equation

#logistic regression using the sigmoid transformation to predict the total months to achieve the enrollment target
#series_name : This is a data series output fron sql assingned to lEnrollment_series.  This has the fields with  values as follows
#            0. metric_type
#            1. studyid
#            2. end_date
#            3. epoch date_part of enddate
#            4. epoch date_part of planned end date
#            5. Enrollment Month Number
#            6. Monthly enrollment count
#            7. Cummulative enrollment count
#studyid : Studyid
#target : Enrollment target
def logistic_monthly_forecast_calculator(conn, series_name, studyid, target):
    result = None
    x, y, x_mod =[], [], []
    cur = None
    try:
        cur = conn.cursor()
        # fetching value the enrollment series values in to the arrays
        for i, valseries in enumerate(series_name):
            if valseries[1] == studyid:
                x.append(valseries[3]) # Fetching the timestamp for each enrollment month
                x_mod.append(valseries[5]) # Fetching the month num for each enrollment month
                y.append(valseries[7]) # Fetching cumulative enrollment numbers for each month

        #Fetching the last 4 datapoints from the enrollment series
        x = x[-4:] # limiting to last 4 x values
        x_mod = x_mod[-4:] # limiting to last 4 x values
        y = y[-4:] # limiting to last 4 y values

        #Calculate the forecast only if the target not met yet
        if len(y) > 0 and max(y) < target :

            # converting the arrays in to float for the accuracy of calculation
            x = numpy.array(x, dtype='float')
            x_mod = numpy.array(x_mod, dtype='float')
            y = numpy.array(y, dtype='float')

            # Using numpy linear regression function to find the slope and intercept
            slope, intercept, r_value, p_value, std_err = stats.linregress(x,y)
            a = intercept
            b = slope

            if a <= 0 and b != 0: # Checking if the intercept is less than zero and slope is not equal to zero  make sure linear regression points will converge to reach the target

                # calcuations to predict the values based on the intercept and slope calculated in the previous step
                if len(x) > 0:
                    x_timestamp, x_monthnum = [], []
                    x_min_timestamp = min(x) # fetching first month timestamp of last 4 months fetched as the starting month for the series
                    x_min_monthnum = 1 # Assinging 1 as the starting month number for first month of last 4 months fetched

                    # Appending the values into temp arrays
                    x_timestamp.append(x_min_timestamp)
                    x_monthnum.append(x_min_monthnum)

                y_linear = []
                y_linear_calc = a + (b * (x_min_timestamp)) # Linear regrssion calculation (a+bx) to find the y values
                y_linear.append(y_linear_calc)

                # while loop to calculate y values till we reach the target
                while max(y_linear) <= (target):

                    vSql = '''select extract(epoch from (to_timestamp({x_timestamp})::date + Interval '1 Month'))'''.format(x_timestamp=x_min_timestamp) # sql to increment the timestamp by 1 month for every y value till reaching the target
                    cur.execute(vSql)
                    next_month= cur.fetchone()
                    x_min_timestamp=next_month[0]
                    x_min_monthnum = x_min_monthnum +1

                    # Lenear regression calculation to find the value of y at given timesatmp
                    y_linear_calc = a + (b * (x_min_timestamp))  # Linear regrssion calculation (a+bx) to find the y values

                    # To avoid the neagtive values in the plot
                    if y_linear_calc < 0:
                        y_linear_calc = 0

                    x_timestamp.append(x_min_timestamp)
                    x_monthnum.append(x_min_monthnum)
                    y_linear.append(y_linear_calc)

                # dividing the values in the dependent value array by the maximum value in the array to normalize all values less than one for facilitating the calculations in sigmoid function
                y_linear_mod = numpy.divide(y_linear,float(max(y_linear)))

                #function to derive the monthly sigmoid curve values
                def sigmoid_monthly(x, x0, k):

                     y = 1 / (1 + numpy.exp(-k*(x-x0)))
                     return y

                # curve fit function to fit coefficients of s-curve

                popt, pcov = curve_fit(sigmoid_monthly, x_monthnum, y_linear_mod,  maxfev=10000)
                x_1 = numpy.linspace(min(x_monthnum), 100, 100) # generating x value series of 100 array values to use as input to the sigmoid function
                y_1 = sigmoid_monthly(x_1, *popt ) # *popt --> Optimal values for the parameters so that the sum of the squared error of f(xdata, *popt) - ydata is minimized
                y_predict = [i*float(max(y_linear)) for i in y_1]

                #checking if y_predict has the predicted y values populated in there
                if len(y_predict) > 0:
                    x_final_timestamp, x_final_monthnum = [], []
                    x_min_timestamp = min(x)
                    x_min_monthnum = 1

                y_final_predict = []

                # for loop to calculate the values till we reach the target
                for i, val in enumerate(y_predict):
                    vSql = '''select extract(epoch from (to_timestamp({x_timestamp})::date + Interval '1 Month'))'''.format(x_timestamp=x_min_timestamp)
                    cur.execute(vSql)
                    next_month= cur.fetchone()

                    # If loop check the point where the prediction values are exceeding the target value
                    if val >= target:
                        y_final_predict.append(target)
                        x_final_timestamp.append(x_min_timestamp)
                        x_final_monthnum.append(x_min_monthnum)
                        break
                    else:
                        y_final_predict.append(val)
                        x_final_timestamp.append(x_min_timestamp)
                        x_final_monthnum.append(x_min_monthnum)

                    x_min_timestamp=next_month[0]
                    x_min_monthnum = x_min_monthnum +1

                # Rerutn prediction values only when the target has not reached
                if len(x_final_timestamp) > 0 and len(y_final_predict)>0 and target > max(y):
                    result = x_final_timestamp, y_final_predict
            else : 
                logger.info('Skipping Logistic forecast calculation since calculated slope will not reach target due to insufficient data : {study}'.format(study=studyid))
                result = None
    except Exception, e:
        logger.exception(e)
        ReturnCodes.exitCode = os.EX_SOFTWARE
    finally:
        if cur is not None:
            cur.close()
        return result

#logistic regression using the sigmoid transformation to predict the total months to achieve the enrollment target
def logistic_forecast_calculator(series_name, studyid, target):
    total_months = None
    try:
        x, y = [], []
        for i, valseries in enumerate(series_name):
            if valseries[1] == studyid:
                x.append(valseries[5])
                y.append(valseries[7])

        x = numpy.array(x, dtype='int')
        y = numpy.array(y, dtype='int')

        p_guess=(numpy.median(x),numpy.median(y),1.0,1.0)
        p, cov, infodict, mesg, ier = scipy.optimize.leastsq(residuals,p_guess,args=(x,y),full_output=1)

        x0,y0,c,k=p
        xp = numpy.linspace(0, 96, 96, endpoint=True ) #letting he linespace to run for 8 years ( 96 months) and loop through the months to find where the target fits in there.
        pxp=sigmoid(p,xp)

        if pxp.max() < target:
            target = int(pxp.max())

        pxplen = len([i for i in pxp if i <= target])
        total_months = int(math.ceil(xp[pxplen]))
    except Exception, e:
        logger.exception(e)
        ReturnCodes.exitCode = os.EX_SOFTWARE
    finally:
        return total_months

def mindate_function (series_name, studyid):
    min_date = None
    try:
        date_series = []
        if series_name is None:
            return None

        for i, datevals in  enumerate(series_name):
            if datevals[1] == studyid:
                date_series.append(datevals[2])
        if date_series and len(date_series) > 0:
            min_date = min(date_series)
    except Exception, e:
        logger.exception(e)
        ReturnCodes.exitCode = os.EX_SOFTWARE
    finally:
        return min_date

# function to execute DML update statements
def dml_sql_update_function (conn, sql_stmnt, sql_field, studyid):
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(sql_stmnt)
        conn.commit()
        if sql_field is not None and sql_field != '':
            logger.info ('Succesfully updated field {field} for the study: {study}'.format(field = sql_field, study = studyid))
    except Exception, e:
        logger.exception(e)
        ReturnCodes.exitCode = os.EX_SOFTWARE
    finally:
        if cur is not None:
            cur.close()
        return None

# function to execute insert, delete DML statments
def dml_sql_execution_function (conn, sql_stmnt, log_message):
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(sql_stmnt)
        conn.commit()
        if log_message is not None and log_message != '':
            logger.info(log_message)
    except Exception, e:
        logger.exception(e)
        ReturnCodes.exitCode = os.EX_SOFTWARE
    finally:
        if cur is not None:
            cur.close()
        return None

# function to execute a SQL query and return all results
def sql_fetchall_function (conn, sql_stmnt):
    return_set = None
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(sql_stmnt)
        if cur.rowcount > 0:
            return_set = cur.fetchall()
    except Exception, e:
        logger.exception(e)
        ReturnCodes.exitCode = os.EX_SOFTWARE
    finally:
        if cur is not None:
            cur.close()
        return return_set

# function to execute a SQL query and return the first record
def sql_fetchone_function (conn, sql_stmnt):
    return_rec = None
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(sql_stmnt)
        if cur.rowcount > 0:
            return_rec = cur.fetchone()
    except Exception, e:
        logger.exception(e)
        ReturnCodes.exitCode = os.EX_SOFTWARE
    finally:
        if cur is not None:
            cur.close()
        return return_rec

# verify schema exists in db, return boolean
def schema_exists(conn, schema):
    schemaExists = False
    cur = None
    try:
        cur = conn.cursor()
        lSql = '''select 1 from information_schema.tables where table_schema = '{schema}' '''.format(schema=schema)
        cur.execute(lSql)
        if cur.rowcount > 0:
            schemaExists = True
    except Exception, e:
        logger.exception(e)
        ReturnCodes.exitCode = os.EX_SOFTWARE
    finally:
        if cur is not None:
            cur.close()
        return schemaExists


# log successful execution start time to ZK
def log_successful_run(zk_client, zk_nodes, zk_path, customer, start):
    success = True
    successTimeNode = '{path}/{customer}{zkLogPath}/{node}'.format(path=zk_path, customer=customer, zkLogPath=zkLogPath, node=zkSuccessTimeNode)

    if start is not None and len(start) > 0:
        logger.info('Logging successful start time to zookeeper: {start}'.format(start=start))
        if zk.zk_node_exists(zk_client, successTimeNode):
            zk.modify_zk_node(zk_client, successTimeNode, start)
        else:
            logger.error('Cannot log start time. Node does not exist: {node}'.format(node=successTimeNode))
            success = False

    return success



class monte_carlo_forecast:
    def __init__(self, conn, pStudyid, pSchema):
        """
        Initializes the class with studyid, schema and database connection parameters
        Args:
        - conn:  Postgres Database Connection
        - pStudyid:  Studyid for generating forecast
        - pSchema:  Schema where forecast algorithm is being used
        """

        self.conn = conn
        self.studyid = pStudyid
        self.schema = pSchema

    def load_data(self, conn, sql, df_columns):
        """
        Executes a SQL query and return a pandas DataFrame.
        Args:
        - conn: postgresql connection 
        - sql: SQL query
        - df_columns: columns to set to the returned data frame (should match columns of the result set)
        Returns:
        - pandas DataFrame containing the result set.
        """
        df = pd.DataFrame([pd.Series(arr) for arr in pd.read_sql(sql, conn).values])

        if not df.empty:
            df.columns = df_columns
        else:
            df = None

        return df

    def get_planned_data(self, study_id, df_recr, df_std_stats):
        """
        From the studyplannedrecruitment and studyplannedstatistic data frames, get the planned data
        Args:
        - study_id: study_id to filter for the right study in the data frames (if not already done)
        - df_recr : data frame for studyplannedrecruitment with at least columns studyid and recruitmentcount.
        - df_std_stats : data frame for studyplannedstatistic with at least columns studyid and statval.
        Returns:
        - 2-tuple containing the planned number of subjects and the planned number of sites. This tuple is never
          None but its elements might be None if not available in the data frames.
        """
        # filter planned recruitment for the requested study_id, in case not already done
        df_filt_recr = df_recr[df_recr.studyid == study_id]
        planned_subjects = df_filt_recr.recruitmentcount.sum()
        if planned_subjects == 0:
            planned_subjects = None # consider no planned subjects available for this study
        
        # planned sites
        df_filt_std_stats = df_std_stats[df_std_stats.studyid == study_id]
        df_planned_sites = df_filt_std_stats[(df_filt_std_stats.statval != 0) & (df_filt_std_stats.statval != 99999)]

        if len(df_planned_sites) > 0:
            planned_sites = int(df_planned_sites.statval.iloc[0])
        else:
            planned_sites = None
        return planned_subjects, planned_sites


    def get_forecast_output(self):
        """
        Returns set of forecast values generated after the Monte Carlo simulation is done 
        """

        # sql to fetch maximum date from ds object. if maximum of dsstdtc is in future then today's date will be taken in to consideration
        # this date is used in forecast algorithm to determine from when to start forecasting.
        vSql = '''SELECT LEAST(MAX(dsstdtc), NOW()::DATE) AS latest_ds_date FROM "{schema}".rpt_subject_disposition WHERE studyid = '{study_id}' '''.format(study_id=self.studyid, schema=self.schema )
        last_update = self.load_data(self.conn, vSql, df_columns=['latest_ds_date'])

        # checking if the study has relevenat dates in ds object. if not last_update will have None value and no forecast will be generated for that study
        if last_update.iloc[0,0] is not None:
            last_update = last_update.iloc[0,0]
        else:
            last_update = None

        # sql to fetch enrolled subjects dataset from ds object
        vSql = '''SELECT studyid, siteid, usubjid, dsseq, dsstdtc, dsevent FROM "{schema}".rpt_subject_disposition 
                    WHERE studyid = '{study_id}' AND dsevent = 'ENROLLED' AND dsstdtc IS NOT NULL 
                    AND dsstdtc <= NOW()::DATE'''.format(study_id=self.studyid, schema=self.schema)      
        df_ds = self.load_data(self.conn, vSql, df_columns=['studyid', 'siteid', 'usubjid', 'dsseq', 'dsstdtc', 'dsevent'])

        # sql to fetch the planned enrollment
        vSql = '''SELECT studyid, enddate, recruitmentcount FROM "{schema}".studyplannedrecruitment 
                    WHERE studyid = '{study_id}' AND LOWER(category) = 'enrollment' AND 
                    LOWER(type) = 'planned' AND LOWER(frequency) = 'monthly' '''.format(study_id=self.studyid, schema=self.schema )
        df_recr = self.load_data(self.conn, vSql, df_columns=['studyid', 'enddate', 'recruitmentcount'])

        # sql to fetch the planned site activation numbers
        vSql = '''SELECT studyid, statval FROM "{schema}".studyplannedstatistic 
                    WHERE studyid = '{study_id}' AND statcat = 'SITE_ACTIVATION' '''.format(study_id=self.studyid, schema=self.schema )
        df_std_stats = self.load_data(self.conn, vSql, df_columns=['studyid', 'statval'])

        forecast_values = None

        if last_update is not None and df_ds is not None and df_recr is not None and df_std_stats is not None:
            planned_subjects, planned_sites = self.get_planned_data(self.studyid, df_recr, df_std_stats)

            # calling simulate_study study function only when planned subject and planned site counts are greater than zero
            if planned_subjects > 0 and planned_sites > 0:
                forecast_values = simulate_study(self.studyid, df_ds, last_update, planned_subjects, planned_sites)

        return forecast_values

    def insert_montly_forecast(self, lower_bound, center, upper_bound, target, min_day, last_update):
        """
        Populated the objects "rpt_pivotal_study_analytics" and "rpt_pivotal_study_analytics_datapoints" with forecast values
        Args:
        - lower_bound: Pessimistic Enrollment count
        - center: Realistic Enrollment count
        - upper_bound: Optimistic Enrollment count
        - target: Enrollment target for the study
        - min_day: First day to start the forecast series
        - last_update: When the database was refreshed last time
        """

        # compute the intersection between the target and lower_bound forecast values
        lower_cut_idx = len(lower_bound[lower_bound < target])

        # computing the length of forecast series data based on availability of target value and length of lower_bound values
        plot_end_len = min(len(lower_bound), lower_cut_idx+1)

        # slicing the upper_bound, center, lower_bound lists based on the length calculated in previous step
        upper_adj  = upper_bound[:plot_end_len]
        center_adj = center[:plot_end_len]
        lower_adj  = lower_bound[:plot_end_len]

        # generating date series from the first day of enrollment till the end of forecast series according to plot_end_len variable
        dr = pd.date_range(start=min_day, periods=plot_end_len)
        lforecast_date =  [d.date() for d in dr] 

        # Deleting the old forecast values from rpt_pivotal_study_analytics_datapoints 
        vSql= '''DELETE FROM "{schema}".rpt_pivotal_study_analytics_datapoints
                WHERE studyid = '{study}' AND 
                (forecast_type IN ('Monte_Carlo_Pessimistic_Forecast','Monte_Carlo_Realistic_Forecast','Monte_Carlo_Optimistic_Forecast') 
                OR forecast_type is NULL)''' .format(study = self.studyid, 
                                                    schema=self.schema)

        dml_sql_execution_function(self.conn, vSql, 'Deleting Monte_Carlo forecast values from rpt_pivotal_study_analytics_datapoints for study {study}'.format(study = self.studyid))

        #Fetching the header column values
        vSql = ''' SELECT comprehendid, therapeuticarea, program, studyid, studyname 
                    FROM "{schema}".rpt_pivotal_study_analytics
                    WHERE studyid = '{study}' '''.format(study = self.studyid, 
                                                        schema=self.schema)
        vHeader_cols = sql_fetchone_function(self.conn, vSql)
    

        i, j, k = 0, 0, 0
        vSql_insert_datapoints = ''
        vSql_update_analytics = ''
        for upper_val, center_val, lower_val, forecast_date in zip(upper_adj,center_adj,lower_adj, lforecast_date):

            # sql template that will be used to insert data in to PLO rpt_pivotal_study_analytics_datapoints
            vSql_insert = '''INSERT INTO "{schema}".rpt_pivotal_study_analytics_datapoints
                                (comprehendid, therapeuticarea, program, studyid, studyname, 
                                forecast_type, forecast_date, forecast_count, comprehend_update_time)
                                VALUES ('{comprehendid}'::text, 
                                NULLIF('{therapeuticarea}', 'None')::text, 
                                NULLIF('{program}', 'None')::text, 
                                '{studyid}'::text, '{studyname}'::text, 
                                '{forecast_type}'::text, ('{forecast_date}')::date, 
                                ({forecast_count})::int, now()::timestamp without time zone); '''.format (schema=self.schema, 
                                                                    studyid=self.studyid, 
                                                                    comprehendid=vHeader_cols[0], 
                                                                    therapeuticarea=vHeader_cols[1], 
                                                                    program=vHeader_cols[2], 
                                                                    studyname=vHeader_cols[4],
                                                                    forecast_type='{forecast_type}',
                                                                    forecast_count='{forecast_count}',
                                                                    forecast_date='{forecast_date}')

            # sql template that will be used to update fields in to PLO rpt_pivotal_study_analytics
            vSql_update = '''UPDATE "{schema}".rpt_pivotal_study_analytics
                            SET monte_carlo_{type}_date = '{forecast_date}'::date
                            WHERE studyid = '{studyid}'; '''.format(schema = self.schema, 
                                                                    forecast_date= '{forecast_date}', 
                                                                    studyid = self.studyid, 
                                                                    type = '{type}')
            
            # following blocks of if statements will do the following
            # - fetch forecast data that is present in last day of every month till the target is reached
            # - when target is reached, fetch the exact date of the month when the target is reached
            # - update the template sql from the previous step to include the appropriate forecast vales among the 3 (upper_val, center_val, lower_val)
            # - finally perform insert and update in to the plo's rpt_pivotal_study_analytics_datapoints, rpt_pivotal_study_analytics respectively
            last_day_of_month = calendar.monthrange(forecast_date.year, forecast_date.month)[1]
            if forecast_date.day == last_day_of_month and int(upper_val) < int(target):
                vSql_insert_datapoints += vSql_insert.format(forecast_count=int(upper_val), forecast_date=forecast_date, forecast_type = 'Monte_Carlo_Optimistic_Forecast')
            elif int(upper_val) >= int(target) and i == 0:
                vSql_insert_datapoints += vSql_insert.format(forecast_count=int(upper_val), forecast_date=forecast_date, forecast_type = 'Monte_Carlo_Optimistic_Forecast')
                vSql_update_analytics += vSql_update.format(forecast_date= forecast_date, type = 'optimistic')
                i+=1

            if forecast_date.day == last_day_of_month and int(center_val) < int(target):
                vSql_insert_datapoints += vSql_insert.format(forecast_count=int(center_val), forecast_date=forecast_date, forecast_type = 'Monte_Carlo_Realistic_Forecast')
            elif int(center_val) >= int(target) and j == 0:
                vSql_insert_datapoints += vSql_insert.format(forecast_count=int(center_val), forecast_date=forecast_date, forecast_type = 'Monte_Carlo_Realistic_Forecast')
                vSql_update_analytics += vSql_update.format(forecast_date= forecast_date, type = 'realistic')
                j+=1

            if forecast_date.day == last_day_of_month and int(lower_val) < int(target) and k == 0:
                vSql_insert_datapoints += vSql_insert.format(forecast_count=int(lower_val), forecast_date=forecast_date, forecast_type = 'Monte_Carlo_Pessimistic_Forecast')
            elif int(lower_val) >= int(target):
                vSql_insert_datapoints += vSql_insert.format(forecast_count=int(lower_val), forecast_date=forecast_date, forecast_type = 'Monte_Carlo_Pessimistic_Forecast')
                vSql_update_analytics += vSql_update.format(forecast_date= forecast_date, type = 'pessimistic')
                k+=1

        if len(vSql_insert_datapoints) > 1:
            dml_sql_execution_function(self.conn, vSql_insert_datapoints, 'Inserting Monte_Carlo forecast values in to rpt_pivotal_study_analytics_datapoints for study {study}'.format(study = self.studyid))
        if len(vSql_update_analytics) > 1:
            dml_sql_execution_function(self.conn, vSql_update_analytics, 'Updating Monte_Carlo forecast values in to rpt_pivotal_study_analytics for study {study}'.format(study = self.studyid))

        return True
        

# Main function responsible for post-processing updates
# class variable ReturnCodes.exitCode is populated with a standard exit code,
# both within main and other functions, depending on error status
# and returned by main
def main(pZKNodes, pZKPath, pCustomer, pSchema, logdir=None, debug=False, starttimelog=False):
    conn =  None
    stdy = None
    ReturnCodes.exitCode = None
    vSchema = pSchema
    zk_nodes = pZKNodes
    zk_path = pZKPath
    customer = pCustomer

    # initialize logger
    initialize_logger(pCustomer, pSchema, debug, logdir)

    # get start date/time for logging
    start = str(int(datetime.now().strftime("%s")) * 1000)
    starttime = time.time()
    

    # zookeeper client
    if zk_nodes is not None:
        zkClient = zk.start_client(zk_nodes)

    if starttimelog and (zk_nodes is not None and zk_path is not None and customer is not None):
        # enable logging to zookeeper
        logToZookeeper = True
    else:
        logToZookeeper = False

    # connect to database
    if zk_nodes is not None and zk_path is not None and customer is not None:
        # lookup credentials in ZK
        conn = db_connect.get_conn(zk_nodes, zk_path, customer, zkClient)
    else:
        # although docopt manages the parameters when called directly this check is here for when imported as a module
        logger.error('Missing database connection parameter(s)')
        return os.EX_SOFTWARE

    if conn is not None:
        cur = conn.cursor()
    else:
        logger.error('Could not connect to database')
        return os.EX_SOFTWARE

    try:

        # confirm schema exists
        if not schema_exists(conn, vSchema):
            raise Exception('Schema {schema} does not exist'.format(schema=vSchema))



    ##################################################
    ## PLO - rpt_pivotal_study_analytics            ##
    ## PLO - rpt_pivotal_study_analytics_datapoints ##
    ##################################################

        #### SQL to get the enrollment historical data series
        vSql = '''with  enrolled_patients as (
                                select studyid,
                                        date_trunc('Month', dsstdtc::date)::date as dt, count(1) monthly_ct
                                        from  "{schema}"."rpt_subject_disposition" where dsevent = 'ENROLLED'
                                        group by 1, 2
                              ) ,
                        planned_enrollment as ( select studyid, min(enddate) planned_startdate
                                                from "{schema}"."studyplannedrecruitment"
                                                where lower(type) = 'planned' and lower(category) = 'enrollment' and lower(frequency) = 'monthly'
                                                group by 1
                                            ),
                        enrollment_time_series as  ( select studyid,
                                                            generate_series(  min (dt)::date, (now()::date - Interval '1 month' ), '1 Month'::interval )::date  as end_date
                                                            from enrolled_patients
                                                            group by 1  ) ,
                        monthly_enrollment as ( select 'enrollment_metrics'::text as metric_type,
                                                        t.studyid::text,
                                                        t.end_date::date,
                                                        extract(epoch from(t.end_date::date)),
                                                        extract(epoch from (p.planned_startdate::date)),
                                                        (row_number() over (partition by t.studyid order by t.end_date))::int as month_num,
                                                        coalesce(e.monthly_ct, 0)::int as month_ct,
                                                        (sum(e.monthly_ct) over (partition by t.studyid order by t.end_date))::int as cum_sum
                                                        from enrollment_time_series t
                                                        join planned_enrollment p on (t.studyid = p.studyid)
                                                        left join enrolled_patients e on t.studyid = e.studyid and t.end_date =  e.dt
                                                        order by t.end_date)
                    select * from monthly_enrollment '''.format(schema = vSchema)
        lEnrollment_series = sql_fetchall_function(conn, vSql)

        #### SQL to get the site activation historical data series
        vSql = '''with site_activation as (  select studyid,
                                                date_trunc('Month', siteactivationdate::date)::date as dt,
                                                count(1) monthly_ct
                                                from  "{schema}"."site"
                                                group by 1, 2 ),
                        planned_first_site_activation as (
                                                            select studyid, expecteddate as planned_startdate from "{schema}"."studymilestone" where milestonetype = 'Planned' and upper(milestonelabel) = upper('FIRST SITE READY TO ENROLL')
                                                            ),
                        site_activation_time_series as ( select studyid,
                                                           generate_series(  min (dt)::date, (now()::date - Interval '1 month' ), '1 Month'::interval )::date  as end_date
                                                         from site_activation
                                                         group by 1),
                        monthly_site_activation as ( select 'site_activation_metrics'::text as metric_type,
                                                        t.studyid::text,
                                                        t.end_date::date,
                                                        extract(epoch from(t.end_date::date)),
                                                        extract(epoch from (p.planned_startdate::date)),
                                                        (row_number() over (partition by t.studyid order by t.end_date))::int as month_num,
                                                        coalesce(e.monthly_ct, 0)::int as month_ct,
                                                        sum(e.monthly_ct) over (partition by t.studyid order by t.end_date)::int as cum_sum
                                                        from site_activation_time_series t
                                                        join planned_first_site_activation p on t.studyid = p.studyid
                                                        left join site_activation e on t.studyid = e.studyid and t.end_date =  e.dt
                                                        order by t.end_date)
                    select * from monthly_site_activation '''.format(schema = vSchema)
        lSite_Activation_series= sql_fetchall_function(conn, vSql)

        studyid = []
        vSql = '''select studyid  from "{schema}".rpt_pivotal_study_analytics order by 1'''.format(schema = vSchema) # fetching the studyid's that are present in rpt_pivotal_study_analytics
        lstudy_id = sql_fetchall_function(conn, vSql)

        if lstudy_id is not None:
            for i, study in enumerate(lstudy_id):
                if study[0] not in studyid:
                    studyid.append(study[0])

        for j, stdy in enumerate(sorted(studyid)):

            # Starting code block to invoke forecastsim.py to generate forecast dates using MONTE CARLO Distribution
            # //////////////////////////////////////////////////////////////////////////////////////////////////////

            #creating instance for the class monte_carlo_forecast and invoking it 
            vMonteCarlo_fcast = monte_carlo_forecast(conn, stdy, vSchema)
            forecast_values =  vMonteCarlo_fcast.get_forecast_output()

            if forecast_values is None:
                # This is the case we didn't actually call invoke forecast.
                # So still checking None for any None value in required arguments. None in arguments will break forecast code
                logger.info("Skipping enrollment forecast calculation for study {studyid} since required datapoints are not available to perform Monte Carlo distribution.".format(studyid=stdy))
            else:
                # This is the adjustment done according to changes on returned result from forecast function.
                # We are expecting 5 type of result_code:
                # "OK"                                         : the forecast could be successfully generated
                # "TOO_FEW_SITES_STARTED_ENROLLING"            : not enough sites started enrolling
                # "NO_SITE_ACTIVELY_ENROLLING"                 : no site has enrolled recently (for the 6 last months)
                # "TOO_FEW_SITES_WITH_RELIABLE_ENROLLMENT_RATE": enough sites enrolled but for each those sites, not enough enrollments occurred to estimate a reliable enrollment rate.
                # "TOO_FEW_ENROLLMENTS"                        : less than 25% of the planned subjects were enrolled
                # We will act on "OK", otherwise log the result_code as is
                if forecast_values.result_code != "OK":
                    logger.info("Skipping enrollment forecast calculation for study {studyid}: {result_code}".format(studyid=stdy,result_code=forecast_values.result_code))
                else:
                    logger.info("Enrollment forecast calculation for study {studyid} successfully generated".format(studyid=stdy))
                    lower_bound, center, upper_bound = forecast_values.lower_bound, forecast_values.center, forecast_values.upper_bound
                    min_day, conf_level, nb_iter = forecast_values.min_day, forecast_values.conf_level, forecast_values.nb_iter
                    std_enr = forecast_values.std_enr
                    target = std_enr.subjects_target
                    last_update = std_enr.last_update

                    # Calling the method to insert forecast values generated using MONTE CARLO Distribution
                    vMonteCarlo_fcast.insert_montly_forecast(lower_bound, center, upper_bound, target, min_day, last_update)


            # End code block dealing with MONTE CARLO Distribution
            # ////////////////////////////////////////////////////


            # Adding extra check to ensure the study has enrollment data   
            vSql = '''select count(*)
                        from "{schema}"."studyplannedrecruitment"
                        where lower(type) = \'planned\' and lower(category) = \'enrollment\' and lower(frequency) = \'monthly\' and studyid = \'{study}\'
                        '''.format(schema=vSchema, study=stdy)
                        
            vStudyTest = sql_fetchone_function(conn,vSql)

            # enrollment series contains all studies so make sure this one exists in the series
            studyInSeries = False
            if lEnrollment_series is not None and len(lEnrollment_series) > 0:
                for i, series in enumerate(lEnrollment_series):
                    if series[1] == stdy:
                        studyInSeries = True
                        break
            if not studyInSeries:
                vStudyTest = (0,)

            # check to ensure study has site activation data
            vSql = '''select count(*)
                        from "{schema}"."site"
                        join "{schema}"."studymilestone" on (site.studyid = studymilestone.studyid and milestonetype = 'Planned' and upper(milestonelabel) = upper('FIRST SITE READY TO ENROLL'))
                        where site.siteactivationdate is not null
                        and site.studyid = '{study}' '''.format(schema=vSchema, study=stdy)
            vSiteTest = sql_fetchone_function(conn,vSql)

            # site activation series contains all studies so make sure this one exists in the series
            studyInSeries = False
            if lSite_Activation_series is not None and len(lSite_Activation_series) > 0:
                for i, series in enumerate(lSite_Activation_series):
                    if series[1] == stdy:
                        studyInSeries = True
                        break
            if not studyInSeries:
                vSiteTest = (0,)

            ############## Enrollement Forecast Update ##############
            vSql = ''' select target_enrollment_count from "{schema}".rpt_pivotal_study_analytics where studyid = \'{study}\''''.format(schema = vSchema, study = stdy)
            target_enrollemnt_count = sql_fetchone_function(conn, vSql)

            polynomial_total_months = ''
            polynomial_total_months_50_percent = ''
            logistic_total_months = ''
            polynomial_monthly_forecast = ''
            logistic_monthly_forecast = ''


            if target_enrollemnt_count is not None and target_enrollemnt_count[0] is not None and lEnrollment_series is not None and len(lEnrollment_series) > 0 and vStudyTest[0] > 0:
                
                ## list to fetch enrolment data for last 3 months from enrollment series and enter the forecast calcualtion 
                lenrollment_data_3months = [studyval[6] for x, studyval in enumerate(lEnrollment_series) if studyval[1]==stdy][-3:]

                ## Conditional block to check and enter the forecast calcualtion only if there is atleast one enrollment in last 3 months.
                ## Note : Eventhough forecase date calculating functions are using last four months data, here only last 3 month data is checked because forecast functions require having atleast a data variation after the first data point.
                ## This check is neccesary to make sure the forecast function has enough data to converge and able to calculate forecast date.
                if lenrollment_data_3months is not None and len(lenrollment_data_3months) > 0:
                    if  map(sum,[lenrollment_data_3months])[0] > 0:
                        polynomial_total_months = polynomial_forecast_calculator(lEnrollment_series, stdy, target_enrollemnt_count[0])
                        polynomial_total_months_50_percent = polynomial_forecast_calculator(lEnrollment_series, stdy, int(target_enrollemnt_count[0])/2)
                        polynomial_monthly_forecast = polynomial_monthly_forecast_calculator(conn, lEnrollment_series, stdy, target_enrollemnt_count[0])
                        #logistic_total_months = logistic_forecast_calculator(lEnrollment_series, stdy, target_enrollemnt_count[0])
                        # above statement is commented out because as part of tp20593 , the logistic regression dates are getting populated using logistic_monthly_forecast_calculator function
                        logistic_monthly_forecast = logistic_monthly_forecast_calculator(conn, lEnrollment_series, stdy, target_enrollemnt_count[0])
                        Min_date = str(mindate_function(lEnrollment_series, stdy))

            if polynomial_total_months is not None and polynomial_total_months != '' and Min_date is not None:
                vSql_lsi_projected_date = ''' Update "{schema}".rpt_pivotal_study_analytics
                                                set lsi_projected_date = (to_timestamp({total_months}))::date
                                                where studyid = \'{study}\''''.format (schema = vSchema, total_months = polynomial_total_months, study = stdy)

                dml_sql_update_function(conn, vSql_lsi_projected_date, 'rpt_pivotal_study_analytics.lsi_projected_date', stdy)

                vSql_lsi_achievement_status = ''' Update "{schema}".rpt_pivotal_study_analytics
                                                    set lsi_achievement_status = case when lsi_planned_date is null then null
                                                                                      when lsi_planned_date < lsi_projected_date then \'Behind Plan\' ELSE \'On Plan\' END
                                                    where studyid = \'{study}\''''.format (schema = vSchema, study = stdy)
                dml_sql_update_function(conn, vSql_lsi_achievement_status, 'rpt_pivotal_study_analytics.lsi_achievement_status', stdy)

            if polynomial_total_months_50_percent is not None and polynomial_total_months_50_percent != '' and Min_date is not None:
                vSql_lsi_projected_date_50percent_patients = ''' Update "{schema}".rpt_pivotal_study_analytics
                                                                    set lsi_projected_date_50percent_patients = (to_timestamp({total_months}))::date
                                                                    where studyid = \'{study}\''''.format (schema = vSchema, total_months = polynomial_total_months_50_percent, study = stdy)

                dml_sql_update_function(conn, vSql_lsi_projected_date_50percent_patients, 'rpt_pivotal_study_analytics.lsi_projected_date_50percent_patients', stdy)

            # section to insert polynomial forecast  values in to rpt_pivotal_study_analytics_datapoints  PLO
            if polynomial_monthly_forecast is not None and polynomial_monthly_forecast != '' and len(polynomial_monthly_forecast) > 0:
                # Deleting the existing values
                vSql= ''' delete from "{schema}".rpt_pivotal_study_analytics_datapoints
                            where studyid = \'{study}\' and (forecast_type = \'Polynomial\' or forecast_type is null)''' .format(schema = vSchema, study = stdy)
                dml_sql_execution_function(conn, vSql, 'Deleting from rpt_pivotal_study_analytics_datapoints where forecast_type is Polynomial or null for study {study}'.format(study = stdy))

                #Fetching the header column values
                vSql = ''' select comprehendid, therapeuticarea, program, studyid, studyname from "{schema}".rpt_pivotal_study_analytics
                            where studyid = \'{study}\''''.format(schema = vSchema, study = stdy)
                vHeader_cols = sql_fetchone_function(conn, vSql)

                month_data=[]
                target_data=[]

                for i, val in enumerate(polynomial_monthly_forecast[0]):
                    month_data.append(val)

                for i, val in enumerate(polynomial_monthly_forecast[1]):
                    target_data.append(val)

                month_target = [str(a)+','+str(b) for a, b in zip(month_data, target_data)]



                logger.info ('Inserting Polynomial monthly_projections for the study {study}'.format(study = stdy))
                #Looping through the forecast values
                for i, values in enumerate(month_target):

                    val = values.split(",")

                    vSql_monthly_projections = ''' Insert into "{schema}".rpt_pivotal_study_analytics_datapoints
                                                     (comprehendid, therapeuticarea, program, studyid, studyname, forecast_type, forecast_date, forecast_count)
                                                    VALUES ('{comprehendid}'::text, NULLIF('{therapeuticarea}', 'None')::text, NULLIF('{program}', 'None')::text, '{studyid}'::text, '{studyname}'::text, '{forecast_type}'::text, to_timestamp({forecast_date})::date, ({forecast_count})::int)
                                                    '''.format (schema = vSchema, comprehendid=vHeader_cols[0], therapeuticarea=vHeader_cols[1], program=vHeader_cols[2], studyid=vHeader_cols[3], studyname=vHeader_cols[4], forecast_type='Polynomial', forecast_date=val[0], forecast_count=val[1])

                    dml_sql_execution_function(conn, vSql_monthly_projections, None)

            # section to insert Logistic Regression values in to rpt_pivotal_study_analytics_datapoints  PLO
            if logistic_monthly_forecast is not None and logistic_monthly_forecast != '' and len(logistic_monthly_forecast) > 0:

                vSql= ''' delete from "{schema}".rpt_pivotal_study_analytics_datapoints
                            where studyid = \'{study}\' and (forecast_type = \'Logistic\' or forecast_type is null)''' .format(schema = vSchema, study = stdy)
                dml_sql_execution_function(conn, vSql, 'Deleting from rpt_pivotal_study_analytics_datapoints where forecast_type is Logistic or null for study {study}'.format(study = stdy))

                vSql = ''' select comprehendid, therapeuticarea, program, studyid, studyname from "{schema}".rpt_pivotal_study_analytics
                            where studyid = \'{study}\''''.format(schema = vSchema, study = stdy)
                vHeader_cols = sql_fetchone_function(conn, vSql)

                month_data=[]
                target_data=[]

                for i, val in enumerate(logistic_monthly_forecast[0]):
                    month_data.append(val)

                for i, val in enumerate(logistic_monthly_forecast[1]):
                    target_data.append(val)

                logistic_total_months = max(month_data)
                vSql_logistic_lsi_projected_date = ''' Update "{schema}".rpt_pivotal_study_analytics
                                                        set logistic_lsi_projected_date = ((to_timestamp({total_months}))::date)
                                                        where studyid = \'{study}\''''.format (schema = vSchema, total_months = logistic_total_months, study = stdy)

                dml_sql_update_function(conn, vSql_logistic_lsi_projected_date, 'rpt_pivotal_study_analytics.logistic_lsi_projected_date', stdy)

                vSql_dimstudy_logistic_lsi_projected_date = ''' Update "{schema}".dimstudy
                                                        set logistic_lsi_projected_date = ((to_timestamp({total_months}))::date)
                                                        where studyid = \'{study}\''''.format (schema = vSchema, total_months = logistic_total_months, study = stdy)

                dml_sql_update_function(conn, vSql_dimstudy_logistic_lsi_projected_date, 'dimstudy.logistic_lsi_projected_date', stdy)

                month_target = [str(a)+','+str(b) for a, b in zip(month_data, target_data)]
                logger.info ('Inserting Logistic monthly_projections for the study {study}'.format(study = stdy))
                for i, values in enumerate(month_target):

                    val = values.split(",")

                    vSql_monthly_projections = ''' Insert into "{schema}".rpt_pivotal_study_analytics_datapoints
                                                     (comprehendid, therapeuticarea, program, studyid, studyname, forecast_type, forecast_date, forecast_count)
                                                    VALUES ('{comprehendid}'::text, NULLIF('{therapeuticarea}', 'None')::text, NULLIF('{program}', 'None')::text, '{studyid}'::text, '{studyname}'::text, '{forecast_type}'::text, to_timestamp({forecast_date})::date  , ({forecast_count})::int)
                                                    '''.format (schema = vSchema, comprehendid=vHeader_cols[0], therapeuticarea=vHeader_cols[1], program=vHeader_cols[2], studyid=vHeader_cols[3], studyname=vHeader_cols[4], forecast_type='Logistic', forecast_date=val[0], forecast_count=val[1])

                    dml_sql_execution_function(conn, vSql_monthly_projections, None)

            ############## Site Activation Forecast Update ################
            vSql = ''' select target_site_activation_count from "{schema}".rpt_pivotal_study_analytics where studyid = \'{study}\''''.format(schema = vSchema, study = stdy)
            target_siteactivation_count = sql_fetchone_function(conn, vSql)

            polynomial_total_months = ''
            polynomial_monthly_forecast = ''

            if target_siteactivation_count is not None and target_siteactivation_count[0] is not None and lSite_Activation_series is not None and len(lSite_Activation_series) > 0 and vSiteTest[0] > 0:

                ## list to fetch site activation data for last 3 months from enrollment series and enter the forecast calcualtion 
                lsiteactiv_data_3months = [studyval[6] for x, studyval in enumerate(lSite_Activation_series) if studyval[1]==stdy][-3:]

                ## Conditional block to check and enter the forecast calcualtion only if there is atleast one site activated in last 3 months.
                ## Note : Eventhough forecase date calculating functions are using last four months data, here only last 3 month data is checked because forecast functions require having atleast one data variation after the first data point.
                ## This check is neccesary to make sure the forecast function has enough data to converge and able to calculate forecast date.
                if lsiteactiv_data_3months is not None and len(lsiteactiv_data_3months) > 0:
                    if map(sum,[lsiteactiv_data_3months])[0] > 0:
                        polynomial_total_months = polynomial_forecast_calculator(lSite_Activation_series, stdy, target_siteactivation_count[0])
                        polynomial_monthly_forecast = polynomial_monthly_forecast_calculator(conn, lSite_Activation_series, stdy, target_siteactivation_count[0])
                        Min_date = str(mindate_function(lSite_Activation_series, stdy))


            if polynomial_total_months is not None and  polynomial_total_months != '' and Min_date is not None:
                vSql_site_activation_projected_date = ''' Update "{schema}".rpt_pivotal_study_analytics
                                                            set site_activation_projected_date = (to_timestamp({total_months}))::date
                                                            where studyid = \'{study}\''''.format (schema = vSchema, total_months = polynomial_total_months, study = stdy)

                dml_sql_update_function(conn, vSql_site_activation_projected_date, 'rpt_pivotal_study_analytics.site_activation_projected_date', stdy)

                vSql_dimstudy_site_activation_projected_date = ''' Update "{schema}".dimstudy
                                                            set site_activation_projected_date = (to_timestamp({total_months}))::date
                                                            where studyid = \'{study}\''''.format (schema = vSchema, total_months = polynomial_total_months, study = stdy)

                dml_sql_update_function(conn, vSql_dimstudy_site_activation_projected_date, 'dimstudy.site_activation_projected_date', stdy)

                vSql_site_activation_days_behind = ''' Update "{schema}".rpt_pivotal_study_analytics
                                                        set site_activation_days_behind = (case when (site_activation_projected_date - site_activation_planned_date) > 0 then (site_activation_projected_date - site_activation_planned_date) else 0 end)
                                                        where studyid = \'{study}\''''.format (schema = vSchema, study = stdy)

                dml_sql_update_function(conn, vSql_site_activation_days_behind, 'rpt_pivotal_study_analytics.site_activation_days_behind', stdy)

            # section to insert site activation polynomial monthly forecast  values in to rpt_pivotal_study_analytics_datapoints  PLO
            if polynomial_monthly_forecast is not None and polynomial_monthly_forecast != '' and len(polynomial_monthly_forecast) > 0:
                # Deleting the existing values
                vSql= ''' delete from "{schema}".rpt_pivotal_study_analytics_datapoints
                            where studyid = \'{study}\' and (forecast_type = \'Polynomial_Siteactivation\' or forecast_type is null)''' .format(schema = vSchema, study = stdy)
                dml_sql_execution_function(conn, vSql, 'Deleting from rpt_pivotal_study_analytics_datapoints where forecast_type is Polynomial_Siteactivation or null for study {study}'.format(study = stdy))

                #Fetching the header column values
                vSql = ''' select comprehendid, therapeuticarea, program, studyid, studyname from "{schema}".rpt_pivotal_study_analytics
                            where studyid = \'{study}\''''.format(schema = vSchema, study = stdy)
                vHeader_cols = sql_fetchone_function(conn, vSql)

                month_data=[]
                target_data=[]

                for i, val in enumerate(polynomial_monthly_forecast[0]):
                    month_data.append(val)

                for i, val in enumerate(polynomial_monthly_forecast[1]):
                    target_data.append(val)

                month_target = [str(a)+','+str(b) for a, b in zip(month_data, target_data)]



                logger.info ('Inserting Site Activation Polynomial monthly_projections for the study {study}'.format(study = stdy))
                #Looping through the forecast values
                for i, values in enumerate(month_target):

                    val = values.split(",")

                    vSql_monthly_projections = ''' Insert into "{schema}".rpt_pivotal_study_analytics_datapoints
                                                     (comprehendid, therapeuticarea, program, studyid, studyname, forecast_type, forecast_date, forecast_count)
                                                    VALUES ('{comprehendid}'::text, NULLIF('{therapeuticarea}', 'None')::text, NULLIF('{program}', 'None')::text, '{studyid}'::text, '{studyname}'::text, '{forecast_type}'::text, to_timestamp({forecast_date})::date, ({forecast_count})::int)
                                                    '''.format (schema = vSchema, comprehendid=vHeader_cols[0], therapeuticarea=vHeader_cols[1], program=vHeader_cols[2], studyid=vHeader_cols[3], studyname=vHeader_cols[4], forecast_type='Polynomial_Siteactivation', forecast_date=val[0], forecast_count=val[1])

                    dml_sql_execution_function(conn, vSql_monthly_projections, None)

            ############## Current Milestone calcualtions ##############
            milesones_historic_data = ['50% SUBJECTS ENROLLED' ,'50% SITES ACTIVATED','LAST SUBJECT IN' , 'ALL SITES ACTIVATED']

            vSql = ''' select current_milestone from "{schema}".rpt_pivotal_study_analytics where studyid = \'{study}\''''.format(schema = vSchema, study = stdy)
            current_milestone = sql_fetchone_function(conn, vSql)

            if current_milestone is not None and current_milestone[0] is not None and current_milestone[0] not in milesones_historic_data:

                ## to fetch the previous milestone
                vSql = '''select milestonelabel from "{schema}".studymilestone
                            where milestonetype = 'Planned' and  studyid = \'{study}\'
                            and milestoneseq < (select milestoneseq from  "{schema}".studymilestone
                                                where milestonelabel = \'{current_milestone}\' and milestonetype = 'Planned' and studyid = \'{study}\')
                                                order by milestoneseq desc limit 1'''.format(schema = vSchema, study = stdy, current_milestone = current_milestone[0])

                previous_milestone = sql_fetchone_function(conn, vSql)

                prev_diff_days = ''
                if previous_milestone is not None and previous_milestone !='':
                ##  to fetch the previous milestone planned and actual days
                    vSql = '''select planned.studyid,  actual.expecteddate , planned.expecteddate, coalesce((actual.expecteddate - planned.expecteddate),0)::int as prev_days_diff from
                                (select studyid, expecteddate from
                                        "{schema}".studymilestone
                                        where studyid = \'{study}\' and milestonetype = 'Planned' and milestonelabel = \'{previous_milestone}\') as planned
                                join (select studyid, expecteddate from
                                        "{schema}".studymilestone
                                        where studyid = \'{study}\' and milestonetype = 'Actual' and milestonelabel = \'{previous_milestone}\') as actual on planned.studyid = actual.studyid'''.format(schema = vSchema, study = stdy, previous_milestone = previous_milestone[0])

                    prev_diff_days = sql_fetchone_function(conn, vSql)

                if prev_diff_days is not None and prev_diff_days != '':
                    ## Updating the current milestone based on the previous milestone status
                    vSql_current_milestone_projected_date= ''' Update "{schema}".rpt_pivotal_study_analytics
                                                                set current_milestone_projected_date = (current_milestone_planned_date)::date +  {prev_diff_days}::int
                                                                where studyid = \'{study}\''''.format (schema = vSchema, study = stdy, prev_diff_days = prev_diff_days[3])

                    dml_sql_update_function(conn, vSql_current_milestone_projected_date, 'rpt_pivotal_study_analytics.current_milestone_projected_date', stdy)

                    ## In case `current_milestone_projected_date` is null (Because of `target_siteactivation_count` is null or `target_enrollment_count` is null) then we don't assign `milestone_achievement_status`
                    vSql_milestone_achievement_status= ''' Update "{schema}".rpt_pivotal_study_analytics
                                                            set milestone_achievement_status = (case when (current_milestone_projected_date is null or current_milestone_planned_date is null) then null
                                                                                                     when (current_milestone_planned_date)::date < (current_milestone_projected_date)::date
                                                                                                     then 'Behind Plan' else 'On Plan' END )
                                                            where studyid = \'{study}\''''.format (schema = vSchema, study = stdy)

                    dml_sql_update_function(conn, vSql_milestone_achievement_status, 'rpt_pivotal_study_analytics.milestone_achievement_status', stdy)

            ## this section is for milestones for the which the forecast date can be calculated using the historic dataset
            elif current_milestone is not None and current_milestone[0] in milesones_historic_data :

                polynomial_total_months = ''
                vSql_milestone_achievement_status = ''

                if current_milestone[0] == '50% SUBJECTS ENROLLED' and lEnrollment_series is not None and len(lEnrollment_series) > 0 :
                    logistic_50_percent_total_months = ''
                    logistic_50_percent_monthly_forecast = ''
                    if target_enrollemnt_count[0] != '' and target_enrollemnt_count[0] is not None:
                        logistic_50_percent_monthly_forecast = logistic_monthly_forecast_calculator(conn, lEnrollment_series, stdy, target_enrollemnt_count[0]/2);
                        if logistic_50_percent_monthly_forecast is not None and logistic_50_percent_monthly_forecast != '' and len(logistic_50_percent_monthly_forecast) > 0:
                            logistic_50_percent_total_months = max(logistic_50_percent_monthly_forecast[0])
                        Min_date = str(mindate_function(lEnrollment_series, stdy))

                    if logistic_50_percent_total_months is not None and logistic_50_percent_total_months != '' and Min_date is not None:
                        vSql_current_milestone_projected_date= ''' Update "{schema}".rpt_pivotal_study_analytics
                                                                    set current_milestone_projected_date = (to_timestamp({total_months}))::date
                                                                    where studyid = \'{study}\''''.format (schema = vSchema, study = stdy, total_months = logistic_50_percent_total_months)

                        dml_sql_update_function(conn, vSql_current_milestone_projected_date, 'rpt_pivotal_study_analytics.current_milestone_projected_date', stdy)

                    ## In case `current_milestone_projected_date` is null (Because of `target_siteactivation_count` is null or `target_enrollment_count` is null) then we don't assign `milestone_achievement_status`
                    vSql_milestone_achievement_status= ''' Update "{schema}".rpt_pivotal_study_analytics
                                                            set milestone_achievement_status = (case when (current_milestone_projected_date is null or current_milestone_planned_date is null) then null
                                                                                                     when (current_milestone_planned_date)::date < (current_milestone_projected_date)::date
                                                                                                     then 'Behind Plan' else 'On Plan' END )
                                                                where studyid = \'{study}\''''.format (schema = vSchema, study = stdy)

                elif current_milestone[0] == '50% SITES ACTIVATED' and lSite_Activation_series is not None and len(lSite_Activation_series) > 0 and vSiteTest[0] > 0:

                    if target_siteactivation_count[0] != '' and target_siteactivation_count[0] is not None:

                        polynomial_target_siteactivation_count_50percent = int(target_siteactivation_count[0]/2)
                        polynomial_total_months = polynomial_forecast_calculator(lSite_Activation_series, stdy, polynomial_target_siteactivation_count_50percent)
                        Min_date = str(mindate_function(lSite_Activation_series, stdy))

                    if polynomial_total_months is not None and polynomial_total_months != '' and Min_date is not None:
                        vSql_current_milestone_projected_date= ''' Update "{schema}".rpt_pivotal_study_analytics
                                                                    set current_milestone_projected_date = (to_timestamp({total_months}))::date
                                                                    where studyid = \'{study}\''''.format (schema = vSchema, study = stdy, mindate = Min_date, total_months = polynomial_total_months)

                        dml_sql_update_function(conn, vSql_current_milestone_projected_date, 'rpt_pivotal_study_analytics.current_milestone_projected_date', stdy)

                    ## In case `current_milestone_projected_date` is null (Because of `target_siteactivation_count` is null or `target_enrollment_count` is null) then we don't assign `milestone_achievement_status`
                    vSql_milestone_achievement_status= ''' Update "{schema}".rpt_pivotal_study_analytics
                                                            set milestone_achievement_status = (case when (current_milestone_projected_date is null or current_milestone_planned_date is null) then null
                                                                                                     when (current_milestone_planned_date)::date < (current_milestone_projected_date)::date
                                                                                                     then 'Behind Plan' else 'On Plan' END )
                                                                where studyid = \'{study}\''''.format (schema = vSchema, study = stdy)

                elif current_milestone[0] == 'LAST SUBJECT IN':

                    vSql_current_milestone_projected_date= ''' Update "{schema}".rpt_pivotal_study_analytics
                                                                set current_milestone_projected_date = logistic_lsi_projected_date
                                                                where studyid = \'{study}\''''.format (schema = vSchema, study = stdy)

                    dml_sql_update_function(conn, vSql_current_milestone_projected_date, 'rpt_pivotal_study_analytics.current_milestone_projected_date', stdy)

                    ## In case `current_milestone_projected_date` is null (Because of `target_siteactivation_count` is null or `target_enrollment_count` is null) then we don't assign `milestone_achievement_status`
                    vSql_milestone_achievement_status= ''' Update "{schema}".rpt_pivotal_study_analytics
                                                            set milestone_achievement_status = (case when (current_milestone_projected_date is null or current_milestone_planned_date is null) then null
                                                                                                     when (current_milestone_planned_date)::date < (current_milestone_projected_date)::date
                                                                                                     then 'Behind Plan' else 'On Plan' END )
                                                            where studyid = \'{study}\''''.format (schema = vSchema, study = stdy)

                elif current_milestone[0] == 'ALL SITES ACTIVATED':

                    vSql_current_milestone_projected_date= ''' Update "{schema}".rpt_pivotal_study_analytics
                                                                set current_milestone_projected_date = site_activation_projected_date
                                                                where studyid = \'{study}\''''.format (schema = vSchema, study = stdy)

                    dml_sql_update_function(conn, vSql_current_milestone_projected_date, 'rpt_pivotal_study_analytics.current_milestone_projected_date', stdy)

                    ## In case `current_milestone_projected_date` is null (Because of `target_siteactivation_count` is null or `target_enrollment_count` is null) then we don't assign `milestone_achievement_status`
                    vSql_milestone_achievement_status= ''' Update "{schema}".rpt_pivotal_study_analytics
                                                            set milestone_achievement_status = (case when (current_milestone_projected_date is null or current_milestone_planned_date is null) then null
                                                                                                     when (current_milestone_planned_date)::date < (current_milestone_projected_date)::date
                                                                                                     then 'Behind Plan' else 'On Plan' END )
                                                            where studyid = \'{study}\''''.format (schema = vSchema, study = stdy)

                if vSql_milestone_achievement_status != '':

                    dml_sql_update_function(conn, vSql_milestone_achievement_status, 'rpt_pivotal_study_analytics.milestone_achievement_status', stdy)

            ############## Study Risk Level ##############
            vSql_study_risk_level= ''' with study_risk_cals as ( select studyid, case when milestone_achievement_status is not null and lsi_achievement_status is not null and site_activation_days_behind is not null then
                                                                    (case when milestone_achievement_status = 'On Plan' then 0 else 1 END) +
                                                                    (case when lsi_achievement_status = 'On Plan' then 0 else 1 END) +
                                                                    (case when site_activation_days_behind <= 0 then 0 else 1 END) else null end as study_risk_val
                                                                from "{schema}".rpt_pivotal_study_analytics where studyid = \'{study}\' )
                                        update  "{schema}".rpt_pivotal_study_analytics
                                        set study_risk_level = (case when study_risk_cals.study_risk_val <= 1 then 'Low'
                                                                    when study_risk_cals.study_risk_val = 2 then 'Medium'
                                                                    when study_risk_cals.study_risk_val > 2 then 'High' else NULL END)
                                        from study_risk_cals where rpt_pivotal_study_analytics.studyid = study_risk_cals.studyid'''.format (schema = vSchema, study = stdy)
            dml_sql_update_function(conn, vSql_study_risk_level, 'rpt_pivotal_study_analytics.study_risk_level', stdy)

            ############## Portfolio Summary Analytics ##############
            vSql_portfolio_summary_analytics= ''' update  "{schema}".rpt_portfolio_summary_analytics
                                                  set current_milestone_projected_date = (select current_milestone_projected_date from "{schema}".rpt_pivotal_study_analytics where studyid = \'{study}\')
                                                  where studyid = \'{study}\' '''.format (schema = vSchema, study = stdy)
            dml_sql_update_function(conn, vSql_portfolio_summary_analytics, 'rpt_portfolio_summary_analytics.current_milestone_projected_date', stdy)

    ##################################################
    ## PLO - rpt_portfolio_oversight                ##
    ##################################################

        ## updating milestone_achievement_status
        vSql_portfolio_milestone_achievement_status = ''' Update "{schema}".rpt_portfolio_oversight a
                                                            set milestone_achievement_status = (select milestone_achievement_status from "{schema}".rpt_pivotal_study_analytics b
                                                            where a.studyid = b.studyid)'''.format (schema = vSchema)
        dml_sql_update_function(conn, vSql_portfolio_milestone_achievement_status, 'rpt_portfolio_oversight.milestone_achievement_status', stdy)

        ## updating enrollment_status
        vSql_portfolio_enrollment_status = ''' Update "{schema}".rpt_portfolio_oversight a
                                                set enrollment_status = (select enrollment_status from "{schema}".rpt_enrollment_analytics b
                                                where a.studyid = b.studyid)'''.format (schema = vSchema)
        dml_sql_update_function(conn, vSql_portfolio_enrollment_status, 'rpt_portfolio_oversight.enrollment_status', stdy)

        ## updating budget_status
        vSql_portfolio_budget_status = '''with budget_details as (
                                                            select studyid,
                                                            sum(planned_expenditure)  as planned_budget,
                                                            sum(actual_expenditure) as actual_buget
                                                            from "{schema}".rpt_expenditure_analytics where budget_month <= now() group by studyid)
                                            Update "{schema}".rpt_portfolio_oversight
                                            set budget_status = (case when actual_buget = planned_budget then 'On Budget'
                                                                       when actual_buget < planned_budget then 'Under Budget'
                                                                       when actual_buget > planned_budget then 'Overrun Budget' END)
                                            from budget_details where rpt_portfolio_oversight.studyid = budget_details.studyid'''.format (schema = vSchema)
        dml_sql_update_function(conn, vSql_portfolio_budget_status, 'rpt_portfolio_oversight.budget_status', stdy)

        ## updating total_overbudget_amount and total_overbudget_unit
        vSql_portfolio_over_budget_amount = '''with over_budget_details as (
                                                                        select studyid, expenditure_units,
                                                                        max(total_overbudget)  as total_overbudget_amount
                                                                        from "{schema}".rpt_expenditure_analytics group by studyid, expenditure_units)
                                                Update "{schema}".rpt_portfolio_oversight
                                                set total_overbudget_amount = over_budget_details.total_overbudget_amount,
                                                    total_overbudget_unit = over_budget_details.expenditure_units
                                                from over_budget_details where rpt_portfolio_oversight.studyid = over_budget_details.studyid'''.format (schema = vSchema)
        dml_sql_update_function(conn, vSql_portfolio_over_budget_amount, 'rpt_portfolio_oversight.total_overbudget_amount', stdy)

        ## updating study_risk_level
        vSql_portfolio_study_risk_level = '''Update "{schema}".rpt_portfolio_oversight
                                                set study_risk_level = (select study_risk_level from "{schema}".rpt_pivotal_study_analytics
                                                where rpt_portfolio_oversight.studyid = rpt_pivotal_study_analytics.studyid)'''.format (schema = vSchema)
        dml_sql_update_function(conn, vSql_portfolio_study_risk_level, 'rpt_portfolio_oversight.study_risk_level', stdy)

        ## pivotal_study_analytics_datapoints_cleanup
        vSql_pivotal_study_analytics_datapoints_cleanup = '''delete from "{schema}".rpt_pivotal_study_analytics_datapoints where forecast_type is null or forecast_date is null'''.format (schema = vSchema)
        dml_sql_execution_function(conn, vSql_pivotal_study_analytics_datapoints_cleanup, 'Deleting from rpt_pivotal_study_analytics_datapoints where forecast_type is null or forecast_date is null for study {study}'.format(study=stdy) )

        ## pivotal_study_analytics_datapoints_cleanup
        vSql_pivotal_study_analytics_datapoints_cleanup = '''delete from "{schema}".rpt_pivotal_study_analytics_datapoints where forecast_type is null or forecast_date is null'''.format (schema = vSchema)
        dml_sql_execution_function (conn, vSql_pivotal_study_analytics_datapoints_cleanup, 'pivotal_study_analytics_datapoints_cleanup')


        if ReturnCodes.exitCode is None:
            # process successful execution
            ReturnCodes.exitCode = os.EX_OK
            
            if logToZookeeper:
                # log successful start time to ZK
                if not log_successful_run(zkClient, zk_nodes, zk_path, customer, start):
                    ReturnCodes.exitCode =  os.EX_SOFTWARE
        
        endtime = time.time()
        runtime = endtime - starttime
        logger.info('Total execution time (seconds) = {runtime}'.format(runtime=runtime))


    except Exception, e:
        logger.exception(e)
        if ReturnCodes.exitCode is None:
            # populate ReturnCodes.exitCode with error code if not previously populated
            ReturnCodes.exitCode = os.EX_SOFTWARE

    finally:
        if zkClient is not None:
            zk.stop_client(zkClient)
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

        return ReturnCodes.exitCode

if __name__ == '__main__':
    args = docopt(__doc__, version='PLO_post_proccessing_script 1.0')
    exitCode = main( args['--zk_nodes'], args['--zk_path'], args['--customer'], args['--schema'] , args['--logdir'], args['--debug'], args['--starttimelog'])
    print 'Exit Code: ' + str(exitCode)
    sys.exit(exitCode) # exit with standard return code when called directly

