#!/usr/bin/env python

"""
Usage:
    data_anomaly_handler.py --zk_nodes=<nodes> --zk_path=<path> --customer=<customer> --schema=<schema-name> --ccdm_path=<path>  
    data_anomaly_handler.py (-h | --help)
    data_anomaly_handler.py --version
Options:
  -h --help                 Show this screen
  --version                 Show version
  --zk_nodes=<nodes>        comma separated string of zookeeper hosts
  --zk_path=<path>          zookeeper path where customer entry is found (ex. /com/comprehend/panacea)
  --customer=<customer>     zookeepr entry containing clinicalDBparameters (ex. client name)
  --schema=<schema>         database schema name
  --ccdm_path=<path>        local path for root of CCDM git repository
"""

# Created By: Adam Kaus
# Created On: 19-Dec-2016
# Revision History: 19-Dec-2016 ACK Initial version
# Notes:
#       - Data anomaly handler driver script that executes all data checks found within the "data_checks" subfolder
#       - Data anomalies are logged in individual "$err" tables for each affected table and a "comprehenderrorsummary" table
#           - The $err tables are droped/recreated with each run but comprehenderrorsummary contains a running log of data anomalies in the specific instance
#       - In order to be picked up by this driver script: 
#           - each check filename in "data_checks" must be prefixed with "check_"
#           - include a "main()" function with the check logic
#           - the inputs for main() are: 
#               - conn: psycopg2 db connection object
#               - schema: schema where data exists 
#               - temp_schema: schema where constraints exist 
#           - the return value for main() must be a list, where each element of the list is a dictionary with the following attributes for a single data anomaly record:
#               - table (string): table name where anomaly record exists
#               - key_columns (list): key columns for anomaly record
#               - key_values (list): key values for anomaly record - must be in same order as <key_columns>
#               - anomaly_columns (list): columns where anomaly identified
#               - anomaly_values (list): values in anomaly columns  -  must be in same order as <anomaly_columns>
#               - keep_one_record (boolean) - If set to True, in the case of duplicate records then one record will be left remaining and the rest deleted. Should be set to False for non-duplicate checks
#               - message (string) -  Error message containg details about the specific anomaly that will be used in the logs
#

import sys, os
from os import path
sys.path.append( path.dirname( path.dirname( path.dirname( path.dirname( path.dirname( path.dirname( path.abspath(__file__) ) ) ) ) ) ) ) # /ccdm/resources is 5 directories above
from utils import db_connect, schema_utils # modules in /ccdm/resources/utils
from docopt import docopt
import data_checks
from data_checks import *
import inspect
import datetime
import logging

# checks if <schema>.<table> exists and returns boolean
def table_exists(conn, schema, table):
    lCur = conn.cursor()
    sql = '''select 1 from information_schema.tables where table_name = '{table}' and table_schema = '{schema}' '''.format(table=table, schema=schema)
    lCur.execute(sql)
    if lCur.rowcount == 0:
        return False
    else:
        return True
    lCur.close()

# this logic is disabled for initial release as recursive record deletions are currently out of scope
# function returns list of dependent objects for <schema>.<table>
#def get_dependent_tables(conn, schema, table):
#    lCur = conn.cursor()
#    lSQL = ''' select distinct tc.table_name as dependent_table
#                from information_schema.constraint_column_usage ccu
#                join information_schema.table_constraints tc on (ccu.table_schema = tc.table_schema and ccu.constraint_name = tc.constraint_name)
#                where ccu.table_schema = '{schema}' 
#                and ccu.table_name = '{table}'
#                and upper(tc.constraint_type) = 'FOREIGN KEY'
#                and tc.table_name not like 'rpt%' '''.format(schema=schema, table=table)
#    lCur.execute(lSQL)
#    lResult = lCur.fetchall()
#    lCur.close()
#    return lResult

# deletes records in schema.table using key columns in <keycols> and the <where> clause
# if keep_one = true then leaves one record in the case of duplicates and deletes the rest
# recursive logic to also delete dependent records currently disabled
def delete_records(conn, schema, temp_schema, table, keycols, where, keep_one):
    lCur = conn.cursor()
    asub = ''
    bsub = ''

    for key in keycols:
        if len(asub) == 0:
            asub += 'a.{col}'.format(col=key)
            bsub += 'b.{col}'.format(col=key)
        else:
            asub += ', a.{col}'.format(col=key)
            bsub += ', b.{col}'.format(col=key)
    if keep_one:
        sql = '''delete from {schema}.{table} a
                    where a.ctid <> (select min(b.ctid)
                                        from {schema}.{table} b
                                        where  ({asub}) = ({bsub}) )
                    and {where};'''.format(schema=schema, table=table, asub=asub, bsub=bsub, where=where)
    else:
        sql = '''delete from {schema}.{table}
                    where {where};'''.format(schema=schema, table=table, where=where)
    lCur.execute(sql)

# recursive logic disabled for initial release
#    dependent_tables = get_dependent_tables(conn, temp_schema, table)
#    if dependent_tables is not None:
#        for rec in dependent_tables:
#            dtable = rec[0] 
#            delete_records(conn, schema, temp_schema, dtable, keycols, where)

    lCur.close()
    return None

# main driver function
def main(zk_nodes, zk_path, customer, schema, ccdm_path):
    conn = None
    cur = None
    temp_schema = None
    jobnum = None
    index_file  = '{path}/resources/mappings/cqs/common/ccdm_constraints/create_ccdm_3_indexes.sql'.format(path=ccdm_path)
    fk_file     = '{path}/resources/mappings/cqs/common/ccdm_constraints/create_ccdm_4_foreign_keys.sql'.format(path=ccdm_path)

    try:
        # initialize logger
        logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)
        
        conn = db_connect.get_conn(zk_nodes, zk_path, customer)
        if conn is not None:
            cur = conn.cursor()
        else:
            raise Exception('Could not connect to database')

        # drop all existing $err tables
        sql = '''select distinct table_name from information_schema.tables where table_name like '%$err' and table_schema='{schema}'  '''.format(schema=schema)
        cur.execute(sql)
        if cur.rowcount > 0:
            table_list = cur.fetchall()
            for err_table in table_list:
                sql = 'drop table {schema}.{table} cascade'.format(schema=schema, table=err_table[0])
                cur.execute(sql)

        # create the empty temp_schema and apply indexes and constraints
        # this schema will be used as a structural map for identification of object properties, constraints, etc. wihin this script and tests as well
        # this is required because it is assumed that at the time this handler is run there are no constraints applied within <schema>
        temp_schema = schema_utils.clone_schema_tables(conn, schema)
        logger.info('Creating temporary schema "{schema}"'.format(schema=temp_schema))
        sql = 'set search_path = {schema};'.format(schema=temp_schema)
        f = open(index_file)
        sql += f.read()
        f.close
        f = open(fk_file)
        sql += f.read()
        f.close()
        cur.execute(sql)

        # loop through all modules in the "data_checks" subfolder and execute the main() function
        for obj_name, obj in inspect.getmembers(data_checks):
            if inspect.ismodule(obj) and obj_name.lower().startswith('check_'):
                logger.info('Executing check: {check}'.format(check=obj_name))
                anomaly_list = obj.main(conn, schema, temp_schema)

                # if results returned by check then process them
                if len(anomaly_list) > 0:
                    logger.warning('Data anomalies found by {check}'.format(check=obj_name))

                    # create comprehenderrorsummary table:
                    if not table_exists(conn, schema, 'comprehenderrorsummary'):
                        logger.warning('Creating table "comprehenderrorsummary"')
                        sql = '''create table {schema}.comprehenderrorsummary (
                                    jobnum integer,
                                    errdate timestamp,
                                    tablename text,
                                    keyvals text,
                                    errmessage text) '''.format(schema=schema)
                        cur.execute(sql)

                    # get next comprehenderrorsummary.jobnum
                    # if already assigned in a previous iteration then will be reused to tie all log records together for this run
                    if jobnum is None:
                        sql = '''select max(jobnum) + 1 from  {schema}.comprehenderrorsummary'''.format(schema=schema)
                        cur.execute(sql)
                        jobnum = cur.fetchone()[0]
                        if jobnum is None:
                            jobnum = 1

                    # loop through each anomaly entry and collect details from the dictionary
                    for entry in anomaly_list:
                        table = entry['table']
                        key_cols = entry['key_columns']
                        key_vals = entry['key_values']
                        anomaly_cols = entry['anomaly_columns']
                        anomaly_vals = entry['anomaly_values']
                        keep_one = entry['keep_one_record']
                        message = entry['message']
                        
                        # see if $err table exists for this table
                        if not table_exists(conn, schema, '{table}$err'.format(schema=schema, table=table)):
                            # if not create $err table
                            logger.warning('Creating table "{table}$err"'.format(table=table))
                            sql = '''create table {schema}.{table}$err as select *, null::timestamp as errdate, null::text as errtext from {schema}.{table} where 1=2'''.format(schema=schema, table=table)
                            cur.execute(sql)

                        # build a where clause and a key value string (for the log)
                        where = ''
                        key_val_string = ''
                        for i, val in enumerate(key_vals): 
                            newval = str(val).replace("'", "''")
                            if len(where) == 0:
                                where += '''{key_col}='{key_val}' '''.format(key_col=key_cols[i], key_val=newval)
                                key_val_string += '''{key_col}={key_val} '''.format(key_col=key_cols[i], key_val=newval)
                            else:
                                where += ''' AND {key_col}='{key_val}' '''.format(key_col=key_cols[i], key_val=newval)
                                key_val_string += ''', {key_col}={key_val} '''.format(key_col=key_cols[i], key_val=newval)
                        where = where.replace('=\'None\'', ' is null')

                        # see if record already exists in $err table for this primary key
                        sql = '''select 1 from {schema}.{table}$err where {where} '''.format(schema=schema, table=table, where=where)
                        cur.execute(sql)
                        if cur.rowcount == 0:

                            # insert into $err table
                            currdate = datetime.datetime.now()
                            sql = '''insert into {schema}.{table}$err select *, '{date}'::timestamp, '{message}'::text from {schema}.{table} where {where} limit 1'''.format(schema=schema, table=table, date=currdate, message=message.replace("'", "''"), where=where)
                            cur.execute(sql)

                            # insert into comprehenderrorsummary
                            sql  = '''insert into {schema}.comprehenderrorsummary(jobnum, errdate, tablename, keyvals, errmessage)
                                        values( {jobnum}::integer, '{date}'::timestamp, '{table}'::text, '{key_vals}'::text, '{message}'::text  )'''.format(schema=schema, jobnum=jobnum, date=currdate, table=table, key_vals=key_val_string, message=message.replace("'", "''"))
                            cur.execute(sql)

                        # delete record
                        delete_records(conn, schema, temp_schema, table, key_cols, where, keep_one)

                    logger.warning('Data anomalies logged and records deleted')

                else:
                    logger.info('No data anomalies found by {check}'.format(check=obj_name))

        return 0

    except Exception, e:
        logger.exception('Error data_check_driver.py: {err}'.format(err=e))
        if conn is not None:
            conn.commit()
        return 1
        exit(1)

    finally:
        if conn is not None:
            conn.commit()

        if temp_schema is not None and cur is not None:
            # drop the temporary schema as part of cleanup 
            logger.info('Dropping temporary schema "{schema}"'.format(schema=temp_schema))
            sql = 'drop schema "{schema}" cascade'.format(schema=temp_schema)
            cur.execute(sql)
            conn.commit()

        if cur is not None:
            cur.close()

        if conn is not None:
            conn.close()

if __name__ == '__main__':
    args = docopt(__doc__, version='Data Anomaly Handler 1.0')
    main( args['--zk_nodes'], args['--zk_path'], args['--customer'], args['--schema'], args['--ccdm_path'] )
