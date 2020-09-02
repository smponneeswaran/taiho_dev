#!/usr/bin/env python

"""
Usage:
    scorecard_post_processing.py --schema=<schema> (--zk_nodes=<nodes> --zk_path=<path> --customer=<customer> | --hostname=<hostname> --port=<db_port> --database=<database>  --username=<username>) [--password=<password>] [--filter=<where_clause>] [--days_limit=<num_days>] [--logdir=<dir>] [--workmem=<intMB>] [--backfill] [--debug] [--retry] [--disable_checks] [(--copyschema=<schema> --copystudy=<studyid>)]
    scorecard_post_processing.py (-h | --help)
    scorecard_post_processing.py --version
Options:
    -h or help              Show this screen
    version                 Show version
    schema=<schema>         Database schema name
    zk_nodes=<nodes>        Comma separated string of zookeeper hosts (used for retrieval of database credentials)
    zk_path=<path>          Zookeeper path where customer entry is found (ex. /com/comprehend/panacea)
    customer=<customer>     Zookeepr entry containing clinicalDBparameters (ex. client name)
    hostname=<hostname>     Database host (used to bypass zookeeper)
    port=<db_port>          Database port (used to bypass zookeeper)
    database=<database>     Database name (used to bypass zookeeper)
    username=<username>     Database username (used to bypass zookeeper)
    password=<password>     Database password (used to bypass zookeeper) - when bypassing zookeeper, if password omitted user will be prompted
    filter=<where_clause>   If included use to filter data load. Must be in standard where clause predicate format (ex. studyid='S1000' and siteid='001')
                                Currently supports the following columns: studyid, croid, siteid, kpiid, startdate, enddate
    days_limit=<num_days>   If included do not process KPIs for dates that are older than current_date - num_days
                                Generate a warning and continue to process but return an error code
    logdir=<dir>            If included generate log file in directory path
    workmem=<intMB>         Set postgresql parameter work_mem at a session level (used for larger datasets)
    backfill                Enable backfill mode (otherwise default to incremental mode)
    debug                   Enable debug mode (temporary tables will not be dropped on cleanup)
    retry                   Enable retry functionality (for use with adapters)
    disable_checks          Disable purely informational soft-validation checks (i.e. checks that do not apply protection filters)
    copyschema              If included copy all historical scorecard data from <copy_schema> into <schema> before merging - used for study-specific build
    copystudy               The study ID to use when copying data - used for study-specific builds
"""

"""
Known Limitations:
------------------
Unique index/constraint names: This module attempts to automatically create USDM indexes/constraints. If indexes/constraints with the same name already exist in the database (for example if the PLOs were previously generated and then backed up) 
an exception caused by non-unique names will be generated.
Workaround: Manually remove the conflicting USDM indexes/constraints 

Dropped columns: This module automatically checks for the presence of expected columns, indexes, constraints, etc. If something is missing or does not match the expected properties it will be created or updated. 
Therefore, it is possible that if a PLO column were to be inadvertently dropped outside of this module the next time it runs it would automatically add the dropped column(s) leaving the column(s) null for a majority of the records in the table with no notification.
Workaround: Run the full backfill to repopulate the data in all columns for all studies, sites, and rates.

Days Limit parameter: If the --days_limit parameter is used in combination with the --backfill parameter then all dates in the range (i.e. current date minus days_limit) will be recalculated even if data had been previously calculated in that range. 
However, when the --days_limit parameter is used in "incremental" mode then it serves more as a protection against inadvertently calculating a large volume of data. Rates will be calculated starting at the last calculation date but not to exceed the days_limit. 
For example, if the rates have not been calculated in 30 days but --days_limit=10 then the rates will only be calculated for the last 10 days and a warning will be generated regarding the gap in calculations.
Workaround: Monitor the warnings logged in the Comprehend Event Log and if a gap warning occurs run a full backfill to refresh the data for all dates. 

Filter for a single site or group of sites: Currently the use of the --filter parameter for a single site or group of sites is not supported. This is because the rollup logic would then attempt to roll up the rates to the study level which would now only include data  
for the specific sites thereby minimizing the study level rates.
Workaround: Generate rates for a full study rather than a site specific filter

Missing Backfill Dates anti-pattern check does not consider filter: The missing backfill dates anti-pattern check is designed to identify when USDM data is present for sites or studies but those dates are missing from the scorecard PLOs. 
If the filter parameter is used to intentionally backfill dates for a specific study while excluding other studies and introduces a scenario where study A has rates calculated for more recent dates than study B, 
this check does not account for the filter setting and will continue to generate warnings that "Sites for Study B are missing backfill dates
Workaround: Ignore the warnings in this scenario
"""

import sys, os
from os import path
from pathlib import Path
sys.path.append( str(Path(path.abspath(__file__)).parents[4] / 'utils') ) # /ccdm/resources/utils
sys.path.append( str(Path(path.abspath(__file__)).parents[4] /'validation'/'cqs'/'global_cqs') ) # /ccdm/resources/validation/cqs/global_cqs
from cqs_dictionary import ploPropertiesDict, ploConstraintsDict, cdmPropertiesDict, cdmConstraintsDict  # variables in /global_cqs/cqs_dictionary
import db_connect, zk_connect, rates_dict # modules in /resources/utils
from docopt import docopt
import logging
import psycopg2
import time
from logging.handlers import TimedRotatingFileHandler
import getpass
import ast

# zk_connect class
zk = zk_connect.zk_class()

# variables from RatesDict
TempObjectsDict = rates_dict.TempObjectsDict
KPIRatesDict = rates_dict.KPIRatesDict
KPIChecksDict = rates_dict.KPIChecksDict

# USDM and temp table names
tempSiteScoreTbl = 'temp_site_cro_scores'
tempStudyScoreTbl = 'temp_study_cro_scores'
tempDateSeriesTbl = 'temp_date_series'
tempDateSeriesDailyTbl = 'temp_date_series_daily'
ploSiteScoreTbl = 'rpt_site_cro_scores'
ploStudyScoreTbl = 'rpt_study_cro_scores'
eventLogTbl = 'comprehendeventlog'

# Zookeeper variables for retry functionality
zkLogPath = '/CQS/cqs/scorecard_post_processing'
zkFailureNode = 'LastFailureStateScoreCardPostProcessing'
zkSuccessTimeNode = 'LastSuccessfulScorecardPostProcessingRunTime'

# dictionary of USDM tables to create the first time backfill mode is run in any environment
# includes the dictionaries where the table/constraints definitions can be found and associated temp tables
USDMTableDict = {
    ploSiteScoreTbl : {'temp_table' : tempSiteScoreTbl,
                        'columns_dictionary' : ploPropertiesDict, 
                        'constraints_dictionary' : ploConstraintsDict,
                        'merge_data' : True},
    ploStudyScoreTbl : {'temp_table' : tempStudyScoreTbl,
                        'columns_dictionary' : ploPropertiesDict,
                        'constraints_dictionary' : ploConstraintsDict,
                        'merge_data' : True},
    eventLogTbl : {'temp_table' : None,
                    'columns_dictionary' : ploPropertiesDict, 
                    'constraints_dictionary' : ploConstraintsDict,
                    'merge_data' : False},}

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

# execute sql
# returnset: if true then execute fetchall() and return dataset
# otherwise just execute sql (ex. for ddl)
def execute_sql(conn, sql, returnset=False):
    returnSet = None
    cur = conn.cursor()
    cur.execute(sql)
    if returnset and cur.rowcount > 0: # in the case of a ddl statement this will be 0
        returnSet = cur.fetchall()
    cur.close()
    return returnSet

# create table or view
# if sql: create table from query
# if coldef: create table from comma separated list of columns
def create_object(conn, schema, table, type, sql=None, coldef=None):
    creSQL = None
    if sql is not None:
        creSQL = 'CREATE {type} "{schema}"."{table}" AS {sql};'.format(type=type, schema=schema, table=table, sql=sql)
        if type.lower() == 'table':
            creSQL += 'ANALYZE {table};'.format(table=table) # analyze if create from sql to optimize queries
    if coldef is not None: 
        creSQL = 'CREATE {type} "{schema}"."{table}"({coldef});'.format(type=type, schema=schema, table=table, coldef=coldef)
    if creSQL is not None:
        logger.info('Creating object: {object}'.format(object=table))
        try:
            execute_sql(conn, creSQL)
            conn.commit()
        except Exception, e:
            conn.rollback()
            logger.exception(e)
            return False
    else:
        msg = 'Invalid parameters for create_object'
        logger.exception(msg)
        return False
    return True

# drop table or view
def drop_object(conn, schema, table, type):
    sql = 'DROP {type} IF EXISTS "{schema}"."{table}" CASCADE'.format(type=type, schema=schema, table=table)
    try:
        execute_sql(conn, sql)
        conn.commit()
    except Exception, e:
        conn.rollback()
        logger.exception(e)
        return False
    return True

# return boolean if table exists in db
def table_exists(conn, schema, table):
    cur = conn.cursor()
    sql = '''SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema}' and table_name = '{table}' '''.format(schema=schema, table=table)
    cur.execute(sql)
    if cur.rowcount > 0:
        result = True
    else:
        result = False
    cur.close()
    return result

# return list of studyids from db
def get_studies(conn, schema):
    sql = '''SELECT DISTINCT studyid FROM "{schema}".study'''.format(schema=schema)
    studyList = execute_sql(conn, sql, True)
    if studyList is not None and len(studyList) > 0:
        return [rec[0] for rec in studyList]
    else:
        return None

# return column properties for table from ploPropertiesDict
def get_column_properties(dictname, table):
    if table in dictname.keys():
        return dictname[table]
    else:
        return None

# return table constraints from ploConstraintsDict
def get_table_constraints(dictname, table):
    if table in dictname.keys():
        return dictname[table]
    else:
        return None

# get list of columns for scorcard PLO from dictionary and return comma separated string
# dtsep = datatype separator (ex. "::"). If included then include datatype such as "col::datatype" or "col datatype"
def get_table_col_list(dictname, table, dtsep=None):
    colList = ''
    dictColProps = get_column_properties(dictname, table)
    if dictColProps is not None:
        for col in dictColProps:
            if 'name' not in col.keys() or 'datatype' not in col.keys():
                return None
            else:
                colList += col['name']
                if dtsep is not None:
                    colList += '{sep}{datatype}'.format(sep=dtsep, datatype=col['datatype'])
                if col != dictColProps[-1]:
                    colList += ', '
    else:
        logger.error('Column properties not found for table: {table}'.format(table=table))
        colList = None
    
    return colList

# create table by looking up definition for <source> in <dictname> and create <schema>.<target>
def create_table_from_def(conn, schema, dictname, source, target):
    success = True
    collist = get_table_col_list(dictname, source, ' ')
    if collist is not None:
        if not create_object(conn, schema, target, 'table', None, collist):
           success = False
    else:
        success = False
    
    if not success:
        logger.error('Failed to create table {table}'.format(table=target))
    
    return success

# retrieve constraint properties from <dictname> for <table>
# return in a formatted dictionary where each key is a constraint type 
def get_constraints_from_dict(dictname, table):
    condict = {}
    conlist = get_table_constraints(dictname, table)
    if conlist is None:
        return None

    for conitem in conlist: # conlist: list of dictionaries where each key is a constraint type
        for conitemtype in conitem:
            condict[conitemtype] = {}
            for constraintdicts in conitem[conitemtype]:
                for conname in constraintdicts: # conname: constraint name
                    condict[conitemtype][conname] = {} 
                    if conitemtype != 'foreignkeys': 
                        condict[conitemtype][conname]['columns'] = constraintdicts[conname] # all other types only have columns
                    else:
                        for moreatts in constraintdicts[conname]:
                            for att in moreatts:
                                condict[conitemtype][conname][att] = moreatts[att]
    return condict

# same as get_constraints_from_dict except retrieve from database
def get_constraints_from_db(conn, schema, table):
    condict = {}
    tabsql = ''
    colsql = ''

    # these two only used for foreign keys
    reftabsql = '''SELECT DISTINCT ccu.table_name
                    FROM information_schema.constraint_column_usage ccu
                    WHERE lower(ccu.constraint_schema) = lower('{schema}') 
                    AND ccu.constraint_name = '{conname}' '''

    refcolsql = '''SELECT DISTINCT ccu.column_name
                    FROM information_schema.constraint_column_usage ccu
                    WHERE lower(ccu.constraint_schema) = lower('{schema}') 
                    AND ccu.constraint_name = '{conname}' 
                    ORDER BY ccu.column_name'''

    for contype in ['primarykeys', 'foreignkeys', 'uniquekeys', 'index']:

        if contype in('primarykeys', 'foreignkeys'):
            tabsql = '''SELECT DISTINCT tc.constraint_name
                        FROM information_schema.table_constraints tc
                        WHERE upper(tc.constraint_type) = CASE WHEN '{contype}' = 'primarykeys' THEN 'PRIMARY KEY' ELSE 'FOREIGN KEY' END
                        AND lower(tc.table_schema) = lower('{schema}') 
                        AND lower(tc.table_name) = '{table}' '''.format(schema=schema, contype=contype, table=table)

            colsql = '''SELECT DISTINCT kcu.column_name, kcu.position_in_unique_constraint
                        FROM information_schema.key_column_usage kcu
                        WHERE lower(kcu.table_schema) = lower('{schema}') 
                        AND kcu.constraint_name = '{conname}'
                        ORDER BY kcu.position_in_unique_constraint'''

        elif contype in ('uniquekeys', 'index'):
            tabsql = '''SELECT i.relname as index_name
                        FROM pg_class t
                        JOIN pg_index ix on (t.oid = ix.indrelid)
                        JOIN pg_class i on (i.oid = ix.indexrelid)
                        JOIN pg_namespace as ns on i.relnamespace = ns.oid
                        WHERE t.relname = '{table}'
                        AND ix.indisprimary is false
                        AND ix.indisunique = CASE WHEN '{contype}' = 'uniquekeys' THEN True ELSE False END
                        AND ns.nspname = '{schema}' '''.format(schema=schema, contype=contype, table=table)

            colsql = '''SELECT a.attname as column_name
                        FROM pg_class t
                        JOIN pg_index ix on (t.oid = ix.indrelid)
                        JOIN pg_class i on (i.oid = ix.indexrelid)
                        JOIN pg_namespace as ns on i.relnamespace = ns.oid
                        JOIN pg_attribute a on (a.attrelid = t.oid AND a.attnum = any(ix.indkey))
                        WHERE ns.nspname = '{schema}'
                        AND i.relname = '{conname}'
                        ORDER BY a.attname'''

        # build the constraint list
        conlist = execute_sql(conn, tabsql, True)
        if conlist is not None and len(conlist) > 0:
            condict[contype] = {}           
            for rec in conlist:
                conname = rec[0]
                colsql = colsql.format(schema=schema, conname=conname) # format here to add the constraint name
                collist = execute_sql(conn, colsql, True)
                if collist is not None and len(conlist) > 0:
                    cols = []
                    for col in collist:
                        cols.append(col[0])

                condict[contype][conname] = {'columns' : cols}

            # for foreign keys add the referred table and columns
            if contype == 'foreignkeys':
                reftabsql = reftabsql.format(schema=schema, conname=conname) # format here to add the constraint name
                reftablist = execute_sql(conn, reftabsql, True)
                if reftablist is not None and len(reftablist) > 0:
                    reftab = reftablist[0][0] #first record, first column
                    refcolsql = refcolsql.format(schema=schema, conname=conname) # format here to add the constraint name
                    refcollist = execute_sql(conn, refcolsql, True)
                    if refcollist is not None and len(refcollist) > 0:
                        cols = []
                        for col in refcollist:
                            cols.append(col[0])
                        condict[contype][conname]['ReferredTable'] = reftab
                        condict[contype][conname]['ReferredColumns'] = cols

    return condict

# retrieve column properties from <dictname> for <table>
# and return in a formatted dictionary where each key is a constraint name
def get_column_properties_from_dict(dictname, table):
    dictCols = {}
    dictColProps = get_column_properties(dictname, table)
    if dictColProps is None:
        logger.error('Column properties missing for {table}'.format(table=table))
        return None

    for item in dictColProps:
        if 'name' not in item.keys() or 'datatype' not in item.keys() or 'comments' not in item.keys() or 'Not_Null' not in item.keys():
            logger.error('Column properties missing for {table}'.format(table=table))
            return None
        
        dictCols[ item['name'] ] = {'datatype' : item['datatype'], 'comments' : item['comments'], 'not_null' : item['Not_Null']}

    return dictCols

# same as get_column_properties_from_dict but retrieve from database
def get_column_properties_from_db(conn, schema, table):
    dbCols = {}
    sql = '''SELECT a.attname, format_type(a.atttypid, a.atttypmod), col_description(a.attrelid, a.attnum) , a.attnotnull as not_null  
                FROM pg_class c, pg_namespace n, pg_attribute a 
                WHERE c.relnamespace = n.oid 
                    and upper(n.nspname) = upper(\'{schema}\')
                    and upper(c.relname) = upper(\'{table}\') 
                    and c.relkind = \'r\' and a.atttypid <> 0::oid and c.oid = a.attrelid AND a.attnum > 0'''.format(schema=schema, table=table)
    dbColProps = execute_sql(conn, sql, True)
    if dbColProps is None or len(dbColProps) == 0:
        logger.error('DB properties for {table} not found'.format(table=table) )
        return None

    for rec in dbColProps:
        colName = rec[0]
        dbCols[colName] = {'datatype' : rec[1], 'comments' : rec[2], 'not_null' : rec[3]}

    return dbCols

# drop constraint <conname> on <table>
# contype = primarykeys, uniquekeys, index, foreignkeys
def drop_constraint(conn, schema, table, conname, contype):
    sql = None
    if contype in ('primarykeys', 'foreignkeys'):
        sql = 'ALTER TABLE "{schema}"."{table}" DROP CONSTRAINT {conname}'.format(schema=schema, table=table, conname=conname)
    elif contype in ('uniquekeys', 'index'):
        sql = 'DROP INDEX "{schema}".{conname}'.format(schema=schema, table=table, conname=conname)
    
    if sql is not None:
        try:
            execute_sql(conn, sql)
            conn.commit()
        except Exception, e:
            conn.rollback()
            logger.exception(e)
            return False

    else:
        logger.error('Error dropping constraint: {constraint}'.format(constraint=conname))
        return False
    return True

# create constraint <conname> on <table>
# contype = primarykeys, uniquekeys, index, foreignkeys
# conlist = list of constraint columns
# reftable and refcollist are referenced table/columns for foreign keys
def create_constraint(conn, schema, table, conname, contype, collist, reftable=None, refcollist=None):
    sql = None
    if contype == 'primarykeys':
        sql = 'ALTER TABLE "{schema}"."{table}" ADD CONSTRAINT {conname} PRIMARY KEY ({collist})'.format(schema=schema, table=table, conname=conname, collist=','.join(collist))
    elif contype == 'uniquekeys':
        sql = 'CREATE UNIQUE INDEX {conname} ON "{schema}"."{table}" ({collist})'.format(schema=schema, table=table, conname=conname, collist=','.join(collist))
    elif contype == 'index':
        sql = 'CREATE INDEX {conname} ON "{schema}"."{table}" ({collist})'.format(schema=schema, table=table, conname=conname, collist=','.join(collist))
    elif contype == 'foreignkeys':
        sql = '''ALTER TABLE "{schema}"."{table}" ADD CONSTRAINT {conname} FOREIGN KEY({collist}) REFERENCES "{schema}"."{reftable}" ({refcollist}) ON
                    DELETE NO ACTION ON
                    UPDATE NO ACTION;'''.format(schema=schema, table=table, conname=conname, collist=','.join(collist), reftable=reftable, refcollist=','.join(refcollist))
    if sql is not None:
        try:
            execute_sql(conn, sql)
            conn.commit()
        except Exception, e:
            conn.rollback()
            logger.exception(e)
            return False
    else:
        logger.error('Error adding constraint: {constraint}'.format(constraint=conname))
        return False
    return True

# applies the following constraint/column changes on <table> PLO before data is merged:
#   drop constraints no longer included for table
#   add a new column
#   change a column data type
#   remove a column not null constraint
def preload_update_constraints(conn, schema, table, columnsdict, constraintsdict):
    success = True
    dictcols = get_column_properties_from_dict(columnsdict, table)
    dictcons = get_constraints_from_dict(constraintsdict, table)
    dbcols = get_column_properties_from_db(conn, schema, table)
    dbcons = get_constraints_from_db(conn, schema, table)

    # loop through db constraints and drop those not found in dictionary
    if dbcons is not None and len(dbcons) > 0:
        for contype in dbcons:
            for constraint in dbcons[contype]: # loop through db constraints and compare to dictionary
                if dictcons is None or len(dictcons) == 0 or contype not in dictcons.keys() or constraint not in dictcons[contype].keys():
                    # constraints not found so remove
                    logger.info('Dropping constraint: {constraint}'.format(constraint=constraint))
                    if not drop_constraint(conn, schema, table, constraint, contype):
                        success = False

    sql = ''
    for colName in dictcols: # loop through dictionary columns and compare to db columns
        if dbcols is None or len(dbcols) == 0 or colName not in dbcols.keys():
            # column not found in db so add it
            logger.info('Adding column: {table}.{column}'.format(table=table, column=colName))
            sql += 'ALTER TABLE "{schema}"."{table}" ADD COLUMN {column} {datatype};'.format(schema=schema, table=table, column=colName, datatype=dictcols[colName]['datatype'])
        else:
            # column found in db
            if dictcols[colName]['datatype'] != dbcols[colName]['datatype']:
                # data type changed
                logger.info('Altering datatype for column: {table}.{column} to {datatype}'.format(table=table, column=colName, datatype=dictcols[colName]['datatype']))
                sql += 'ALTER TABLE "{schema}"."{table}" ALTER COLUMN {column} type {datatype} using {column}::{datatype};'.format(schema=schema, table=table, column=colName, datatype=dictcols[colName]['datatype'])
            if not dictcols[colName]['not_null'] and dbcols[colName]['not_null']:
                # not_null changed and needs to be dropped
                logger.info('Removing not null constraint for {table}.{column}'.format(table=table, column=colName))
                sql += 'ALTER TABLE "{schema}"."{table}" ALTER COLUMN {column} DROP NOT NULL;'.format(schema=schema, table=table, column=colName)

    if len(sql) > 0:
        try:
            execute_sql(conn, sql)
            conn.commit()
        except Exception, e:
            conn.rollback()
            logger.exception(e)
            success = False

    return success

# applies the following constraint/column changes on <table> PLO:
#   constraint has changed (columns, etc.) so drop and recreate
#   add new constraint
#   update column comment
#   add not null constraint
#   drop column
def postload_update_constraints(conn, schema, table, columnsdict, constraintsdict):
    success = True
    dictcols = get_column_properties_from_dict(columnsdict, table)
    dictcons = get_constraints_from_dict(constraintsdict, table)
    dbcols = get_column_properties_from_db(conn, schema, table)
    dbcons = get_constraints_from_db(conn, schema, table)

    if dictcons is not None and len(dictcons) > 0:
        for contype in dictcons:
            for constraint in dictcons[contype]: # loop through dictionary constraints and compare to db
                if dbcons is not None and len(dbcons) > 0 and contype in dbcons.keys() and constraint in dbcons[contype].keys():
                    # found in db so compare
                    if set(dictcons[contype][constraint]['columns']) != set(dbcons[contype][constraint]['columns']) or ( contype == 'foreignkeys' and ( dictcons[contype][constraint]['ReferredTable'] != dbcons[contype][constraint]['ReferredTable'] or set(dictcons[contype][constraint]['ReferredColumns']) != set(dbcons[contype][constraint]['ReferredColumns']) ) ):
                        # something does not match so drop and recreate constraint
                        logger.info('Recreating constraint: {constraint}'.format(constraint=constraint))
                        if not drop_constraint(conn, schema, table, constraint, contype):
                            success =  False

                        if contype == 'foreignkeys':
                            if not create_constraint(conn, schema, table, constraint, contype, dictcons[contype][constraint]['columns'],  dictcons[contype][constraint]['ReferredTable'], dictcons[contype][constraint]['ReferredColumns']):
                                success = False                    
                        else:
                            if not create_constraint(conn, schema, table, constraint, contype, dictcons[contype][constraint]['columns'] ):
                                success = False

                else:
                    # constraints not in db so add if
                    logger.info('Adding new constraint: {constraint}'.format(constraint=constraint))
                    if contype == 'foreignkeys':
                        if not create_constraint(conn, schema, table, constraint, contype, dictcons[contype][constraint]['columns'],  dictcons[contype][constraint]['ReferredTable'], dictcons[contype][constraint]['ReferredColumns']):
                            success = False
                    else:
                        if not create_constraint(conn, schema, table, constraint, contype, dictcons[contype][constraint]['columns'] ):
                            success = False

    sql = ''
    for colName in dbcols:
        if dictcols is not None and len(dictcols) > 0 and colName not in dictcols.keys():        
            # column not found in dictionary so drop it
            logger.info('Dropping column: {table}.{column}'.format(table=table, column=colName))
            sql += 'ALTER TABLE "{schema}"."{table}" DROP COLUMN IF EXISTS {column};'.format(schema=schema, table=table, column=colName)

    for colName in dictcols: # loop through dict columns and compare to db
        if dictcols[colName]['comments'] is not None and (dbcols is None or len(dbcols) == 0 or colName not in dbcols.keys() or (dictcols[colName]['comments'] != dbcols[colName]['comments'])):
            # comments need to be updated
            logger.info('Updating comment for {table}.{column}'.format(table=table, column=colName))
            sql += '''COMMENT ON COLUMN "{schema}"."{table}"."{column}" IS '{comment}';'''.format(schema=schema, table=table, column=colName, comment=dictcols[colName]['comments'])
        if dictcols[colName]['not_null'] and (dbcols is None or len(dbcols) == 0 or colName not in dbcols.keys() or not dbcols[colName]['not_null']):
            # not null needs to be added
            logger.info('Adding not null constraint for {table}.{column}'.format(table=table, column=colName))
            sql += 'ALTER TABLE "{schema}"."{table}" ALTER "{column}" SET NOT NULL;'.format(schema=schema, table=table, column=colName)

    if len(sql) > 0:
        try:
            execute_sql(conn, sql)
            conn.commit()
        except Exception, e:
            conn.rollback()
            logger.exception(e)
            success = False

    return success

# confirm if PLO table exists. If not create tables
def create_usdm_tables(conn, schema, dictname):
    success = True
    for table in dictname:
        if not table_exists(conn, schema, table):
            success = create_table_from_def(conn, schema, dictname[table]['columns_dictionary'], table, table)
    return success

# remove all temporary objects as part of setup and cleanup
# templist: list of temporary tables created by this script not included in dictionary
def drop_temp_objects(conn, schema, templist):
    # first loop through dictionary and drop any objects from TempObjectsDict
    success = True
    for tempItem in sorted(TempObjectsDict):
        if 'type' in TempObjectsDict[tempItem].keys():
            objType = TempObjectsDict[tempItem]['type']
            objName = tempItem.lower()
            if not drop_object(conn, schema, objName, objType):
                success = False
        else:
            logger.error('Temporary object {object} not properly configured in RatesDict.TempObjectsDict'.format(object=tempItem))
            success = False

    # next drop any tables passed in templist
    for table in templist:
        if not drop_object(conn, schema, table, 'TABLE'):
            success = False

    return success

# loop through TempObjectsDict and build temporary objects
# if <wherestr> and object has property "apply_filter" = True then apply filter logic
def build_temp_objects(conn, schema, backfill, kpisql, wherestr=None, dayslimit=None):
    for item in sorted(TempObjectsDict):
        tempObj = TempObjectsDict[item]
        if 'type' not in tempObj.keys() or 'sql' not in tempObj.keys():
            logger.error('Temporary object {object} not properly configured in RatesDict.TempObjectsDict'.format(object=item))
            return False
        else:
            objType = tempObj['type']
            objName = item.lower()

            # this option is used for temp_date_series where the start date logic differs based on backfill vs. incremental mode
            startDateSQL = ''
            if 'startdate_sql' in tempObj.keys():
                if backfill:
                    startDateSQL = tempObj['startdate_sql']['backfill']
                else:
                    startDateSQL = tempObj['startdate_sql']['incremental']

            # this applies an optional filter based on the wherestr parameter and the apply_filter setting of the temporary object
            whereStr = ''
            if 'apply_filter' in tempObj.keys() and wherestr is not None and len(wherestr) > 0:
                if tempObj['apply_filter']:
                    whereStr = 'AND ({wherestr})'.format(wherestr=wherestr)
                    logger.info('Applying filter to {object}: {wherestr}'.format(object=objName, wherestr=wherestr))

            if dayslimit is None:
                # default days limit to a large range to bypass if it was not set
                dayslimit = 99999

            objSQL = tempObj['sql'].format(schema=schema, kpisql=kpisql, startdatesql=startDateSQL, filter=whereStr, dayslimit=dayslimit)

            if not create_object(conn, schema, objName, objType, objSQL):
                return False

            if 'index' in tempObj.keys():
                indexList = tempObj['index']
                for ind in indexList:
                    create_constraint(conn, schema, objName, ind['name'], ind['type'], [ind['columns']])

    return True

# check and remove the site's from in rpt_site_cro_scores if the site has been removed from site object.
# This function uses the "SITE_REMOVED" key from KPIChecksDict dictionary to identify the deleted site 
def detect_deleted_site(conn, schema, sitetable):
    studiesSitesImpacted = []
    checkDeletedSiteSQL = KPIChecksDict['SITE_REMOVED']

    if checkDeletedSiteSQL :
        checkSQL = checkDeletedSiteSQL['sql'].format(schema=schema,  plositescoretbl=sitetable)
        checkset = execute_sql(conn, checkSQL, True)

        if checkset is not None and len(checkset) > 0:
            for rec in checkset:
                studiesSitesImpacted.append([rec[1],rec[2]])
    else:
        logger.error('KPIChecksDict dictionary is not propery configured for : {check}'.format(check=check))
    return studiesSitesImpacted   

# loop through KPIRatesDict, calculate rates, and insert into temp tables
def calculate_rates(conn, schema, target, logtable, backfill, dayslimit=None, lastfaillist=None, debug=False, disablechecks=False):
    success = True
    failedRates = []

    # run series of pre-checks using log_events_antipatterns
    # any required filter conditions will be returned and applied below
    checkResults = log_events_antipatterns(conn, schema, ploSiteScoreTbl, ploStudyScoreTbl, eventLogTbl, 'BEFORE', backfill, dayslimit, disablechecks)
    if not checkResults[0]:
        # at least one check failed
        success = False

    filterStr = checkResults[1]
    if filterStr is not None and len(filterStr) > 0:
        filterStr = 'WHERE {filterStr}'.format(filterStr=filterStr)

    # if in retry mode and there were previous failures then iterate through failure list
    # otherwise iterate through all configured KPIs
    if lastfaillist is not None and len(lastfaillist) > 0:
        kpiCalcList = ast.literal_eval(lastfaillist) # need this in order to convert string from ZK to list
    else:
        kpiCalcList = KPIRatesDict

    # get list of KPIs included in temp_date_series which may be reduced by --filter
    sql = '''SELECT DISTINCT kpiid FROM "{schema}"."{table}"'''.format(schema=schema, table=tempDateSeriesTbl)
    kpiFilterList = execute_sql(conn, sql, True)
    if kpiFilterList is not None and len(kpiFilterList) > 0:
        kpiFilterList = [x[0] for x in kpiFilterList]
    else: 
        kpiFilterList = []

    if dayslimit is None:
        # default days limit to a large range to bypass if it was not set
        dayslimit = 99999

    for kpi in kpiCalcList:
        if kpi not in kpiFilterList:
            logger.info('No required calculations identified for KPI: {kpi}'.format(kpi=kpi) )
        else:
            logger.info('Calculating KPI: {kpi}'.format(kpi=kpi))  
            rateSQL = KPIRatesDict[kpi]['sql'].format(schema=schema)
            if rateSQL is not None and len(rateSQL) > 0:
                # each kpi sql in rates_dict should refer to a cte of "this_series" which is added here
                sql = '''WITH this_series AS (SELECT comprehendid, studyid, croid, siteid, kpiid, kpicategory, period_date
                                                FROM "{schema}"."{table}"
                                                WHERE kpiid = '{kpiid}'),

                                kpi_calc AS ({ratesql})
                         
                        INSERT INTO "{schema}".{target} (comprehendid, studyid, croid, siteid, kpiid, kpicategory, numerator, denominator, multiplier, kpiscore, kpicalculationdate, currentflag, comprehend_update_time)
                        SELECT dser.comprehendid::text, dser.studyid, dser.croid, dser.siteid, dser.kpiid, dser.kpicategory, kpi.numerator, kpi.denominator, kpi.multiplier, kpi.kpiscore, dser.period_date, False::bool, now()::timestamp without time zone
                        FROM this_series dser
                        LEFT JOIN kpi_calc kpi ON(dser.studyid = kpi.studyid AND dser.croid = kpi.croid AND dser.siteid = kpi.siteid AND dser.period_date = kpi.kpicalculationdate)
                        {filterstr}'''.format(schema=schema, table=tempDateSeriesDailyTbl, kpiid=kpi, ratesql=rateSQL, target=target, dayslimit=dayslimit, filterstr=filterStr)

                try:
                    start = time.time()
                    execute_sql(conn, sql)
                    conn.commit()
                    end = time.time()
                    runtime = end - start
                    if debug:
                        # log execution time for debugging
                        logger.info('{kpi} calculation time: {runtime}'.format(kpi=kpi, runtime=runtime))
                except Exception, e:
                    # even if one kpi computation fails we want to continue to process the remaining kpis so log this error and continue 
                    conn.rollback()
                    success = False
                    failedRates.append(kpi)
                    logger.exception('Failed to calculate KPI {kpi}: {err}'.format(kpi=kpi, err=e))
            else:
                success = False
    return [success, failedRates]

# check the data in the temp tables for certain events/anti-patterns when in incremental mode
# if found then log records and remove from dataset to be loaded
# table: PLO table used for checks
# logtable: Name of USDM table where events are logged
# cmode: when checks are to be executed (BEFORE=before rate calculations; AFTER=after PLO data is updated)
def log_events_antipatterns(conn, schema, sitetable, studytable, logtable, cmode, backfill, dayslimit=None, disablechecks=False):

    # sub-function to log events to the database
    def generate_log(conn, schema, rec, logtable, jobnum, jobseq):
        logSQL = ''
        moduleCat = 'CRO-O_SCORECARD_POST_PROCESSING'

        comprehendId=rec[0]
        studyId=rec[1]
        siteId=rec[2]
        croId=rec[3]
        moduleSubCat=rec[4]
        eventId=rec[5]
        eventName=rec[6]
        eventDesc=rec[7]
        eventMessage=rec[8]

        logger.warning(eventMessage)
        logSQL += '''INSERT INTO "{schema}"."{logtable}" (jobnum, jobseq, comprehendid, studyid, siteid, croid, modulecategory, modulesubcategory, eventid, eventdtc, eventname, eventdesc, eventmessage) 
                        VALUES({jobnum}, {jobseq}, '{comprehendid}', '{studyid}', '{siteid}', '{croid}', '{modulecat}', '{modulesubcat}', 
                                '{eventid}', now()::timestamp without time zone, '{eventname}', '{eventdesc}', '{eventmessage}');'''.format(schema=schema, logtable=logtable, jobnum=jobnum, jobseq=jobSeq, comprehendid=comprehendId, studyid=studyId, siteid=siteId, croid=croId, modulecat=moduleCat, modulesubcat=moduleSubCat, eventid=eventId, eventname=eventName, eventdesc=eventDesc, eventmessage=eventMessage) 

        return logSQL

    # these strings will be built as the checks run
    success = True
    filterSQL = 'CASE'
    insertSQL = ''
    logger.info('Running events/anti-pattern checks {cmode} PLO updates'.format(cmode=cmode))
    if disablechecks:
        logger.info('Informational checks disabled')

    # get next job number
    jobSeq = 0
    jobNum = 0
    sql = 'SELECT coalesce(max(jobnum), 0) + 1 as jobnum FROM "{schema}"."{logtable}"'.format(schema=schema, logtable=logtable)
    returnSet = execute_sql(conn, sql, True)
    if returnSet is not None and len(returnSet) > 0:
        jobNum = returnSet[0][0]

    # iterate through checks defined in the dictionary and execute
    for check in KPIChecksDict:
        if not set(['sql', 'filtersql', 'condition', 'mode']).issubset(KPIChecksDict[check].keys()):
            logger.error('KPI check not properly configured: {check}'.format(check=check) )
            success = False
        else:
            checkSQL = KPIChecksDict[check]['sql'].format(schema=schema, tempdateseriestbl=tempDateSeriesTbl, plositescoretbl=sitetable, plostudyscoretbl=studytable, dayslimit=dayslimit)
            checkFilterSQL = KPIChecksDict[check]['filtersql']
            checkCondition = KPIChecksDict[check]['condition']
            checkMode = KPIChecksDict[check]['mode']

            if disablechecks and checkFilterSQL is None:
                continue # this disables any "informational only" checks and moves to the next one

            if checkMode is None or cmode.lower() == checkMode.lower():
                if (checkCondition is None # no condition, always execute
                    or (checkCondition == 'incremental' and not backfill) # incremental mode checks
                    or (checkCondition == 'backfill' and backfill) # backfill mode checks
                    or (checkCondition == 'days_limit' and dayslimit is not None)): # days_limit check
                    checkset = execute_sql(conn, checkSQL, True)
                    if checkset is not None and len(checkset) > 0:
                        # records flagged by check so log 
                        for rec in checkset:
                            jobSeq += 1
                            insertSQL += generate_log(conn, schema, rec, logtable, jobNum, jobSeq)
                            if checkFilterSQL is not None:
                                filterSQL += checkFilterSQL.format(comprehendid=rec[0], studyid=rec[1], siteid=rec[2], croid=rec[3], kpiid=rec[4])

    if len(insertSQL) > 0:
        try:
            execute_sql(conn, insertSQL)
            conn.commit()
        except Exception, e:
            conn.rollback()
            logger.exception(e)
            success = False

    if filterSQL == 'CASE':
        filterSQL = '' # no filter criteria identified
    else:
        filterSQL += ' ELSE True END' # add end condition for case statement

    return [success, filterSQL]

# delete and re-insert final data into PLOs
def merge_data(conn, schema, backfill, source, target):
    sql = 'SELECT COUNT(*) FROM "{schema}"."{source}"'.format(schema=schema, source=source)
    tCnt = execute_sql(conn, sql, True)[0][0]
    sql = ''
    
    if backfill == True:
        # swap the schemas for a backfill
        logger.info('Swapping {target} after backfill'.format(target=target))
        sql += '''DROP TABLE IF EXISTS "{target}_old";
                    ALTER TABLE "{target}" RENAME TO "{target}_old";
                    ALTER table "{source}" RENAME TO "{target}";
                    DROP TABLE IF EXISTS "{target}_old";'''.format(target=target, source=source)

        if tCnt == 0:
            logger.warning('No data found for {target}'.format(target=target))

    elif tCnt == 0:
        logger.info('No records identified to merge into {target}. Skipping.'.format(target=target))
        return True
    else:
        # upserts for incremental mode
        # TODO: Add actual upsert logic after upgrade to postgres 9.5 or greater
        logger.info('Merging {num} records into: {target}'.format(num=tCnt, target=target) )
        sql += '''UPDATE "{schema}"."{target}" r
                    SET kpicategory = t.kpicategory,
                        numerator = t.numerator,
                        denominator = t.denominator,
                        multiplier = t.multiplier,
                        kpiscore = t.kpiscore,
                        comprehend_update_time = now()::timestamp without time zone
                    FROM "{schema}"."{source}" t
                    WHERE r.comprehendid = t.comprehendid
                    AND r.croid = t.croid
                    AND r.kpicalculationdate = t.kpicalculationdate
                    AND r.kpiid = t.kpiid;'''.format(schema=schema, source=source, target=target)

        # build column list from dictionary
        tgtColList = get_table_col_list(ploPropertiesDict, target)
        if tgtColList is not None and len(tgtColList) > 0:
            srcColList = tgtColList.replace('comprehend_update_time', 'now()::timestamp without time zone')

            #insert data
            sql += '''INSERT INTO "{schema}"."{target}" ({tgtcols})
                        (SELECT t.* -- using select * because get_table_col_list does not currently support prefixing with a table alias which would be required here  
                            FROM "{schema}"."{source}" t
                            LEFT JOIN "{schema}"."{target}" r ON (r.comprehendid = t.comprehendid
                                                                    AND r.croid = t.croid
                                                                    AND r.kpicalculationdate = t.kpicalculationdate
                                                                    AND r.kpiid = t.kpiid)
                            WHERE r.comprehendid IS NULL);'''.format(schema=schema, source=source, target=target, tgtcols=tgtColList, srccols=srcColList)
        else: 
            return False

    # update currentflag for latest records
    for values in [['true', 'false'], ['false', 'true']]:
        sql += '''WITH max_date AS (SELECT comprehendid, croid, kpiid, max(kpicalculationdate) AS maxdate FROM "{schema}"."{target}" GROUP BY comprehendid, croid, kpiid)
                    UPDATE "{schema}"."{target}" r
                    SET currentflag = {bool1}
                    FROM max_date m
                    WHERE r.comprehendid = m.comprehendid
                    AND r.croid = m.croid
                    AND r.kpiid = m.kpiid
                    AND (r.kpicalculationdate = m.maxdate) = {bool1}
                    AND currentflag = {bool2};'''.format(schema=schema, target=target, bool1=values[0], bool2=values[1])

    try:
        execute_sql(conn, sql)
        conn.commit() 
    except Exception, e:
        conn.rollback()
        logger.exception(e)
        return False
    return True

## Function to insert study level data from rpt_site_cro_scores to temp_site_cro_scores for study which the site has been removed in USDM object. 
## Insertion will exclude removed site data from rpt_site_cro_scores from selection
def insert_into_tempsitetbl(conn, schema, deletedsitelst, ploSiteScoreTbl, tempSiteScoreTbl, tempDateSeriesDailyTbl):
    lSQL = ''
    studySiteDict= {}
    insSQL = '''INSERT INTO {tempSiteScoreTbl} 
                WITH date_series as (SELECT studyid, croid, siteid, kpiid, min(period_date) as startdate 
                FROM  "{schema}"."{table}" 
                GROUP BY studyid, croid, siteid, kpiid )'''.format(schema=schema, table=tempDateSeriesDailyTbl, tempSiteScoreTbl=tempSiteScoreTbl)

    for studysite in deletedsitelst:
        studyid = studysite[0] # fetching studyid of deleted sites
        siteid = studysite[1] # fetching siteids of deleted sites

        if studyid in studySiteDict:
            studySiteDict[studyid].append(siteid)
        else : 
            studySiteDict[studyid] = [siteid]

    for study, sites in studySiteDict.items() :
        siteList =  str(studySiteDict[study])[1:-1]
        lSQL += '''SELECT plo.comprehendid, plo.studyid, plo.croid, plo.siteid, plo.kpiid, plo.kpicategory, plo.numerator, plo.denominator, plo.multiplier, plo.kpiscore, plo.kpicalculationdate, plo.currentflag, plo.comprehend_update_time
                    FROM "{schema}"."{ploSiteScoreTbl}" plo
                    LEFT JOIN date_series dser ON (dser.studyid = plo.studyid AND dser.croid = plo.croid AND dser.siteid = plo.siteid AND dser.kpiid = plo.kpiid) 
                    WHERE plo.studyid = '{studyid}' AND plo.siteid NOT IN ({sitelist}) AND plo.kpicalculationdate < dser.startdate::date UNION ALL '''.format(schema=schema, ploSiteScoreTbl=ploSiteScoreTbl, studyid=study, sitelist=siteList)  
    #Insert SQL framed by string operation
    lSQL = insSQL+' ( '+lSQL[:-11]+' )'

    try:
        execute_sql(conn, lSQL)
        conn.commit()
        return True
    except Exception, e:
        conn.rollback()
        logger.exception(e)
        return False


# roll up the kpi calculations from the site to the study level
def rollup_study_rates(conn, schema, source, target):
    sql = '''INSERT INTO "{schema}"."{target}" (comprehendid, studyid, croid, kpiid, kpicategory, numerator, denominator, multiplier, kpiscore, kpicalculationdate, currentflag, comprehend_update_time)
                SELECT studyid, studyid, croid, kpiid, kpicategory, numerator, denominator, multiplier,
                        (CASE WHEN coalesce(denominator, 0) > 0 THEN (numerator / denominator) * multiplier
                        ELSE 
                            NULL
                        END)::numeric as kpiscore,
                        kpicalculationdate,
                        false::boolean as currentflag,
                        comprehend_update_time
                FROM (SELECT studyid, croid, kpiid, kpicategory, 
                              sum(numerator) AS numerator, 
                              sum(denominator) AS denominator, 
                            multiplier,
                            null::numeric AS kpiscore,
                            kpicalculationdate, 
                            false::boolean AS currentflag, 
                            max(comprehend_update_time) AS comprehend_update_time
                        FROM "{schema}"."{source}"
                        GROUP BY studyid, croid, kpiid, kpicategory, multiplier, kpicalculationdate) t'''.format(schema=schema, source=source, target=target)

    try:
        execute_sql(conn, sql)
        conn.commit()
    except Exception, e:
        conn.rollback()
        logger.exception(e)
        return False

    return True

# update constraints and merge data for target PLO
def apply_constraints_merge_data(conn, schema, source, target, columnsdict, constraintsdict, backfill, merge, checkdeletedSites):
    success = True

    # structural updates to scorecard PLOs that need to happen BEFORE the data is merged
    # mainly applicable for combo of in-place / incremental modes (otherwise tables are always rebuilt anyway) 
    if not preload_update_constraints(conn, schema, target, columnsdict, constraintsdict):
        logger.error('Failed to update constraints for {table}'.format(table=target))
        success = False

    # this will build/update any indexes/constraints as needed
    # this is BEFORE data is merged for incremental loads in order for upserts to take advantage of indexes in query plans
    if backfill == False:
        if not postload_update_constraints(conn, schema, target, columnsdict, constraintsdict):
            logger.error('Failed to update constraints for {table}'.format(table=target))
            success = False

    # check to identify if there are deleted sites and if yes remove them rpt_site_cro_scores table before merging data in to rpt_site_cro_scores from temp_site_cro_scores
    if len(checkdeletedSites) > 0 and checkdeletedSites is not None and target == ploSiteScoreTbl:
        for studysite in checkdeletedSites:
            studyid = studysite[0] # fetching studyid of deleted sites
            siteid = studysite[1] # fetching siteids of deleted sites
            sql = '''DELETE FROM "{schema}"."{target}" WHERE studyid = '{studyid}' and siteid = '{siteid}' '''.format(schema=schema, target=target, studyid=studyid, siteid=siteid)

            try:
                logger.info('Deleting the removed site: "{siteid}" of study: "{studyid}" from "{target}"'.format(target=target, studyid=studyid, siteid=siteid))
                execute_sql(conn, sql)
                conn.commit()
            except Exception, e:
                conn.rollback()
                logger.exception(e)

    # merge source into target (based on setting for table)
    if merge:
        if not merge_data(conn, schema, backfill, source, target):
            logger.error('Failed to merge data from {source} into {target}'.format(source=source, target=target))
            success = False

    # in backfill mode this occurs after the tables are swapped
    # otherwise the indexes would be applied to the target but then lost on swap
    if backfill == True:
        if not postload_update_constraints(conn, schema, target, columnsdict, constraintsdict):
            logger.error('Failed to update constraints for {table}'.format(table=target))
            success = False

    logger.info('Finished processing updates to {table}'.format(table=target))
    return success

# log failed rate calculations to ZK in retry mode
# return list: [success(boolean), list of failed items]
def log_failure_details(zk_client, zk_nodes, zk_path, customer, failed_items):
    success = True
    failedNode = '{path}/{customer}{zkLogPath}/{node}'.format(path=zk_path, customer=customer, zkLogPath=zkLogPath, node=zkFailureNode)

    if failed_items is not None and len(failed_items) > 0:
        logger.warning('Logging failed rates to Zookeeper: {rates}'.format(rates=", ".join(failed_items)))
        if zk.zk_node_exists(zk_client, failedNode):
            zk.modify_zk_node(zk_client, failedNode,  failed_items)
        else:
            logger.error('Cannot log failure details. Node does not exist: {node}'.format(node=failedNode))
            success = False

    return success

# retrieve details of last failure state from ZK in retry mode
def get_failure_details(zk_client, zk_nodes, zk_path, customer):
    success = True
    failListObj = []
    failedNode = '{path}/{customer}{zkLogPath}/{node}'.format(path=zk_path, customer=customer, zkLogPath=zkLogPath, node=zkFailureNode)

    if zk.zk_node_exists(zk_client, failedNode):
        failList = zk.get_zk_val(zk_client, failedNode)
        failListObj = failList
        if failListObj is not None and len(failListObj) > 0:
            logger.info('Retrieved previous fail state: {rates}'.format(rates=failListObj))
    else:
        logger.error('Cannot retrieve previous failure details. Node does not exist: {node}'.format(node=failedNode))
        success = False

    return [success, failListObj]

# log successful execution start time to ZK in retry mode (and clear out any failure details)
def log_successful_run(zk_client, zk_nodes, zk_path, customer, start):
    success = True
    successTimeNode = '{path}/{customer}{zkLogPath}/{node}'.format(path=zk_path, customer=customer, zkLogPath=zkLogPath, node=zkSuccessTimeNode)
    failedNode = '{path}/{customer}{zkLogPath}/{node}'.format(path=zk_path, customer=customer, zkLogPath=zkLogPath, node=zkFailureNode)

    if start is not None and len(start) > 0:
        logger.info('Logging successful start time to zookeeper: {start}'.format(start=start))
        if zk.zk_node_exists(zk_client, successTimeNode):
            zk.modify_zk_node(zk_client, successTimeNode, start)
        else:
            logger.error('Cannot log start time. Node does not exist: {node}'.format(node=successTimeNode))
            success = False

    # clear out any failure states
    if zk.zk_node_exists(zk_client, failedNode):
        zk.modify_zk_node(zk_client, failedNode, '')

    return success

# copy the scorecard data for the srcStudy from the srcSchema to the targSchema (applicable for study-specific builds)
def copystudy_data(conn, srcSchema, srcStudy, targSchema, srcTable, targTable, srcCols):
    # confirm that study to copy exists in source
    srcSQL = '''select studyid from "{srcSchema}"."{srcTable}" WHERE studyid='{srcStudy}' LIMIT 1'''.format(srcCols=srcCols, srcSchema=srcSchema, srcTable=srcTable, srcStudy=srcStudy)
    studyList = execute_sql(conn, srcSQL, True)
    if studyList is None or len(studyList) == 0:
        logger.warning('Study ID {srcStudy} not found in {srcSchema}.{srcTable}'.format(srcSchema=srcSchema, srcTable=srcTable, srcStudy=srcStudy))

    logger.info('Copying {srcTable} from {srcSchema} to {targSchema} for study {srcStudy}'.format(srcTable=srcTable, srcSchema=srcSchema, targSchema=targSchema, srcStudy=srcStudy))
    sql = '''TRUNCATE "{targSchema}"."{targTable}";
                COPY (select {srcCols} from "{srcSchema}"."{srcTable}" WHERE studyid='{srcStudy}') TO '/tmp/{srcTable}.dat'  WITH (FORMAT BINARY);
                COPY "{targSchema}"."{targTable}" FROM '/tmp/{srcTable}.dat' WITH BINARY;'''.format(srcCols=srcCols, srcSchema=srcSchema, srcTable=srcTable, srcStudy=srcStudy, targTable=targTable, targSchema=targSchema)
    try:
        execute_sql(conn, sql)
        conn.commit()
    except Exception, e:
        conn.rollback()
        logger.exception(e)
        return False

    return True


# main method that controls the logic
def main(schema, zk_nodes=None, zk_path=None, customer=None, hostname=None, port=None, database=None, username=None, password=None, filterstr=None, dayslimit=None, logdir=None, backfill=False, debug=False, retry=False, disablechecks=False, workmem=None, copyschema=None, copystudy=None):
    cur = None
    conn = None
    exitCode = None
    zkClient = None

    # initialize logger
    initialize_logger(customer or database, schema, debug, logdir)

    # get start date/time for logging
    start = time.strftime("%c")
    starttime = time.time()

    # zookeeper client
    if zk_nodes is not None:
        zkClient = zk.start_client(zk_nodes)

    if retry and (zk_nodes is not None and zk_path is not None and customer is not None):
        # enable logging success/failure states to zookeeper
        logToZookeeper = True
        lastFailResults = get_failure_details(zkClient, zk_nodes, zk_path, customer)
        if not lastFailResults[0]:
            # if last failure details not successfully retrieved then stop
            return os.EX_SOFTWARE
        else:
            lastFailList = lastFailResults[1]

    else:
        logToZookeeper = False
        lastFailList = None

    # connect to database
    if zk_nodes is not None and zk_path is not None and customer is not None:
        # lookup credentials in ZK
        conn = db_connect.get_conn(zk_nodes, zk_path, customer, zkClient)
    elif hostname is not None and port is not None and database is not None and username is not None:
        # connect via manual parameters
        if password is None or len(password) == 0:
            password = getpass.getpass('Please enter password for {user}: '.format(user=username))
        conn = db_connect.open_conn({'hostname' : hostname, 'port' : port, 'database' : database, 'username' : username, 'password' : password})
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
        # set the global search_path to support the sql in the rates_dict not needing to specify the schema
        # set work_mem within this session to improve performance
        sql = '''SET search_path = "{schema}";'''.format(schema=schema)
        if workmem:
            logger.info('Setting work_mem to {wmem}'.format(wmem=workmem))
            sql += '''SET work_mem = '{wmem}';'''.format(wmem=workmem)
        try:
            execute_sql(conn, sql)
            conn.commit
        except Exception, e:
            conn.rollback()
            logger.exception(e)
            return os.EX_SOFTWARE

        # check if PLOs previously created in this db. if not create the tables
        ploResult = create_usdm_tables(conn, schema, USDMTableDict)
        if not ploResult:
            return os.EX_SOFTWARE

        # copy scorecard data from source (study-specific mode)
        if copyschema is not None and copystudy is not None:
            if not backfill:
                siteCols = get_table_col_list(USDMTableDict[ploSiteScoreTbl]['columns_dictionary'], ploSiteScoreTbl)
                studyCols = get_table_col_list(USDMTableDict[ploStudyScoreTbl]['columns_dictionary'], ploStudyScoreTbl)
                logCols = get_table_col_list(USDMTableDict[eventLogTbl]['columns_dictionary'], eventLogTbl)

                # copy the tables
                if not copystudy_data(conn, copyschema, copystudy, schema, ploSiteScoreTbl, ploSiteScoreTbl, siteCols):
                    return os.EX_SOFTWARE
                if not copystudy_data(conn, copyschema, copystudy, schema, ploStudyScoreTbl, ploStudyScoreTbl, studyCols):
                    return os.EX_SOFTWARE
                if not copystudy_data(conn, copyschema, copystudy, schema, eventLogTbl, eventLogTbl, logCols):
                    return os.EX_SOFTWARE
            else:
                logger.warning('Existing scorecard data not copied in backfill mode') # if doing a full backfill there is no reason to copy existing data as we will recalculate everything.

        # get list of current kpi names/categories from KPIRatesDict and build sql string 
        # that will be used to create the temp_date_series object
        kpiSQL = ''
        studyList = get_studies(conn, schema)
        if studyList is not None:
            kpiSQL =' UNION ALL '.join('SELECT \'{studyid}\'::text as studyid, \'{kpiid}\'::text AS kpiid, \'{kpicategory}\'::text as kpicategory'.format(studyid=studyid, kpiid=kpiid, kpicategory=KPIRatesDict[kpiid]['category']) for studyid in studyList for kpiid in KPIRatesDict)
        else:
            # No study ids found which is typically unexpected except in certain use cases. Continue processing so that table properties/constraints are still created.
            # This will not result in any data updates to the PLOs
            kpiSQL = 'SELECT null::text as studyid, null::text as kpiid, null::text as kpicategory'
            logger.warning('No Study IDs found')

        # drop (if necessary) and recreate temporary objects from the dictionary
        if not drop_temp_objects(conn, schema, [tempSiteScoreTbl, tempStudyScoreTbl]):
            return os.EX_SOFTWARE
        if not build_temp_objects(conn, schema, backfill, kpiSQL, filterstr, dayslimit):
            return os.EX_SOFTWARE

        # drop (if necessary) and recreate temp scores tables
        if not create_table_from_def(conn, schema, ploPropertiesDict, ploStudyScoreTbl, tempStudyScoreTbl):
            return os.EX_SOFTWARE
        if not create_table_from_def(conn, schema, ploPropertiesDict, ploSiteScoreTbl, tempSiteScoreTbl):
            return os.EX_SOFTWARE

        # checking the deleted sites function
        checkdeletedSites = detect_deleted_site(conn, schema, ploSiteScoreTbl)

        # Block to insert data in to tempSiteScoreTbl with impacted studies data fetched from rpt_site_cro_scores studies because of sites removed from site table.
        if len(checkdeletedSites) > 0 and checkdeletedSites is not None:
            if not insert_into_tempsitetbl(conn, schema, checkdeletedSites, ploSiteScoreTbl, tempSiteScoreTbl,tempDateSeriesDailyTbl):
                return os.EX_SOFTWARE

        # calculate the rates and insert into the temp scores tables
        rateCalcResult = calculate_rates(conn, schema, tempSiteScoreTbl, eventLogTbl, backfill, dayslimit, lastFailList, debug, disablechecks)
        if not rateCalcResult[0]:
            # if calculate_rates returns false we still want to proceed with processing those KPIs that succeeded
            # set the exit code in order to trigger a retry
            exitCode = os.EX_SOFTWARE
            if logToZookeeper:
                # log failed state to ZK for future retries
                if not log_failure_details(zkClient, zk_nodes, zk_path, customer, rateCalcResult[1]):
                    return os.EX_SOFTWARE                    
        
        if not rollup_study_rates(conn, schema, tempSiteScoreTbl, tempStudyScoreTbl):
            return os.EX_SOFTWARE

        # loop through tables and apply constraints/merge data as applicable
        for table in USDMTableDict:
            tempTable = USDMTableDict[table]['temp_table']
            columnsDict = USDMTableDict[table]['columns_dictionary']
            constraintsDict = USDMTableDict[table]['constraints_dictionary']
            merge_data = USDMTableDict[table]['merge_data'] # this is used to turn off the "merge" logic for a table (ex. the log table)

            if not apply_constraints_merge_data(conn, schema, tempTable, table, columnsDict, constraintsDict, backfill, merge_data, checkdeletedSites):
                # proceed to next PLO even on failure
                exitCode = os.EX_SOFTWARE

        # run checks after data updates
        checkResults = log_events_antipatterns(conn, schema, ploSiteScoreTbl, ploStudyScoreTbl, eventLogTbl, 'AFTER', backfill, dayslimit, disablechecks)
        if not checkResults[0]:
            # at least one check failed
            # set exit code and proceed
            exitCode = os.EX_SOFTWARE

        if exitCode is None:
            # process successful execution
            exitCode = os.EX_OK
            if logToZookeeper:
                # log successful start time to ZK
                if not log_successful_run(zkClient, zk_nodes, zk_path, customer, start):
                    return os.EX_SOFTWARE
        
        endtime = time.time()
        runtime = endtime - starttime
        logger.info('Total execution time (seconds) = {runtime}'.format(runtime=runtime))

        return exitCode

    finally:
        if zkClient is not None:
            zk.stop_client(zkClient)
        if cur is not None:
            cur.close()
        if conn is not None:
            if  conn.get_transaction_status() == psycopg2.extensions.TRANSACTION_STATUS_INERROR:
                # if true then execution of a sql has generated an exception and we want to rollback
                # this is especially important in order to proceed with drop_temp_objects
                conn.rollback()
            if not debug:
                # always drop temporary objects as part of cleanup except if debug is enabled
                logger.info('Dropping temporary objects')
                drop_temp_objects(conn, schema, [tempSiteScoreTbl, tempStudyScoreTbl])
                conn.commit()
            conn.close()

if __name__ == '__main__':
    args = docopt(__doc__, version='Scorecard Post-processing 1.0')
    exCode = main(args['--schema'], args['--zk_nodes'], args['--zk_path'], args['--customer'], args['--hostname'], args['--port'], args['--database'], args['--username'], args['--password'], args['--filter'], args['--days_limit'], args['--logdir'], args['--backfill'], args['--debug'], args['--retry'], args['--disable_checks'], args['--workmem'], args['--copyschema'], args['--copystudy'])
    print 'Exit Code: {code}'.format(code=exCode)
    sys.exit(exCode)
