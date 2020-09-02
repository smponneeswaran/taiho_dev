#!/usr/bin/env python

# Created By: Adam Kaus
# Created On: 19-Dec-2016
# Revision History: 19-Dec-2016 ACK Initial version
# Notes:
#       - This data anomaly check loops through a list of primary and unique keys and returns records that violate those keys
#       - Uses temp_schema as a map of constraints

import sys, os
from os import path
sys.path.append( path.dirname( path.dirname( path.dirname( path.dirname( path.dirname( path.dirname( path.dirname( path.abspath(__file__) ) ) ) ) ) ) ) ) # /ccdm/resources is 6 directories above
from utils import schema_utils

# get unique indexes for schema
def get_indexes(conn, schema, unique):
    lCur = conn.cursor()
    lSQL = '''select t.relname as table_name,
                    i.relname as index_name
                from pg_class t
                join pg_index ix on (t.oid = ix.indrelid)
                join pg_class i on (i.oid = ix.indexrelid)
                join pg_namespace as ns on i.relnamespace = ns.oid
                where ix.indisprimary is false
                    and ix.indisunique is {unique}
                    and ns.nspname = '{schema}'
                order by
                    t.relname,
                    i.relname'''.format(schema=schema, unique=unique)
    lCur.execute(lSQL)
    lResult = lCur.fetchall()
    lCur.close()
    return lResult

# get columns associated with index obj
def get_index_columns(conn, schema, obj):
    lCur = conn.cursor()
    lSQL = '''select a.attname as column_name
                from pg_class t
                join pg_index ix on (t.oid = ix.indrelid)
                join pg_class i on (i.oid = ix.indexrelid)
                join pg_namespace as ns on i.relnamespace = ns.oid
                join pg_attribute a on (a.attrelid = t.oid
                        and a.attnum = any(ix.indkey))
                where ns.nspname = '{schema}'
                and i.relname = '{idxname}'
                order by a.attname'''.format(schema=schema, idxname=obj)
    lCur.execute(lSQL)
    lResult = lCur.fetchall()
    lCur.close()
    return lResult

# get get primray key constraints for schema (could also be implemented for use with foreign keys)
def get_constraints(conn, schema, contype):
    lCur = conn.cursor()
    lSQL = '''select distinct tc.table_name, 
                    tc.constraint_name,
                    tc.constraint_type
            from information_schema.table_constraints tc
            where upper(tc.constraint_type) in ('{contype}') and
            lower(tc.table_schema) = lower('{schema}')
            order by tc.table_name, constraint_type desc, tc.constraint_name'''.format(schema=schema, contype=contype)
    lCur.execute(lSQL)
    lResult = lCur.fetchall()
    lCur.close()
    return lResult

# get columns assocaited with pk or fk constraint obj
def get_constraint_columns(conn, schema, obj):
    lCur = conn.cursor()
    lSQL = '''select distinct kcu.column_name, 
                    kcu.position_in_unique_constraint
                from information_schema.key_column_usage kcu
                where lower(kcu.table_schema) = lower('{schema}') and
                kcu.constraint_name = '{conname}'
                order by kcu.position_in_unique_constraint'''.format(schema=schema, conname=obj)
    lCur.execute(lSQL)
    lResult = lCur.fetchall()
    lCur.close()
    return lResult

# identifies constraint violations in <schema> using constraints of type <contype> found in <temp_schema>
def find_anomalies(conn, schema, temp_schema, contype):
    return_list = []
    cur = conn.cursor()
    keep_one = False

    if contype.lower() == 'primary key':
        constraint_list = get_constraints(conn, temp_schema, 'PRIMARY KEY')
        keep_one = True
    elif contype.lower() == 'unique key':
        constraint_list = get_indexes(conn, temp_schema, True)
        keep_one = True

    # loop through constraints
    for rec in constraint_list:
        table = rec[0]
        constraint = rec[1]
        
        column_string = ''
        column_list = []

        if contype.lower() == 'primary key':
            constraint_columns = get_constraint_columns(conn, temp_schema, constraint)
        elif contype.lower() == 'unique key':
            constraint_columns = get_index_columns(conn, temp_schema, constraint)

        # build a comma separated select list of columns
        for column in constraint_columns:
            column_list.append(column[0])
            if len(column_string) == 0:
                column_string += column[0]
            else:
                column_string += ', {col}'.format(col=column[0])
        
        sql = 'select {columns} from "{schema}".{table} group by {columns} having count(*) > 1'.format(columns=column_string, schema=schema, table=table)
        cur.execute(sql)
        if cur.rowcount > 0:
            anomaly_recs = cur.fetchall()
            key_columns = []
            key_values = []
            for rec in anomaly_recs:
                # this function is used to look up the primary key columns/values assocaited with record identified
                keys_converted = schema_utils.get_pk_values( conn, temp_schema, schema, table, column_list, list(rec) )
                anomaly_dict = {}
                anomaly_dict['table'] = table
                anomaly_dict['key_columns'] = keys_converted[0]
                anomaly_dict['key_values'] = keys_converted[1]
                anomaly_dict['anomaly_columns'] = column_list
                anomaly_dict['anomaly_values'] = list(rec)
                anomaly_dict['keep_one_record'] = keep_one
                anomaly_dict['message'] = '{contype} "{conname}" on table "{table}" violated. Duplicate records found for columns "{keys}"'.format( contype=contype, conname=constraint, table=table, keys=column_string )
                return_list.append(anomaly_dict)
  
    cur.close
    return return_list

# main function
def main(conn, schema, temp_schema):
    cur = None
    master_list = []

    try:
        cur = conn.cursor()

        # identify primary key anomalies
        pk_anomalies = find_anomalies(conn, schema, temp_schema, 'Primary Key')
        for entry in pk_anomalies:
            master_list.append(entry)
    
        # identify unique key anomalies
        uk_anomalies = find_anomalies(conn, schema, temp_schema, 'Unique Key')
        for entry in uk_anomalies:
            master_list.append(entry)

        return master_list

    except Exception, e:
        print 'Error check_primary_unique_keys.py: {err}'.format(err=e)
        exit(1)

    finally:
        conn.commit()
        if cur is not None:
            cur.close()


