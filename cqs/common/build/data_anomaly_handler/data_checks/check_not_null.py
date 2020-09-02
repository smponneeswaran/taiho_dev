#!/usr/bin/env python

# Created By: Adam Kaus
# Created On: 19-Dec-2016
# Revision History: 19-Dec-2016 ACK Initial version
# Notes:
#       - This data anomaly check loops through a list of not null constraints and returns records that violate them
#       - Uses temp_schema as a map of constraints

import sys, os
from os import path
sys.path.append( path.dirname( path.dirname( path.dirname( path.dirname( path.dirname( path.dirname( path.dirname( path.abspath(__file__) ) ) ) ) ) ) ) ) # /ccdm/resources is 6 directories above
from utils import schema_utils

# return list of table.columns with not null constraints
def get_notnull_cols(conn, schema):
    lCur = conn.cursor()
    lSQL = '''select c.relname as table_name,
                        a.attname as column_name
                from pg_class c,
                    pg_namespace n,
                    pg_attribute a,
                    information_schema.columns col
                where c.relnamespace = n.oid and
                    a.atttypid <> 0::oid and
                    c.oid = a.attrelid and
                    a.attnum > 0 and
                    col.table_schema = n.nspname and
                    col.table_name = c.relname and
                    col.column_name = a.attname and
                    lower(n.nspname) = lower('{schema}') and
                    col.is_nullable = 'NO' 
                order by c.relname'''.format(schema=schema)
    lCur.execute(lSQL)
    lResult = lCur.fetchall()
    lCur.close()
    return lResult

# main function
def main(conn, schema, temp_schema):
    cur = None
    master_list = []

    try:
        cur = conn.cursor()
        # get not null table.columns
        notnull_cols = get_notnull_cols(conn, temp_schema)

        curTable = ''
        where = ''
        select_list = ''
        col_list = ''
        pk_cols = []

        # loop through list and query each table once (for performance) looking for any instances of not null columns with null values
        for rec in notnull_cols:
            table = rec[0]
            column = rec[1]

            if table == curTable:
                # same table as last record so append to list and keep looping
                where += ' OR {column} is null'.format(column=column) 
                col_list += ', {column}'.format(column=column)

            else:
                if len(curTable) > 0: 
                    # we moved on to a new table so execute the query for the previous table/columns
                    sql = 'select distinct {pk_columns} from {schema}.{table} where {where}'.format(pk_columns=select_list, schema=schema, table=curTable, where=where)
                    cur.execute(sql)
                    if cur.rowcount > 0:
                        anomaly_recs = cur.fetchall()
                        for rec in anomaly_recs:
                            anomaly_dict = {}
                            anomaly_dict['table'] = curTable
                            anomaly_dict['key_columns'] = pk_cols
                            anomaly_dict['key_values'] = list(rec)
                            anomaly_dict['anomaly_columns'] = [col_list]
                            anomaly_dict['anomaly_values'] = None
                            anomaly_dict['keep_one_record'] = False
                            anomaly_dict['message'] = 'Not Null constraint(s) violated on table {table}. Null values found in one or more of the following columns: {columns}'.format( table=curTable, columns=col_list )
                            master_list.append(anomaly_dict)

                # new table so reset and start to rebuild variables
                curTable = table
                select_list = ''
                col_list = ''
                pk_cols = schema_utils.get_pk_columns(conn, temp_schema, table)

                for col in pk_cols:
                    if len(select_list) == 0:
                        select_list += col
                    else:
                        select_list += ' ,{col}'.format(col=col)

                where = '{column} is null'.format(column=column) 
                col_list = '{column}'.format(column=column)

        return master_list

        '''for col in notnull_cols:
            table = col[0]
            column = col[1]
            select_list = ''

            # get list of primary key columns for this table
            pk_cols = schema_utils.get_pk_columns(conn, temp_schema, table)
            for col in pk_cols:
                if len(select_list) == 0:
                    select_list += col
                else:
                    select_list += ' ,{col}'.format(col=col)

            # identify records violating constraints
            sql = 'select distinct {pk_columns} from {schema}.{table} where {notnull_column} is null'.format(pk_columns=select_list, schema=schema, table=table, notnull_column=column)
            cur.execute(sql)
            if cur.rowcount > 0:
                anomaly_recs = cur.fetchall()
                for rec in anomaly_recs:
                    anomaly_dict = {}
                    anomaly_dict['table'] = table
                    anomaly_dict['key_columns'] = pk_cols
                    anomaly_dict['key_values'] = list(rec)
                    anomaly_dict['anomaly_columns'] = [column]
                    anomaly_dict['anomaly_values'] = None
                    anomaly_dict['keep_one_record'] = False
                    anomaly_dict['message'] = 'Not Null constraint violated on {table}.{column}. Null values found'.format( table=table, column=column )
                    master_list.append(anomaly_dict)

        return master_list'''

    except Exception, e:
        print 'Error check_not_null.py: {err}'.format(err=e)
        exit(1)

    finally:
        conn.commit()
        if cur is not None:
            cur.close()


