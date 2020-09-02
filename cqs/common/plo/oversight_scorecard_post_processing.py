#!/usr/bin/env python

"""
Usage:
    oversight_scorecard_post_processing.py --schema=<schema> --zk_nodes=<nodes> --zk_path=<path> --customer=<customer> [--num_processes=<num_processes>] [--logdir=<logdir>] [--debug] [--retry]
    oversight_scorecard_post_processing.py (-h | --help)
    oversight_scorecard_post_processing.py --version
Options:
    -h or help                      Show this screen
    version                         Show version
    schema=<schema>                 Database schema name
    zk_nodes=<nodes>                Comma separated string of zookeeper hosts (used for retrieval of database credentials)
    zk_path=<path>                  Zookeeper path where customer entry is found (ex. /com/comprehend/panacea)
    customer=<customer>             Zookeeper entry containing clinicalDBParameters (ex. client name)
    num_processes=<num_processes>   Optional: Number of processes
    logdir=<logdir>                 Optional: Directory for keeping logs
    debug                           Optional: Switch level of logs between DEBUG and INFO
    retry                           Enable retry functionality (for use with adapters)
"""
#
# Oversight Scorecard Post Processing Script
# 1. plo_post_processing_wrapper.py will be calling this script on the main() function
# 2. this script can be executed with command line
# Both of the approaches accept 4 mandatory arguments which are used for get ZK / DB connection
# and 2 optional arguments that defines number of processes will be running in parallel and output dir to store logs
#
from os import path
from pathlib import Path
from docopt import docopt
from multiprocessing import Pool
import os
import sys
import time
import logging
from logging.handlers import TimedRotatingFileHandler
import psycopg2.extras as pe

sys.path.append(str(Path(path.abspath(__file__)).parents[4] / 'utils'))  # /ccdm/resources/utils
sys.path.append(str(Path(path.abspath(__file__)).parents[4] / 'validation' / 'cqs' / 'global_cqs'))  # /ccdm
import db_connect
import zk_connect
import oversight_scorecard_metrics as query_dict

logger = logging.getLogger(__name__)
zk = zk_connect.zk_class()
insertion_template = '''INSERT INTO {schema}.{table} ({columns}) VALUES %s'''

# Zookeeper variables for retry functionality
_zk_log_path = '/CQS/cqs/oversight_scorecard_post_processing'
_zk_failure_node = 'LastFailureStateOversightScoreCardPostProcessing'
_zk_last_success_time_node = 'LastSuccessfulOversightScorecardPostProcessingRunTime'


# Logger of this module is defined to display also the process PID, since we use multiprocessing.
def init_logger(schema, customer, debug=False, logdir=None):
    logger.propagate = False

    if debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logger.setLevel(log_level)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)-s - [pid:%(process)-d] - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if logdir is not None:
        filename = (os.path.split(__file__)[1]).split('.', 1)[0]
        log_filename = "{path}/{filename}_{customer}_{schema}.log".format(path=logdir,
                                                                          filename=filename,
                                                                          customer=customer.replace("/", "-"),
                                                                          schema=schema)
        Path(log_filename).touch(exist_ok=True)
        file_handler = TimedRotatingFileHandler(log_filename, when="D", interval=1, backupCount=10, delay=False, utc=True)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s:%(lineno)-5s - %(process)-5d - %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)


# A shortcut for getting current time in ms
def millis():
    return int(round(time.time() * 1000))


# An util for appending value in tuple
def tuple_append(t, v):
    l = list(t)
    l.append(v)
    return tuple(l)


# Append invalid_value in single row
def get_updated_row(result, rules, numerator_index, denominator_index):
    numerator = result[numerator_index]
    denominator = result[denominator_index]
    invalid_value = get_invalid_value(numerator, denominator, rules)
    return tuple_append(result, invalid_value)


def get_invalid_value(numerator, denominator, rules):
    if "negative_values" in rules and any((n < 0 and n != None) for n in [numerator, denominator]):
        return True
    if "greater_numerator" in rules and numerator > denominator:
        return True
    return False


def get_updated_columns_and_rows(fetched_rows, columns):
    rules = query_dict.get_validation_rules(fetched_rows[0][0])
    updated_columns = [x.name for x in columns]
    updated_columns.append("invalidvalue")

    numerator_index = updated_columns.index("numerator")
    denominator_index = updated_columns.index("denominator")

    rows = []
    for item in fetched_rows:
        rows.append(get_updated_row(item, rules, numerator_index, denominator_index))

    return updated_columns, rows


# Use global zk_info, schema, create new zookeeper connection and then database connection.
# This function will be mapped into a single process
# Each process returns None if success, returns metric_id if failed on fetching or inserting step
def execute_fetch_and_insert(sql_with_args):
    execute_start_time = millis()
    sql, schema, zk_nodes, zk_path, customer = sql_with_args
    conn = None
    zk_client = None
    metric_id = None
    try:
        metric_id = sql["metric_id"]
        zk_client = zk.start_client(zk_nodes)
        conn = db_connect.get_conn(zk_nodes, zk_path, customer, zk_client)
        cur = conn.cursor()
        # Start of fetch
        cur.execute("set search_path = '{s}';".format(s=schema))
        cur.execute(sql["query"])
        if cur.rowcount > 0:  # in the case of a ddl statement this will be 0
            fetched_rows = cur.fetchall()
        else:
            fetched_rows = []
        columns = cur.description
        cur.close()
        # End of fetch
        logger.info("{metric_id} - {size} fetched in {delta} ms".format(metric_id=metric_id,
                                                                        size=len(fetched_rows),
                                                                        delta=millis() - execute_start_time))
        logger.debug("{metric_id} - SQL: {q}".format(metric_id=metric_id,
                                                     q=sql["query"]))

        # Start of insert
        execute_start_time = millis()
        if not fetched_rows:
            logger.info("{metric_id} - 0/0 inserted in {delta} ms".format(metric_id=metric_id, delta=millis() - execute_start_time))
            return None
        updated_columns, updated_rows = get_updated_columns_and_rows(fetched_rows, columns)
        insert_query = insertion_template.format(schema=schema,
                                                 table="rpt_oversight_metrics",
                                                 columns=", ".join(updated_columns))
        cur = conn.cursor()
        pe.execute_values(cur, insert_query, updated_rows)
        inserted = len(updated_rows)
        cur.close()
        if inserted == len(fetched_rows):
            conn.commit()
            logger.info("{metric_id} - {inserted}/{fetched} inserted in {delta} ms".format(metric_id=metric_id,
                                                                                           inserted=inserted, fetched=len(fetched_rows),
                                                                                           delta=millis() - execute_start_time))
            return None
        else:
            logger.warn("{metric_id} - fetched/inserted - {fetched} / {inserted}".format(metric_id=metric_id,
                                                                                         inserted=inserted,
                                                                                         fetched=len(fetched_rows)))
            return metric_id
        # End of insert
    except Exception as e:
        logger.exception(e)
        return metric_id
    finally:
        if zk_client is not None:
            zk.stop_client(zk_client)
        if conn is not None:
            conn.close()


# Log current time of successful run and empty LastFailures
def log_success_time(zk_nodes, zk_path, customer, t):
    last_success_time_node = '{path}/{customer}{zkLogPath}/{node}'.format(path=zk_path, customer=customer, zkLogPath=_zk_log_path, node=_zk_last_success_time_node)
    failure_node = '{path}/{customer}{zkLogPath}/{node}'.format(path=zk_path, customer=customer, zkLogPath=_zk_log_path, node=_zk_failure_node)
    zk_client = zk.start_client(zk_nodes)
    if not zk.zk_node_exists(zk_client, last_success_time_node):
        zk_client.ensure_path(last_success_time_node)
    zk.modify_zk_node(zk_client, last_success_time_node, t)
    if not zk.zk_node_exists(zk_client, failure_node):
        zk_client.ensure_path(failure_node)
    zk.modify_zk_node(zk_client, failure_node, "")


# Log failed metric_ids in a string to zk_failure_node, create node if not exist
def log_failures(zk_nodes, zk_path, customer, failures):
    failure_node = '{path}/{customer}{zkLogPath}/{node}'.format(path=zk_path, customer=customer, zkLogPath=_zk_log_path, node=_zk_failure_node)
    zk_client = zk.start_client(zk_nodes)
    if not zk.zk_node_exists(zk_client, failure_node):
        zk_client.ensure_path(failure_node)
    zk.modify_zk_node(zk_client, failure_node, failures)


# Return a list of failed metric_ids, or empty list if node not exist
def load_failures(zk_nodes, zk_path, customer):
    failure_node = '{path}/{customer}{zkLogPath}/{node}'.format(path=zk_path, customer=customer, zkLogPath=_zk_log_path, node=_zk_failure_node)
    zk_client = zk.start_client(zk_nodes)
    if zk.zk_node_exists(zk_client, failure_node):
        failures = zk.get_zk_val(zk_client, failure_node)
        return (failures.split(","), [])["" is failures]
    else:
        logger.error("Cannot load failures, failure_node not exist")
        return []


# Result is reflected in a list of process results. None for success, metric_id for failure. A list full of None means the whole script is successful
# Missing essential arguments are not accepted, will return code EX_SOFTWARE immediately
def main(schema, zk_nodes, zk_path, customer, num_processes=1, logdir=None, debug=False, retry=False):
    main_started_time = millis()
    if None in (schema, zk_nodes, zk_path, customer):
        logger.error("Cannot start - Invalid arguments. schema:{}, zk_nodes:{}, zk_path:{}, customer:{}".format(schema, zk_nodes, zk_path, customer))
        return os.EX_SOFTWARE

    try:
        init_logger(schema, customer, debug, logdir)

        queries = query_dict.build_query()

        todo_list = []
        if retry:
            failures = load_failures(zk_nodes, zk_path, customer)
            if len(failures) != 0:
                for query in queries:
                    if query["metric_id"] in failures:
                        todo_list.append(query)
            else:
                todo_list = queries
        else:
            todo_list = queries
        todo_list_with_args = []
        for item in todo_list:
            todo_list_with_args.append((item, schema, zk_nodes, zk_path, customer))

        num_processes = num_processes or 1
        logger.info("Number of processes in pool: {}".format(int(num_processes)))

        result_value = []
        if int(num_processes) == 1:
            # bypass multiprocessing and execute serially if only one process is enabled
            # this is a workaround to avoid some issues with the multiprocessing package     
            for item in todo_list_with_args:
                result_value.append(execute_fetch_and_insert(item))
        else:
            pool = Pool(int(num_processes))
            result = pool.map_async(execute_fetch_and_insert, todo_list_with_args, chunksize=1)

            # Block main process and check for unfinished metric tasks
            while not result.ready():
                logger.info("Tasks left: {}".format(result._number_left))
                result.wait(timeout=5)

            pool.close()
            pool.join()
            result_value = result._value         

        if result_value.count(None) == len(result_value):
            log_success_time(zk_nodes, zk_path, customer, time.strftime("%c"))
            logger.info('Exit: {code} - Duration: {delta} ms'.format(code=os.EX_OK, delta=millis() - main_started_time))
            return os.EX_OK
        else:
            failed_metrics = ",".join(filter(None, result_value));
            log_failures(zk_nodes, zk_path, customer, failed_metrics)
            logger.error("Finish with failures on: {}".format(failed_metrics))
            logger.error('Exit: {code} - Duration: {delta} ms'.format(code=os.EX_SOFTWARE, delta=millis() - main_started_time))
            return os.EX_SOFTWARE
    except Exception, e:
        logger.exception(e)
        logger.error('Exit: {code} - Duration: {delta} ms'.format(code=os.EX_SOFTWARE, delta=millis() - main_started_time))
        return os.EX_SOFTWARE


if __name__ == '__main__':
    args = docopt(__doc__, version='Oversight Post Processing 0.1')
    start_time = millis()
    exit_code = main(args['--schema'],
                     args['--zk_nodes'],
                     args['--zk_path'],
                     args['--customer'],
                     args['--num_processes'],
                     args['--logdir'],
                     args['--debug'],
                     args['--retry'])
    logger.info('Exit: {code} - Duration: {delta} ms'.format(code=exit_code, delta=millis() - start_time))
    sys.exit(exit_code)
