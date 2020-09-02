#!/usr/bin/env python

"""
Usage:
    plo_post_processing_wrapper.py --zk_nodes=<nodes> --zk_path=<path> --customer=<customer> --schema=<schema> [--filter=<where_clause>] [--days_limit=<num_days>] [--logdir=<dir>] [--workmem=<intMB>] [--backfill] [--debug] [--retry] [--disable_checks] [(--copyschema=<schema> --copystudy=<studyid>)] [--starttimelog]  [--num_processes=<num_processes>]
    plo_post_processing_wrapper.py (-h | --help)
    plo_post_processing_wrapper.py --version
Description: Wrapper script used to execute individual PLO post-processing steps
Options:
    -h or help              Show this screen
    version                 Show version
    zk_nodes=<nodes>        Comma separated string of zookeeper hosts (used for retrieval of database credentials)
    zk_path=<path>          Zookeeper path where customer entry is found (ex. /com/comprehend/panacea)
    customer=<customer>     Zookeepr entry containing clinicalDBparameters (ex. client name)
    schema=<schema>         Database schema name
    
    Specific to Scorecard Post-Processing:
    --------------------------------------
    filter=<where_clause>   If included use to filter data load. Must be in standard where clause predicate format (ex. studyid='S1000' and siteid='001')
                            	Currently supports the following columns: studyid, croid, siteid, kpiid, startdate, enddate
    days_limit=<num_days>   If included do not process KPIs for dates that are older than current_date - num_days
                            	Generate a warning and continue to process but return an error code
    logdir=<dir>            If included generate log file in directory path
    backfill                Enable backfill mode (otherwise default to incremental mode)
    debug                   Enable debug mode (temporary tables will not be dropped on cleanup)
    retry                   Enable retry functionality (for use with adapters)
    workmem=<intMB>         Set postgresql parameter work_mem at a session level (used for larger datasets)
    disable_checks          Disable purely informational soft-validation checks (i.e. checks that do not apply protection filters)
    copyschema              If included copy all historical scorecard data from <copy_schema> into <schema> before merging - used for study-specific build
    copystudy               The study ID to use when copying data - used for study-specific builds


    Specific to PLO Post-Processing:
    --------------------------------------
    starttimelog    Enable time logging for successful execution of plo post processing script
    
    Specific to Oversigh scorecard Post-Processing:
    --------------------------------------
    num_processes=<num_processes>   Optional: Number of processes
"""

import sys, os
from docopt import docopt
import plo_post_proccessing_script, scorecard_post_processing, oversight_scorecard_post_processing

# main method that controls the logic
def main(zk_nodes, zk_path, customer, schema, filterstr=None, dayslimit=None, logdir=None, backfill=False, debug=False, retry=False, starttimelog=False, workmem=None, disablechecks=False, copyschema=None, copystudy=None, num_processes=1):

	# execute PLO post-processing
	ploCode = plo_post_proccessing_script.main(zk_nodes, zk_path, customer, schema, logdir, debug, starttimelog)
	# execute scorecard post processing
	# "None" parameters related to non-zookeeper, manual authentication which is not currently supported by this script
	# as zookeeper authentication currently required by plo_post_proccessing_script.py
	scoreCode = scorecard_post_processing.main(schema, zk_nodes, zk_path, customer, None, None, None, None, None, filterstr, dayslimit, logdir, backfill, debug, retry, disablechecks, workmem, copyschema, copystudy)
	oversightCode = oversight_scorecard_post_processing.main(schema, zk_nodes, zk_path, customer, num_processes, logdir, debug, retry)
	return max(ploCode, scoreCode, oversightCode)

if __name__ == '__main__':
    args = docopt(__doc__, version='PLO post-processing wrapper 1.0')
    exCode = main(args['--zk_nodes'], args['--zk_path'], args['--customer'], args['--schema'], args['--filter'], args['--days_limit'], args['--logdir'], args['--backfill'], args['--debug'], args['--retry'], args['--starttimelog'], args['--workmem'], args['--disable_checks'], args['--copyschema'], args['--copystudy'], args['--num_processes'])
    print 'Exit Code: {code}'.format(code=exCode)
    sys.exit(exCode)

