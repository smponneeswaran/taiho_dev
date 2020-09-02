#!/usr/bin/env python

# This is a customized version of the pgsanity package (https://github.com/markdrago/pgsanity)
# The original version is designed to be called from the command line. 
# This version can be importted as a python package.
#
# Revision History: 15-Sep-2016 Adam Kaus - Initial custom version
#                   05-Oct-2016 Adam Kaus - Remove UTF8 byte object mark from beginning of files
#

from __future__ import print_function
from __future__ import absolute_import
import argparse
import sys
import codecs

from pgsanity_cust import sqlprep
from pgsanity_cust import ecpg

def get_config(argv):
    parser = argparse.ArgumentParser(description='Check syntax of SQL for PostgreSQL')
    parser.add_argument('files', nargs='*', default=None)
    return parser.parse_args(argv)


def check_file(filename=None, show_filename=False):
    """
    Check whether an input file is valid PostgreSQL. If no filename is
    passed, STDIN is checked.

    Returns a status code: 0 if the input is valid, 1 if invalid.
    """
    # either work with sys.stdin or open the file
    if filename is not None:
        with open(filename, "r") as filelike:
            sql_string = filelike.read()
    else:
        with sys.stdin as filelike:
            sql_string = sys.stdin.read()

    # this line commented out to force failure if file encoded in UTF8
    #sql_string = sql_string.lstrip(codecs.BOM_UTF8) # strip UTF8 BOM if exists

    success, msg = check_string(sql_string)

    # report results
    #result = 0 # ACK - changing to string
    result = None 
    if not success:
        # possibly show the filename with the error message
        prefix = ""
        #if show_filename and filename is not None: # ACK - Always prefix for our purposes
        if filename is not None: 
            prefix = filename + ": "
        #print(prefix + msg) # ACK - Commented out. Value will be returned instead of printed
        #result = 1 # ACK - Changing to return message instead of code
        result = prefix + msg

    return result 

def check_string(sql_string):
    """
    Check whether a string is valid PostgreSQL. Returns a boolean
    indicating validity and a message from ecpg, which will be an
    empty string if the input was valid, or a description of the
    problem otherwise.
    """
    prepped_sql = sqlprep.prepare_sql(sql_string)
    success, msg = ecpg.check_syntax(prepped_sql)
    return success, msg

def check_files(files):
    if files is None or len(files) == 0:
        return check_file()
    else:
        # show filenames if > 1 file was passed as a parameter
        show_filenames = (len(files) > 1)

        # ACK - updated to list instead of int
        #accumulator = 0 
        accumulator = []
        for filename in files:
            # ACK - updated to use list
            #accumulator |= check_file(filename, show_filenames) 
            error_msg = check_file(filename, show_filenames)
            if error_msg is not None:
                accumulator.append(error_msg)
        #return (check_file(filename, show_filenames))
        return accumulator

#def main():
def main(files):
    #ACK bypass get_config function to parse command line parameters and just pass list object
    #config = get_config()
    #return check_files(config.files)
    return check_files(files)

# ACK - remove this since intention is to import as module and not call directly
#if __name__ == '__main__':
#    try:
#        #sys.exit(main())
#        sys.exit(main(sys.argv[1:]))
#    except KeyboardInterrupt:
#        pass
