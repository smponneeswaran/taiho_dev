#!/usr/bin/env python

# This script builds to the SQL file to add the constraints to the existing CCDM objects
# Must be located in directory with /ccdm_constraints subdirectory

import sys
from os import path
import os
import glob
import datetime 
import re

# Passed Parameters
# 1. CQS Schema name
# 2. Include slashes (y/n)?
if len(sys.argv) < 3:
	print 'Usage: ' + str(sys.argv[0]) + ' <CQS Schema Name>' + ' <Include Slashes? (y/n)>' + "\n"
	exit()

vSchema = str(sys.argv[1]).lower()
vIncludeSlashes = str(sys.argv[2]).lower()

if vIncludeSlashes.lower() =='y':
	vSlashes = True
else:
	vSlashes = False

current_time = datetime.datetime.now().time() 

vBuildScript = './bld_ccdm_constraints.sql'

o = open(vBuildScript,'w')
o.write('-- Build Script for CCDM Constraints' + "\n")
o.write('-- Generated on: ' + current_time.isoformat() + "\n\n")
o.write('set search_path to ' + vSchema + ';' + "\n")

# Create statements
vCreateFiles = glob.glob('ccdm_constraints/create_ccdm_*.sql')
vCreateFiles = sorted(vCreateFiles)

print "\nInclude Create Files:"
for i in vCreateFiles:
	vNewFile = i.replace('ccdm_constraints/', '')
	vNewLines = '-- ' + vNewFile + "\n"
	f = open(i,'r')
	vNewLines += f.read()
	if vSlashes:
		vNewLines = vNewLines.replace(";\n",";\n/\n")
	o.write(vNewLines + "\n")
	f.close()
	print " >> " + vNewFile

o.close()

print 'more ' + vBuildScript

