#!/usr/bin/env python

# This script builds the SQL file to create the PLO constraints
# Must be located in directory with /plo_constraints subdirectory

import sys
from os import path
import os
import glob
import datetime 

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

vBuildScript = './bld_plo_constraints.sql'

o = open(vBuildScript,'w')
o.write('-- Build Script for Presentation Layer Objects Constraints' + "\n")
o.write('-- Generated on: ' + current_time.isoformat() + "\n\n")
o.write('set search_path to ' + vSchema + ';')

# Create statements now
vCreateFiles = glob.glob('plo_constraints/create_plo_*.sql')
vCreateFiles = sorted(vCreateFiles)

print "\nInclude Create Files:"
for i in vCreateFiles:
	vNewFile = i.replace('plo_constraints/', '')
	vNewLines = '-- ' + vNewFile + "\n"
	f = open(i,'r')
	vNewLines += f.read()
	if vSlashes:
		vNewLines = vNewLines.replace(';\n',';\n/\n')
	o.write(vNewLines + "\n")

	print " >> " + vNewFile

o.close()

print 'more ' + vBuildScript

