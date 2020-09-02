#!/usr/bin/env python

# This script builds the SQL file to create the PLO objects 
# Must be located in directory with /plo subdirectory

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

vBuildScript = './bld_plo.sql'

vPloFiles = glob.glob('plo/*.sql')

o = open(vBuildScript,'w')
o.write('-- Build Script for Presentation Layer Objects' + "\n")
o.write('-- Generated on: ' + current_time.isoformat() + "\n\n")
o.write('set search_path to ' + vSchema + ';')

print "Include PLO Files:"
for i in vPloFiles:
	vNewTable = i.replace('plo/','')
	vNewTable = vNewTable.replace('.sql','')
	print " >> " + vNewTable 
	vNewLines = "\n\n" + '-- ' + vNewTable + "\n"
	vNewLines += 'drop table if exists ' + vNewTable + ';' + "\n\n"
	f = open(i,'r')
	mylines = f.read()
	vNewLines +=  mylines
	f.close()
	if vSlashes:
		vNewLines +=  "/\n"
	o.write(vNewLines)

o.close()

print 'more ' + vBuildScript

