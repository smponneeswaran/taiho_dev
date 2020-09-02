#!/usr/bin/env python

# This script builds the SQL file to create the CCDM objects using the client-specific mappings (does not include table constraints or PLOs)
# Must be located in directory with /layerX and /cleanup subdirectories

import sys
from os import path
import os
import glob
import datetime 
import re

# Passed Parameters
# 1. CQS Schema name
# 2. Include slashes (y/n)?
# 3. Test Mode (limit to 100 records) (y/n)
if len(sys.argv) < 3:
	print 'Usage: ' + str(sys.argv[0]) + ' <CQS Schema Name>' + ' <Include Slashes? (y/n)> <Test Mode? (y/n)>' + "\n"
	exit()

vSchema = str(sys.argv[1]).lower()
vIncludeSlashes = str(sys.argv[2]).lower()
vSetTestMode = str(sys.argv[3].lower())

if vIncludeSlashes.lower() =='y':
	vSlashes = True
else:
	vSlashes = False

vTestLimit = ''

if vSetTestMode.lower() == 'y':
	vTestLimit = ' limit 100'
 
current_time = datetime.datetime.now().time() 

vBuildScript = './bld_ccdm.sql'

vCCDM2Files = sorted(glob.glob('cleanup/*.sql'))

o = open(vBuildScript,'w')
o.write('-- Build Script for CCDM Objects' + "\n")
o.write('-- Generated on: ' + current_time.isoformat() + "\n\n")
o.write('set search_path to ' + vSchema + ';' + "\n")

print "Include CCDM Files:"
j = 1
while os.path.isdir("layer"+str(j)):

	vCCDMFiles = sorted(glob.glob('layer'+str(j)+'/*.sql'))
	for i in vCCDMFiles:
		vNewTable = re.sub('layer./', '', i)
		vNewTable = vNewTable.replace('.sql','')
		print " >> " + vNewTable 
		vNewLines = "\n\n" + '-- ' + vNewTable + "\n"
		vNewLines += 'DO $$' + "\n" + 'DECLARE' + "\n" + 'timevar timestamp;' + "\n" + 'BEGIN' + "\n" + 'timevar := now();' + "\n" + 'RAISE NOTICE \'' + vNewTable + ' --> START time=%\', timevar;' +  "\n" + "END" + "\n" + '$$;' + "\n\n"
	 
		vNewLines += 'BEGIN;' + "\n"
		vNewLines += 'drop table if exists ' + vNewTable + ' cascade;' + "\n\n"
		vNewLines += 'create table ' + vNewTable + ' as ' + "\n" 
		f = open(i,'r')
	
		for ml in (f):
			nl = ml.replace('/*KEY','')
			nl = nl.replace(';\n',vTestLimit + ';\n')
			vNewLines += nl.replace('KEY*/','')
	
		f.close()

		vNewLines += 'END;'
		if vSlashes:
			vNewLines += "\n/\n"
		o.write(vNewLines + "\n")
	j = j + 1

if vSetTestMode.lower() != 'y':
	print "Include CCDM2 Files:"
	for i in vCCDM2Files:
		vNewTable = i.replace('cleanup/','')
		vNewTable = vNewTable.replace('.sql','')
		print " >> " + vNewTable 
		vNewLines = 'DO $$' + "\n" + 'DECLARE' + "\n" + 'timevar timestamp;' + "\n" + 'BEGIN' + "\n" + 'timevar := now();' +  "\n" + 'RAISE NOTICE \'' + vNewTable + ' --> START time=%\', timevar;' +  "\n" + "END" + "\n" + '$$;' + "\n\n"     
		vNewLines += "\n\n" + '-- ' + vNewTable + "\n"
		f = open(i,'r') or die
	

		for ml in (f):
			if vSlashes:
				nl = ml.replace(';\n',';\n/\n')
			else:
				nl = ml

			vNewLines += nl

		f.close()
		o.write(vNewLines + "\n")

	o.close()

print 'more ' + vBuildScript

