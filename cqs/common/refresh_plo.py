#!/usr/bin/env python

import sys
from os import path
import os
import glob
import datetime 

current_time = datetime.datetime.now().time() 

vSchema = 'cqs_demo'
vRefreshScript = './refresh_plo_' + current_time.isoformat() + '.sql'

vPloFiles = glob.glob('./plo_*')

o = open(vRefreshScript,'w')
o.write('-- Refresh Script for Presentation Layer Objects' + "\n")
o.write('-- Generated on: ' + current_time.isoformat() + "\n\n")
o.write('set search_path to ' + vSchema + ';')

print "Include PLO Files:"
for i in vPloFiles:
	vNewTable = i.replace('./plo_','')
	vNewTable = vNewTable.replace('.sql','')
	print " >> " + vNewTable 
	vNewLines = "\n\n" + '-- ' + vNewTable + "\n"
	vNewLines += 'delete from ' + vNewTable + ';' + "\n\n"
	vNewLines += 'insert into ' + vNewTable + '  ' + "\n" 
	f = open(i,'r')
	mylines = f.read()
	vNewLines +=  mylines
	f.close()
	o.write(vNewLines + "\n")

o.close()

print 'more ' + vRefreshScript

