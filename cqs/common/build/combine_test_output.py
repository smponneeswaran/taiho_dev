#!/usr/local/bin/python

import pdfkit
import os
import re
newfiles = []
import sys

mydirectory = sys.argv[1]
myoutfilename = sys.argv[2]

files = [f for f in os.listdir(mydirectory) if re.match(r'test.*\.html',f)]
for f in files:
	vnum = re.search(r'\d+', f).group(0)
	newfiles.append('{:03d}'.format(int(vnum)) + '~' + f)

sorted_files = sorted(newfiles,key=str.lower)
mf = ['main_report.html']

groomed_files = [mydirectory + 'main_report.html']

for f in sorted_files:
	newfile = mydirectory + re.search(r'.*\~(.*\.html)', f).group(1) 
	print '**'+newfile+'**'
	groomed_files.append(newfile)

options = {
	'orientation': 'Landscape',
	'page-size': 'Letter',
	'zoom':.95,
	'footer-right': '[page]/[topage]',
	'header-center': '[webpage]'
}


pdfkit.from_file(groomed_files,myoutfilename,options=options)


