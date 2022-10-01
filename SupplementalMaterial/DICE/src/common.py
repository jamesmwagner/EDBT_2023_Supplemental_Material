# **********************************************************************
# Copyright 2016. James Wagner and Alexander Rasin. All rights reserved. 
# This work is distributed pursuant to the Software License for DICE
# (Database Image Content Explorer), dated Aug. 15, 2016. For terms and
# conditions, please see the license file, which is included in this
# distribution.
# **********************************************************************

import glob
import os
import time

MB = 2**20

def get_parameter_files(DICE_path):
	param_dir = "%s\scripts\ParameterFiles/" % (DICE_path)
	return glob.glob("%s*" % param_dir)	
	
def get_parameters(pf, config):
	params = {"context": {"name": config.get('Namespace', 'name'), "uri": config.get('Namespace', 'uri')}}	
	param = []
	for line in open(pf, "r").readlines():
		if line.find(":") < 1:
			continue
		param = [x.strip() for x in line.split(":", 1)]
		try:
			param[1] = eval(param[1])
		except:
			pass
		params[param[0]] = param[1]
	return params	
	
def get_input_files(dir):
	return glob.glob("%s*" % dir)

def verify_dir(dir):
	if not os.path.exists(dir):
		os.makedirs(dir)

def checkpoint1(pfiles, infiles, config):
	print "Parameter Files:"
	for pfile in pfiles:
		print "\t%s" % pfile
	print "Input Files"
	for infile in infiles:
		print "\t%s" % infile.replace(config.get('Basic_Values', 'input_files'), "")

		
def checkpoint2(pfile, infile, config):
	line = 20*"*"
	print "\n%s\nParameter File: %s:" % (line, pfile)
	print "Input File: %s\n%s" % (infile.replace(config.get('Basic_Values', 'input_files'), ""), line)

def checkpoint3(pfiles, infiles, indir):
	print "Parameter Files:"
	for pfile in pfiles:
		print "\t%s" % pfile
	print "Input Files"
	for infile in infiles:
		print "\t%s" % infile.replace(indir, "")

		
def checkpoint4(pfile, infile, indir):
	line = 20*"*"
	print "\n%s\nParameter File: %s:" % (line, pfile)
	print "Input File: %s\n%s" % (infile.replace(indir, ""), line)	
	
def status_bar(high_addr, file_sz, start, current):
	"|####------| 450/1000  45% [elapsed: 00:04 left: 00:05, 99.15 iters/sec]"
	elapsed = time.strftime("%H:%M:%S", time.gmtime(current - start))
	
	if high_addr > file_sz:
		high_addr = file_sz
	
	percent = float(high_addr)/float(file_sz) * 100
	
	speed = (high_addr/MB)/(current - start)
	
	bar = (int(20 * percent/100) * "=").ljust(20)
	
	remaining = time.strftime("%H:%M:%S", time.gmtime((current - start)/percent * 100 - (current - start)))
	
	
	string = "|%s| %s/%sMBs  %s%%   [elapsed: %s remaining: %s, %s MBs/sec] \r" % (bar, str(int(high_addr/MB)).rjust(6), int(file_sz/MB), int(percent), elapsed, remaining, int(speed))
	print string,
	
def print_status(high_addr, file_sz, start):
	status = "%s/%s MBs searched. %s minutes elapsed \r" % (round(high_addr/MB), round(file_sz/MB), round((time.time()-start)/60,2))
	print status, 		