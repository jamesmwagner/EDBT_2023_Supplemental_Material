# **********************************************************************
# Copyright 2016. James Wagner and Alexander Rasin. All rights reserved. 
# This work is distributed pursuant to the Software License for DICE
# (Database Image Content Explorer), dated Aug. 15, 2016. For terms and
# conditions, please see the license file, which is included in this
# distribution.
# **********************************************************************

import sys
import os
import time
import src.common as common
import src.page as pg
import src.output as output
import src.file_search as file_search
import json
import ConfigParser



def hardcoded_params(db, params):
	if db in ("sqlite1k", "sqlite4k"):
		params['gen_pg_id'] = chr(13) 
		params['indx_pg_id'] = chr(2) + chr(0)
		params['indx_pg_id2'] = chr(5) + chr(0)
		params['indx_pg_id3'] = chr(10) #+ chr(0)
		params['recordcnt_size'] = 2
	if db in ("mysql"):	
		params['structureid_posn'] = 36
		params['structureid_size'] = 2
		pass
	if db in ("oracle"):
		params['gen_pg_id'] = chr(162) + chr(0) + chr(0)
		params['recordcnt_posn'] = 94
		params['row_dir_start'] = 142
	return params


	
def DICE(indir, outdir_top):
	#config = ConfigParser.ConfigParser()
	#config.read('config.ini')
	DICE_path = os.path.dirname(os.path.realpath(__file__))
	print DICE_path
	infiles = common.get_input_files(indir) #common.get_input_files(config.get('Basic_Values', 'input_files'))
	pfiles = common.get_parameter_files(DICE_path)
	common.checkpoint3(pfiles, infiles, indir)
		
	for pfile in pfiles:
		params = common.get_parameters(pfile)
		db = params['db_type']
		params = hardcoded_params(db, params)
		outdir = "%s%s" % (outdir_top, db) #(config.get('Basic_Values', 'output_directory'), db)
		common.verify_dir(outdir)
		for infile in infiles:
			common.checkpoint4(pfile, infile, indir)
			json_dir = "%s/%s_JSON/" % (outdir, os.path.basename(infile))
			common.verify_dir(json_dir)
			csv_dir = "%s/%s_csv/" % (outdir, os.path.basename(infile))
			common.verify_dir(csv_dir)
			file_search.search(db, infile, params, json_dir, csv_dir)	
	print "Complete!"	

if __name__ == "__main__":
	config = ConfigParser.ConfigParser()
	config.read('config.ini')
	DICE_path = os.path.dirname(os.path.realpath(__file__))

	infiles = common.get_input_files(config.get('Basic_Values', 'input_files'))
	pfiles = common.get_parameter_files(DICE_path)
	common.checkpoint1(pfiles, infiles, config)
		
	for pfile in pfiles:
		params = common.get_parameters(pfile, config)
		db = params['db_type']
		params = hardcoded_params(db, params)
		outdir = "%s%s" % (config.get('Basic_Values', 'output_directory'), db)
		common.verify_dir(outdir)
		for infile in infiles:
			common.checkpoint2(pfile, infile, config)
			json_dir = "%s/%s_JSON/" % (outdir, os.path.basename(infile))
			common.verify_dir(json_dir)
			csv_dir = "%s/%s_csv/" % (outdir, os.path.basename(infile))
			common.verify_dir(csv_dir)
			file_search.search(db, infile, params, json_dir, csv_dir)	
	print "Complete!"
