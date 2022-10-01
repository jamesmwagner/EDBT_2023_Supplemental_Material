# **********************************************************************
# Copyright 2016. James Wagner and Alexander Rasin. All rights reserved. 
# This work is distributed pursuant to the Software License for DICE
# (Database Image Content Explorer), dated Aug. 15, 2016. For terms and
# conditions, please see the license file, which is included in this
# distribution.
# **********************************************************************

"""
This describes the general page layout. Every page should have at least a header, row directory,
and row data.
"""

import header as hdr
import row_directory as rdir
import psql_row_directory as rdir2
import rowdata1 as data1
import rowdata2 as data2
import rowdata3 as data3
import rowdata4 as data4

def page(params, raw, addr, file_addr, first=False):	
	
	header = hdr.header(params, raw, first)
	if header is None or header['status'] is None:
		return None, None, None
	if header['record_cnt'] == 0 and params['db_type'] in ("sqlite1k", "sqlite4k"):
		#This condition may have to be removed to catch some deleted data. Too many false-positives without this.
		return None, None, None
	#if params['db_type'] in ("sqlite1k", "sqlite4k") and header['row_data_start'] > params['pg_sz']:
	#	#This condition may have to be removed to catch some deleted data. Too many false-positives without this.
	#	return None, None, None
	
	if params['db_type'] in ("postgresql"):
		row_dir = rdir2.row_directory(params, raw, header['record_cnt'])
		header['row_directory'] = row_dir.keys()	
	else:		
		row_dir = rdir.row_directory(header, params, raw, header['record_cnt'], first)
		
		header['row_directory'] = row_dir
	if row_dir is None:		
		return header, row_dir, None	
	
	if params['db_type'] in ("sqlite1k", "sqlite4k"):
		if len(row_dir) > 600: #a record needs to be more than 7 bytes. removes false-positives.		
			return None, None, None
	
	
	
	
	#if header['page_type'] == "Index" and addr < 40000:
	#	print "Found", addr 
	#	
	#else:
	#	return None, None, None
	
	
			
	if params['db_type'] in ("sqlite1k", "sqlite4k"):
		rows, schema, column_cnt = data1.row_data(params, raw, row_dir, header['row_data_start'], header['page_type'])	
		if rows is None:
			return None, None, None
		if len(rows.keys()) == 1 and len(schema) > 10: #informal condition to remove false positives
			return None, None, None
		
	elif params['db_type'] in ("postgresql"):
		oid, page_id, rows, schema, column_cnt = data3.row_data(params, header, raw, row_dir)		
		header['object_id'] = oid
		header['page_id'] = page_id
		
	elif params['db_type'] in ("mysql"):
		
		if header['page_type'] == "IOT Index":
			#Hardcoded quick parsing for IOT Index. Only works for 1 column ints
			rows, schema, column_cnt = data4.index_data(params, raw, row_dir)
						
			
		else:
			rows, schema, column_cnt = data4.row_data(params, raw, row_dir)
			row_dir = rows.keys()
			header['row_directory'] = row_dir
			if len(row_dir) == 0:
				#Lets try an index
				#Hardcoded quick parsing for IOT Index. Only works for 1 column ints
				header['page_type'] = "IOT Index"
				header['node_type'] = "Intermediate"
				rows, schema, column_cnt = data4.index_data(params, raw, row_dir)
			
	else:
		rows, schema, column_cnt = data2.row_data(params, raw, row_dir)
	header['schema'] = schema
	header['column_cnt'] = column_cnt
	
	
	
	#if rows is None or header['record_cnt'] > len(rows.values()) or header['record_cnt'] > len(row_dir):
	#	return None, None, None

	return header, row_dir, rows