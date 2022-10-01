# **********************************************************************
# Copyright 2016. James Wagner and Alexander Rasin. All rights reserved. 
# This work is distributed pursuant to the Software License for DICE
# (Database Image Content Explorer), dated Aug. 15, 2016. For terms and
# conditions, please see the license file, which is included in this
# distribution.
# **********************************************************************

import json
import datetime

def write_output(params, dmp, rows, header, file_addr, row_dir, out3, outfile4, pg_cnt):
	if pg_cnt == 1:
		db3f = getDB3F(params)
		out3.write("%s\n" % db3f)
		
	json = get_json_object(params, dmp, file_addr, header, row_dir, rows)
	out3.write("%s\n" % json)
	csv = get_csv(rows)
	out4 = open(outfile4, "a")
	out4.write("%s\n" % csv)
	out4.close()

def get_csv(rows):
	output = ""
	for key, value in rows.iteritems():
	
		if value['values'] is None:
			pass
		else:
			output += "%s\n" % "|".join(value['values'])
	return output.strip()

def convert_schema(schema):
	output = ""
	for item in schema:
		output += item[0]
	return output

def getDB3F(params):
	db_type = params['db_type']
	pg_sz = params['pg_sz']
	dct = {"@context": {"name": params['context']['name'], "uri": params['context']['uri']}, "dbms": db_type, "page_size": pg_sz, "forensic_tool": "DBCarver", "carving_time": str(datetime.datetime.now()), "evidence_file": "DiskImage01.img"}	
	output = json.dumps(dct)
	return output

def formatAddress(addr):
	#2017033270 = 1GB, 899MB, 607KB, 54B
	GB = addr // 2**30
	MB = (addr - 2**30 * GB) // 2**20
	KB = (addr - 2**30 * GB - 2**20 * MB) // 2**10	
	B = (addr - 2**30 * GB - 2**20 * MB - 2**10 * KB)
	return "%sGB, %sMB, %sKB, %sB" % (GB, MB, KB, B)
 
	
def get_json_object(params, dmp, addr, header, row_directory, row_data):
	out_records = []
	
	for key, value in row_data.iteritems():
		row = {"offset": key, "allocated": value['status'], "row_id": value['row_id'], "values": value['values']}
		out_records.append(row)
		
	try:
		output = {
			'offset': formatAddress(addr),
			'page_id': header['page_id'], 
			'object_id': header['object_id'], 
			'recordcnt': header['record_cnt'],
			'page_type': header['page_type'],
			#'node_type': header['node_type'],
			#'row_directory': list(header['row_directory']),
			'schema': header['schema']#,
			#'records': out_records					
		}
		output = json.dumps(output)
	except Exception as e:
		#print str(e)
		return ""
	return output 