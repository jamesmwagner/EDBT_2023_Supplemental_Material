# **********************************************************************
# Copyright 2016. James Wagner and Alexander Rasin. All rights reserved. 
# This work is distributed pursuant to the Software License for DICE
# (Database Image Content Explorer), dated Aug. 15, 2016. For terms and
# conditions, please see the license file, which is included in this
# distribution.
# **********************************************************************

import datatypes as dtypes
	
def raw_data(params, raw, str_szs):
	columns, schema = [], []
	if len(raw) == 0:
		return ["DICE Error#05"], ["DICE Error#05"], None
	while len(raw) > 0:
		if ord(raw[0]) in (params['rawnbr_marker'], 129, 130):
			try:
				colm = dtypes.decode_nbr_mysql(params, raw[1:params['rawnbrs_size']], ord(raw[0]))
				raw = raw[params['rawnbrs_size']:]
				schema.append("Nbr")
			except:
				colm = "DICE Error#06"
		elif ord(raw[0]) > 140 and ord(raw[0]) < 145:
			try:
				colm = dtypes.decode_date_mysql(params, raw[:3])
				raw = raw[3:]
				schema.append("Date")
			except:
				colm = "DICE Error#07" 
		elif len(str_szs) > 0:
			try:
				sz = str_szs.pop()
				colm = dtypes.decode_string_mysql(raw[:sz])
				raw = raw[sz:]
				schema.append("Str")
			except:
				colm = "DICE Error#08"
		else:
			if len(columns) == 0:
				colm = "DICE Error#09 Byte1:%s" % (ord(raw[0]))
			else:
				break
		columns.append(str(colm))
		
		if colm in ("DICE Error#06", "DICE Error#07", "DICE Error#08", "DICE Error#09"):
			break
	return schema, columns, raw		

def get_colm_szs(raw, cnt, posn):
	"""Return a list of column sizes @szs for strings."""
	szs, long = [], 0
	for i in range(cnt, 0, -1):
		sz = ord(raw[posn - i])
		if sz in (129, 130):
			long = 256*(sz-128)
		else:
			sz += long
			long = 0
			szs.append(sz)
	return szs

def define_status(raw1, raw2):	
	"""Determine if a row is deleted or active."""
	deleted = 32
	active = 7
	status = "?"
	if ord(raw1) >= deleted or ord(raw2) >= deleted:
		status = False
	elif ord(raw1) <= active or ord(raw2) <= active:
		status = True
	return status

def mysql_rawdata_trim(raw):
	"""Remove extra data before the row data begins."""
	x = "supremum"
	start = raw.find(x) + len(x)
	return raw[start:]
	

def index_data(params, raw, row_dir):
	#Hardcoded quick parsing for IOT Index. Only works for 1 column ints
	raw = mysql_rawdata_trim(raw)
	
	
	posn = raw.find(chr(0) + chr(13) + chr(128))
	rows = {}
	cnt = 1
	offset = 3
	while posn > 0:
		#rows[1] = {'row_id': 1, 'status': True, 'values': ["1"]}		
		value = "%s" % str(ord(raw[posn + offset])*256*256 + ord(raw[posn + offset+1])*256 + ord(raw[posn + offset + 2]))
		rows[cnt] = {'row_id': str(cnt), 'status': True, 'values': [value]}
		posn = raw.find(chr(0) + chr(13) + chr(128), posn+10)
		cnt += 1

	raw = mysql_rawdata_trim(raw)
	posn = raw.find(chr(0) + chr(14) + chr(129))
	offset = 3
	while posn > 0:
		#rows[1] = {'row_id': 1, 'status': True, 'values': ["1"]}		
		value = "%s" % str(ord(raw[posn + offset])*256*256 + ord(raw[posn + offset+1])*256 + ord(raw[posn + offset + 2]))
		rows[cnt] = {'row_id': str(cnt), 'status': True, 'values': [value]}
		posn = raw.find(chr(0) + chr(14) + chr(129), posn+10)
		cnt += 1

	


	return rows, ["Nbr"], 1 #data


def row_data(params, raw, row_dir):
	"""MySQL row data parsing."""
	data = {'schema': None, 'rows':None}
	
	try:
		raw = mysql_rawdata_trim(raw)
		posn1 = raw.find(chr(0) + chr(16)) - 1
		string_cnt = len(raw[:posn1]) - raw[:posn1].count(chr(0))
		zeros = raw[:posn1].count(chr(0))
		rows = {}
		posn = raw.find(chr(0)*2, posn1+14)
		if raw[posn1:posn].count(chr(128)) > 1:
			posn = raw.find(chr(0)*2, posn + 1)
		
		
		raw_dlm = raw[posn:posn+3]
		while ord(raw_dlm[-1]) == 0:
			posn = raw.find(chr(0)*2, posn+1) 
			raw_dlm = raw[posn:posn+3]
		jump = posn - posn1
		#raw_dlm = chr(0)*2	
		string_posn = posn -  string_cnt
	except:
		data['schema'] = ["DICE Error#04"]
		data['rows'] = {0: "DICE Error#04"}
		return None, None, None
		
	cnt = 1
	columns = []
	while posn >= 0:
		r_start = 10
		r_size = 4
		pk_posn = posn - r_start
		row_id = dtypes.decode_id_mysql(raw[pk_posn:posn - (r_start-r_size+1)])
		str_szs = get_colm_szs(raw, string_cnt, posn - string_posn)
		status = define_status(raw[posn - string_posn], raw[posn - string_posn + 1])
		if 0 in str_szs or sum(str_szs) > 16000:
			#data['schema'] = ["DICE Error#10"]
			break
		raw_start = posn + 6#11
		schema, columns, raw = raw_data(params, raw[raw_start:], str_szs)
		posn = 0
		if schema == ["DICE Error#05"]:
			break
		
		if data['schema'] is None:
			data['schema'] = ['PK'] + schema
		rows[cnt] = {'row_id': str(row_id), 'status': status, 'values': columns} #[status, columns]
		if raw is None:
			break
		posn += string_cnt + jump + zeros
		if posn > len(raw):
			break
		cnt += 1
	data['rows'] = rows
	if data['schema'] is None:
		data['schema'] = ["DICE Error#11"]	
	return rows, data['schema'], len(columns) #data