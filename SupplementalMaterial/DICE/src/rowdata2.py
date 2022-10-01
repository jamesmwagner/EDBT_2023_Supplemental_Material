# **********************************************************************
# Copyright 2016. James Wagner and Alexander Rasin. All rights reserved. 
# This work is distributed pursuant to the Software License for DICE
# (Database Image Content Explorer), dated Aug. 15, 2016. For terms and
# conditions, please see the license file, which is included in this
# distribution.
# **********************************************************************

import datatypes as dtypes

zero_compress_dict = {128: 0, 192: 1, 193: 2, 194: 4, 195: 6, 196: 8, 197: 10, 198: 12, 199: 14, 200: 16, 201: 18, 202: 20, 203: 22}

def decode_columns(schema, raw):
	"""Iterate through row and determine decoding action."""
	columns = []#{}
	cnt = 0
	for dtype in schema:
		try:
			size = ord(raw[0])
		except:
			return None
		colm = None
		if size == 0:
			return None		
		if size == 255:
			colm = "NULL"
		elif dtype == "Str":			
			colm = dtypes.decode_str_oracle(raw[1:size + 1])
		elif dtype == "Nbr":			
			colm = dtypes.decode_nbr_oracle(raw[:size + 1])		
		raw = raw[size + 1:]
		if colm is None:
			return None		
		cnt += 1
		#columns[cnt] = {'data': str(colm), 'dtype': dtype}
		columns.append(str(colm))		
	return columns		
	
def decode_column_schema(column_cnt, raw):
	"""Determine the schema for the entire page."""
	schema = []
	try:		
		for i in range(column_cnt):
			colm = "_"
			if ord(raw[1]) in zero_compress_dict.keys():
				colm = "Nbr"
			elif ord(raw[0]) > 0:
				colm = "Str"
			schema.append(colm)
			raw = raw[ord(raw[0]) + 1:]			
		return schema
	except:
		return None

def get_schema(page, row_dir):
	"""Find a valid row within the page to decode schema."""
	schema, cnt = None, None #schema and column count
	for addr in row_dir:
		addr = addr - 8
		try:
			#print "here", ord(page[addr-8]), page[addr], page[addr + 1], page[addr + 2], page[addr + 3]
			if ord(page[addr]) not in (44, 66):
				
				continue
		except:
			break
		posn = addr + 2 #parameter
		cnt = ord(page[posn])
		posn += 1 #move to raw data	
		raw = page[posn:]		
		if cnt > 0:
			schema = decode_column_schema(cnt, raw)
			break		
	return cnt, schema

def get_status(row_dlm):
	if row_dlm == 66:
		return False
	return True
			
def row_data(params, page, row_dir):
	#data = {'schema': None, 'rows':None, 'rows2': None}
	rows2 = {}
	column_cnt, schema = get_schema(page, row_dir)
	if schema is None or len(schema) > 40:
		#return {'schema': None, 'rows':None, 'rows2': None}
		return None, None, None
	#data['schema'] = schema  
	#print schema
	rows = {} 
	rows2 = {}
	i = 1	
	for addr in sorted(row_dir):
		addr = addr - 8 # may need to fix this later
		try:
			status = get_status(ord(page[addr]))	
			
		except:
			continue
		raw = page[addr + 3:]
		columns = decode_columns(schema, raw)
		#print columns
		if columns is None:
			continue
		#rows[i] = [status, columns]
		#rows2[addr] = {'schema': schema, 'status': status, 'data': columns, 'column_cnt': column_cnt, 'row_id': None}
		rows2[addr] = {'row_id': i, 'status': status, 'values': columns}
		i += 1
	
	if len(rows2.values()) > 0:
		#data['rows'] = rows
		#data['rows2'] = rows2
		pass
	else:
		#return {'schema': None, 'rows2': None}
		rows2 = None	
	return rows2, schema, column_cnt
	