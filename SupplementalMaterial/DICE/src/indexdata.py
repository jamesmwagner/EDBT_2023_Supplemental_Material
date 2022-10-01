# **********************************************************************
# Copyright 2016. James Wagner and Alexander Rasin. All rights reserved. 
# This work is distributed pursuant to the Software License for DICE
# (Database Image Content Explorer), dated Aug. 15, 2016. For terms and
# conditions, please see the license file, which is included in this
# distribution.
# **********************************************************************

import datatypes as dtypes

zero_compress_dict = {128: 0, 192: 1, 193: 2, 194: 4, 195: 6, 196: 8, 197: 10, 198: 12, 199: 14, 200: 16, 201: 18, 202: 20, 203: 22}

def decode_values(schema, raw):	
	columns = []
	for dtype in schema:
		try:
			size = ord(raw[0])
			colm = None
			if size == 0:
				return None, None		
			if size == 255:
				colm = "NULL"
			elif dtype == "Str":			
				colm = dtypes.decode_str(raw[1:size + 1])
			elif dtype == "Nbr":			
				colm = dtypes.decode_nbr_oracle(raw[:size + 1])		
			raw = raw[size + 1:]
			if colm is None:
				return None, None		
			columns.append(str(colm))
		except:
			return None, None
	return columns, raw	
	
def get_schema(raw):
	column_cnt = 0
	schema = []
	while len(raw) > 0:
		try:
			if ord(raw[1]) in zero_compress_dict.keys():
				colm = "Nbr"
			elif ord(raw[0]) > 0:
				colm = "Str"
			schema.append(colm)
			raw = raw[ord(raw[0]) + 1:]
		except:
			return None
	return schema
	
def decode_pointer(raw):
	bytes = raw[1:7]
	id = () 
	slot_nbr = (ord(bytes[5]) + ord(bytes[4])*256, )
	page_nbr = (ord(bytes[3]) + ord(bytes[2])*256, )
	file_nbr = (ord(bytes[0]), ord(bytes[1]))
	id += file_nbr
	id += page_nbr
	id += slot_nbr
		
	return id	

	
"""
Row Data- only the ASCII without parsing
"""

def row_data(params, page, row_dir):
	data = {}
	pointer_length = 7
	data = {'schema':None, 'rows':None}
	
	i = 1
	row_dir2 = sorted(list(row_dir))
	stop = len(row_dir)
	rows = {}
	for addr in sorted(row_dir):
		if i < stop:
			if len(page[addr:row_dir2[i]].strip()) < 4:
				continue
			data[i] = page[addr:row_dir2[i]] #Read until the next address
		else:
			if len(page[addr:].strip()) < 4:
				continue
			data[i] = page[addr:] #Read to end of page if last address
		
		if data['schema'] is None:
			values = data[i][2:pointer_length * -1]
			data['schema'] = get_schema(values)
		if data['schema'] is None:
			i += 1
			continue
			
		values = data[i][2:]
		columns, raw = decode_values(data['schema'], values)
		pointer = raw
		if raw is not None and len(raw) >= pointer_length:
			rows[decode_pointer(pointer)] = ["+", columns]
		i += 1
		
	if len(data.keys()) == 0:
		return None
	data['rows'] = rows
	return data