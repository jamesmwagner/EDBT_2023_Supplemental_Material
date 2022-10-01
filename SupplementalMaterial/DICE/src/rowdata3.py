# **********************************************************************
# Copyright 2016. James Wagner and Alexander Rasin. All rights reserved. 
# This work is distributed pursuant to the Software License for DICE
# (Database Image Content Explorer), dated Aug. 15, 2016. For terms and
# conditions, please see the license file, which is included in this
# distribution.
# **********************************************************************

import datatypes as dtypes

psql_nbr_size = 4
psql_raw_data_dlm = chr(2) + chr(9) + chr(24)
psql_raw_data_dlm = chr(146) + chr(1) + chr(24)

def monitor_string_padding(schema, sizes, i):
	j = 1
	jump = 0
	while True:
		if i - j < 0:
			break
		if schema[i-j] == "Str":
			jump += sizes[i-j]
		else:
			break
		j += 1
	jump = jump%4
	if jump > 0:
		jump = 4 - jump
	return jump

def decode_columns(schema, raw):
	"""Iterate through row and determine decoding action."""
	columns = []
	sizes = []
	i = 0
	
	for dtype in schema:
		try:
			size = ord(raw[0])
			colm = None
			if dtype != "Str" and i > 0 and schema[i-1] == "Str":
				pad = monitor_string_padding(schema, sizes, i)
				raw = raw[pad:]
			i += 1
			
			if dtype == "Str":				
				size = (size - 3)/2 + 1				
				colm = dtypes.decode_str(raw[1:size])
			elif dtype == "Text":
				size = (ord(raw[0])/4) + 64*ord(raw[1]) #- 4
				colm = raw[4:size]
				#size += 4
			elif dtype == "Compressed":	
				size = ((ord(raw[0]) - 2)/4) + 64*ord(raw[1]) #- 4
				try:
					decompressed_size = ord(raw[4]) + ord(raw[5])*256 + ord(raw[6])*256**2 + ord(raw[7])*256**3
					colm = "Decompressed Size: %s, Compressed Data: %s" % (decompressed_size, raw[8:size])
				except:
					colm = "compressed data error"
			elif dtype == "Nbr":			
				size = 4
				colm = dtypes.decode_nbr_psql(raw[:4])
			raw = raw[size:]
			sizes.append(size)
			if colm is None:
				return None		
			columns.append(str(colm))		
		except Exception as e:
			return None
	return columns		
	
def decode_column_schema(column_cnt, raw_in, row_size):
	"""Determine the schema for the entire page."""
	schema = []
	sizes = []
	string_pad = 0
	raw = raw_in
	try:
		#while row_size - sum(sizes) > 0 and len(raw) > 0 and column_cnt < 50:	
			
			schema = []
			sizes = []
			string_pad = 0
			raw = raw_in
			for i in range(column_cnt):
				colm = "?"
				byte = ord(raw[0])
				byte2 = None
				byte3 = None
				byte4 = None
				byte5 = None
				byte6 = None
				byte7 = None
				byte8 = None
				byte9 = None
				try:
					byte2 = ord(raw[1])
					byte3 = ord(raw[2])
					byte4 = ord(raw[3])
					byte5 = ord(raw[4])
					byte6 = ord(raw[5])
					byte7 = ord(raw[6])
					byte8 = ord(raw[7])
					byte9 = ord(raw[8])
				except:
					pass
				
				if byte > 0 and byte % 2 == 1 and (byte - 3)/2 > 0:
					colm = "Str"
					size = (byte - 3)/2 + 1
					
					if byte == 5 and ord(raw[1]) != 48 and len(schema) < 7:# and schema[-1] != "Str":
						colm = "Nbr"
					else:
					
						for c in raw[1:size]:
							if ord(c) < 32 or ord(c) > 128:
								colm = "Nbr"
								break
						else:
							string_pad += size
					
					
					"""
					elif byte != 256 and byte%4 == 0 and byte2 >= 2 and byte3 == 0 and byte4 == 0:
						colm = "Text"
						size = (ord(raw[0])/4) + 64*ord(raw[1])# - 4
						#size += 4
						#elif (byte - 2)%4 == 0  and byte3 == 0 and byte4 == 0 and None not in (byte5, byte6, byte7, byte8, byte9) and 2000 < byte5 + byte6*256 + byte7*256**2 + byte8*256**3 and 32 <= byte9 < 128:
							#Compression does not occur until 2000 bytes
						#	colm = "Compressed"
						#	size = ((ord(raw[0]) - 2)/4) + 64*ord(raw[1])				"""
				else:				
					colm = "Nbr"
					
				if colm == "Str" and string_pad % 4 != 0:
						try:
							nxt_byte = ord(raw[size])
							nxt_size = (ord(raw[size]) - 3)/2
							if nxt_byte != 0 and (nxt_byte % 2 == 0 or nxt_size <= 0):
								colm = "Nbr"							
							for c in raw[size + 1:size + 1+ nxt_size]:
								if ord(c) < 32 or ord(c) > 128:
									colm = "Nbr"
									break
						except:
							pass				
				if colm == "Nbr":
					string_pad = 0
					size = 4
					
				sizes.append(size)
				schema.append(colm)
				
				if colm != "Str" and schema[i-1] == "Str" and i > 0:# and schema[i-1] == "Str":
					jump = monitor_string_padding(schema, sizes, i)
					raw = raw[jump:]
					sizes[-1] = sizes[-1] + jump
				raw = raw[size:]
			
			return schema
	except Exception as e:
		print "rowdata3", str(e)		
		return None

def get_schema(page, row_dir, params):
	"""Find a valid row within the page to decode schema."""
	schema, cnt = None, None #schema and column count
	for addr in sorted(row_dir.keys()):
		dlm = page.find(params['rawdata_dlm'], addr + 1)		
		if dlm < 1:
			dlm2 = page.find(chr(146) + chr(1) + chr(24), addr + 1)		
		if dlm2 > 1:		
			dlm = dlm2
		if dlm < 1:	
			continue
		
		cnt = ord(page[dlm + params['colmcnt_dlm_posn']])
		
		raw = page[dlm + params['rawdata_dlm_posn']:]
		if cnt > 0:			
			row_size = row_dir[addr]
			schema = decode_column_schema(cnt, raw, row_size)				
			break		
	return cnt, schema
	
	
def get_index_schema(page, row_dir, params):
	"""Find a valid row within the page to decode schema."""
	schema, cnt = None, None #schema and column count
	for addr in sorted(row_dir.keys()):
		dlm = addr + 10#page.find(psql_raw_data_dlm, addr + 1)
		if dlm < 1:
			continue
		cnt = 1#ord(page[dlm + params['colmcnt_dlm_posn']])
		raw = page[dlm:]
		if cnt > 0:			
			row_size = row_dir[addr]
			schema = decode_column_schema(cnt, raw, row_size)
			break	
	return cnt, schema	

def get_status(raw):
	if raw.find(psql_raw_data_dlm) > 2 and raw.find(psql_raw_data_dlm) < 30:
		return True
	return False

def decode_id_tuple(bytes):
	id, n = 0, 0
	for nbr in bytes:		
		id += (256**n)*int(nbr)
		n += 1
	return id		
	
def get_structure_id(raw):
	id = []
	start = raw.find(psql_raw_data_dlm)	
	posn = 20
	try:
		for i in range(4):
			id.append(ord(raw[start - posn + i]))
	except:
		return None
	id = decode_id_tuple(id)
	return id

def get_page_id(raw):
	id = []
	start = raw.find(psql_raw_data_dlm)	
	posn = 6
	id = ord(raw[start - posn]) + ord(raw[start - posn + 1]) * 256
	return str(id)
	
def get_row_id(raw):
	start = raw.find(psql_raw_data_dlm)	
	posn = 4	
	id = ord(raw[start - posn]) + ord(raw[start - posn + 1]) * 256	
	return str(id)
	
def get_index_page_id(raw):
	id = []
	#start = raw.find(psql_raw_data_dlm)	
	posn = 4
	id = ord(raw[posn]) + ord(raw[posn + 1]) * 256
	return str(id)
	
def get_index_row_id(raw):
	#start = raw.find(psql_raw_data_dlm)	
	posn = 6	
	id = ord(raw[posn]) + ord(raw[posn + 1]) * 256	
	return str(id)	
	
	
def row_data(params, header, page, row_dir):
	data = {'schema': None, 'rows':None}
	try:
		if header['page_type'] == "Index":
			column_cnt, schema = get_index_schema(page, row_dir, params)
		else:
			column_cnt, schema = get_schema(page, row_dir, params)
	except Exception as e:				
		return None, None, None, None, None
	if schema is None or len(schema) > 40:						
		return None, None, None, None, None
	data['schema'] = schema  	
	rows = {} 
	rows2 = {}
	i = 1	
	for addr in sorted(row_dir.keys())[::-1]:
		if header['page_type'] == "Index":
			status = True
			oid = "NA"
			page_id = "NA"
			row_id = i
			pointer = "%s-%s" % (str(get_index_page_id(page[addr:])).zfill(2), str(get_index_row_id(page[addr:])).zfill(2)   )
		else:
			status = get_status(page[addr:])
			oid = get_structure_id(page[addr:])
			if oid is None:
				continue
			page_id = get_page_id(page[addr:])
			row_id = get_row_id(page[addr:])
			
		if header['page_type'] == "Index":
			dlm = addr + 10
			try:
				raw = page[dlm:]
				columns = decode_columns(schema, raw)
				columns.append(pointer)	
			except:
				continue
			
		else:
			dlm = page.find(psql_raw_data_dlm, addr + 18)		
		
			try:
				raw = page[dlm + params['rawdata_dlm_posn']:]
				columns = decode_columns(schema, raw)
					
			except Exception as e:
				continue
			if columns is None:
				continue
				
		#rows[i] = [status, columns]
		#rows2[i] = {'schema': schema, 'status': status, 'structure_id': oid, 'data': columns, 'column_cnt': column_cnt, 'row_id': row_id, 'page_id': page_id}	
		rows2[addr] = {'row_id': row_id, 'status': status, 'values': columns}
		#print schema, columns
		i += 1
		
	
	if len(rows2.values()) > 0 and oid is not None and page_id is not None:
		#data['rows'] = rows
		#data['rows2'] = rows2
		pass
	else:
		return None, None, None, None, None
	if header['page_type'] == "Index":
		schema.append("Ptr")
	return oid, page_id, rows2, schema, column_cnt	