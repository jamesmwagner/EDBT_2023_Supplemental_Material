# **********************************************************************
# Copyright 2016. James Wagner and Alexander Rasin. All rights reserved. 
# This work is distributed pursuant to the Software License for DICE
# (Database Image Content Explorer), dated Aug. 15, 2016. For terms and
# conditions, please see the license file, which is included in this
# distribution.
# **********************************************************************

import datatypes as dtypes

def decode_columns(schema, sizes, raw):
	"""Iterate through row and determine decoding action."""
	columns = []#{}
	i = 0
	for dtype in schema:
		
		if sizes[i] == None:
			colm = None#" "#"DICE Error#8"
		else:
			colm = raw[:sizes[i]]
			if sizes[i] <= 0:
				sizes[i] = 0 
				colm = "NULL"
			#elif dtype == "Flt" or (sizes[i] in (7, 8) and dtype == "Nbr"):
			#	colm = dtypes.decode_real(colm)
			elif dtype != "Str" and sizes[i] == 9:
				sizes[i] = 0
				colm = "1"		
			elif dtype == "Str" or (dtype == "Nbr" and sizes[i] > 8):
				colm = dtypes.decode_str_sqlite(colm)	
				if len(colm) == 0:
					return None
			elif dtype == "Nbr":			
				if sizes[i] == 9:
					sizes[i] = 0
					colm = "1"		
				colm = dtypes.decode_nbr_sqlite(colm)
			else:
				colm = None#" "#"DICE Error#4"
			raw = raw[sizes[i]:]
			if colm is None:
				return None
			
		i += 1
		#columns[i] = {'data': str(colm), 'dtype': dtype}
		columns.append(str(colm))
		
	return columns		

def decode_columns_sizes(schema, raw, status):
	sizes, i = [], 0
	
	try:
		for colm in schema:
			
			byte = ord(raw[i])
			if i == 1 and byte == 1:
				i += 1
				continue
			elif i == 0 and byte == 0:
				i += 1
				continue
			elif byte in (10, 11):
				return None, i
			elif colm == "Flt" or byte in (7, 8):
				size = 8	
			#elif colm == "Str" and (status == True or (status == False and i > 0)):
			#	if byte  > 128:
			#		size = (((byte - 128) * 128 + ord(raw[i+1]))  - 13)/2
			#		i +=1
			#	else:
			#		if status == False and byte%2 == 0:
			#			byte += 1
			#		size = (byte - 13)/2

			elif colm == "Str" and byte > 12:
				size = (byte - 13)/2
			elif colm == "Nbr" and (status == True or (status == False and i > 0)):
				size = byte
				if size == 5:
					size = 6
			#elif i == 0 and status == False:
			#	if colm == "Nbr":
			#		size = 1
			#	elif colm == "Str":
			#		size = byte - 6
			#		if size < 1:
			#			return None, i
			#	else:
			#		return None, i		
			else:
				return None, i
			i += 1
			sizes.append(size)
			
		return sizes, i
	except:		
		return None, i

def decode_column_schema(column_cnt, raw):
	schema, i = [], 0
	try:
		
		while i < column_cnt:
			byte = ord(raw[i])			
			if i == 1 and byte == 1:
				pass
			elif i == 0 and byte == 0:
				pass
			elif byte in (10, 11):				
				return None
			elif byte in (7,8):
				return None
				schema.append("Flt")	
			elif ord(raw[i]) > 128:
				return None
				schema.append("Str")
				i += 1
			elif byte % 2 == 1 and (byte - 13)/2 > 0:
				schema.append("Str")				
			elif byte <= 6 or byte == 9:
				schema.append("Nbr")
				
			else:
				pass
				#return None
				#schema.append("Unknown")
			i += 1
		return schema
	except:
		return None

def get_schema(page, row_dir, page_type):
	"""Find a valid row within the page to decode schema."""
	schema, cnt = None, None
	for addr in row_dir:
		try:
			if page_type == "BTree":
				#raw_len, raw_sz = dtypes.decode_row_id(page[addr+3:])
				#posn = addr + raw_sz
				#colm_cnt = ord(page[posn]) - 1
				#print colm_cnt

				continue
			elif page_type == "Index":
				#print "Here"
				#slot = ord(page[addr+2])
				#page = ord(page[addr+3])
				#colm_cnt = ord(page[addr+4]) - 1
				#print slot, page, colm_cnt
				continue
			else:		
		
		
		
		
				raw_len, raw_sz = dtypes.decode_row_id(page[addr:]) 
				if raw_len is None:
					
					continue
				raw_len -= 1
				posn = addr + raw_sz #move to the rowid
				row_id, row_id_sz = dtypes.decode_row_id(page[posn:])
				if row_id is None:
					
					continue
				posn += row_id_sz #move to column count
			cnt = ord(page[posn]) - 1
			posn += 1 #move to raw data	
			raw = page[posn: posn + raw_len]
			
			if cnt > 0:
				schema = decode_column_schema(cnt, raw)
				break		
		except:
			continue
	return cnt, schema

def get_status(colm_cnt):
	#if colm_cnt == -1:
	#	return False
	return True
			
def row_data(params, page, row_dir, start, page_type):
	data = {'schema': None, 'rows':None}
	column_cnt, schema = get_schema(page, row_dir, page_type)
	
	if schema is None or len(schema) > 40:
		return None, None, None
		
	data['schema'] = schema  	
	valid_raw = []
	row_id = 0
	row_id_sz = 0
	raw_sz = 2
	rows = {} 
	rows2 = {}
	cnt = 0
	columns = []
	
	for addr in row_dir:
		cnt += 1
		
		try:
			#Get the important row structures
			if page_type == "BTree":
				#bytes = []
				#for i in range(5):
				#	bytes.append(ord(page[addr+i]))
				#print "New Page: %s" % (str(bytes))	
				"""
				raw_len, raw_sz = dtypes.decode_row_id(page[addr+1:])
				posn = addr + raw_sz
				colm_cnt = ord(page[posn]) - 1
				print colm_cnt

				raw_len, raw_sz = dtypes.decode_row_id(page[addr-1:])
				#print raw_len, raw_sz
				posn = addr + raw_sz
				colm_cnt = ord(page[posn]) - 1
				print colm_cnt
				"""
				continue
			else:
				raw_len, raw_sz = dtypes.decode_row_id(page[addr:])
				posn = addr + raw_sz
				row_id, row_id_sz = dtypes.decode_row_id(page[posn:])
				
				if row_id is None or raw_len is None:				
					return None, None, None
				raw_len -= 1
				
				posn += row_id_sz #move to column cnt
				colm_cnt = ord(page[posn]) - 1

				posn += 1 #move to raw data
				raw = page[posn: posn + raw_len]
				
			
			
			status = get_status(colm_cnt)		
			colm_sizes, storage = decode_columns_sizes(schema, raw, status)
			if colm_sizes is None:
				continue
			raw = raw[column_cnt:]
			columns = decode_columns(schema, colm_sizes, raw)
			if columns is None:
				continue
			if columns.count(" ") > 3 or columns.count(" ") >= len(columns) - 1: #Informal way to remove some false-positives
				continue
			rows2[addr] = {'row_id': row_id, 'status': status, 'values': columns}
			valid_raw.append(page[addr: posn + raw_len])
			
		except Exception as e:			
			continue
	raw_page = page[start:]
	for raw in valid_raw:
		raw_page = raw_page.replace(raw, "")	
	i = 0
	
	
	"""
	7/19/18: Jay is commenting this out for testing purposes
	"""
	
	
	"""	
	while (len(raw_page) > 10 or i < 100) and None not in (raw_sz, row_id_sz):		
		i += 1
		status = False
		posn = raw_sz
		posn += row_id_sz #move to column cnt
		try:
			colm_cnt = ord(raw_page[posn])
		except:
			raw_page = raw_page[1:]	
			continue
		#if colm_cnt != 0:
		#	raw_page = raw_page[1:]	
		#	continue
		colm_sizes, storage = decode_columns_sizes(schema, raw_page[raw_sz + row_id_sz + 1:], status)
		if colm_sizes is None or len(colm_sizes) != len(schema):			
			raw_page = raw_page[1:]	
			continue
		raw_page = raw_page[raw_sz + row_id_sz + 1 + column_cnt:]	
		columns = decode_columns(schema, colm_sizes, raw_page)
		if columns is None:
			continue
		raw_page = raw_page[sum(colm_sizes):]
		#rows[str(i)] = [status, columns]
		#rows2[str(i)] = {'schema': schema, 'status': status, 'data': columns, 'column_cnt': column_cnt, 'row_id': row_id}	
		#rows2[i] = {'row_id': row_id, 'status': status, 'column_cnt': column_cnt, 'values': columns}
		rows2[i] = {'row_id': row_id, 'status': status, 'values': columns}
	"""
	if len(rows2.values()) == 0:
		return None, None, None
	return rows2, schema, column_cnt	