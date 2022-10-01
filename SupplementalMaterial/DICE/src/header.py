# **********************************************************************
# Copyright 2016. James Wagner and Alexander Rasin. All rights reserved. 
# This work is distributed pursuant to the Software License for DICE
# (Database Image Content Explorer), dated Aug. 15, 2016. For terms and
# conditions, please see the license file, which is included in this
# distribution.
# **********************************************************************

"""
Describes the structure of the page header.
"""

def decode_id_tuple(bytes):
	id = () 
	for byte in bytes[::-1]:
		id += (ord(byte),)
	return id

def decode_id_integer(bytes):
	id, n = 0, 0 
	for byte in bytes:
		id += (256**n)*(ord(byte))
	return id

def header(params, raw, first=False):
	"""Put all structures into a dictionary."""
	try:
		
		header_info = {}
		header_info['schema'] = None #Updated once rowdata is parsed
		header_info['column_cnt'] = None #Updated once rowdata is parsed
		header_info['record_cnt'] = get_record_cnt(params, raw, first)
		header_info['status'] = get_status(params['db_type'], raw, header_info)
		header_info['object_id'] = get_object_id(params, raw)
		header_info['page_id'] = get_page_id(params, raw)
		header_info['unallocated_space'] = get_unallocated_space()
		header_info['free_pointer']	= get_freespace_pointer()
		header_info['page_type'] = get_page_type(params, raw)
		header_info['row_data_start'] = get_rowdata_start(params, raw)
		header_info['node_type'] = get_node_type(params, raw, header_info['page_type'])
		
		#if header_info['object_id'] not in (102, 103, 104, 105, 106):
		#	return None
		#if header_info['object_id'] < 102:
		#	return None
		
	except Exception as e:	
		#print e
		return None
	return header_info

def get_rowdata_start(params, raw):
	"""The spot where the first row is located."""
	try:
		posn = ord(raw[params['row_data_start']])
	except:
		return None
	return posn	
	
def get_page_id(params, raw):
	try:
		addr = params['uniq_pg_id_addr']
		size = params['uniq_pg_id_sz']
		if None in (addr, size):
			#print None
			return None
		bytes = raw[addr: addr + size]
		
		return decode_id_integer(bytes) #str(decode_id_tuple(bytes))	
	except Exception as e:
		#print str(e)
		return None
def get_record_cnt(params, raw, first):
	"""The number of rows (according to the header)."""
	try:
		addr = params['recordcnt_posn']# May need to change this
		size = params['recordcnt_size']
		if None in (addr, size):
			return None
		if params['db_type'] in ("sqlite1k", "sqlite4k") and first:
			cnt = ord(raw[params['record_cnt_addr']+100])# + ord(raw[params['record_cnt_addr'] + 101])*256 
		bytes = raw[addr: addr + size]
		cnt = decode_id_integer(bytes)	
		return cnt
	except Exception as e:
		#print e
		return None

	
def get_status(db, raw, header_info):
	"""Determine if this is a valid page."""
	#if db in ("sqlite1k", "sqlite4k"):
	#	if ord(raw[1]) > 180:
	#		return False
	#	if ord(raw[3]) > 1:
	#		return False		
	return True
	
def get_object_id(params, raw):
	try:
		addr = params['structureid_posn']
		size = params['structureid_size']
		if None in (addr, size):			
			return None
		bytes = raw[addr: addr + size]	
		t = decode_id_tuple(bytes)	
		id, n = 0, 0
		for nbr in t:		
			id += (256**n)*int(nbr)
			n += 1
		return id	
	except Exception as e:		
		return None
		
def get_unallocated_space():
	#TODO
	return None	
	
def get_freespace_pointer():
	#TODO
	return None	
	
def get_page_type(params, raw):
	
	if params['db_type'] in ("oracle"):
		marker_posn = raw.find(chr(255)*2)
		if marker_posn == 68:
			return "Index"
		elif marker_posn == 104:
			return "Table"
		else:
			return "Unknown"	
	elif params['db_type'] in ("sqlite1k", "sqlite4k"):	
		
		flag = ord(raw[0])
		if flag == 2:		
			return "BTree"
		elif flag == 5:
			return "IOT BTree"
		elif flag == 10:
			return "Index"
		elif flag == 13:			
			return "Table"
		else:
			return "Unknown"
	elif params['db_type'] in ("postgresql"):
		if ord(raw[17]) == 32 and ord(raw[18]) == 4:
			return "Table"	
		elif ord(raw[17]) == 31 and ord(raw[18]) == 4:
			return "Index"
		else:
			return "Unknown"
	elif params['db_type'] in ("mysql"):
		size = 4
		previousNodeAddress = 8
		nextNodeAddress = 12
		previousNodeBytes = raw[previousNodeAddress : previousNodeAddress + size]
		nextNodeBytes = raw[nextNodeAddress : nextNodeAddress + size]
		previousNodeID = decode_id_tuple(previousNodeBytes)
		nextNodeID = decode_id_tuple(nextNodeBytes)
		#print previousNodeID, nextNodeID
		if previousNodeID == (0, 0, 0, 0) or nextNodeID == (0, 0, 0, 0):
			return "Unknown"
		elif previousNodeID == (255, 255, 255, 255) and nextNodeID == (255, 255, 255, 255):
			return "IOT Index"
		else:
			return "IOT Table"
	else:
		return None
	
	
def get_node_type(params, raw, object_type):
	if params['db_type'] in ("oracle"):
		return "TODO"	
	elif params['db_type'] in ("sqlite1k", "sqlite4k"):	
		return "TODO"
	elif params['db_type'] in ("postgresql"):
		if ord(raw[17]) == 32 and ord(raw[18]) == 4:
			return "Table"	
		elif ord(raw[17]) == 31 and ord(raw[18]) == 4:
			if ord(raw[-4]) == 2:
				return "Root"
			elif ord(raw[-4]) == 1:	
				return "Leaf"
			return "Leaf"
		else:
			return "Unknown"
	elif params['db_type'] in ("mysql"):
		#The IOT leaf pages store a double linked list. The intermediate and root nodes don't
		size = 4
		previousNodeAddress = 8
		nextNodeAddress = 12
		previousNodeBytes = raw[previousNodeAddress : previousNodeAddress + size]
		nextNodeBytes = raw[nextNodeAddress : nextNodeAddress + size]
		previousNodeID = decode_id_tuple(previousNodeBytes)
		nextNodeID = decode_id_tuple(nextNodeBytes)
		#print previousNodeID, nextNodeID
		if previousNodeID == (0, 0, 0, 0) or nextNodeID == (0, 0, 0, 0):
			return "Unknown"
		elif previousNodeID == (255, 255, 255, 255) and nextNodeID == (255, 255, 255, 255):
			return "Root"
		else:
			return "Leaf"

	else:
		return "TODO"
	
	