# **********************************************************************
# Copyright 2016. James Wagner and Alexander Rasin. All rights reserved. 
# This work is distributed pursuant to the Software License for DICE
# (Database Image Content Explorer), dated Aug. 15, 2016. For terms and
# conditions, please see the license file, which is included in this
# distribution.
# **********************************************************************

"""
Reconstructs the row directory.
"""

import math

def row_directory(header, params, raw, record_cnt, first=False):
	"""
	Address = x + (y - cy)*256 + cx
	"""
	addrs = set([])
	#addr_cnt = int(math.ceil(record_cnt/params['slot_size']))
	nbr = 0
	while True:
		try:
			posn = params['row_dir_start'] + params['addr_size'] * nbr
			if params['db_type'] in ("sqlite1k", "sqlite4k") and first: # This is a special case for SQLite because it uses a file header
				posn += 100
			if params['db_type'] in ("sqlite1k", "sqlite4k") and header['page_type'] == "BTree":
				posn = params['row_dir_start'] + params['addr_size'] * nbr + 4
				
			addr = decode_address(params, raw, posn)
			if test_addr(params, addr, posn):
				addrs.add(addr)
			else:				
				break
			nbr += 1
		except Exception as e:
			break
	return addrs
	
def decode_address(params, raw, posn):
	"""
	Reconstructs an address.
	"""
	
	x = ord(raw[posn])
	y = ord(raw[posn + params['hg_val_posn']])
	addr = x + (y - params['cy'])*256 + params['cx']
	return addr

def test_addr(params, addr, posn):
	"""
	Rules to decide if an address is valid:
	1. An address cannot be greater than the page size
	2. An address must be greater than the address of the pointer.
	"""
	
	test = False
	if addr < params['pg_sz'] and addr > posn:
		test = True		
	return test
	
def row_directory_index_oracle(params, raw):
	#Address = x + (y - cy)*256 + cx
	addrs = set([])
	nbr = 0
	while True:
		posn = 136 + params['addr_size'] * nbr
		try:
			addr = decode_address(params, raw, posn)
		except:
			break
		if 0 < addr < posn:
			nbr += 1
			continue
			
		elif test_addr(params, addr):
			addrs.add(addr)
		else:
			break
		nbr += 1
	return addrs		
	