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

def row_directory(params, raw, pg_type):
	#Address = x + (y - cy)*256 + cx
	addrs = {}
	sizes = {}
	nbr = 0
	while True:
		posn = params['row_dir_start'] + params['addr_size'] * nbr
		try:
			addr = decode_address(params, raw, posn)			
		except:
			break
		if test_addr(params, addr):
			if pg_type == "table":
				addrs[addr] = decode_size(params, raw, posn)
			else:
				addrs[addr] = decode_indexsize(params, raw, posn)
		else:
			break
		nbr += 1
	return addrs
	
def decode_address(params, raw, posn):
	"""Reconstructs an address."""
	x = ord(raw[posn])
	y = ord(raw[posn + params['hg_val_posn']])
	addr = x + (y - params['cy'])*256 + params['cx']
	return addr

def test_addr(params, addr):
	"""Rules to decide if an address is valid."""
	test = False
	if addr < params['pg_sz'] and addr > params['gen_pg_id_addr']:
		test = True
	if addr == 0:
		test = False
	return test
	
def decode_size(params, raw, posn):
	"""Reconstructs an address."""
	x = ord(raw[posn + 2])
	y = ord(raw[posn + params['hg_val_posn'] + 2])
	size = ((x + y * 256))/2 - 24
	return size

def decode_indexsize(params, raw, posn):
	"""Reconstructs an address."""
	x = ord(raw[posn + 2])
	y = ord(raw[posn + params['hg_val_posn'] + 2])
	size = ((x + y * 256))/2# - 24
	return size	