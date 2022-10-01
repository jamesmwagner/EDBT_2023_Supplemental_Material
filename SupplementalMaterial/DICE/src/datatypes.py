# **********************************************************************
# Copyright 2016. James Wagner and Alexander Rasin. All rights reserved. 
# This work is distributed pursuant to the Software License for DICE
# (Database Image Content Explorer), dated Aug. 15, 2016. For terms and
# conditions, please see the license file, which is included in this
# distribution.
# **********************************************************************

def decode_nbr_mysql(params, raw, marker):
	val = decode_id_mysql(raw)
	val += 256**3 * (marker - params['rawnbr_marker'])#greater than 256**3
	return val
	
def decode_id_mysql(raw):
	"""Reconstruct an identifier."""
	id, n = 0, 0
	for chr in raw[::-1]:
		id += (256**n)*(ord(chr))
		n += 1
	return id


def decode_nbr_psql(raw):
	"""Decode number/integer """
	id, n = 0, 0
	for chr in raw:
		id += (256**n)*(ord(chr))
		n += 1
	return id	

def decode_nbr_oracle(raw):
		zero_compress_dict = {128: 0, 192: 1, 193: 2, 194: 4, 195: 6, 196: 8, 197: 10, 198: 12, 199: 14, 200: 16, 201: 18, 202: 20, 203: 22}
		size = ord(raw[0])
		
		try:
			key = ord(raw[1])
		
			dgts = zero_compress_dict[key]
			if dgts == 0 and size == 1:
				nbr = "0"
			else:
				raw2 = raw[2:2+size-1]
				nbr = "".join([str(ord(x) - 1).zfill(2) for x in raw2])
				nbr = nbr.ljust(dgts, "0")
				nbr = nbr.lstrip("0")	
			raw = raw[size + 1:]
		except:
			nbr = None	
		return nbr	
		
def decode_nbr_sqlite(raw):
	"""Decode number/integer """
	if len(raw) == 0:
		return " "#"DICE Error#5(%s)" % len(raw)#return 1
	id, n = 0, 0
	for chr in raw[::-1]:
		if ord(chr) == 0 and n > 0:
			continue
		id += (256**n)*(ord(chr))
		n += 1
	#if id > (256**4)*255 + (256**3)*255 + (256**2)*255 + 256*255 + 255:
	#	#id = decode_str(raw)
	#	return "DICE Error#5(%s)" % len(raw)
	return id		

def decode_str_oracle(colm):
	"""Make sure a string is reasonable characters."""
	if len(colm) == 0:
		return None
	colm = colm.replace(chr(10), " ")
	for c in colm:
		x = ord(c)
		if x < 5 or x > 128:
			return None
	return colm	

def decode_str(colm):
	"""Make sure a string is reasonable characters."""
	if len(colm) == 0:
		return None
	colm = colm.replace(chr(10), " ")
	for c in colm:
		x = ord(c)
		if x < 5 or x > 128:
			return None
	return colm	
	
def decode_str_sqlite(colm):
	"""Decode a string """
	if len(colm) == 0:
		return " "#"DICE Error#7(%s)" % len(colm)
	colm = colm.replace(chr(10), " ")
	output = colm
	cnt = 0
	for c in colm:
		x = ord(c)
		if x < 32 or x > 127:			
			output = output.replace(c, "~")#return "DICE Error#6(%s)" % len(colm)
			cnt += 1
	if cnt > 10:
		return None#output = "DICE Error#6"
	return output
	
def decode_string_mysql(raw):
	output = raw
	for c in raw:
		if ord(c) < 32 or ord(c) > 127:
			output = output.replace(c, ".")
	return output	

def decode_date_mysql(params, raw):
	if len(raw) != 3:
		return None
	b1 = ord(raw[0])
	b2 = ord(raw[1])
	b3 = ord(raw[2])

	day = str(b3%32).zfill(2)
	month = str(b3/32).zfill(2)
	year = str((b1 - 128)*128 + b2/2)
	return "%s-%s-%s" % (year, month, day)	
	


def decode_row_id(raw):
	"""Decode row identifier for SQLite"""
	size = 0
	try:
		while ord(raw[size]) >= 128:
			size += 1
		size += 1	
	except:
		return None, None
	raw_row_id = [ord(x) for x in raw[:size]]
	id, n = raw_row_id.pop(), 1	
	for c in raw_row_id[::-1]:
		id += (c - 128)*(128**n)
		n += 1
	return id, size
	
def decode_real(raw):
	"""Decode a string """
	
	if len(raw) not in (7, 8):
		return " "#"DICE Error#8(%s)" % len(colm)
	
	nbr = 1
	dec_raw = raw[-6:]
	nbr_raw = raw[:-5]
	dec, n = 0, 0
	for chr in dec_raw[::-1]:
		if ord(chr) == 0 and n > 0:
			continue
		dec += (256**n)*(ord(chr))
		n += 1
		
	dec = float(dec * (2*(10**-16)))
	nbr = float(nbr + dec)
	output = "%.16f" % (nbr) 
	return output