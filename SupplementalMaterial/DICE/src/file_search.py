# **********************************************************************
# Copyright 2016. James Wagner and Alexander Rasin. All rights reserved. 
# This work is distributed pursuant to the Software License for DICE
# (Database Image Content Explorer), dated Aug. 15, 2016. For terms and
# conditions, please see the license file, which is included in this
# distribution.
# **********************************************************************

import os
import time
import output
import page as pg
import common

MB = 2**20


def get_oid(header):
	if header['object_id'] is None:
		return output.convert_schema(header['schema'])     
	else:
		return header['object_id']

def get_next_address(db, addr, chunk, params, scan_nbr):
	"""Find the next page address using the @gen_pd_id"""
	id = params['gen_pg_id']
	id_addr = params['gen_pg_id_addr']
	if db in ("sqlite1k", "sqlite4k"):
		if scan_nbr == 1:
			id = params['indx_pg_id']
		if 	scan_nbr == 2:
			id = params['indx_pg_id3']
	
		addr2 = chunk.find(id, addr + len(id) + id_addr + 1) - id_addr			
		if addr2 == -1 or addr2 <= addr:
			return -1
		return addr2
	elif db == "postgresql":		
		id2 = chr(240) + chr(31) + chr(4)  #index header
		if addr == 0:
			addr1 = chunk.find(id) - id_addr 
			addr2 =	chunk.find(id2) - id_addr 
			if addr2 < 0 and addr1 > -1:
				return addr1
			elif addr1 < 0 and addr2 > -1:
				return addr2
			elif addr1 < addr2 and addr1 > -1 and addr2 > -1:
				return addr1
			else:
				return addr2
		addr1 = chunk.find(id, addr + len(id) + id_addr) - id_addr
		addr2 = chunk.find(id2, addr + len(id) + id_addr) - id_addr
		if addr2 < 0 and addr1 > -1:
			return addr1
		elif addr1 < 0 and addr2 > -1:
			return addr2
		elif addr1 < addr2 and addr1 > -1 and addr2 > -1:
			return addr1
		else:
			return addr2
		
	elif db == "oracle" or  db == "mysql":
		if addr == 0:			
			return chunk.find(id) - id_addr 
		return chunk.find(id, addr + len(id) + id_addr) - id_addr


	
def search(db, dmp, params, dir3, dir4, scan_nbr = 0):
	start = time.time()
	chunk_sz, high_addr, pg_cnt, file_sz = 200*MB, 0, 0, os.stat(dmp).st_size	
	infile = open(dmp, "rb")	
	json_file = "%s%s" % (dir3, "json_data.json")
	json_out = open(json_file, "w")
	objectID_dct = {}
	chunk = infile.read(chunk_sz)
	addr = 0	
	while chunk:
		if db in ("sqlite1k", "sqlite4k"):
			sqlite_addr = chunk.find("SQLite format 3")
		addr = get_next_address(db, addr, chunk, params, scan_nbr)
		
		while addr >= 0:
			if db in ("sqlite1k", "sqlite4k") and addr - sqlite_addr < params['pg_sz'] and pg_cnt == 0:
				pg_cnt += 1
				file_addr = high_addr + sqlite_addr
				addr = sqlite_addr
				header, row_dir, rows = pg.page(params, chunk[addr:addr+params['pg_sz']], addr, file_addr, True)
				addr += 101			
			else:				
				file_addr = high_addr + addr
				
				header, row_dir, rows = pg.page(params, chunk[addr:addr+params['pg_sz']], addr, file_addr)
						

			if header is not None and rows is not None and header['status'] and row_dir is not None:
				pg_cnt += 1
				if db in ("sqlite1k", "sqlite4k"):
					oid = output.convert_schema(header['schema'])
				else:
					oid = get_oid(header)				
				try:
					outfile4 = objectID_dct[oid]
				except:
					outfile4 = "%s%s.csv" % (dir4, oid)
					objectID_dct[oid] = outfile4
				output.write_output(params, dmp, rows, header, file_addr, row_dir, json_out, outfile4, pg_cnt)	
				
			addr = get_next_address(db, addr + 1, chunk, params, scan_nbr)			
		high_addr += chunk_sz		
		common.status_bar(high_addr, file_sz, start, time.time())
		chunk = infile.read(chunk_sz)
	if db in ("sqlite1k", "sqlite4k") and scan_nbr != 0:
		return
	if db in ("sqlite1k", "sqlite4k"):	
		search(db, dmp, params, dir3, dir4, 1)
		search(db, dmp, params, dir3, dir4, 2)
	print "\n %s pages found" % pg_cnt
	infile.close()	
