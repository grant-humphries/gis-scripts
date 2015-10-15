import os
import csv
import fiona
import shutil
from os import path
from sys import stdout
from shapely.ops import unary_union
from shapely.geometry import shape, mapping
from collections import OrderedDict

rlis_dir = '//gisstore/gis/Rlis'
project_dir = '//gisstore/gis/PUBLIC/GIS_Projects/Trapeze'

counties_path = path.join(rlis_dir, 'BOUNDARY', 'co_fill.shp')
rlis_streets_path = path.join(rlis_dir, 'STREETS', 'streets.shp')
rlis_oneway_path = path.join(project_dir, 'dbf', 'RLIS_Streets_Directionality.dbf')
rlis_city_names_csv = path.join(project_dir, 'csv', 'rlis_streets_city_values.csv')
rlis_addr_points = path.join(rlis_dir, 'TAXLOTS', 'master_address.shp')
tpz_streets_path = path.join(project_dir, 'shp', 'rlis_trapeze_streets.shp')

def convertRlisStreets():
	"""Convert the attributes of the RLIS streets into format used by
	Trapeze for their PASS application"""

	# establish trapeze streets schema and mapping between fields are field
	# values amongst the rlis and trapeze schemas
	tpz_schema = OrderedDict([
		('SEG_ID', 'int'),		('DIRPRE', 'str'),		('FEANAME', 'str'),
		('FEATYPE', 'str'),		('DIRSUF', 'str'),		('ONEWAY', 'str'),
		('CFCC', 'str'),		('FRADDRL', 'int'),		('TOADDRL', 'int'),
		('FRADDRR', 'int'),		('TOADDRR', 'int'),		('ZIPIDL', 'str'),
		('ZIPIDR', 'str'),		('CITY_L', 'str'),		('CITY_R', 'str'),
		('STATE', 'str:2'),		('REF_ZLEV', 'int'),	('NREF_ZLEV', 'int')])

	field_map = {
		'LOCALID': 'SEG_ID',	'PREFIX': 'DIRPRE',		'STREETNAME': 'FEANAME',
		'FTYPE': 'FEATYPE',		'DIRECTION': 'DIRSUF',	'LEFTADD1': 'FRADDRL',
		'LEFTADD2': 'TOADDRL',	'RGTADD1': 'FRADDRR',	'RGTADD2': 'TOADDRR',
		'LEFTZIP': 'ZIPIDL',	'RIGHTZIP': 'ZIPIDR',	'TYPE': 'CFCC',
		'LCITY': 'CITY_L',		'RCITY': 'CITY_R',		'F_ZLEV': 'REF_ZLEV',
		'T_ZLEV': 'NREF_ZLEV'}

	# it's more concise to write the mapping this way and then flip it
	cfcc_type_map = {
		'A10': [1110, 5101],
		'A20': [1200, 1300, 5201, 5301],
		'A30': [1400, 5401, 5402],
		'A40': [1450, 1500, 1550, 1700, 5451, 5500, 5501, 8224],
		'A50': [9000],
		'A63': [1120, 1121, 1122, 1123, 1221, 1222, 1223, 1321, 1421, 1471, 1521],
		'A73': [1600],
		'A74': [1560, 1800, 2000]}
	type_cfcc_map = {i:k for k,v in cfcc_type_map.iteritems() for i in v}

	# rlis description of oneway values:
	# 0: No Data
	# 1: Two Way Street
	# 2: One Way Street with digitized direction
	# 3: One Way Street against digitized direction.
	oneway_map = {0: '',  1: '', 2: 'FT', 3: 'TF'}
	oneway_dict = getRlisOnewayValues()
	
	city_name_map = expandRlisCityNameAbbrs()
	city_name_map[None] = None
	trico_geom = getTriCountyGeom()
	
	# create empty trapeze record for attributes that will be populated,
	# the value for state is alway oregon and can't be derived from rlis
	# so that is defined here
	tpz_props = {k:None for k,v in tpz_schema.iteritems()}
	tpz_props['STATE'] = 'OR'

	with fiona.open(rlis_streets_path) as rlis_streets:
		# copy rlis metadata and replace its schema with the trapeze schema
		metadata = rlis_streets.meta.copy()
		metadata['schema']['properties'] = tpz_schema

		i = 1
		with fiona.open(tpz_streets_path, 'w', **metadata) as tpz_streets:
			for rs in rlis_streets:
				rs_geom = shape(rs['geometry'])

				# only add features to trapeze if they fall at least
				# partilly within the tri-county area
				if rs_geom.intersects(trico_geom):
					for k,v in rs['properties'].iteritems():
						if k in field_map:
							if k == 'TYPE':
								tpz_props[field_map[k]] = type_cfcc_map[v]
							elif k in ('LCITY', 'RCITY'):
								tpz_props[field_map[k]] = city_name_map[v]
							else:
								tpz_props[field_map[k]] = v

					rlis_id = rs['properties']['LOCALID']
					tpz_props['ONEWAY'] = oneway_map[oneway_dict[rlis_id]]

					tpz_feat = rs.copy()
					tpz_feat['properties'] = tpz_props

					tpz_streets.write(tpz_feat)

				# give the user an indication of the progress towards the
				# creation of the new shapefile
				if i % 100 == 0:
					stdout.write('.')
					stdout.flush()
				if i % 5000 == 0:
					# add commas after three digits in output numbers
					stdout.write('{:,}'.format(i))
					stdout.flush()
				i += 1

def getTriCountyGeom():
	"""Create a single geometry that comprises the three county area, this 
	will be used to test if streets segments fall in that region"""

	geom_list = []
	trico = ['Clackamas', 'Multnomah', 'Washington']
	with fiona.open(counties_path) as counties:
		for c in counties:
			if c['properties']['COUNTY'] in trico:
				geom_list.append(shape(c['geometry']))

	return unary_union(geom_list)

def getRlisOnewayValues():
	"""Oneway values from RLIS streets are stored in a separate table that
	is not distributed as a part of the RLIS release, this function extracts
	those values so they can be joined to the appropriate street segment"""

	oneway_dict = {}
	with fiona.open(rlis_oneway_path) as rlis_oneway:
		for ro in rlis_oneway:
			rlis_id = ro['properties']['LOCALID']
			drct = ro['properties']['drct']
			oneway_dict[rlis_id] = drct

	return oneway_dict

def expandRlisCityNameAbbrs():
	"""City names are abbreviated to four letters in rlis and this won't work
	for Trapeze, this function reads a table that has the name expansions and
	writes the mappings to a dictionary"""

	city_name_map = {}
	with open(rlis_city_names_csv) as rlis_city_names:
		city_reader = csv.reader(rlis_city_names)
		
		# skip header
		city_reader.next()
		for abbr, name in city_reader:
			city_name_map[abbr] = name

	return city_name_map

def copyAddrPointsToProjFolder():
	"""The address points need to be zipped up with streets to be sent to 
	Trapeze this function copies them to folder with the streets shapefile"""

	addr_pt_dir = path.dirname(rlis_addr_points)
	addr_pt_shp = path.basename(rlis_addr_points)
	addr_pt_name = path.splitext(addr_pt_shp)[0]
	shp_exts = ['.dbf', '.prj', '.shp', '.shx']
	tpz_addr_name = 'rlis_trapeze_addr_points{0}'

	print '\nCopying address point to Trapeze directory\n'
	for shp_element in os.listdir(addr_pt_dir):
		name, ext = path.splitext(shp_element)
		if name == addr_pt_name and ext in shp_exts:
			src = path.join(addr_pt_dir, shp_element)
			dst = path.join(project_dir, 'shp', tpz_addr_name.format(ext))
			
			print 'source: {0}'.format(src)
			print 'target: {0}\n'.format(dst)
			shutil.copy2(src, dst)

convertRlisStreets()
copyAddrPointsToProjFolder()