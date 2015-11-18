import fiona, psycopg2, argparse
from fiona import crs
from arcpy import da, env, analysis, management
from os.path import join
from shapely import wkb
from pprint import pprint
from collections import OrderedDict
from psycopg2.extras import RealDictCursor
from shapely.ops import unary_union
from shapely.geometry import mapping, shape

dbname = 'trimet'
host = 'maps6.trimet.org'

env.overwriteOutput = True

project_dir = '//gisstore/gis/PUBLIC/GIS_Projects/eFare_Project'
subproj_dir = join(project_dir, 'Vendor_Deserts')
oproj_dir = join(project_dir, 'Vendor_Analysis', 'shp')

rc_vendors = join(oproj_dir, 'rc_vendors_ospn_2015_05.shp')
current_stops = join(subproj_dir, 'shp', 'stops.shp')
master_stops = join(oproj_dir, 'master_efare_stops.shp')
desert_gaps = join(subproj_dir, 'shp', 'desert_gaps.shp')
 
def getCurrentStops():
	""""""

	db_template = 'dbname={0} user={1} host={2} password={3}'
	db_str = db_template.format(dbname, user, host, password)
	conn = psycopg2.connect(db_str)
	cur = conn.cursor()

	q_params = {
		'schema': 'current',
		'stop_table': 'stop' }

	q = """SELECT geom, id, name, type, begin_date,
		     end_date, street_direction as street_dir
		   from {schema}.{stop_table}"""
	
	cur.execute(q.format(**q_params))
	fields = [desc[0] for desc in cur.description]

	stops_list = []
	for row in cur.fetchall():
		s = OrderedDict(zip(fields, row))
		s['geom'] = wkb.loads(s['geom'], hex=True)
		stops_list.append(s)

	# setting up a mapping between the field names and their values'
	# python field types
	stop = OrderedDict([(k,v) for k,v in stops_list[0].items() if k != 'geom'])
	fields = OrderedDict([(k, type(v).__name__) for k,v in stop.items()])

	# get the rest of table's metadata from postgis
	q1 = """SELECT GeometryType(geom) as geom_type, 
			  ST_SRID(geom) as epsg
			from {schema}.{stop_table}
			limit 1"""
	
	dict_cur = conn.cursor(cursor_factory=RealDictCursor)
	dict_cur.execute(q1.format(**q_params))
	pg_meta = dict_cur.fetchone()

	conn.close()

	# don't forget that fields names must be 10 characters or less
	# for a shapefile and fiona has basically no error messages
	metadata = {
		'crs': crs.from_epsg(pg_meta['epsg']),
		'driver': 'ESRI Shapefile',
		'schema': {
			'geometry': pg_meta['geom_type'].title(),
			'properties': fields
		}
	}

	with fiona.open(current_stops, 'w', **metadata) as stops_shp:
		for s in stops_list:
			s_dict = {'geometry': mapping(s.pop('geom'))}
			s_dict['properties'] = s

			stops_shp.write(s_dict)

def addNearestVendorDistance(stops):
	"""Non-open source function, booooooooooo!!!"""

	stop_vend_tbl = join(subproj_dir, 'shp', 'stop_vend_near_tbl.dbf')
	analysis.GenerateNearTable(stops, rc_vendors, stop_vend_tbl)

	dist_dict = {}
	tbl_fields = ['IN_FID', 'NEAR_DIST']
	with da.SearchCursor(stop_vend_tbl, tbl_fields) as s_cursor:
		for fid, dist in s_cursor:
			dist_dict[fid] = dist

	dist_field, dbl_type = 'vend_dist', 'DOUBLE'
	management.AddField(stops, dist_field, dbl_type)

	stop_fields = ['OID@', dist_field]
	with da.UpdateCursor(stops, stop_fields) as u_cursor:
		for oid, dist in u_cursor:
			dist = dist_dict[oid]
			u_cursor.updateRow((oid, dist))

def generateDesertsFeature(stops, desert_dist):
	""""""

	b_box = getPgTableBbox('load.county')

	stops_buffs = []
	with fiona.open(stops) as dist_stops:
		metadata = dist_stops.meta.copy()

		for feat in dist_stops:
			geom = shape(feat['geometry'])
			fields = feat['properties']
			dist = fields['vend_dist']

			if dist > desert_dist:
				buff = geom.buffer(desert_dist)
				stops_buffs.append(buff)

	desert_area = unary_union(stops_buffs)
	
	vendor_buffs = []
	with fiona.open(rc_vendors) as vendors:
		for feat in vendors:
			geom = shape(feat['geometry'])
			buff = geom.buffer(desert_dist)
			vendor_buffs.append(buff)

	vendor_area = unary_union(vendor_buffs)
	desert_trim = desert_area.difference(vendor_area)
	desert_mask = b_box.difference(desert_trim)

	schema = metadata['schema']
	schema['geometry'] = desert_mask.geom_type
	schema['properties'] = {'id': 'int'}

	with fiona.open(desert_gaps, 'w', **metadata) as gaps_shp:
		feat = {'geometry': mapping(desert_mask)}
		feat['properties'] = {'id': 1}

		gaps_shp.write(feat)

def getPgTableBbox(table):
	""""""

	db_template = 'dbname={0} user={1} host={2} password={3}'
	db_str = db_template.format(dbname, user, host, password)
	conn = psycopg2.connect(db_str)
	cur = conn.cursor()

	q = """SELECT ST_Envelope(ST_Collect(geom)) as bbox, 1 as one
		   from {table}
		   group by one"""
	
	cur.execute(q.format(table=table))
	b_box = wkb.loads(cur.fetchone()[0], hex=True)

	conn.close()
	return b_box

def process_options(arglist=None):
	"""Define option that can be pass through the command line, the purpose
	of doing this in this case is so that all sensitive and variable 
	argumentscan be passed through a single command without prompting the user
	or storing them in the code """

	parser = argparse.ArgumentParser()
	parser.add_argument(
		'-u', '--username',
		dest='user',
		required=True,
		help='user name for postgres database "trimet"'
	)
	parser.add_argument(
		'-p', '--password',
		dest='password',
		required=True,
		help='password for postgres database "trimet"'
	)

	options = parser.parse_args(arglist)
	return options

def main():
	global user, password

	args = sys.argv[1:]
	options = process_options(args)

	user = options.user
	password = options.password

	# getCurrentStops()
	generateDesertsFeature(master_stops, 5280)

if __name__ == '__main__':
	main()
