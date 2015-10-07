import fiona
import cx_Oracle
from os import path
from fiona import crs
from datetime import datetime
from collections import OrderedDict
from shapely.geometry import mapping, shape, LineString

pwd_msg = 'enter pwd for db: {0}, user: {1}\n'
date_msg1 = 'enter a summary begin date as m/d/yy, note that a valid'
date_msg2 = ' date must appear in the \'summary_period\' table:\n'

o_dbname = 'HAWAII'
o_user = 'tmpublic'
o_password = raw_input(pwd_msg.format(o_dbname, o_user))

date_str = raw_input(date_msg1 + date_msg2)
serv_date = datetime.strptime(date_str, '%m/%d/%y').date()
path_date = serv_date.strftime('%Y-%m-%d')

project_dir = '//gisstore/gis/PUBLIC/GIS_Projects/Vehicle_Miles'
rlis_dir = '//gisstore/gis/Rlis'

patterns_name = 'oracle_patterns_{0}.shp'.format(path_date)
patterns_path = path.join(project_dir, 'shp', patterns_name)
cities_path = path.join(rlis_dir, 'BOUNDARY', 'cty_fill.shp')

def createPatternGeomFromOracle():
	""""""

	o_conn = cx_Oracle.connect(o_user, o_password, o_dbname)
	o_cur = o_conn.cursor()

	q = """SELECT x_coordinate as x, y_coordinate as y, 
		     route_begin_date as begin_date, route_number as route,
		     direction, pattern_id as pattern, 
		     shape_point_distance as seq
		   from shape_point_distance
		   where route_begin_date = :begin_date"""

	o_cur.execute(q, begin_date=serv_date)
	field_names = [d[0].lower() for d in o_cur.description]

	pattern_pts = {}
	for row in o_cur.fetchall():
		pt = dict(zip(field_names, row))
		
		coords = (pt.pop('x'), pt.pop('y'))
		pt['coords'] = coords

		date = pt.pop('begin_date')
		route =  pt.pop('route')
		direct = pt.pop('direction')
		patt = pt.pop('pattern')
		k = (date, route, direct, patt)

		pattern_pts[k] = pattern_pts.get(k,[]) + [pt]
	o_conn.close()

	pattern_lines = {}
	pattern_props = getVolumeUsageModeAttributes()
	for pk, pt_list in pattern_pts.iteritems():
		pt_list.sort(key=lambda k: k['seq'])
		geom = LineString([d['coords'] for d in pt_list])

		props = pattern_props[pk]
		props['geom'] = geom
		pattern_lines[pk] = props

	metadata = {
		'crs': crs.from_epsg(2913),
		'driver': 'ESRI Shapefile',
		'schema': {
			'geometry': 'LineString',
			'properties': OrderedDict([
				('begin_date', 'str'),
				('route', 'int'),
				('direction', 'int'),
				('pattern', 'int'),
				('trip_count', 'float'),
				('trip_mode', 'str'),
				('usage', 'str'),
				('len_miles', 'float')
			])
		}
	}

	with fiona.open(patterns_path, 'w', **metadata) as oracle_patterns:
		for pk, fields in pattern_lines.iteritems():
			feat = {
				'geometry': mapping(fields['geom']),
				'properties': {
					'begin_date': pk[0].strftime('%m/%d/%Y'),
					'route': pk[1],
					'direction': pk[2],
					'pattern': pk[3],
					'trip_count': fields['trip_count'],
					'trip_mode': fields['trip_mode'],
					'usage': fields['usage'],
					'len_miles': fields['geom'].length / 5280
				}
			}

			oracle_patterns.write(feat)

def getVolumeUsageModeAttributes():
	""""""

	o_conn = cx_Oracle.connect(o_user, o_password, o_dbname)
	o_cur = o_conn.cursor()

	q = """WITH pct_operated AS (
		     SELECT c.service_key, p.summary_begin_date,
		       COUNT(c.calendar_date) / 
		         (p.summary_end_date - p.summary_begin_date) AS pct
		     FROM schedule_calendar c, summary_period p
		     WHERE c.calendar_date BETWEEN
		       p.summary_begin_date AND p.summary_end_date
		     GROUP BY c.service_key, p.summary_begin_date, p.summary_end_date)
  
		   SELECT t.trip_begin_date AS begin_date, t.route_number AS route,
		     t.direction, t.pattern_id AS pattern, SUM(op.pct) AS daily_trips,
		     COALESCE(st.route_sub_type_description, 'Bus') AS trip_mode, 
		     r.route_usage AS usage
		   FROM trip t, pct_operated op, route r
		   LEFT JOIN route_sub_type st
		     ON st.route_sub_type = r.route_sub_type
		   WHERE t.trip_begin_date = :begin_date
		     AND t.trip_begin_date = op.summary_begin_date
		     AND t.service_key = op.service_key
		     AND r.route_begin_date = t.trip_begin_date
		     AND r.route_number = t.route_number
		   GROUP BY t.route_number, t.direction, t.pattern_id, 
		     t.trip_begin_date, st.route_sub_type_description,
		     r.route_usage"""

	o_cur.execute(q, begin_date=serv_date)
	field_names = [d[0].lower() for d in o_cur.description]
	
	pattern_props = {}
	usage_dict = {'D': 'deadhead', 'N': 'deadhead', 'R': 'revenue'}
	for row in o_cur.fetchall():
		attrs = dict(zip(field_names, row))

		date = attrs['begin_date']
		route = attrs['route']
		direct = attrs['direction']
		patt = attrs['pattern']
		k = (date, route, direct, patt)

		pattern_props[k] = {
			'trip_count': attrs['daily_trips'],
			'trip_mode': attrs['trip_mode'],
			'usage': usage_dict[attrs['usage']]
		}

	o_conn.close()
	return pattern_props

def clipPatternsToCityLimits(city_name, buffer=False):
	""""""

	with fiona.open(cities_path) as cities:
		for c in cities:
			if c['properties']['CITYNAME'] == city_name:
				city_geom = shape(c['geometry'])
				break

	# the option for a 100 foot buffer on the city exists because some
	# routes run roughly along city boundary where it uses roads, but 
	# the line work in the two datsets is not identical
	if buffer:
		city_geom = city_geom.buffer(100)
	

	name_template = 'oracle_pattern_clip_{0}_{1}'
	clip_name = name_template.format(city_name, path_date)
	clip_path = path.join(project_dir, 'shp', clip_name)
	
	with fiona.open(patterns_path) as oracle_patterns:
		metadata = oracle_patterns.meta.copy()
		
		with fiona.open(clip_path, 'w', **metadata) as pattern_clip:
			for p in oracle_patterns:
				geom = shape(p['geometry'])

				add_flag = False
				if geom.within(city_geom):
					add_flag = True
				elif geom.intersects(city_geom):
					geom = geom.intersection(city_geom)
					p['geometry'] = mapping(geom)
					add_flag = True

				if add_flag:
					props = p['properties']
					props['len_miles'] = (geom.length / 5280)

					pattern_clip.write(p)

	return clip_path

def getVehicleMilesTraveled(patterns_path):
	"""Tally up the vehicle miles travel, sorting the totals by
	revenue/deadhead routes as well as by mode"""

	miles_dict = {'revenue': {}, 'deadhead': {}}
	
	with fiona.open(patterns_path) as patterns:
		for p in patterns:
			props = p['properties']
			route = props['route']
			mode = props['trip_mode']
			miles = props['len_miles']
			count = props['trip_count']
			distance = (miles * count)

			k = props['usage']
			miles_dict[k][mode] = miles_dict[k].get(mode, 0) + distance

	for name, mode_dict in miles_dict.iteritems():
		print '\n{0} miles:'.format(name).title()
		for mode, miles in mode_dict.iteritems():
			print '{0}: {1:,.2f}'.format(mode, miles)

createPatternGeomFromOracle()
clip_path = clipPatternsToCityLimits('Beaverton', True)
getVehicleMilesTraveled(clip_path)