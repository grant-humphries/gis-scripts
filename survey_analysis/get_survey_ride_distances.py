import os, csv, arcpy
import argparse, psycopg2, cx_Oracle
from os import path
from arcpy import da, env, management
from shapely import geometry
from psycopg2 import extras
from datetime import datetime

# configure environment settings
env.overwriteOutput = True

# Postgres credentials
pg_user = 'postgres'
pg_host = 'localhost'

# Oracle credentials
o_dbname = 'HAWAII'
o_user = 'tmpublic'

survey_rows = []
proj_dir = '//gisstore/gis/PUBLIC/GIS_Projects/Survey_Analysis/streetcar15_r3'
survey_gdb = path.join(proj_dir, 'gdb', 'survey.gdb')
survey_fc = path.join(survey_gdb, 'streetcar_survey_r3')
matched_csv_path = path.join(proj_dir, 'csv', 'streetcar_survey_r3.csv')
switch_dir_csv_path = path.join(proj_dir, 'csv', 'sc_dir_switch_r3.csv')
bad_ts_csv_path = path.join(proj_dir, 'csv', 'sc_bad_timestamp_r3.csv')
oregon_spn = arcpy.SpatialReference(2913)
output_fields = [
	'ride_id', 		'serv_date',	'time',			'passage',
	'pass_desc', 	'route', 		'rte_desc',		'direction',
	'dir_desc', 	'stop_id',		'stop_name', 	'dist_point',
	'dist_travl', 	'stop_travl'
]

def createSubDirs():
	"""Create sub-directories that project data will be stored in"""

	new_dirs = ['gdb', 'csv']
	for nd in new_dirs:
		nd_path = path.join(proj_dir, nd)
		if not path.exists(nd_path):
			os.makedirs(nd_path)

def createOracleCursor():
	"""Create cursor that can send queries to the hawaii database"""

	o_conn = cx_Oracle.connect(o_user, o_password, o_dbname)
	o_cur = o_conn.cursor()
	return o_cur

def readPgTable():
	"""This an alternate to the readCsv function that allows the data to be \
	read directly from a postgres database if that is where it is stored"""
	
	global survey_rows

	pg_template = 'dbname={0} user={1} host={2} password={3}'
	pg_str = pg_template.format(pg_dbname, pg_user, pg_host, pg_password)
	pg_conn = psycopg2.connect(pg_str)
	pg_cur = pg_conn.cursor(cursor_factory=extras.RealDictCursor)

	# This code is built on the survey data coming in the following
	# schema, if the table is not set up as such write a query to
	# return it in this form:
	#
	# ride_id, 			route,			on_tstamp,		off_tstamp,
	# on_dir,	 		off_dir, 		on_stop_id,		off_stop_id

	q = """SELECT uri as ride_id, rte as route,
			  (srv_date + start_time) as on_tstamp,
			  (srv_date + end_time) as off_tstamp,
			  on_stop_dir as on_dir, off_stop_dir as off_dir,
			  on_stop_id, off_stop_id
			from {0}""".format(pg_table)
	
	pg_cur.execute(q)
	survey_rows = pg_cur.fetchall()
	addMidPointTimestamp()

	pg_cur.close()

def addMidPointTimestamp():
	"""For most purposes we actually just want to know a time when the rider
	was actually on board the vehicle, the mid point of the trip is the best
	bet for that and this function generates that"""

	global survey_rows
	test_row = survey_rows[1]
	
	if set(test_row) >= {'on_tstamp', 'off_tstamp'}:
		for row in survey_rows:
			on = row['on_tstamp']
			off = row['off_tstamp']
			mid = on + (off - on ) / 2

			row['mid_tstamp'] = mid
	else:
		print '\non or off timestamp missing, can\'t generate'
		print 'midpoint timestamp'
		exit()

def getPatternRouteDate():
	"""Get the pattern id and route begin date for each surveyed ride based on 
	the rider's timestamp, route, direction and on & off stops"""

	global survey_rows
	o_cur = createOracleCursor()

	q = """SELECT distinct pattern_id, route_begin_date
			from trip t
			where t.route_number = :route
			  and t.direction = :direction
			  and to_number(to_char(:mid_tstamp, 'SSSSS'))
			    between t.trip_begin_time and t.trip_end_time
			  and exists (
			    select null from schedule_calendar sc
			    where sc.calendar_date = trunc(:mid_tstamp)
			      and sc.calendar_date 
			        between t.trip_begin_date and t.trip_end_date
			      and sc.service_key = t.service_key)
			  and exists (
			    select null from stop_distance sd1, stop_distance sd2
			    where sd1.location_id = :on_stop_id
			      and sd2.location_id = :off_stop_id
			      and sd1.route_begin_date = sd2.route_begin_date
			      and sd1.route_number = sd2.route_number
			      and sd1.direction = sd2.direction
			      and sd1.pattern_id = sd2.pattern_id
			      and sd1.stop_sequence_number < sd2.stop_sequence_number
			      and sd1.route_begin_date = t.trip_begin_date
			      and sd1.route_number = t.route_number
			      and sd1.direction = t.direction
			      and sd1.pattern_id = t.pattern_id)"""

	for row in survey_rows:
		if row['on_dir'] == row['off_dir']:
			query_dict = {
				'mid_tstamp': row['mid_tstamp'],
				'route': row['route'],
				'direction': row['on_dir'],
				'on_stop_id': row['on_stop_id'],
				'off_stop_id': row['off_stop_id']
			}
			
			o_cur.execute(q, query_dict)
			desc = [d[0].lower() for d in o_cur.description]
			try:
				new_fields = dict(zip(desc, o_cur.fetchone()))
				row.update(new_fields)
			except TypeError:
				print 'no results returned from pattern_id query'
				print 'input parameters are below:'
				for k, v in query_dict.iteritems():
					print '{0}: {1}'.format(k,v)
				print ''
	
	o_cur.close()

def getStopsDistanceTraveled():
	"""Get the distance that each rider traveled and the number of stops they visited
	(excluding the boarding stop) while they were on board the transit vehicle"""

	global survey_rows
	o_cur = createOracleCursor()

	q1 = """SELECT (lead(stop_distance, 1, 0) 
			  over (order by stop_sequence_number) - stop_distance) as dist_travl
			from stop_distance
			where route_begin_date = :route_begin_date
			  and route_number = :route
			  and direction = :direction
			  and pattern_id = :pattern_id
			  and location_id in (:on_stop_id, :off_stop_id)
			order by dist_travl desc"""

	q2 = """SELECT count(*) - 1 as stop_travl
			from (
			  select route_begin_date, route_number, direction, 
			    pattern_id, location_id, stop_distance
			  from stop_distance sd
			  where route_begin_date = :route_begin_date
			    and route_number = :route
			    and direction = :direction
			    and pattern_id = :pattern_id
			    and stop_distance between (
			        select stop_distance 
			        from stop_distance sd1
			        where sd1.location_id = :on_stop_id
			          and sd1.route_begin_date = sd.route_begin_date
			          and sd1.route_number = sd.route_number
			          and sd1.direction = sd.direction
			          and sd1.pattern_id = sd.pattern_id)
			      and (
			        select stop_distance 
			        from stop_distance sd2
			        where sd2.location_id = :off_stop_id
			          and sd2.route_begin_date = sd.route_begin_date
			          and sd2.route_number = sd.route_number
			          and sd2.direction = sd.direction
			          and sd2.pattern_id = sd.pattern_id)
			  --The group by technique here eliminates dwells, etc.
			  --that are often in the stop_location table
			  group by route_begin_date, route_number, direction, 
			    pattern_id, location_id, stop_distance)"""

	queries = [q1, q2]

	for row in survey_rows:
		if 'pattern_id' in row:
			query_dict = {
				'route_begin_date': row['route_begin_date'],
				'route': row['route'],
				'direction': row['on_dir'],
				'pattern_id': row['pattern_id'],
				'on_stop_id': row['on_stop_id'],
				'off_stop_id': row['off_stop_id']
			}
			
			for q in queries:
				o_cur.execute(q, query_dict)
				desc = [d[0].lower() for d in o_cur.description]
				temp_dict = dict(zip(desc, o_cur.fetchone()))
				row.update(temp_dict)

	o_cur.close()

def getStraightLineDistance():
	"""Get the distance between the on and off stops of for each survey ride"""

	global survey_rows
	
	if 'on_x' not in survey_rows:
		getStopNameAndCoords() 

	for row in survey_rows:
		on_pt = geometry.Point(row['on_x'], row['on_y'])
		off_pt = geometry.Point(row['off_x'], row['off_y'])
		dist_point = on_pt.distance(off_pt)

		row['dist_point'] = dist_point

def getRouteDesc():
	"""Get the get the verbal description of the route provided from the
	oracle HAWAII database"""

	global survey_rows
	o_cur = createOracleCursor()

	q = """SELECT public_route_description as rte_desc
			from route_def
			where route_number = :route
			  and TRUNC(:mid_tstamp) 
			  	BETWEEN route_begin_date and route_end_date"""

	for row in survey_rows:
		query_dict = {
			'route': row['route'],
			'mid_tstamp': row['mid_tstamp']
		}	  

		o_cur.execute(q, query_dict)
		desc = [d[0].lower() for d in o_cur.description]
		temp_dict = dict(zip(desc, o_cur.fetchone()))
		row.update(temp_dict)

	o_cur.close()

def getDirectionDesc():
	"""Get the direction description for all direction codes included in the
	survey data"""

	global survey_rows
	o_cur = createOracleCursor()

	q = """SELECT onn.public_direction_description as on_dir_desc,
			  off.public_direction_description as off_dir_desc
			from route_direction_def onn, route_direction_def off
			where onn.direction = :on_dir
			  and onn.route_number = :route
			  and off.direction = :off_dir
			  and off.route_number = onn.route_number
			  and off.route_begin_date = onn.route_begin_date
			  and exists (
			  	select null
			  	from route_def rd
			  	where rd.route_number = onn.route_number
			  	  and rd.route_begin_date = onn.route_begin_date
			  	  and TRUNC(:mid_tstamp)
			  	    BETWEEN rd.route_begin_date AND rd.route_end_date)"""

	for row in survey_rows:
		query_dict = {
			'route': row['route'],
			'on_dir': row['on_dir'],
			'off_dir': row['off_dir'],
			'mid_tstamp': row['mid_tstamp']
		}	  

		o_cur.execute(q, query_dict)
		desc = [d[0].lower() for d in o_cur.description]
		temp_dict = dict(zip(desc, o_cur.fetchone()))
		row.update(temp_dict)

	o_cur.close()

def getStopNameAndCoords():
	"""Get the name of x, y coordinates for each both the on and off stops
	for each survey record from the HAWAII db"""

	global survey_rows
	o_cur = createOracleCursor()

	q = """SELECT loc1.x_coordinate as on_x, loc1.y_coordinate as on_y,
			  loc1.public_location_description as on_stop_name,
			  loc2.x_coordinate as off_x, loc2.y_coordinate as off_y,
			  loc2.public_location_description as off_stop_name
			from location loc1, location loc2
			where loc1.location_id = :on_stop_id
			  and loc2.location_id = :off_stop_id"""

	for row in survey_rows:
		query_dict = {
			'on_stop_id': row['on_stop_id'],
			'off_stop_id': row['off_stop_id']
		}
		
		o_cur.execute(q, query_dict)
		desc = [d[0].lower() for d in o_cur.description]
		location_dict = dict(zip(desc, o_cur.fetchone()))
		row.update(location_dict)

	o_cur.close()

def createGdbFeatureClass():
	"""Create a gdb feature class to hold all of the data that has been
	gathered using the other functions"""

	if not arcpy.Exists(survey_gdb):
		management.CreateFileGDB(path.dirname(survey_gdb), 
			path.basename(survey_gdb))

	geom = 'POINT'
	management.CreateFeatureclass(path.dirname(survey_fc),
		path.basename(survey_fc), geom, spatial_reference=oregon_spn)

	tn_dict = {
		'DOUBLE': ['dist_point', 'dist_travl'],
		'LONG': ['direction', 'passage', 'route', 'stop_id', 'stop_travl'],
		'TEXT': ['dir_desc', 'pass_desc', 'ride_id', 'rte_desc',
			'serv_date', 'stop_name', 'time']
	}
	nt_dict = {n:t for t, nl in tn_dict.iteritems() for n in nl}

	for f_name in output_fields:
		management.AddField(survey_fc, f_name, nt_dict[f_name])

def writeToFeatureClass(write_rows):
	"""Write output rows to shapefile"""

	i_fields = ['SHAPE@'] + output_fields
	i_cursor = da.InsertCursor(survey_fc, i_fields)
	for row in write_rows:
		i_cursor.insertRow(row)

	del i_cursor

def writeToCsv(write_rows, csv_path):
	"""Write output rows to csv"""

	with open(csv_path, 'wb') as output_csv:
		output_writer = csv.writer(output_csv)
		for row in write_rows:
			output_writer.writerow(row)

def writeToOutputs():
	"""Write all data gathered about the survey rides from the hawaii database
	(and the source data) to a feature class that has two entries per ride, one
	each for the one and off stops, the geometry will be the location of the stop"""

	geo_rows = []
	csv_header = (
		'ride id',				'route',				'route description',
		'service date',			'on time',				'on direction',
		'on dir description',	'on stop id',			'on stop name',
		'on x-coordinate',		'on y-coordinate',		'off time',
		'off direction',		'off dir description',	'off stop id',
		'off stop name',		'off x-coordinate',		'off y-coordinate',
		'point distance',		'route distance',		'stops visited'
	)
	matched_rows = [csv_header]
	switch_rows = [csv_header[:-2]]
	bad_ts_rows = [csv_header[:-2]]

	for row in survey_rows:
		# 'on' attributes
		on_x = row['on_x']
		on_y = row['on_y']
		on_pt = arcpy.PointGeometry(arcpy.Point(on_x, on_y), oregon_spn)
		on_date = datetime.strftime(row['on_tstamp'], '%Y-%m-%d')
		on_time = datetime.strftime(row['on_tstamp'], '%H:%M:%S')
		on_direction = row['on_dir']
		on_dir_desc = row['on_dir_desc']
		on_passage = 0
		on_pass_desc = 'on'
		on_stop_id = row['on_stop_id']
		on_stop_name = row['on_stop_name']
		on_dist_pt = 0
		on_dist_travl = 0
		on_stop_travl = 0
		
		# 'off' attributes
		off_x = row['off_x']
		off_y = row['off_y']
		off_pt = arcpy.PointGeometry(arcpy.Point(off_x, off_y), oregon_spn)
		off_date = datetime.strftime(row['off_tstamp'], '%Y-%m-%d')
		off_time = datetime.strftime(row['off_tstamp'], '%H:%M:%S')
		off_direction = row['off_dir']
		off_dir_desc = row['off_dir_desc']
		off_passage = 1
		off_pass_desc = 'off'
		off_stop_id = row['off_stop_id']
		off_stop_name = row['off_stop_name']
		off_dist_pt = row['dist_point']

		try:
			off_dist_travl = row['dist_travl']
			off_stop_travl = row['stop_travl']
			
		except KeyError:
			off_dist_travl = None
			off_stop_travl = None

		# ride attributes
		ride_id = row['ride_id']
		route = row['route']
		rte_desc = row['rte_desc']
		
		geo_on_row = (
			on_pt,			ride_id,		on_date,			on_time,
			on_passage,		on_pass_desc,	route,				rte_desc,
			on_direction,	on_dir_desc,	on_stop_id,			on_stop_name,
			on_dist_pt,		on_dist_travl,	on_stop_travl
		)

		geo_off_row = (
			off_pt,			ride_id,		off_date,			off_time,
			off_passage,	off_pass_desc,	route,				rte_desc,
			off_direction,	off_dir_desc,	off_stop_id,		off_stop_name,
			off_dist_pt,	off_dist_travl,	off_stop_travl
		)
		geo_rows.extend([geo_on_row, geo_off_row])
		
		csv_row = (
			ride_id,		route,			rte_desc,			on_date,
			on_time,		on_direction,	on_dir_desc,		on_stop_id,
			on_stop_name,	on_x,			on_y,				off_time,
			off_direction,	off_dir_desc,	off_stop_id,		off_stop_name,
			off_x,			off_y,			off_dist_pt,		off_dist_travl,
			off_stop_travl
		)
		if off_dist_travl:
			matched_rows.append(csv_row)
		elif on_direction != off_direction:
			switch_rows.append(csv_row[:-2])
		else:
			bad_ts_rows.append(csv_row[:-2])

	writeToFeatureClass(geo_rows)
	writeToCsv(matched_rows, matched_csv_path)
	writeToCsv(switch_rows, switch_dir_csv_path)
	writeToCsv(bad_ts_rows, bad_ts_csv_path)

def process_options(arglist=None):
	"""Define option that can be pass through the command line, the purpose
	of doing this in this case is so that all sensitive and variable 
	argumentscan be passed through a single command without prompting the user
	or storing them in the code """

	parser = argparse.ArgumentParser()
	parser.add_argument(
		'-d', '--dbname',
		dest='pg_dbname',
		required=True,
		help='name of postgres database containing survey data'
	)
	parser.add_argument(
		'-t', '--table',
		dest='pg_table',
		required=True,
		help='name of table in postgres db containing survey data'
	)
	parser.add_argument(
		'-pp', '--pg_password',
		dest='pg_password',
		required=True,
		help='password for postgres user: {0}'.format(pg_user)
	)
	parser.add_argument(
		'-op', '--o_password',
		dest='o_password',
		required=True,
		help='password for Oracle db: {0}, user: {1}'.format(
			o_dbname, o_user)
	)

	options = parser.parse_args(arglist)
	return options

def main():
	global pg_dbname, pg_table, pg_password, o_password

	args = sys.argv[1:]
	options = process_options(args)

	pg_dbname = options.pg_dbname
	pg_table = options.pg_table
	pg_password = options.pg_password
	o_password = options.o_password

	createSubDirs()
	readPgTable()
	getPatternRouteDate()
	getStopsDistanceTraveled()
	getStraightLineDistance()
	getRouteDesc()
	getDirectionDesc()
	createGdbFeatureClass()
	writeToOutputs()

if __name__ == '__main__':
	main()