import fiona
import cx_Oracle
from os import path
from fiona import crs
from datetime import datetime
from shapely.geometry import shape, mapping

pwd_msg = 'enter pwd for db: {0}, user: {1}\n'
dat_msg = 'enter begin date of service period as m/d/yy:\n'

o_dbname = 'HAWAII'
o_user = 'tmpublic'
o_password = raw_input(pwd_msg.format(o_dbname, o_user))

date_str = raw_input(date_msg)
serv_date = datetime.strptime(date_str, '%m/%d/%y').date()

project_dir = '//gisstore/gis/PUBLIC/GIS_Projects/Vehicle_Miles'
hastus_dir = '//Csfile2/Dev/Hastus Schedule Files'
rlis_dir = '//gisstore/gis/Rlis'

name_template = 'vehicle_mile_routes_{0}.shp'
vm_routes_name = name_template.format(serv_date.strftime('%Y-%m-%d'))
vm_routes_path = path.join(project_dir, 'shp', vm_routes_name)
cities_path = path.join(rlis_dir, 'BOUNDARY', 'cty_fill.shp')

def getHastusShapefilePath():
	"""The appropriate shapefile is in folder that is based the service
	date of interest, so the user entered date is used to grab the 
	appropriate shapefile"""

	hastus_name = 'TFM_Pat_Seg_Stop.shp'
	hastus_date = serv_date.strftime('%d%b%Y')
	hastus_path = path.join(hastus_dir, hastus_date, hastus_name)

	return hastus_path

def getPatternVolumesFromOracle():
	"""Execute a query that gets the number of times each pattern
	is driven in a week as well as the mode the mode of the vehicle
	executing the pattern and store the results as a set of dicts"""

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

		SELECT t.trip_begin_date, t.route_number, t.direction, t.pattern_id, 
		  SUM(op.pct) AS daily_trips, st.route_sub_type_description AS trip_mode
		FROM trip t, pct_operated op, route r, route_sub_type st
		WHERE t.trip_begin_date = :begin_date
		  AND t.trip_begin_date = op.summary_begin_date
		  AND t.service_key = op.service_key
		  AND r.route_begin_date = t.trip_begin_date
		  AND r.route_number = t.route_number
		  AND st.route_sub_type = r.route_sub_type
		GROUP BY t.route_number, t.direction, t.pattern_id, 
		  t.trip_begin_date, st.route_sub_type_description"""

	o_cur.execute(q, begin_date=serv_date)
	
	volumes = []
	field_names = [d[0] for d in o_cur.description]
	for row in o_cur.fetchall():
		volumes.append(dict(zip(field_names, row)))

	volume_dict = {}
	for row in volumes:
		date = row['TRIP_BEGIN_DATE'].strftime('%m/%d/%Y')
		rte = row['ROUTE_NUMBER']
		dirctn = row['DIRECTION']
		pat = row['PATTERN_ID']
		
		key = (date, rte, dirctn, pat)
		volume_dict[key] = {
			'trip_count': row['DAILY_TRIPS'],
			'trip_mode': row['TRIP_MODE']}

	o_conn.close()
	return volume_dict

def clipAddTripVolumeToRoutes():
	"""Clip the hastus routes to the extent of the city of interest and
	join the pattern volume information pulled from HAWAII to the hastus
	patterns"""

	hastus_path = getHastusShapefilePath()
	volume_dict = getPatternVolumesFromOracle()
	city_geom = getCityLimitsGeom('Beaverton')

	# note that fiona doesn't have the ability modify existing shapefiles
	# thus the creation of a new one here
	with fiona.open(hastus_path) as hastus_routes:
		metadata = hastus_routes.meta.copy()
		
		# the source shapefile does not have its projection defined (no
		# .prj file), but it is in ospn so define it here
		metadata['crs'] = crs.from_epsg(2913)

		# add new fields to hold trip count and segment length
		fields = metadata['schema']['properties']
		fields['trip_count'] = 'float:12.3'
		fields['trip_mode'] = 'str'

		unmatched_patterns = set()
		with fiona.open(vm_routes_path, 'w', **metadata) as vm_routes:
			for hr in hastus_routes:
				add_flag = False
				geom = shape(hr['geometry'])
				
				# include segments only if they are in the city of interest
				# and if they straddle the city limits then clip them
				if geom.within(city_geom):
					add_flag = True
				elif geom.intersects(city_geom):
					hr['geometry'] = mapping(geom.intersection(city_geom))
					add_flag = True

				if add_flag:
					# get the pattern volume from the volume dict
					props = hr['properties']
					date = props['EFFDATE']
					rte = props['ROUTE']
					dirctn = props['DIRECTION']
					pat = props['PATTERN_ID']
					key = (date, rte, dirctn, pat)
					
					try:
						props['trip_count'] = volume_dict[key]['trip_count']
						props['trip_mode'] = volume_dict[key]['trip_mode']
					except KeyError:
						unmatched_patterns.add(key)
						props['trip_count'] = None
						props['trip_mode'] = None
					
					# update length field
					props['LENGTH'] = geom.length
					
					vm_routes.write(hr)

	if unmatched_patterns:
		print 'There was no volume match for the following patterns:'
		for key in unmatched_patterns:
			print key

def getCityLimitsGeom(name):
	"""Extract thet geometry of the city of interest and assign it to a 
	variable"""

	with fiona.open(cities_path) as cities:
		for c in cities:
			if c['properties']['CITYNAME'] == name:
				city_geom = shape(c['geometry'])

	return city_geom

def getVehicleMilesTraveled():
	"""Tally up the vehicle miles travel, sorting the totals by
	revenue/deadhead routes as well as by mode"""

	with fiona.open(vm_routes_path) as vm_routes:
		miles_dict = {'revenue': {}, 'deadhead': {}}
		for vmr in vm_routes:
			props = vmr['properties']
			route = props['ROUTE']
			hastus_rte = props['HASTUS_RTE']
			mode = props['trip_mode']
			
			miles = (props['LENGTH'] / 5280)
			count = props['trip_count'] or 0
			distance = miles * count

			if hastus_rte in ('dhd', 'pull'):
				k = 'deadhead'
			else:
				k = 'revenue'

			miles_dict[k][mode] = miles_dict[k].get(mode, 0) + distance

	for name, mode_dict in miles_dict.iteritems():
		print '\n{0} miles:'.format(name).title()
		for mode, miles in mode_dict.iteritems():
			print '{0}: {1:,.2f}'.format(mode, miles)
			
clipAddTripVolumeToRoutes()
getVehicleMilesTraveled()