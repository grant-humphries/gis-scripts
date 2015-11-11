# Grant Humphries, 2015
# python version: 2.7.8
#--------------------------------

import os, re, csv, arcpy, requests
from arcpy import da, env, management
from os.path import basename, dirname, exists, join
from collections import defaultdict, OrderedDict

# Set year variables to the year the analysis is to be run upon
yr = '2014'
env.overwriteOutput = True
token = raw_input('Enter token for RLIS API')

data_dir = '//gisstore/gis/Data/Employer/Confidential_Data/QCEW/'
project_dir = '//gisstore/gis/PUBLIC/GIS_Projects/Streetcar_LID_Employment'
csv_dir = join(project_dir, yr, 'csv')

src_qcew = join(data_dir, yr, 'QCEW2014_ClackMultWash.shp')
qcew_2913 = join(data_dir, yr, 'qcew_{0}_ospn.shp'.format(yr))
regeo_qcew = join(data_dir, yr, 'regeocoded_qcew_{0}.shp'.format(yr))
west_lid = join(project_dir, 'shp', 'westside_streetcar_lid.shp')
east_lid = join(project_dir, 'shp', 'eastside_streetcar_lid.shp')

def reprojectQcew(overwrite=False):
	"""Reproject the QCEW data to Oregon State Plane North adjusting any
	attributes that are affected"""

	if exists(qcew_2913) and not overwrite:
		print '\nstate plane qcew already exists, if you wish to'
		print 'overwrite the existing file use the "overwrite" flag\n'
		return

	geom_type = 'POINT'
	template = src_qcew
	ospn = arcpy.SpatialReference(2913)
	management.CreateFeatureclass(dirname(qcew_2913),
		basename(qcew_2913), geom_type, template, spatial_reference=ospn)

	i_cursor = da.InsertCursor(qcew_2913, '*')

	s_fields = ['Shape@', '*']
	with da.SearchCursor(src_qcew, s_fields) as s_cursor:
		# replace point coordinates with geometry object in field
		# definition
		fields = list(s_cursor.fields)
		fields[1] = fields.pop(0)

		for row in s_cursor:
			list_row = list(row)
			list_row[1] = list_row.pop(0)
			d = OrderedDict(zip(fields, list_row))

			geom = d['Shape@']
			geom_2913 = geom.projectAs(ospn) 
			d['Shape@'] = geom_2913
			d['POINT_X'] = geom_2913.firstPoint.X
			d['POINT_Y'] = geom_2913.firstPoint.Y

			write_row = [v for v in d.values()]
			i_cursor.insertRow(write_row)

	del i_cursor

def regeocodeZipLevelPts(overwrite=False):
	"""Use the RLIS API to regecode any point that were matched at the zip
	code level or worse, for our purpose that level of accuracy is not good
	enough"""

	if exists(regeo_qcew) and not overwrite:
		print '\nthis year\'s qcew has already been regecoded, if you wish'
		print 'to overwrite the existing file use the "overwrite" flag\n'
		return

	management.CopyFeatures(qcew_2913, regeo_qcew)
	manual_geos = retrieveManualGeocodes()

	regeo, manual = 0, 0
	with da.UpdateCursor(regeo_qcew, '*') as cursor:
		for row in cursor:
			d = OrderedDict(zip(cursor.fields, row))

			if int(d['PRECISION_']) > 250:
				addr_str = '{0}, {1}, {2}, {3}'.format(
					d['STREET'], d['CITY'], d['ST'], d['ZIP'])

				rsp = geocode(addr_str)
				if isinstance(rsp, int):
					print 'there seems to a problem in connecting with'
					print 'the rlis api halting geoprocessing until this'
					print 'is resolved'
					exit()
				elif rsp:
					# assign now geometry to row
					d['Shape'] = (rsp['ORSP_x'], rsp['ORSP_y'])

					# update geocoding attributes
					d['DESC_'] = rsp['locator']
					d['PRECISION_'] = 10
					d['GISDATA'] = 'RLIS API Geocoder'
					d['GSCR'] = rsp['score']
					d['Match_TYPE'] = 'A'
					d['POINT_X'] = rsp['ORSP_x']
					d['POINT_Y'] = rsp['ORSP_y']
					regeo+=1

				elif d['BIN'] in manual_geos:
					mg_dict = manual_geos[d['BIN']]
					coords = mg_dict['Shape']
					
					d['Shape'] = coords
					d['DESC_'] = mg_dict['Loc_name']
					d['PRECISION_'] = 10
					d['GISDATA'] = 'Address Massaging + RLIS'
					d['GSCR'] = mg_dict['Score']
					d['Match_TYPE'] = mg_dict['Match_type']
					d['POINT_X'] = coords[0]
					d['POINT_Y'] = coords[1]

					manual+=1

			write_row = [v for v in d.values()]	
			cursor.updateRow(write_row)

	print '\nregocoded: {0}, from manual: {1}'.format(regeo, manual)

def retrieveManualGeocodes():
	"""In previous years I've manually massaged address in order to get
	this them to match within a geocoder, I won't be doing that anymore, 
	but I can here I grab that information and apply it to the current 
	year if the precision hasn't improved"""

	shp_2013 = join(project_dir, '2013', 'shp')
	w_lid = join(shp_2013, 'west_lid_qcew13_zip_regeocoded.shp')
	e_lid = join(shp_2013, 'east_lid_qcew13_zip_regeocoded.shp')

	bin_dict = {}
	for lid in (w_lid, e_lid):
		with da.SearchCursor(lid, '*') as cursor:
			for row in cursor:
				d = OrderedDict(zip(cursor.fields, row))
				# if the geometry wasn't matched in the geocoding it has
				# a value of (None, None) in the 'Shape' field
				if d['Status'] != 'U':
					geo_fields = (
						'Shape', 'Loc_name', 'Score', 'Match_type')
					geo_dict = {k: d[k] for k in geo_fields}
					bin_dict[d['BIN']] = geo_dict
	
	return bin_dict

def geocode(addr_str):
	"""Take an input address string, send it to the rlis api and return
	a dictionary that are the state plane coordinated for that address, 
	handle errors in the request fails in one way or another"""

	base_url = 'http://gis.oregonmetro.gov/rlisapi2/locate/'
	url_template = '{0}?token={1}&input={2}&form=json'
	url = url_template.format(base_url, token, addr_str)
	response = requests.get(url)

	if response.status_code != 200:
		print 'unable to establish connection with rlis api'
		print 'status code is: {0}'.format(response.status_code)
		return response.status_code
	
	json_rsp = response.json()
	if json_rsp['error']:
		print 'the following address could not be geocoded:'
		print '\'{0}\''.format(addr_str)
		print 'the following error message was returned:'
		print '\'{0}\''.format(json_rsp['error']), '\n'
	else:
		return json_rsp['data'][0]

def selectQcewNearLid(lid, region):
	"""Select QCEW points that are within 100 feet of the supplied
	Streetcar LID boundary, exclude records that have an invalid
	address and or low precision in their geocode"""

	qcew_lyr = 'regeocoded_qcew'
	if not arcpy.Exists(qcew_lyr):
		management.MakeFeatureLayer(regeo_qcew, qcew_lyr)

	lid_lyr = 'streetcar_lid'
	management.MakeFeatureLayer(lid, lid_lyr)

	spatial_relationship = 'WITHIN_A_DISTANCE'
	search_dist = '100 FEET'
	new_select = 'NEW_SELECTION'
	management.SelectLayerByLocation(qcew_lyr, spatial_relationship, 
		lid_lyr, search_dist, new_select)

	lid_rows = []
	discard_rows = []
	with da.SearchCursor(qcew_lyr, '*') as cursor:
		for row in cursor:
			d = OrderedDict(zip(cursor.fields, row))

			# exlude for analysis if street address in something
			# like 'NEED ADDRESS'
			if re.match('.*NEED\s.*', d['STREET']):
				discard_rows.append(d)
			# exclude from analysis if precision is greater than
			# 500 feet
			elif int(d['PRECISION_']) > 500:
				discard_rows.append(d)
			else:
			 	lid_keys = (
			 		'NAME',		'NAICS',	'ATYPE', 
			 		'MEEI',		'AVGEMP',	'TOTPAY')
			 	lid_dict = {k: d[k] for k in lid_keys}
			 	lid_rows.append(lid_dict)

	# write to csv as a record of removing this entry
	csv_name = '{0}_discarded_{1}.csv'.format(region, yr)
	discard_csv = join(csv_dir, 'discarded', csv_name)
	
	with open(discard_csv, 'wb') as discard:
		fields = [k for k in discard_rows[0]]
		writer = csv.DictWriter(discard, fields)
		writer.writeheader()

		for row in discard_rows:
			writer.writerow(row)

	return lid_rows

def compileQcewStats(lid):
	"""Compile stats from the QCEW that is within the supplied LID and
	write them to csv in the format in which they have been arranged
	in the previous iterations of this project"""

	region = re.match('(^[a-zA-Z]+)', basename(lid)).group(1)
	lid_rows = selectQcewNearLid(lid, region)

	lists = defaultdict(list)
	counts = defaultdict(float)
	prf_keys = ('NAME', 'NAICS')
	agg_keys = ('NAME', 'AVGEMP', 'TOTPAY', 'MEEI')
	
	for lr in lid_rows:
		# non-profit
		if re.match('82.*|813.*', lr['NAICS']):
			counts['np_emp'] += lr['AVGEMP']
			counts['np_pay'] += lr['TOTPAY']

			np_row = OrderedDict((k, lr[k]) for k in prf_keys)
			lists['np'].append(np_row)

			# reporting is for multiple locations (aggregated)
			if re.match('.*2.*|.*4.*', lr['MEEI']):
				agg_row = OrderedDict((k, lr[k]) for k in agg_keys)
				lists['agg'].append(agg_row)
		# for-profit
		else:
			counts['fp_emp'] += lr['AVGEMP']
			counts['fp_pay'] += lr['TOTPAY']
			counts['fp_bus'] += 1

			p_row = OrderedDict((k, lr[k]) for k in prf_keys)
			lists['fp'].append(p_row)

			# reporting is for multiple locations (aggregated)
			if re.match('.*2.*|.*4.*', lr['MEEI']):
				counts['agg_fp_emp'] += lr['AVGEMP']
				counts['agg_fp_pay'] += lr['TOTPAY']
				counts['agg_fp_bus'] += 1

				agg_row = OrderedDict((k, lr[k]) for k in agg_keys)
				lists['agg'].append(agg_row)
			# reporting address used is not physical address
			if lr['ATYPE'] != 'P':
				counts['nph_fp_emp'] += lr['AVGEMP']
				counts['nph_fp_pay'] += lr['TOTPAY']
				counts['nph_fp_bus'] += 1

		counts['emp'] += lr['AVGEMP']
		counts['pay'] += lr['TOTPAY']

	# write the lists to csv
	csv_template = '{0}_{1}_employers_{2}.csv'
	for grp, l in lists.iteritems():
		csv_name = csv_template.format(region, grp, yr)
		csv_path = join(csv_dir, csv_name)

		with open(csv_path, 'wb') as csv_file:
			fields = [k for k in l[0]]
			writer = csv.DictWriter(csv_file, fields)
			writer.writeheader()

			# sort list (in some cases by multiple fields)
			rsort = {('NAICS', 'NAME'): ['fp', 'np'], ('NAME',): ['agg']}
			sort = {i:k for k,v in rsort.iteritems() for i in v}
			for row in sorted(l, key=lambda d: [d[k] for k in sort[grp]]):
				writer.writerow(row)

	# prep 'opverview' and 'for-profit' stats for writing
	stats = {}
	ov_keys = ('fp_emp', 'np_emp', 'emp', 'fp_pay', 'np_pay', 'pay')
	overview = {k: int(counts[k]) for k in ov_keys}
	stats['overview'] = {'stats': overview, 'keys': ov_keys}

	p_emp = counts['fp_emp']
	p_pay = counts['fp_pay']
	p_bus = counts['fp_bus']
	for_profit = OrderedDict([
		('for-prof emps', p_emp),
		('agg for-prof emp pct', 
			round(counts['agg_fp_emp'] / p_emp, 4)),
		('non-phys addr for-prof emp pct', 
			round(counts['nph_fp_emp'] / p_emp, 4)),
		
		('prof pay', p_pay),
		('agg for-prof pay pct', 
			round(counts['agg_fp_pay'] / p_pay, 4)),
		('non-phys addr for-prof pay pct', 
			round(counts['nph_fp_pay'] / p_pay, 4)),
		
		('agg for-prof bus pct', 
			round(counts['agg_fp_bus'] / p_bus, 4)), 
		('non-phys addr for-prof bus pct', 
			round(counts['nph_fp_bus'] / p_bus, 4))])
	fp_keys = [k for k in for_profit]
	stats['for_profit'] = {'stats': for_profit, 'keys': fp_keys}

	# write stats to csv
	for grp, d in stats.iteritems():
		csv_name = '{0}_{1}_stats_{2}.csv'.format(region, grp, yr)
		csv_path = join(csv_dir, csv_name)
		with open(csv_path, 'wb') as csv_file:
			writer = csv.DictWriter(csv_file, d['keys'])
			writer.writeheader()
			writer.writerow(d['stats'])

reprojectQcew()
regeocodeZipLevelPts()
compileQcewStats(west_lid)
compileQcewStats(east_lid)