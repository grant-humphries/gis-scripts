import argparse
import sys
from collections import OrderedDict
from datetime import datetime
from os.path import join

import cx_Oracle
import fiona
from fiona import crs
from shapely.geometry import mapping, shape, LineString

DBNAME = 'HAWAII'
USER = 'tmpublic'
DATE_FORMAT = '%m/%d/%y'

HOME = '//gisstore/gis/PUBLIC/GIS_Projects/Vehicle_Miles'
RLIS_DIR = '//gisstore/gis/Rlis'
CITIES_PATH = join(RLIS_DIR, 'BOUNDARY', 'cty_fill.shp')


def create_pattern_geom_from_oracle():
    """"""

    o_conn = cx_Oracle.connect(USER, ops.password, DBNAME)
    o_cur = o_conn.cursor()

    q = """SELECT x_coordinate as x, y_coordinate as y, 
             route_begin_date as begin_date, route_number as route,
             direction, pattern_id as pattern, 
             shape_point_distance as seq
           from shape_point_distance
           where route_begin_date = :begin_date"""

    o_cur.execute(q, begin_date=ops.summary_date)
    field_names = [d[0].lower() for d in o_cur.description]

    pattern_pts = {}
    for row in o_cur.fetchall():
        pt = dict(zip(field_names, row))
        
        coords = (pt.pop('x'), pt.pop('y'))
        pt['coords'] = coords

        date = pt.pop('begin_date')
        route = pt.pop('route')
        direct = pt.pop('direction')
        patt = pt.pop('pattern')
        k = (date, route, direct, patt)

        pattern_pts[k] = pattern_pts.get(k,[]) + [pt]
    o_conn.close()

    pattern_lines = {}
    pattern_props = get_volume_usage_mode_attributes()
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

    with fiona.open(ops.patterns_path, 'w', **metadata) as oracle_patterns:
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


def get_volume_usage_mode_attributes():
    """"""

    o_conn = cx_Oracle.connect(USER, ops.password, DBNAME)
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

    o_cur.execute(q, begin_date=ops.summary_date)
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


def clip_patterns_to_city_limits(city_name, buffer=False):
    """"""

    with fiona.open(CITIES_PATH) as cities:
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
    clip_name = name_template.format(city_name, ops.path_date)
    clip_path = join(HOME, 'shp', clip_name)
    
    with fiona.open(ops.patterns_path) as oracle_patterns:
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


def get_vehicle_miles_traveled(patterns):
    """Tally up the vehicle miles travel, sorting the totals by
    revenue/deadhead routes as well as by mode"""

    miles_dict = {'revenue': {}, 'deadhead': {}}
    
    with fiona.open(patterns) as pats:
        for p in pats:
            props = p['properties']
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


def valid_date(date_str):
    """"""

    try:
        sum_date = datetime.strptime(date_str, DATE_FORMAT).date()
        return sum_date
    except ValueError:
        msg = 'supplied date: {0} not in required format of {1}'.format(
            date_str, DATE_FORMAT)
        raise argparse.ArgumentTypeError(msg)


def process_options(args=None):
    """"""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-p', '--password',
        required=True,
        help='password for database: {0}, user: {1}'.format(
            DBNAME, USER)
    )
    parser.add_argument(
        '-sd', '--summary_date',
        required=True,
        type=valid_date,
        help='summary begin date routes in the TRANS schema, must be in the'
             'format m/d/yy and appear in the "summary_period" table'
    )

    options = parser.parse_args(args)
    return options


def main():
    """"""

    global ops
    args = sys.argv[1:]
    ops = process_options(args)

    ops.path_date = ops.summary_date.strftime('%Y-%m-%d')
    patterns_name = 'oracle_patterns_{0}.shp'.format(ops.path_date)
    ops.patterns_path = join(HOME, 'shp', patterns_name)

    create_pattern_geom_from_oracle()
    clip_path = clip_patterns_to_city_limits('Beaverton', True)
    get_vehicle_miles_traveled(clip_path)


if __name__ == '__main__':
    main()
