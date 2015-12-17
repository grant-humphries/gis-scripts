import sys
import argparse
from os.path import join
from collections import OrderedDict

import fiona
import psycopg2
from fiona import crs
from shapely import wkb
from shapely.ops import unary_union
from shapely.geometry import mapping, shape
from psycopg2.extras import RealDictCursor
from arcpy import da, env, analysis, management

dbname = 'trimet'
host = 'maps6.trimet.org'

env.overwriteOutput = True

project_dir = '//gisstore/gis/PUBLIC/GIS_Projects/eFare_Project'
deserts_dir = join(project_dir, 'Vendor_Deserts')
analysis_dir = join(project_dir, 'Vendor_Analysis')
third_mile_dir = join(analysis_dir, 'Third_Mile_Maps')

rc_vendors = join(analysis_dir, 'shp', 'rc_vendors_ospn_2015_05.shp')
current_stops = join(deserts_dir, 'shp', 'stops.shp')
master_stops = join(analysis_dir, 'shp', 'master_efare_stops.shp')
desert_gaps = join(deserts_dir, 'shp', 'desert_gaps.shp')
t6_block_groups = join(third_mile_dir, 'shp', 'min_pov_acs5_2012.shp')
t6_desert_mask = join(deserts_dir, 'shp', 't6_desert_mask.shp')
t6_desert_feats = join(deserts_dir, 'shp', 't6_desert_features.shp')


def get_current_stops():
    """"""

    db_template = 'dbname={0} user={1} host={2} password={3}'
    db_str = db_template.format(dbname, user, host, password)
    conn = psycopg2.connect(db_str)
    cur = conn.cursor()

    q_params = {
        'schema': 'current',
        'stop_table': 'stop'
    }

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
    stop = OrderedDict(
        [(k, v) for k, v in stops_list[0].items() if k != 'geom'])
    fields = OrderedDict([(k, type(v).__name__) for k, v in stop.items()])

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


def add_nearest_vendor_distance(stops):
    """Non-open source function, booooooooooo!!!"""

    stop_vend_tbl = join(deserts_dir, 'shp', 'stop_vend_near_tbl.dbf')
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


def generate_deserts_feature(stops, desert_dist, t6=None):
    """"""

    b_box = get_pg_table_b_box('load.county')

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

    if t6:
        create_t6_deserts(desert_trim, b_box, metadata)
        return

    with fiona.open(desert_gaps, 'w', **metadata) as gaps_shp:
        feat = {
            'geometry': mapping(desert_mask),
            'properties': {
                'id': 1
            }
        }
        gaps_shp.write(feat)


def get_pg_table_b_box(table):
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


def create_t6_deserts(desert_geom, b_box, mask_metadata):
    """"""

    geom_list = list()
    with fiona.open(t6_block_groups) as block_groups:
        t6_metadata = block_groups.meta.copy()

        with fiona.open(t6_desert_feats, 'w', **t6_metadata) as t6_deserts:
            for bg in block_groups:
                geom = shape(bg['geometry'])
                props = bg['properties']

                # 'neither' is misspelled in dataset so (sic)
                if props['min_pov'] != 'niether' and \
                        geom.intersects(desert_geom):
                    geom_list.append(geom)

                    new_geom = geom.intersection(desert_geom)
                    bg['geometry'] = mapping(new_geom)
                    t6_deserts.write(bg)

    t6_geom = unary_union(geom_list)
    t6_desert_geom = t6_geom.intersection(desert_geom)
    t6_mask_geom = b_box.difference(t6_desert_geom)

    with fiona.open(t6_desert_mask, 'w', **mask_metadata) as t6_mask:
        feat = {
            'geometry': mapping(t6_mask_geom),
            'properties': {
                'id': 1
            }
        }
        t6_mask.write(feat)


def process_options(arglist=None):
    """Define option that can be pass through the command line, the
    purpose of doing this in this case is so that all sensitive and
    variable arguments can be passed through a single command without
    prompting the user or storing them in the code
    """

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

    # get_current_stops()
    generate_deserts_feature(master_stops, 5280, t6=True)


if __name__ == '__main__':
    main()
