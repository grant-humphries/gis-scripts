import csv
import os
import re
import sys
from argparse import ArgumentParser
from collections import defaultdict
from os.path import abspath, dirname, join

import fiona
from geoalchemy2.shape import to_shape
from rtree import index
from shapely.geometry import mapping, shape
from shapely.geometry.base import BaseGeometry
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

# add path to censuspgsql package to PYTHONPATH
MOD_PATH = 'G:/PUBLIC/GIS_Projects/Census/census-postgres-py'
sys.path.append(abspath(MOD_PATH))
from censuspgsql.model.tiger2015.bg import Bg
from censuspgsql.model.tiger2015.tract import Tract

RLIS_DIR = 'G:/Rlis'
HOME = dirname(abspath(sys.argv[0]))
TAXLOTS = join(RLIS_DIR, 'TAXLOTS', 'taxlots.shp')
ADDR_PTS = join(RLIS_DIR, 'TAXLOTS', 'master_address.shp')
CENSUS_PTS = join(HOME, 'shp', 'census_address_points.shp')
POINTS_COUNT = join(HOME, 'csv', 'address_count_by_census_unit.csv')

TLID_FIX_STR = '\s+-*0*'
PG_URL = 'postgresql://{user}:{password}@{host}/{db}'


def create_census_address_points():
    """"""

    # coded against prop code definitions on pg 136 in this manual:
    # http://www.oregon.gov/DOR/forms/FormsPubs/ratio_manual_150-303-437.pdf
    # prop codes that match this string are address that have people
    # living at them (as best as I could discern)
    prop_code_str = '0[014-79][139]|' \
                    '[14-7][0-9]?[1-9]?|' \
                    '9[08][1-69]'
    prop_code_map = get_prop_code_by_tlid()

    homes = dict()
    with fiona.open(ADDR_PTS) as addr_pts:
        homes_meta = addr_pts.meta.copy()

        for fid, feat in addr_pts.items():
            fields = feat['properties']
            tlid = fields['TLID']

            if tlid:
                clean_tlid = re.sub(TLID_FIX_STR, '', tlid)
                try:
                    prop_code = prop_code_map[clean_tlid]
                except KeyError:
                    # if the landuse is unknown the address is dropped
                    # since a ratio of the points and the lack of a prop
                    # code seems random things should even out
                    continue

                if re.match(prop_code_str, prop_code):
                    fields['prop_code'] = prop_code
                    homes[fid] = feat

    # get census tracts and block groups from the postgres database on
    # the map server
    pg_url = PG_URL.format()
    engine = create_engine(pg_url)
    bg = get_spatial_table_from_db(engine, Bg, ['geoid'])
    bg_ix = generate_spatial_index(bg)
    bg_mapping = spatial_join(homes, bg, bg_ix)

    tract = get_spatial_table_from_db(engine, Tract, ['geoid'])
    tract_ix = generate_spatial_index(tract)
    tract_mapping = spatial_join(homes, tract, tract_ix)

    home_fields = homes_meta['schema']['properties']
    home_fields['bg'] = 'str'
    home_fields['tract'] = 'str'
    home_fields['prop_code'] = 'str'

    with fiona.open(CENSUS_PTS, 'w', **homes_meta) as census_pts:
        for fid, feat in homes.items():
            fields = feat['properties']
            fields['bg'] = bg[bg_mapping[fid]]['properties']['geoid']
            fields['tract'] = tract[tract_mapping[fid]]['properties']['geoid']

            census_pts.write(feat)


def get_prop_code_by_tlid():
    """"""

    prop_code_map = dict()
    with fiona.open(TAXLOTS) as taxlots:
        for feat in taxlots:
            fields = feat['properties']
            clean_tlid = re.sub(TLID_FIX_STR, '', fields['TLID'])
            prop_code = fields['PROP_CODE']
            if prop_code:
                prop_code_map[clean_tlid] = prop_code

    return prop_code_map


def get_spatial_table_from_db(engine, table, fields, geom_col='geom'):
    """"""

    session_maker = sessionmaker(bind=engine)
    session = session_maker()

    fields.insert(0, geom_col)
    field_objs = [getattr(table, f) for f in fields]

    results = (
        session.query(*field_objs).
        filter(getattr(table, geom_col).intersects(
            func.ST_MakeEnvelope(7469314, 452285, 7891194, 781860, 2913))).
        all()
    )

    features = dict()
    for i, t in enumerate(results):
        fields = t._asdict()
        geom = mapping(to_shape(fields.pop(geom_col)))
        features[i] = dict(geometry=geom, properties=fields)

    return features


def generate_spatial_index(features):
    """"""

    spatial_ix = index.Index()
    for fid, feat in features.items():
        geom = feat['geometry']
        if not isinstance(geom, BaseGeometry):
            geom = shape(geom)

        spatial_ix.insert(fid, geom.bounds)

    return spatial_ix


def spatial_join(target_feats, join_feats, s_index):
    """"""

    join_mapping = dict()

    for t_fid, t_feat in target_feats.items():
        t_geom = shape(t_feat['geometry'])

        for j_fid in s_index.intersection(t_geom.bounds):
            j_geom = shape(join_feats[j_fid]['geometry'])
            if t_geom.intersects(j_geom):
                if t_fid not in join_mapping:
                    join_mapping[t_fid] = j_fid
                else:
                    print 'The address point with fid {} is being reported ' \
                          'as being a part of more than one census unit ' \
                          'which should not be possible, examine the code' \
                          'and data for issues'.format(t_fid)
                    exit()

    return join_mapping


def write_census_pt_counts_to_csv():
    """"""

    census_count = defaultdict(int)
    with fiona.open(CENSUS_PTS) as census_pts:
        for feat in census_pts:
            bg = feat['properties']['bg']
            census_count[bg] += 1

            tract = feat['properties']['tract']
            census_count[tract] += 1

    with open(POINTS_COUNT, 'wb') as pt_count_csv:
        pt_writer = csv.writer(pt_count_csv)
        header = ('geoid', 'residence point count')
        pt_writer.writerow(header)
        for geoid, count in census_count.items():
            pt_writer.writerow((geoid, count))


def process_postgres_options(args=None):
    """"""

    # if the PGPASSWORD environment variable has been set use it
    password = os.environ.get('PGPASSWORD')
    if password:
        pw_require = False
    else:
        pw_require = True

    parser = ArgumentParser()
    parser.add_argument(
        '-H', '--host',
        default='localhost',
        help='url of postgres host server'
    )
    parser.add_argument(
        '-u', '--user',
        required=True,
        help='postgres user name'
    )
    parser.add_argument(
        '-d', '--dbname',
        required=True,
        help='name of target database'
    )
    parser.add_argument(
        '-p', '--password',
        required=pw_require,
        default=password,
        help='postgres password for supplied user, if PGPASSWORD environment'
             'variable is set it will be read from that setting'
    )

    options = parser.parse_args(args)
    return options


def main():
    """"""

    global pg
    args = sys.argv[1:]
    pg = process_postgres_options(args)

    create_census_address_points()
    write_census_pt_counts_to_csv()


if __name__ == '__main__':
    main()
