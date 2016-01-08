import csv
import sys
import argparse
import requests
from os.path import join
from functools import partial
from collections import OrderedDict

import fiona
import pyproj
from fiona import crs
from shapely.ops import transform
from shapely.geometry import mapping, Point

home = 'G:/PUBLIC/GIS_Projects/eFare_Project/Vendor_Analysis'
plaid_csv_path = join(home, 'csv', 'plaid_pantry_locations.csv')
plaid_shp_path = join(home, 'shp', 'plaid_pantry_locations.shp')
ready_credit_path = join(home, 'shp', 'rc_vendors_ospn_2015_05.shp')
plaid_rc_path = join(home, 'shp', 'rc_and_plaid_locations.shp')


def create_plaid_pantry_shp():
    """"""

    with open(plaid_csv_path) as plaid_csv:
        # there are a couple of lines at the top of the csv containing
        # information that is not needed
        for i in range(2):
            next(plaid_csv)
        reader = csv.DictReader(plaid_csv)

        # create a metadata for shapefile that locations will be written to
        features = list()
        metadata = {
            'crs': crs.from_epsg(2913),
            'driver': 'ESRI Shapefile',
            'schema': {
                'geometry': 'Point',
                'properties': OrderedDict(
                    [(n, 'str') for n in reader.fieldnames if n])
            }
        }
        latlon2ospn = partial(
            pyproj.transform,
            pyproj.Proj(init='epsg:4326'),
            pyproj.Proj(init='epsg:2913', preserve_units=True)
        )

        addr_template = '{num} {pre} {street}, {city}, {st} {zip}'
        for r in reader:
            addr_str = addr_template.format(
                num=r['Address 1'], pre=r['Address 2'],
                street=r['Street'], city=r['City'],
                st=r['State'],      zip=r['Zip'])

            rsp = rlis_geocode(addr_str)
            if isinstance(rsp, int):
                print 'there seems to a problem in connecting with'
                print 'the rlis api halting geoprocessing until this'
                print 'is resolved'
                exit()
            elif rsp:
                geom = Point(rsp['ORSP_x'], rsp['ORSP_y'])
            else:
                rsp = google_geocode(addr_str)
                print rsp
                geom_wgs84 = Point(rsp['lng'], rsp['lat'])
                geom = transform(latlon2ospn, geom_wgs84)
                print geom.x, geom.y

            feat = {
                'geometry': mapping(geom),
                'properties': {k: v for k, v in r.items() if k}}
            features.append(feat)

    with fiona.open(plaid_shp_path, 'w', **metadata) as plaid_shp:
        for feat in features:
            plaid_shp.write(feat)


def rlis_geocode(addr_str):
    """Take an input address string, send it to the rlis api and return
    a dictionary that are the state plane coordinated for that address,
    handle errors in the request fails in one way or another"""

    base_url = 'http://gis.oregonmetro.gov/rlisapi2/locate/'
    url_template = '{0}?token={1}&input={2}&form=json'
    url = url_template.format(base_url, ops.rlis_token, addr_str)
    response = requests.get(url)

    if response.status_code != 200:
        print 'unable to establish connection with rlis api'
        print 'status code is: {0}'.format(response.status_code)
        return response.status_code

    json_rsp = response.json()
    if json_rsp['error']:
        print 'the following address could not be geocoded by the rlis api:'
        print "'{0}'".format(addr_str)
        print 'the following error message was returned:'
        print "'{0}'".format(json_rsp['error']), '\n'
        return None
    else:
        return json_rsp['data'][0]


def google_geocode(addr_str):
    """"""

    url = 'https://maps.googleapis.com/maps/api/geocode/json'
    params = {'address': addr_str}
    response = requests.get(url, params=params)
    location = response.json()['results'][0]['geometry']['location']
    return location


def combine_plaid_and_rc_locs():
    """"""

    rc_feats = []
    with fiona.open(ready_credit_path) as ready_credit:
        metadata = ready_credit.meta.copy()

        for feat in ready_credit:
            rc_feats.append(feat)

    template = {k: '' for k in rc_feats[0]['properties']}
    with fiona.open(plaid_rc_path, 'w', **metadata) as plaid_rc:
        for feat in rc_feats:
            plaid_rc.write(feat)

        with fiona.open(plaid_shp_path) as plaid:
            for feat in plaid:
                np = template.copy()
                p = feat['properties']

                np['name'] = 'Plaid Pantry #{}'.format(p['Store #'])
                np['address1'] = '{0} {1} {2}'.format(
                    p['Address 1'], p['Address 2'], p['Street'])
                np['city'] = p['City']
                np['state'] = p['State']
                np['zip'] = p['Zip']

                feat['properties'] = np
                plaid_rc.write(feat)


def process_options(arglist=None):
    """"""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-t', '--rlis_token',
        dest='rlis_token',
        required=True,
        help='Token for RLIS API'
    )

    options = parser.parse_args(arglist)
    return options


def main():

    global ops

    args = sys.argv[1:]
    ops = process_options(args)

    create_plaid_pantry_shp()
    combine_plaid_and_rc_locs()


if __name__ == '__main__':
    main()
