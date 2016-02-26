import sys
import urllib
import argparse
import requests
import subprocess
from pprint import pprint
from os.path import basename, join
from functools import partial
from collections import OrderedDict

import pyproj
from shapely import ops
from shapely.geometry import Point

MIN_LON = -122.5658146
MIN_LAT = 45.4131664
MAX_LON = -122.5579471
MAX_LAT = 45.4166018


def get_images_from_service(token):
    """"""

    image_dir = 'G:/PUBLIC/GIS_Projects/Aerials/metro_image_service/images'
    photo_url = 'https://gis.oregonmetro.gov/arcgis/rest/services/' \
                'photo/aerialimage2015early/ImageServer/exportImage?' \
                'f={0}&bbox={1}&size={2}&token={3}'

    factor = 0.5
    coords = get_ospn_coords_from_latlon()
    dimensions = (
        coords['max_x'] - coords['min_x'],
        coords['max_y'] - coords['min_y'])

    f = 'json'  # response format
    bbox = ','.join([str(i) for i in coords.values()])
    pixel_size = ','.join([str(int(round(i / factor))) for i in dimensions])

    rsp = requests.get(photo_url.format(f, bbox, pixel_size, token))
    json_rsp = rsp.json()

    if 'error' in json_rsp:
        pprint(json_rsp['error'])
        exit()

    ext = json_rsp['extent']
    image_url = json_rsp['href']
    image_name = basename(image_url)
    image_path = join(image_dir, image_name)

    urllib.urlretrieve(image_url, image_path)

    # convert the image to a geotiff so that it can be read by JOSM
    gdal_translate = 'gdal_translate -of "GTiff" -co "TILED=YES" ' \
                     '-co "COMPRESS=JPEG" -co "PHOTOMETRIC=YCBCR" ' \
                     '-a_ullr {0} -a_srs "{1}" {2} {3}'

    ullr_coords = [ext['xmin'], ext['ymax'], ext['xmax'], ext['ymin']]
    ullr = ' '.join([str(xy) for xy in ullr_coords])
    epsg = 'EPSG:{}'.format(ext['spatialReference']['wkid'])
    tif_path = image_path.replace('.jpg', '.tif')

    print gdal_translate.format(
        ullr, epsg, image_path, tif_path)

    subprocess.call(gdal_translate.format(
        ullr, epsg, image_path, tif_path))


def get_ospn_coords_from_latlon():
    """"""

    lower_left = Point(MIN_LON, MIN_LAT)
    upper_right = Point(MAX_LON, MAX_LAT)

    latlon2ospn = partial(
            pyproj.transform,
            pyproj.Proj(init='epsg:4326'),
            pyproj.Proj(init='epsg:2913', preserve_units=True)
        )

    ospn_ll = ops.transform(latlon2ospn, lower_left)
    ospn_ur = ops.transform(latlon2ospn, upper_right)

    return OrderedDict([
        ('min_x', ospn_ll.x),
        ('min_y', ospn_ll.y),
        ('max_x', ospn_ur.x),
        ('max_y', ospn_ur.y)
    ])


def process_options(arg_list=None):
    """"""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-t', '--token',
        required=True,
        dest='token',
        help='rlis user token required to gain access to their image service'
             'and other services that they offer'
    )

    options = parser.parse_args(arg_list)
    return options


def main():
    """"""

    args = sys.argv[1:]
    options = process_options(args)
    get_images_from_service(options.token)


if __name__ == '__main__':
    main()
