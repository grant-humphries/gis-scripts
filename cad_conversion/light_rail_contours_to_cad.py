import os
import logging
from glob import glob
from os.path import abspath, dirname, exists, join

import fiona
from arcpy.conversion import ExportCAD
from rtree import index
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry

G = '//gisstore/gis'
CONTOUR_DIR = 'C:/Users/humphrig/Desktop/contours/ALL_WITH_WESTSIDE'
HOME = dirname(abspath(__name__))
RLIS_DIR = join(G, 'Rlis')
TRIMET_DIR = join(G, 'TRIMET')

RAIL_LINES = join(TRIMET_DIR, 'rail_line.shp')
SECTIONS = join(RLIS_DIR, 'TAXLOTS', 'sections.shp')
TEMP_CONTOUR = join(HOME, 'temp', 'temp_contours.shp')
TEMP_RAIL = 'C:/Users/humphrig/Desktop/temp/rail_lines_backup.shp'


def convert_contours_to_cad(overwrite=False):
    """"""

    # docs on arcgis cad conversion tool:
    # https://desktop.arcgis.com/en/arcmap/latest/tools/conversion-toolbox/export-to-cad.htm

    cad_color = 192
    cad_layer = 'contours'
    cad_format = 'DWG_R2013'
    color_field = 'Color'
    layer_field = 'Layer'

    missing_sections = list()
    max_sections = get_light_rail_sections()
    for sect in max_sections:
        contour_shp = join(CONTOUR_DIR, '{}.shp'.format(sect))

        if exists(contour_shp):
            cur_contour = fiona.open(contour_shp)
            metadata = cur_contour.meta.copy()

            fields = metadata['schema']['properties']
            fields[layer_field] = 'str'
            fields[color_field] = 'int'

            with fiona.open(TEMP_CONTOUR, 'w', **metadata) as temp_contour:
                for feat in cur_contour:
                    fields = feat['properties']
                    fields[layer_field] = cad_layer
                    fields[color_field] = cad_color

                    temp_contour.write(feat)

            contour_cad = join(HOME, 'cad', '{}_contours.dwg'.format(sect))
            if exists(contour_cad):
                if overwrite:
                    for dwg_part in glob('{}*'.format(contour_cad)):
                        os.remove(dwg_part)
                else:
                    continue

            # previously I had used a seed file here as well, at this
            # it doesn't look like that had any effect
            ExportCAD(TEMP_CONTOUR, cad_format, contour_cad)

        else:
            missing_sections.append(sect)

    if missing_sections:
        logging.info("The following sections did not exist in the source "
                     "data and thus couldn't be converted: {}".format(
                         missing_sections))


def get_light_rail_sections():
    """"""

    # units derived from srs of input data
    buffer_dist = 500

    sect_dict = dict()
    sections = fiona.open(SECTIONS)
    sect_ix = generate_spatial_index(sections)

    with fiona.open(RAIL_LINES) as rail_lines:
        for rail_id, rail_feat in rail_lines.items():
            rail_type = rail_feat['properties']['TYPE']

            if 'MAX' in rail_type:
                rail_geom = shape(rail_feat['geometry'])
                rail_buffer = rail_geom.buffer(buffer_dist)

                for sect_id in sect_ix.intersection(rail_buffer.bounds):
                    # for some reason fiona sees integers and long
                    # integers of the same value as not equal
                    sect_id = int(sect_id)

                    if sect_id not in sect_dict:
                        sect_feat = sections[sect_id]
                        sect_geom = shape(sect_feat['geometry'])

                        if rail_buffer.intersects(sect_geom):
                            sect_name = sect_feat['properties']['SECTION']
                            sect_dict[sect_id] = sect_name

    return sect_dict.values()


def generate_spatial_index(features):
    """"""

    spatial_ix = index.Index()
    for fid, feat in features.items():
        geom = feat['geometry']
        if not isinstance(geom, BaseGeometry):
            geom = shape(geom)

        spatial_ix.insert(fid, geom.bounds)

    return spatial_ix


def refresh_rail_lines():
    """The rail lines can't be rendered properly with QGIS when I write
    them with geogit, but writing them with fiona seems to fix the problem
    """

    # backup in case something goes wrong
    with fiona.open(RAIL_LINES) as rail_lines:
        metadata = rail_lines.meta.copy()

        with fiona.open(TEMP_RAIL, 'w', **metadata) as temp_rail:
            for feat in rail_lines:
                temp_rail.write(feat)

    with fiona.open(TEMP_RAIL) as temp_rail:
        metadata = temp_rail.meta.copy()

        with fiona.open(RAIL_LINES, 'w', **metadata) as rail_lines:
            for feat in temp_rail:
                rail_lines.write(feat)


def main():

    logging.basicConfig(
        filename=join(HOME, 'log', 'conversion.log'),
        format='%(asctime)s %(levelname)s: %(message)s',
        level=logging.INFO)
    convert_contours_to_cad()

    # refresh_rail_lines()

if __name__ == '__main__':
    main()
