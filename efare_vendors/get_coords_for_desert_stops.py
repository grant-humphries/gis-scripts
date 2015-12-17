import csv
from os.path import join

import fiona
from rtree import index
from shapely.geometry import shape

home = '//gisstore/gis/PUBLIC/GIS_Projects/eFare_Project'
deserts_dir = join(home, 'Vendor_Deserts')
analysis_dir = join(home, 'Vendor_Analysis')
master_stops = join(analysis_dir, 'shp', 'master_efare_stops.shp')
t6_block_groups = join(analysis_dir, 'Third_Mile_Maps',
                       'shp', 'min_pov_acs5_2012.shp')

rlis_dir = '//gisstore/gis/Rlis'
nbo_hoods = join(rlis_dir, 'BOUNDARY', 'nbo_hood.shp')
cities = join(rlis_dir, 'BOUNDARY', 'cty_fill.shp')
counties = join(rlis_dir, 'BOUNDARY', 'co_fill.shp')

desert_stops_csv = join(deserts_dir, 'csv', 'desert_stops.csv')


def generate_spatial_index(features):
    """"""

    # spatial index technique derived from this post:
    # http://gis.stackexchange.com/questions/120955

    spatial_ix = index.Index()
    with fiona.open(features) as feats:
        for fid, f in feats.items():
            geom = shape(f['geometry'])
            spatial_ix.insert(fid, geom.bounds)

    return spatial_ix


def get_desert_stop_loc_info():
    """"""

    csv_rows = []
    retain = ['agency', 'stop_id']

    cty_ix = generate_spatial_index(cities)
    co_ix = generate_spatial_index(counties)
    nbo_ix = generate_spatial_index(nbo_hoods)
    t6_ix = generate_spatial_index(t6_block_groups)

    with fiona.open(master_stops) as stops:
        for row in stops:
            props = row['properties']
            if props['vend_dist'] > 5280:
                print props['stop_id']

                for key in props:
                    if key not in retain:
                        props.pop(key)

                geom = shape(row['geometry'])
                props['x'] = geom.x
                props['y'] = geom.y

                city = find_intersecting_region(
                    geom, cities, cty_ix, 'CITYNAME')
                county = find_intersecting_region(
                    geom, counties, co_ix, 'COUNTY')
                props['city/county'] = city or county + ' County'

                props['hood'] = find_intersecting_region(
                    geom, nbo_hoods, nbo_ix, 'NAME')
                props['t6_status'] = find_intersecting_region(
                    geom, t6_block_groups, t6_ix, 'min_pov')

                csv_rows.append(props)

    return csv_rows


def find_intersecting_region(pt_geom, regions, regions_ix, name_field):
    """"""

    with fiona.open(regions) as reg:
        # the index returns the fids of all features whose bounding
        # box intersects with the supplied point
        fids = [int(i) for i in regions_ix.intersection(pt_geom.bounds)]

        # now that option have been narrowed down determine if the
        # point really intersects the supplied candidates
        for fid in fids:
            row = reg[fid]
            reg_geom = shape(row['geometry'])
            if reg_geom.intersects(pt_geom):
                props = row['properties']
                name = props[name_field]

                return name


def export_loc_info_to_csv():
    """"""

    csv_rows = get_desert_stop_loc_info()
    fields = [k for k in csv_rows[0]]

    with open(desert_stops_csv, 'wb') as stops_csv:
        writer = csv.DictWriter(stops_csv, fieldnames=fields)

        writer.writeheader()
        for row in csv_rows:
            writer.writerow(row)


def main():
    """"""

    export_loc_info_to_csv()


if __name__ == '__main__':
    main()
