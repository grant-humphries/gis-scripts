from os import path
from collections import OrderedDict

import fiona
import cx_Oracle
from fiona import crs
from shapely import geometry

project_dir = '//gisstore/gis/PUBLIC/GIS_Projects/Passenger_Census'

o_user = 'tmpublic'
o_password = 'tmpublic'
o_dbname = 'HAWAII'


def get_ridership_from_oracle():
    """Send a query to the HAWAII database that gets the average number of
    ramp deployments on a weekday, saturday and sunday during the latest
    passenger census study period"""

    o_conn = cx_Oracle.connect(o_user, o_password, o_dbname)
    o_cur = o_conn.cursor()

    q = """SELECT loc.location_id as stop_id, 
          loc.public_location_description as stop_desc, 
          pcw.ons as wkdy_ons, pcw.offs as wkdy_offs, 
          pcs.ons as sat_ons, pcs.offs as sat_offs, 
          pcu.ons as sun_ons, pcu.offs as sun_offs, 
          round(loc.x_coordinate, 2) as x_coord, 
          round(loc.y_coordinate, 2) as y_coord,
          pcw.summary_begin_date as begin_date
        from (
            select location_id, sum(ons) as ons, sum(offs) as offs, 
              summary_begin_date 
            from passenger_census 
            where service_key = 'W'
            group by location_id, summary_begin_date) pcw
          left join (
            select location_id, sum(ons) as ons, sum(offs) as offs, 
              summary_begin_date 
            from passenger_census 
            where service_key = 'S'
            group by location_id, summary_begin_date) pcs
          on pcw.location_id = pcs.location_id
            and pcw.summary_begin_date = pcs.summary_begin_date
          left join (
            select location_id, sum(ons) as ons, sum(offs) as offs, 
              summary_begin_date 
            from passenger_census 
            where service_key = 'U'
            group by location_id, summary_begin_date) pcu
          on pcw.location_id = pcu.location_id 
            and pcw.summary_begin_date = pcu.summary_begin_date, 
          location loc
        where pcw.location_id = loc.location_id
          and pcw.summary_begin_date = (
            select max(summary_begin_date) from passenger_census)
        order by loc.location_id"""

    o_cur.execute(q)
    
    # get the name and type of each of the columns, then get the rows
    # themselves
    field_info = [(d[0].lower(), d[1].__name__) for d in o_cur.description]
    field_names = [n for n, t in field_info]
    
    rows = []
    for row in o_cur.fetchall():
        rows.append(OrderedDict(zip(field_names, row)))

    o_conn.close()
    return field_info, rows


def get_ridership_w_sqlalchemy(summary_date):
    """"""

    # !!!As of now this function is incomplete, primarily due to the
    # fact that the passenger_census table doesn't appear to be a
    # part of the 'trans' sqlalchemy model at this time

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.sql import func

    from trimet.model.oracle.trans \
        import Location as loc, Passenger_Census as pc

    oracle_url = 'oracle://{user}:{pw}@{db}'
    engine = create_engine(oracle_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    if not summary_date:
        summary_date = session.query(func.max(pc.summary_begin_date)).scalar()

    day_dict = dict()
    for service_day in ('W', 'S', 'U'):
        day_query = (
            session.query(
                pc.location_id,
                func.sum(pc.ons).label('ons'),
                func.sum(pc.offs).label('offs'),
                pc.summary_begin_date)
            .filter(pc.service_key == service_day)
            .group_by(pc.location_id, pc.summary_begin_date))

        day_dict[service_day] = day_query

    w = day_dict['W']
    s = day_dict['S']
    u = day_dict['U']

    rows = (
        session.query(
            loc.public_location_description.label('stop_desc'),
            w.ons.label('wkdy_ons'), w.offs.label('wkdy_offs'),
            s.ons.label('sat_ons'), s.offs.label('sat_offs'),
            u.ons.label('sun_ons'), u.offs.label('sun_offs'),
            func.round(loc.x_coordinate, 2).label('x_coord'),
            func.round(loc.y_coordinate, 2).label('y_coord'),
            w.summary_begin_date.label('begin_date'))
        .join(loc)
        .outerjoin(s)
        .outerjoin(u)
        .filter(
            loc.location_id == w.location.id,
            w.summary_begin_date == summary_date,
            w.location_id == s.location_id,
            w.summary_begin_date == s.summary_begin_date,
            s.location_id == u.location_id,
            s.summary_begin_date == u.summary_begin_date)
        .order_by(loc.location_id)
        .all()
    )

    # http://stackoverflow.com/questions/2258072
    row_types = [col.type for col in rows.columns]

    return rows, row_types


def write_ridership_to_shp():
    """Create a shapefile based on the schema of the result of the
    ridership query and write the results of the query to the shapefile"""

    field_info, rows = get_ridership_from_oracle()
    test_row = rows[0]
    fiona_props = get_fiona_field_types(field_info, test_row)

    # get the year and season of the survey to use in the file name
    survey_date = test_row['begin_date']
    if survey_date.month in (2, 3):
        season = 'spring'
    elif survey_date.month in (8, 9):
        season = 'fall'
    else:
        print 'month of survey ({0}), not handled'.format(survey_date.month)
        print 'make sure date is valid and if so update code'
        return

    shp_name = 'ridership_{0}{1}'.format(season, survey_date.year)
    rider_shp_path = path.join(project_dir, 'shp', shp_name)

    # set up metadata for new shapefile
    metadata = {
        'crs': crs.from_epsg(2913),
        'driver': 'ESRI Shapefile',
        'schema': {
            'geometry': 'Point',
            'properties': fiona_props
        }
    }

    with fiona.open(rider_shp_path, 'w', **metadata) as rider_shp:
        for r in rows:
            geom = geometry.Point(r['x_coord'], r['y_coord'])
            feature = {
                'geometry': geometry.mapping(geom),
                'properties': r
            }

            rider_shp.write(feature)


def get_fiona_field_types(field_info, test_row):
    """Convert oracle field types into fiona field types"""

    # get all fiona types with the following cmd:
    # `print fiona.FIELD_TYPES_MAP`:
    oracle_fiona_map = {
        'STRING': 'str',
        'DATETIME': 'date'
    }

    fiona_props = OrderedDict()
    for f_name, f_type in field_info:
        if f_type == 'NUMBER':
            # oracle doesn't make a distinction between int and float
            # so this must be discovered by testing a value
            test_val = test_row[f_name]

            if type(test_val) is int:
                fiona_type = 'int'
            if type(test_val) is float:
                fiona_type = 'float'
        else:
            fiona_type = oracle_fiona_map[f_type]

        fiona_props[f_name] = fiona_type

    return fiona_props


if __name__ == '__main__':
    write_ridership_to_shp()
