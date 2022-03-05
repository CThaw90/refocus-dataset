from common import utils, constants
from data import database
from resource import census

import requests
import types
import json
import csv
import io

URL = 'https://raw.githubusercontent.com/washingtonpost/data-police-shootings/master/fatal-police-shootings-data.csv'
errors = 0


def get_county(record):
    global errors
    county_name = 'N/A'
    geo_county_request_url = 'https://geo.fcc.gov/api/census/area?lat={}&lon={}&format=json'\
        .format(record['latitude'], record['longitude'])
    geo_county_request = requests.request('GET', geo_county_request_url)
    if geo_county_request.status_code == 502:
        errors += 1
        return get_county(record)

    elif geo_county_request.status_code == 200:
        geo_county_content = json.loads(geo_county_request.content.decode('utf-8'))
        if len(geo_county_content['results']) != 0:
            state_name = geo_county_content['results'][0]['state_name']
            county_name = geo_county_content['results'][0]['county_name'] \
                if geo_county_content.__contains__('results') and len(geo_county_content['results']) > 0 \
                else 'N/A'
            db = database.Database()
            db.connect()

            results = db.select(
                'county_location_data',
                ['id'],
                where="county={} and state={}".format(
                    utils.escape_quotes(county_name + ' County'),
                    utils.escape_quotes(state_name)
                )
            )
            if len(results) != 0:
                db.start_transaction()
                columns = ['longitude', 'latitude', 'city', 'county_location_data_id']
                for result in results:
                    values = [record['longitude'], record['latitude'], record['city'], result[0]]
                    db.insert('county_coordinates_data', columns, values)

                db.commit()
                db.close()

    return county_name


def get_signs_of_mental_illness(data):
    return utils.bool_to_int(data['signs_of_mental_illness'])


def get_body_camera(data):
    return utils.bool_to_int(data['body_camera'])


def get_is_geocoding_exact(data):
    return utils.bool_to_int(data['is_geocoding_exact'])


def get_state(record):
    return constants.state_abbrev_map[record['state']]\
        if constants.state_abbrev_map.__contains__(record['state']) else 'N/A'


def int_or_null(value):
    try:
        int(value)
    except ValueError:
        value = None
    except TypeError:
        value = None
    return value


def get_age(value):
    return int_or_null(value['age'])


def ensure_latitude_float(record):
    return utils.ensure_float(record['latitude'])


def ensure_longitude_float(record):
    return utils.ensure_float(record['longitude'])


def create_cache_key(record):
    return '{},{}'.format(record['latitude'], record['longitude'])


class PoliceShootings:

    def __init__(self):
        self.table_name = 'police_shooting_data'
        self.geo_locations = None
        self.raw_data = None
        self.fields = [
            {'field': 'date'},
            {'field': 'name'},
            {'field': 'manner_of_death'},
            {'field': 'armed'},
            {'field': 'age', 'data': get_age},
            {'field': 'gender'},
            {'field': 'race'},
            {'field': 'city'},
            {'field': 'state', 'data': get_state},
            {'field': 'signs_of_mental_illness', 'data': get_signs_of_mental_illness},
            {'field': 'threat_level'},
            {'field': 'flee'},
            {'field': 'body_camera', 'data': get_body_camera},
            {'field': 'longitude', 'data': ensure_longitude_float},
            {'field': 'latitude', 'data': ensure_latitude_float},
            {'field': 'is_geocoding_exact', 'data': get_is_geocoding_exact},
            {'field': 'id', 'column': 'county', 'data': get_county}
        ]

    def fetch(self):
        request = requests.request('GET', URL + '')
        self.raw_data = csv.DictReader(io.StringIO(request.content.decode('utf-8')))

        census_geo_locations = census.GeoLocations()
        census_geo_locations.fetch()
        if census_geo_locations.has_data():
            self.geo_locations = census_geo_locations.get_data()

    def has_data(self):
        return self.raw_data is not None

    def save(self):
        mysql_database = database.Database()
        mysql_database.connect()

        if mysql_database.is_connected():

            records = list(self.raw_data)
            record_count = len(records)
            records_processed = 0
            record_cache = {}

            for location in self.geo_locations:
                cache_key = create_cache_key(location)
                record_cache[cache_key] = location

            mysql_database.start_transaction()

            for record in records:
                columns = []
                values = []

                for field in self.fields:
                    if field.__contains__('column'):
                        columns.append(field['column'])
                    elif field.__contains__('field'):
                        if isinstance(field['field'], str):
                            columns.append(field['field'])

                    # Populating the values array
                    if field.__contains__('data'):
                        if field['field'] == 'id' and record_cache.__contains__(create_cache_key(record)):
                            values.append(record_cache[create_cache_key(record)]['county'])
                        elif isinstance(field['data'], types.FunctionType):
                            values.append(field['data'].__call__(record))

                    elif field.__contains__('field'):
                        if isinstance(field['field'], str):
                            values.append(record[field['field']])

                mysql_database.insert(self.table_name, columns, values)

                records_processed += 1
                utils.log("\rProgress: {} - Records processed: {} of {}"
                          .format(utils.percentage(records_processed, record_count), records_processed, record_count),
                          newline=records_processed == record_count)

            mysql_database.commit()
            utils.log('\nFinished uploading police_shooting_data with {} errors'.format(errors))
