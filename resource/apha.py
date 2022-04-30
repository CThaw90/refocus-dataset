from common import constants, utils
from resource import census
from data import database

import requests
import types
import time
import math
import json
import csv
import io

nominatim_api_url = 'https://nominatim.openstreetmap.org/reverse?format=json&lat={}&lon={}'

URL = 'https://docs.google.com/spreadsheets/d/e/' + \
      '2PACX-1vRkLVhZFb2K0K9LWxxrZujKaGP1qcpsbZ9gCtAfM6eHGZuNa_qxBHwKpSPYfAiPSoMChphBJsMd4o7Z/pub' + \
      '?gid=1592883182&single=true&output=csv'
FIELDNAMES = [
    'State', 'Region', 'Address', 'Latitude', 'Longitude', 'Type', 'Sub-Type', 'Entity',
    'Political Affiliation', 'Declaration', 'Date of Declaration', 'Link', 'Notes'
]


def ensure_iso_date(record):
    date_value = record['Date of Declaration']
    return utils.ensure_iso_date(date_value) if len(date_value) > 0 else None


def get_state(record):
    return constants.state_abbrev_map[record['State']]


def create_cache_key(record):
    latitude = record['Latitude'] if record.__contains__('Latitude') else record.get('latitude')
    longitude = record['Longitude'] if record.__contains__('Longitude') else record.get('longitude')
    return '{},{}'.format(latitude, longitude)


def diff(value1, value2):
    return math.ceil(value1) - math.ceil(value2)


def should_start_processing(record):
    start_processing = True
    for field_name in FIELDNAMES:
        start_processing = record[field_name] == field_name and start_processing

    return start_processing


def escape_quotes(value):
    return value.replace('\'', '\\\'')


class RacismDeclarations:

    def __init__(self):
        self.last_api_call_time = None
        self.geo_locations = None
        self.record_cache = None
        self.raw_data = None
        self.table_name = 'apha_map'
        self.fields = [
            {'field': 'Date of Declaration', 'data': ensure_iso_date, 'column': 'date'},
            {'field': 'Longitude', 'column': 'longitude'},
            {'field': 'Latitude', 'column': 'latitude'},
            {'field': 'city', 'data': self.get_city},
            {'field': 'county', 'data': self.get_county},
            {'field': 'State', 'column': 'state', 'data': get_state},
            {'field': 'Sub-Type', 'column': 'entity_type'},
            {'field': 'Type', 'column': 'entity_geo'},
            {'field': 'Entity', 'column': 'entity_name'},
            {'field': 'Declaration', 'column': 'link_to_declaration'}
        ]

    def fetch(self):
        request = requests.request('GET', URL)
        request_content = request.content.decode('utf-8')
        self.raw_data = csv.DictReader(io.StringIO(request_content), fieldnames=FIELDNAMES)

        census_geo_locations = census.GeoLocations()
        census_geo_locations.fetch()
        if census_geo_locations.has_data():
            self.geo_locations = census_geo_locations.get_data()

    def has_data(self):
        return self.raw_data is not None

    def get_address_by_coordinates(self, latitude, longitude, retry=False):

        if retry:
            time.sleep(1)

        if self.last_api_call_time is None or diff(time.perf_counter(), self.last_api_call_time) > 1:
            request = requests.request('GET', nominatim_api_url.format(latitude, longitude))
            if request.status_code == 200:
                null_response = {'city': 'N/A', 'county': 'N/A', 'state': 'N/A'}
                response = json.loads(request.content.decode('utf-8'))
                self.last_api_call_time = time.perf_counter()
                return response['address'] if response.__contains__('address') else null_response
            else:
                return self.get_address_by_coordinates(latitude, longitude, retry=True)
        else:
            return self.get_address_by_coordinates(latitude, longitude, retry=True)

    def get_county(self, record):
        longitude = record['Longitude']
        latitude = record['Latitude']
        cache_key = create_cache_key(record)
        county = 'N/A'
        city = 'N/A'
        state = 'N/A'

        if self.record_cache.__contains__(cache_key):
            county = self.record_cache[cache_key]['county']
        else:
            address = self.get_address_by_coordinates(latitude, longitude)

            db = database.Database()
            db.connect()
            county_location_data_fields = ['id', 'county', 'state']
            county = address['county'] if address.__contains__('county') else county
            county = county.replace('City and County of ', '')
            city = address['city'] if address.__contains__('city') else city
            state = address['state'] if address.__contains__('state') else state
            # There is an edge case to how Washington, DC is represented in the api
            if state == 'District of Columbia':
                state = 'Washington, DC'
                county = 'District of Columbia'
                city = 'Washington'
            elif county == 'Saint Joseph County':
                county = 'St. Joseph County'
                state = 'Indiana'
            elif county == 'Saint Clair County':
                county = 'St. Clair County'
            where_clause = "county like '{}%' and state = '{}'".format(escape_quotes(county), state)
            county_locations = db.select('county_location_data', fields=county_location_data_fields, where=where_clause)
            if len(county_locations) == 1:
                county_coordinates_data_columns = ['longitude', 'latitude', 'city', 'county_location_data_id']
                county_coordinates_data_values = [longitude, latitude, city, county_locations[0][0]]
                db.insert('county_coordinates_data', county_coordinates_data_columns, county_coordinates_data_values)
                db.close()
                self.record_cache[cache_key] = {
                    'longitude': longitude,
                    'latitude': latitude,
                    'city': city,
                    'county': county
                }
            else:
                utils.log(
                    'No location data found for record with Latitude={}, Longitude={}, county={}, state={}'
                    .format(latitude, longitude, county, address['state'])
                )

        return county

    def get_city(self, record):
        longitude = record['Longitude']
        latitude = record['Latitude']
        cache_key = create_cache_key(record)
        county = 'N/A'
        city = 'N/A'
        state = 'N/A'
        if self.record_cache.__contains__(cache_key):
            city = self.record_cache[cache_key]['city']
        elif self.last_api_call_time is None or diff(time.perf_counter(), self.last_api_call_time) > 1:
            address = self.get_address_by_coordinates(latitude, longitude)

            db = database.Database()
            db.connect()
            county_location_data_fields = ['id', 'county', 'state']
            county = address['county'] if address.__contains__('county') else county
            county = county.replace('City and County of ', '')
            city = address['city'] if address.__contains__('city') else city
            state = address['state'] if address.__contains__('state') else state
            # There is an edge case to how Washington, DC is represented in the api
            if state == 'District of Columbia':
                state = 'Washington, DC'
                county = 'District of Columbia'
                city = 'Washington'
            elif county == 'Saint Joseph County':
                county = 'St. Joseph County'
                state = 'Indiana'
            elif county == 'Saint Clair County':
                county = 'St. Clair County'
            where_clause = "county like '{}%' and state = '{}'".format(escape_quotes(county), state)
            county_locations = db.select('county_location_data', fields=county_location_data_fields, where=where_clause)
            if len(county_locations) == 1:
                county_coordinates_data_columns = ['longitude', 'latitude', 'city', 'county_location_data_id']
                county_coordinates_data_values = [longitude, latitude, city, county_locations[0][0]]
                db.insert('county_coordinates_data', county_coordinates_data_columns, county_coordinates_data_values)
                db.close()
                self.record_cache[cache_key] = {
                    'longitude': longitude,
                    'latitude': latitude,
                    'city': city,
                    'county': county
                }
            else:
                utils.log(
                    'No location data found for record with Latitude={}, Longitude={}, county={}, state={}'
                    .format(latitude, longitude, county, address['state'])
                )

        return city

    def save(self):
        mysql_database = database.Database()
        mysql_database.connect()

        if mysql_database.is_connected():

            records = list(self.raw_data)
            record_count = len(records)
            self.record_cache = {}
            records_processed = 0
            start_processing = False

            for location in self.geo_locations:
                cache_key = create_cache_key(location)
                self.record_cache[cache_key] = location

            mysql_database.start_transaction()

            for record in records:
                records_processed += 1
                columns = []
                values = []

                if not start_processing:
                    start_processing = should_start_processing(record)
                    continue

                for field in self.fields:
                    if field.__contains__('column'):
                        columns.append(field['column'])
                    elif field.__contains__('field'):
                        columns.append(field['field'])

                    # Populating the values array
                    if field.__contains__('data'):
                        if field['field'] in ('city', 'county') and isinstance(field['data'], types.MethodType):
                            values.append(field['data'].__call__(record))
                        elif isinstance(field['data'], types.FunctionType):
                            values.append(field['data'].__call__(record))

                    elif field.__contains__('field'):
                        values.append(record[field['field']])

                mysql_database.insert(self.table_name, columns, values)

                utils.progress(records_processed, record_count)

            mysql_database.commit()
