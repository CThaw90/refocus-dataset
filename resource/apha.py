from common import constants, utils
from resource import census, abstract
from data import database

import requests
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


def ensure_iso_date(*record):
    date_value = record[0]['Date of Declaration']
    return utils.ensure_iso_date(date_value) if len(date_value) > 0 else None


def get_state(*record):
    return constants.state_abbrev_map[record[0]['State']]


def create_cache_key(record):
    latitude = record['Latitude'] if 'Latitude' in record else record.get('latitude')
    longitude = record['Longitude'] if 'Longitude' in record else record.get('longitude')
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


def should_start_processing(record):
    start_processing = True
    for field_name in FIELDNAMES:
        start_processing = record[field_name] == field_name and start_processing

    return start_processing


class RacismDeclarations(abstract.Resource):

    def __init__(self):
        super(RacismDeclarations, self).__init__()
        self.last_api_call_time = None
        self.start_processing = False
        self.geo_locations = None
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
            {'field': 'Link', 'column': 'link_to_declaration'},
            {'field': 'Declaration', 'column': 'declaration'}
        ]

    def fetch(self):
        request = requests.request('GET', URL)
        request_content = request.content.decode('utf-8')
        self.raw_data = csv.DictReader(io.StringIO(request_content), fieldnames=FIELDNAMES)

        census_geo_locations = census.GeoLocations()
        census_geo_locations.fetch()
        if census_geo_locations.has_data():
            self.geo_locations = census_geo_locations.get_data()

    def skip_record(self, record):
        should_skip_record = not self.start_processing
        self.start_processing = self.start_processing if self.start_processing else should_start_processing(record)
        return should_skip_record

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

    def get_county(self, record, record_key, record_cache):
        longitude = record['Longitude']
        latitude = record['Latitude']
        cache_key = create_cache_key(record)
        county = 'N/A'
        city = 'N/A'
        state = 'N/A'

        if cache_key in record_cache:
            county = record_cache[cache_key][record_key]
        else:
            address = self.get_address_by_coordinates(latitude, longitude)

            db = database.Database()
            db.connect()
            county_location_data_fields = ['id', 'county', 'state']
            county = address[record_key] if record_key in address else county
            county = county.replace('City and County of ', '')
            city = address['city'] if 'city' in address else city
            state = address['state'] if 'state' in address else state
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
                record_cache[cache_key] = {
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

    def get_city(self, record, record_key, record_cache):
        longitude = record['Longitude']
        latitude = record['Latitude']
        cache_key = create_cache_key(record)
        county = 'N/A'
        city = 'N/A'
        state = 'N/A'
        if cache_key in record_cache:
            city = record_cache[cache_key][record_key]
        elif self.last_api_call_time is None or diff(time.perf_counter(), self.last_api_call_time) > 1:
            address = self.get_address_by_coordinates(latitude, longitude)

            db = database.Database()
            db.connect()
            county_location_data_fields = ['id', 'county', 'state']
            county = address['county'] if 'county' in address else county
            county = county.replace('City and County of ', '')
            city = address[record_key] if record_key in address else city
            state = address['state'] if 'state' in address else state
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
                record_cache[cache_key] = {
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

    def save(self, record_cache=None):
        record_cache = {}
        for location in self.geo_locations:
            cache_key = create_cache_key(location)
            record_cache[cache_key] = location

        abstract.Resource.save(self, record_cache)
