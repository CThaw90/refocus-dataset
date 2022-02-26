from common import constants, utils
from data import database
from resource import census

import requests
import types
import csv
import io

URL = 'https://eviction-lab-data-downloads.s3.amazonaws.com/ets/all_sites_weekly_2020_2021.csv'


def get_city(city_state):
    return city_state.split(',')[0].strip() if city_state is not None else city_state


def get_state(city_state):
    state_abbrev = city_state.split(',')[1].strip() if city_state is not None else city_state
    return constants.state_abbrev_map[state_abbrev] if state_abbrev is not None else state_abbrev


class WeeklyEvictions:

    def __init__(self):
        self.table_name = 'weekly_evictions'
        self.geo_locations = {}
        self.raw_data = None
        self.fields = [
            {'field': 'week_date', 'column': 'date', 'data': utils.ensure_iso_date},
            {'field': 'week'},
            {'field': 'city', 'column': 'city', 'data': get_city},
            {'field': 'GEOID', 'column': 'county', 'data': self.get_county},
            {'field': 'city', 'column': 'state', 'data': get_state},
            {'field': 'racial_majority'},
            {'field': 'filings_2020', 'column': 'filings', 'data': utils.ensure_int},
            {'field': 'filings_avg', 'data': utils.ensure_float},
            {'field': 'last_updated', 'data': utils.ensure_iso_date},
            {'field': 'GEOID', 'column': 'geo_id'}
        ]

    def fetch(self):
        request = requests.request('GET', URL)
        request_content = request.content.decode('utf-8')
        self.raw_data = csv.DictReader(io.StringIO(request_content))

        county_geo_codes = census.CountyGeoCodes()
        geo_code_locations = county_geo_codes.get_saved_data()
        for location in geo_code_locations:
            self.geo_locations[location['geo_id']] = location

        geo_locations = census.GeoLocations()
        geo_locations.fetch()
        if geo_locations.has_data():
            geo_locations_data = geo_locations.get_data()
            for location in geo_locations_data:
                city_state_key = '{}_{}'.format(location['city'], location['state'])
                self.geo_locations[city_state_key] = location

    def has_data(self):
        return self.raw_data is not None

    def get_county(self, record):
        city_state_key = '{}_{}'.format(get_city(record['city']), get_state(record['city']))
        geo_code = record['GEOID'][0:5]
        county = 'N/A'
        if geo_code.isdigit() and self.geo_locations.__contains__(geo_code):
            county = self.geo_locations[geo_code]['county']
        elif self.geo_locations.__contains__(city_state_key):
            county = self.geo_locations[city_state_key]['county']

        return county

    def save(self):
        mysql_database = database.Database()
        mysql_database.connect()

        if mysql_database.is_connected():
            mysql_database.start_transaction()

            records = list(self.raw_data)
            record_count = len(records)
            progress_threshold = record_count // 100
            records_processed = 0

            for record in records:
                columns = []
                values = []

                for field in self.fields:
                    if field.__contains__('column'):
                        columns.append(field['column'])
                    elif field.__contains__('field'):
                        columns.append(field['field'])

                    # Populating the values array
                    if field.__contains__('data'):
                        if isinstance(field['data'], types.FunctionType):
                            values.append(field['data'].__call__(record[field['field']]))
                        elif isinstance(field['data'], types.MethodType):
                            values.append(field['data'].__call__(record))
                        elif isinstance(field['data'], str):
                            values.append(field['data'])

                    elif field.__contains__('field'):
                        values.append(record[field['field']])

                mysql_database.insert(self.table_name, columns, values)

                records_processed += 1
                utils.log("\rProgress: {}% - Records processed: {} of {}"
                          .format(records_processed // progress_threshold, records_processed, record_count),
                          newline=records_processed == record_count)

            mysql_database.commit()
