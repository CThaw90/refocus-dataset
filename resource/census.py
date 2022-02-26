from common import constants, utils
from data import database

import requests
import types
import re

URL = 'https://www2.census.gov/geo/docs/reference/county_adjacency.txt'

state_county_pattern = re.compile('\\w.*, \\w{2}')


def get_county_data(record):
    parsed_data = record.strip("\t").split("\t")
    matched_data = False
    county_index = 0
    while county_index < len(parsed_data) and not matched_data:
        if state_county_pattern.match(parsed_data[county_index].strip("\"")) is not None:
            matched_data = True
        else:
            county_index += 1
    county_data = parsed_data[county_index].split(",")[0].strip("\"") if matched_data else None
    return county_data if matched_data else None


def get_state_data(record):
    parsed_data = record.strip("\t").split("\t")
    matched_data = False
    state_index = 0
    while state_index < len(parsed_data) and not matched_data:
        if state_county_pattern.match(parsed_data[state_index].strip("\"")) is not None:
            matched_data = True
        else:
            state_index += 1

    state_data = parsed_data[state_index].split(",")[1].strip("\"") if matched_data else None
    return constants.state_abbrev_map[state_data.strip()] if matched_data else None


def create_county_state_key(county, state):
    return '{}_{}'.format(county, state)


def get_geo_id_data(record):
    parsed_data = record.strip("\t").split("\t")
    matched_data = False
    geo_index = 0
    while geo_index < len(parsed_data) and matched_data is False:
        if parsed_data[geo_index].strip("\"").isdigit():
            matched_data = True
        else:
            geo_index += 1

    return parsed_data[geo_index].strip("\"") if matched_data else None


class CountyGeoCodes:

    def __init__(self):
        self.table_name = 'county_location_data'
        self.raw_data = None
        self.fields = [
            {'column': 'county', 'data': get_county_data},
            {'column': 'geo_id', 'data': get_geo_id_data},
            {'column': 'state', 'data': get_state_data}
        ]

    def fetch(self):
        self.raw_data = requests.request('GET', URL).content.decode('cp437')

    def has_data(self):
        return self.raw_data is not None

    def save(self):
        mysql_database = database.Database()
        mysql_database.connect()

        if mysql_database.is_connected():
            records = self.raw_data.split("\n")
            record_count = len(records)
            records_processed = 0
            record_cache = set()

            county_data = mysql_database.select(self.table_name, utils.array_map_by_key(self.fields, 'column'))
            for county in county_data:
                # 0 = county; 2 = state;
                county_state_key = create_county_state_key(county[0], county[2])
                record_cache.add(county_state_key)

            mysql_database.start_transaction()

            for record in records:
                columns = []
                values = []

                for field in self.fields:

                    if field.__contains__('column'):
                        columns.append(field['column'])

                    # Populating the values array
                    if field.__contains__('data'):
                        if isinstance(field['data'], types.FunctionType):
                            values.append(field['data'].__call__(record))

                county_state_key = create_county_state_key(get_county_data(record), get_state_data(record))
                if len(record) > 0 and not record_cache.__contains__(county_state_key):
                    mysql_database.insert(self.table_name, columns, values)
                    record_cache.add(county_state_key)

                records_processed += 1
                utils.log("\rProgress: {} - Records processed: {} of {}"
                          .format(utils.percentage(records_processed, record_count), records_processed, record_count),
                          newline=records_processed == record_count)

            mysql_database.commit()

    def get_saved_data(self):
        saved_data = []
        mysql_database = database.Database()
        mysql_database.connect()
        if mysql_database.is_connected():
            county_location_data = mysql_database.select(self.table_name, utils.array_map_by_key(self.fields, 'column'))
            for data in county_location_data:
                index = 0
                row_data = {}
                for field in self.fields:
                    row_data[field['column']] = data[index]
                    index += 1

                saved_data.append(row_data)

        return saved_data


class GeoLocations:

    def __init__(self):
        self.raw_data = None
        self.table_name = 'county_location_data cld, county_coordinates_data ccd'
        self.fields = [
            # {'column': 'cld.id'},
            {'column': 'longitude'},
            {'column': 'latitude'},
            {'column': 'geo_id'},
            {'column': 'county'},
            {'column': 'city'},
            {'column': 'state'}
        ]

    def fetch(self):
        mysql_database = database.Database()
        mysql_database.connect()
        if mysql_database.is_connected():
            self.raw_data = mysql_database.select(
                self.table_name,
                utils.array_map_by_key(self.fields, 'column'),
                where='ccd.county_location_data_id = cld.id'
            )

    def has_data(self):
        return self.raw_data is not None

    def get_data(self):
        constructed_data = []
        for data in self.raw_data:
            data_object = {}
            index = 0
            for field in self.fields:
                data_object[field['column']] = data[index]
                index += 1

            constructed_data.append(data_object)

        return constructed_data
