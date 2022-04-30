from common import constants, utils
from data import database

import requests
import datetime
import types
import math
import csv
import re
import io

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
                utils.progress(records_processed, record_count)

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


EST2019 = 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/state/detail/SCPRC-EST2019-18+POP-RES.csv'
EST2020_2021 = 'https://www2.census.gov/programs-surveys/popest/datasets/2020-2021/state/totals/NST-EST2021-alldata.csv'


class Population:

    def __init__(self):
        self.table_name = 'population'
        self.raw_data = None
        self.fields = [
            {'field': 'date'},
            {'field': 'state'},
            {'field': 'estimate'}
        ]

    def fetch(self):
        population_estimates_map = {'2019': {}, '2020': {}, '2021': {}, '2022': {}}

        request = requests.request('GET', EST2019)
        population_estimates_2019_raw_data = list(csv.DictReader(io.StringIO(request.content.decode('utf-8'))))

        for estimate in population_estimates_2019_raw_data:
            if estimate['NAME'] in constants.state_abbrev_map:
                population_estimates_map['2019'][estimate['NAME']] = int(estimate['POPESTIMATE2019'])
            elif estimate['NAME'] == 'Puerto Rico Commonwealth':
                population_estimates_map['2019']['Puerto Rico'] = int(estimate['POPESTIMATE2019'])

        request = requests.request('GET', EST2020_2021)
        population_estimates_2020_2021_raw_data = list(csv.DictReader(io.StringIO(request.content.decode('utf-8'))))

        for estimate in population_estimates_2020_2021_raw_data:
            if estimate['NAME'] in constants.state_abbrev_map:
                population_estimates_map['2020'][estimate['NAME']] = int(estimate['POPESTIMATE2020'])
                population_estimates_map['2021'][estimate['NAME']] = int(estimate['POPESTIMATE2021'])
                # No estimates currently for 2022 so using the population data from 2021
                population_estimates_map['2022'][estimate['NAME']] = int(estimate['POPESTIMATE2021'])

        self.raw_data = []
        today = datetime.datetime.today()
        end_of_this_year = datetime.datetime(today.year, 12, 31)
        for state in constants.state_list:
            date = datetime.datetime(2020, 1, 1)
            population_increment = 0.0
            population_estimate = 0
            current_year = None

            while date < end_of_this_year:
                if state not in population_estimates_map[str(date.year)]:
                    break

                if current_year is None or current_year < date.year:
                    current_year_timedelta = datetime.datetime(date.year, 1, 1) - datetime.datetime(date.year - 1, 1, 1)
                    days_in_current_year = current_year_timedelta.days
                    estimates_for_previous_year = population_estimates_map[str(date.year - 1)][state]
                    estimates_for_current_year = population_estimates_map[str(date.year)][state]
                    population_difference = estimates_for_current_year - estimates_for_previous_year
                    population_increment = population_difference / days_in_current_year
                    population_estimate = estimates_for_previous_year
                    current_year = date.year

                self.raw_data.append({
                    'date': date.isoformat(),
                    'state': state,
                    'estimate': math.floor(population_estimate)
                })

                population_estimate += population_increment
                date += datetime.timedelta(days=1)

    def has_data(self):
        return self.raw_data is not None

    def save(self):
        mysql_database = database.Database()
        mysql_database.connect()

        if mysql_database.is_connected():
            mysql_database.start_transaction()

            records = self.raw_data
            record_count = len(records)
            records_processed = 0

            for record in records:
                columns = []
                values = []
                for field in self.fields:
                    if 'column' in field:
                        columns.append(field['column'])
                    elif 'field' in field:
                        columns.append(field['field'])

                    # Populating the values array
                    if 'field' in field:
                        values.append(record[field['field']])

                mysql_database.insert(self.table_name, columns, values)

                records_processed += 1
                utils.log("\rProgress: {} - Records processed: {} of {}"
                          .format(utils.percentage(records_processed, record_count), records_processed, record_count),
                          newline=records_processed == record_count)

            mysql_database.commit()


class PopulationEstimates:

    def __init__(self):
        self.raw_data = None
        self.table_name = 'population'
        self.fields = [
            {'column': 'date', 'columnIndex': 0},
            {'column': 'state', 'columnIndex': 1},
            {'field': 'population', 'column': 'estimate', 'columnIndex': 2}
        ]

    def fetch(self):
        mysql_database = database.Database()
        mysql_database.connect()
        if mysql_database.is_connected():
            self.raw_data = mysql_database.select(
                self.table_name,
                utils.array_map_by_key(self.fields, 'column')
            )

    def has_data(self):
        return self.raw_data is not None

    def get_data(self):
        constructed_data = []
        for data in self.raw_data:
            data_object = {}
            for field in self.fields:
                data_object[field['field'] if 'field' in field else field['column']] = data[field['columnIndex']]

            constructed_data.append(data_object)

        data_map = {}
        for data in constructed_data:
            iso_date = data['date'].isoformat() + 'T00:00:00'
            if iso_date not in data_map:
                data_map[iso_date] = {}

            data_map[iso_date][data['state']] = data['population']

        return data_map
