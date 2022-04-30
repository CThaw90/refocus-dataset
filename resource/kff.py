from common import utils, constants
from data import database
from git.cmd import Git

import types
import csv
import io
import os
import re

CASES_BY_RE_REGEX = re.compile('^\\d{8}.*Cases by RE\\.csv$')
DEATHS_BY_RE_REGEX = re.compile('^\\d{8}.*Deaths by RE\\.csv$')
VACCINATIONS_BY_RE_REGEX = re.compile('^\\d{8}.*Vaccinations by RE\\.csv$')
FLOAT_REGEX = re.compile('^\\d{1,3}\\.\\d{1,3}$')
TIMESTAMP_REGEX = re.compile('^\\d{8}')

GIT_REPO_URL = 'https://github.com/KFFData/COVID-19-Data'


def skip_record(record):
    return record['cases'] == 'NA' and record['deaths'] == 'NA' and record['tests'] == 'NA'


class StateTrends:

    def __init__(self):
        self.table_name = 'state_trend_data'
        self.raw_data = None
        self.fields = [
            {'field': 'state'},
            {'field': 'date'},
            {'field': 'cases', 'data': utils.ensure_int},
            {'field': 'deaths', 'data': utils.ensure_int},
            {'field': 'tests', 'data': utils.ensure_int},
            {'field': 'casechange', 'column': 'cases_change', 'data': utils.ensure_int},
            {'field': 'deathchange', 'column': 'deaths_change', 'data': utils.ensure_int},
            {'field': 'test_change', 'column': 'tests_change', 'data': utils.ensure_int},
            {'field': 'case_means', 'column': 'cases_7_day_mean', 'data': utils.ensure_float},
            {'field': 'death_mean', 'column': 'deaths_7_day_mean', 'data': utils.ensure_float},
            {'field': 'test_means', 'column': 'tests_7_day_mean', 'data': utils.ensure_float},
            {'field': 'case_permill', 'column': 'cases_per_million', 'data': utils.ensure_float},
            {'field': 'death_permill', 'column': 'deaths_per_million', 'data': utils.ensure_float},
            {'field': 'test_permill', 'column': 'tests_per_million', 'data': utils.ensure_float},
            {'field': 'pos_rate', 'column': 'positivity_rate_7_day_mean', 'data': utils.ensure_float},
            {'field': 'rp2', 'column': 'positivity_rate_7_day_plus_mean', 'data': utils.ensure_float},
            {'field': 'pct_change_weekly_cases_7', 'data': utils.ensure_float},
            {'field': 'pct_change_weekly_cases_14', 'data': utils.ensure_float},
            {'field': 'pct_change_weekly_deaths_7', 'data': utils.ensure_float},
            {'field': 'pct_change_weekly_deaths_14', 'data': utils.ensure_float},
            {'field': 'pct_change_weekly_tests_7', 'data': utils.ensure_float},
            {'field': 'pct_change_weekly_tests_14', 'data': utils.ensure_float},
            {'field': 'pct_change_positivity_rate_7', 'data': utils.ensure_float},
            {'field': 'pct_change_positivity_rate_14', 'data': utils.ensure_float},

            {'field': 'pop', 'column': 'population', 'data': utils.ensure_int},
            {'field': 'distributed', 'column': 'vaccines_distributed', 'data': utils.ensure_int},
            {'field': 'administered', 'column': 'vaccines_administered', 'data': utils.ensure_int},
            {'field': 'one_dose', 'column': 'vaccines_one_dose', 'data': utils.ensure_int},
            {'field': 'two_dose', 'column': 'vaccines_two_dose', 'data': utils.ensure_int},
            {'field': 'hotspot', 'data': utils.ensure_int},
        ]
        self.filepath = '/'.join([constants.temp_dir, 'COVID-19-Data/State Trend Data/State_Trend_Data.csv'])
        self.git = Git(constants.temp_dir)

    def fetch(self):
        if not os.path.isdir(constants.temp_dir):
            os.mkdir(constants.temp_dir)

        if not os.path.isfile(self.filepath):
            self.git.clone(GIT_REPO_URL, depth=1)

        with open(self.filepath, newline='') as csvfile:
            self.raw_data = csv.DictReader(io.StringIO(csvfile.read()))

    def has_data(self):
        return self.raw_data is not None

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

                records_processed += 1

                if skip_record(record):
                    continue

                for field in self.fields:
                    if field.__contains__('column'):
                        columns.append(field['column'])
                    elif field.__contains__('field'):
                        if isinstance(field['field'], str):
                            columns.append(field['field'])

                    # Populating the values array
                    if field.__contains__('data'):
                        if isinstance(field['data'], types.FunctionType):
                            values.append(field['data'].__call__(record[field['field']]))
                        elif isinstance(field['data'], str):
                            values.append(field['data'])

                    elif field.__contains__('field'):
                        if isinstance(field['field'], str):
                            values.append(record[field['field']])
                        elif isinstance(field['field'], types.FunctionType):
                            values.append(field['field'].__call__(record))

                mysql_database.insert(self.table_name, columns, values)

                utils.log("\rProgress: {}% - Records processed: {} of {}"
                          .format(records_processed // progress_threshold, records_processed, record_count),
                          newline=records_processed == record_count)

            mysql_database.commit()


def matches_case_by_race(filename):
    return CASES_BY_RE_REGEX.match(filename) is not None


def convert_filename_to_date(filename):
    timestamp = TIMESTAMP_REGEX.match(filename).group(0)
    return '-'.join([timestamp[0:4], timestamp[4:6], timestamp[6:8]])


def convert_to_float(value, retried=False):
    valid_float = FLOAT_REGEX.match(value) is not None
    if not valid_float:
        value = convert_to_float(value.replace('<', '0'), True) if not retried else 0.0

    return float(value)


class CasesByRace:

    def __init__(self):
        self.table_name = 'cases_by_race_ethnicity'
        self.raw_data = None
        self.fields = [
            {'field': 'date', 'column': 'date', 'data': utils.ensure_iso_date},
            {'field': 'Location', 'column': 'state'},
            {
                'field': 'Race Categories Include Hispanic Individuals',
                'column': 'hispanic_included',
                'data': utils.bool_to_int,
                'default': 0
            },
            {'field': 'White % of Cases', 'column': 'white_percentage_of_cases', 'data': convert_to_float},
            {
                'field': 'White % of Total Population',
                'column': 'white_percentage_of_population',
                'data': convert_to_float
            },
            {'field': 'Black % of Cases', 'column': 'black_percentage_of_cases', 'data': convert_to_float},
            {
                'field': 'Black % of Total Population',
                'column': 'black_percentage_of_population',
                'data': convert_to_float
            },
            {'field': 'Hispanic % of Cases', 'column': 'hispanic_percentage_of_cases', 'data': convert_to_float},
            {
                'field': 'Hispanic % of Total Population',
                'column': 'hispanic_percentage_of_population',
                'data': convert_to_float
            },
            {'field': 'Asian % of Cases', 'column': 'asian_percentage_of_cases', 'data': convert_to_float},
            {
                'field': 'Asian % of Total Population',
                'column': 'asian_percentage_of_population',
                'data': convert_to_float
            },
            {
                'field': 'American Indian or Alaska Native % of Cases',
                'column': 'american_indian_percentage_of_cases',
                'data': convert_to_float
            },
            {
                'field': 'American Indian or Alaska Native % of Total Population',
                'column': 'american_indian_percentage_of_population',
                'data': convert_to_float
            },
            {
                'field': 'American Indian or Alaska Native % of Cases',
                'column': 'alaska_native_percentage_of_cases',
                'data': convert_to_float
            },
            {
                'field': 'American Indian or Alaska Native % of Total Population',
                'column': 'alaska_native_percentage_of_population',
                'data': convert_to_float
            },
            {
                'field': 'Native Hawaiian or Other Pacific Islander % of Cases',
                'column': 'native_hawaiian_percentage_of_cases',
                'data': convert_to_float
            },
            {
                'field': 'Native Hawaiian or Other Pacific Islander % of Total Population',
                'column': 'native_hawaiian_percentage_of_population',
                'data': convert_to_float
            },
            {
                'field': 'Native Hawaiian or Other Pacific Islander % of Cases',
                'column': 'pacific_islander_percentage_of_cases',
                'data': convert_to_float
            },
            {
                'field': 'Native Hawaiian or Other Pacific Islander % of Total Population',
                'column': 'pacific_islander_percentage_of_population',
                'data': convert_to_float
            },
            {'field': 'Other % of Cases', 'column': 'other_percentage_of_cases', 'data': convert_to_float},
            {
                'field': 'Other % of Total Population',
                'column': 'other_percentage_of_population',
                'data': convert_to_float
            },
            {'field': '% of Cases with Known Race', 'column': 'known_race_percentage_of_cases', 'data': convert_to_float},
            {
                'field': '% of Cases with Unknown Race',
                'column': 'unknown_race_percentage_of_cases',
                'data': convert_to_float
            },
            {
                'field': '% of Cases with Known Ethnicity',
                'column': 'known_ethnicity_percentage_of_cases',
                'data': convert_to_float,
                'default': 0
            },
            {
                'field': '% of Cases with Missing Ethnicity',
                'column': 'unknown_ethnicity_percentage_of_cases',
                'data': convert_to_float,
                'default': 0
            }
        ]
        self.folder_path = '/'.join([constants.temp_dir, 'COVID-19-Data/Race Ethnicity COVID-19 Data/Cases and Deaths'])
        self.git = Git(constants.temp_dir)

    def fetch(self):
        if not os.path.isdir(constants.temp_dir):
            os.mkdir(constants.temp_dir)

        if not os.path.isdir(self.folder_path):
            self.git.clone(GIT_REPO_URL, depth=1)

        all_files = os.listdir(self.folder_path)
        all_files_length = len(all_files)
        index = 0

        self.raw_data = []
        while index < all_files_length:
            filename = all_files[index]
            if matches_case_by_race(filename):
                with open('/'.join([self.folder_path, filename]), newline='') as csvfile:
                    self.raw_data.append({'filename': filename, 'data': csv.DictReader(io.StringIO(csvfile.read()))})

            index += 1

    def has_data(self):
        return self.raw_data is not None and len(self.raw_data) > 0

    def save(self):
        mysql_database = database.Database()
        mysql_database.connect()

        if mysql_database.is_connected():
            mysql_database.start_transaction()

        records = []
        for raw_record_data in self.raw_data:
            for record_data in raw_record_data['data']:
                record_data['date'] = convert_filename_to_date(raw_record_data['filename'])
                if '' in record_data:

                    if record_data[''] in constants.state_abbrev_map:
                        record_data['Location'] = record_data['']

                records.append(record_data)

        record_count = len(records)
        records_processed = 0

        for record in records:
            records_processed += 1
            columns = []
            values = []

            if 'Location' in record and record['Location'] in constants.state_abbrev_map:

                for field in self.fields:
                    if field['field'] in record:

                        if 'column' in field:
                            columns.append(field['column'])
                        elif 'field' in field:
                            columns.append(field['field'])

                        # Populating the values array
                        if field.__contains__('data'):
                            if isinstance(field['data'], types.FunctionType):
                                values.append(field['data'].__call__(record[field['field']]))

                        elif 'field' in field:
                            values.append(record[field['field']])

                    elif 'default' in field:
                        if 'column' in field:
                            columns.append(field['column'])
                        elif 'field' in field:
                            columns.append(field['field'])

                        # Populating the values array
                        values.append(field['default'])

                mysql_database.insert(self.table_name, columns, values)

            utils.progress(records_processed, record_count)

        mysql_database.commit()


def matches_death_by_race(filename):
    return DEATHS_BY_RE_REGEX.match(filename) is not None


class DeathsByRace:

    def __init__(self):
        self.table_name = 'deaths_by_race_ethnicity'
        self.raw_data = None
        self.fields = [
            {'field': 'date', 'column': 'date', 'data': utils.ensure_iso_date},
            {'field': 'Location', 'column': 'state'},
            {'field': 'White % of Deaths', 'column': 'white_percentage_of_deaths', 'data': convert_to_float},
            {
                'field': 'White % of Total Population',
                'column': 'white_percentage_of_population',
                'data': convert_to_float
            },
            {'field': 'Black % of Deaths', 'column': 'black_percentage_of_deaths', 'data': convert_to_float},
            {
                'field': 'Black % of Total Population',
                'column': 'black_percentage_of_population',
                'data': convert_to_float
            },
            {'field': 'Hispanic % of Deaths', 'column': 'hispanic_percentage_of_deaths', 'data': convert_to_float},
            {
                'field': 'Hispanic % of Total Population',
                'column': 'hispanic_percentage_of_population',
                'data': convert_to_float
            },
            {'field': 'Asian % of Deaths', 'column': 'asian_percentage_of_deaths', 'data': convert_to_float},
            {
                'field': 'Asian % of Total Population',
                'column': 'asian_percentage_of_population',
                'data': convert_to_float
            },
            {
                'field': 'American Indian or Alaska Native % of Deaths',
                'column': 'american_indian_percentage_of_deaths',
                'data': convert_to_float
            },
            {
                'field': 'American Indian or Alaska Native % of Total Population',
                'column': 'american_indian_percentage_of_population',
                'data': convert_to_float
            },
            {
                'field': 'American Indian or Alaska Native % of Deaths',
                'column': 'alaska_native_percentage_of_deaths',
                'data': convert_to_float
            },
            {
                'field': 'American Indian or Alaska Native % of Total Population',
                'column': 'alaska_native_percentage_of_population',
                'data': convert_to_float
            },
            {
                'field': 'Native Hawaiian of Other Pacific Islander % of Deaths',
                'column': 'native_hawaiian_percentage_of_deaths',
                'data': convert_to_float
            },
            {
                'field': 'Native Hawaiian or Other Pacific Islander % of Total Population',
                'column': 'native_hawaiian_percentage_of_population',
                'data': convert_to_float
            },
            {
                'field': 'Native Hawaiian of Other Pacific Islander % of Deaths',
                'column': 'pacific_islander_percentage_of_deaths',
                'data': convert_to_float
            },
            {
                'field': 'Native Hawaiian or Other Pacific Islander % of Total Population',
                'column': 'pacific_islander_percentage_of_population',
                'data': convert_to_float
            },
            {'field': 'Other % of Deaths', 'column': 'other_percentage_of_deaths', 'data': convert_to_float},
            {
                'field': 'Other % of Total Population',
                'column': 'other_percentage_of_population',
                'data': convert_to_float
            },
            {
                'field': '% of Deaths with Known Race',
                'column': 'known_race_percentage_of_deaths',
                'data': convert_to_float
            },
            {
                'field': '% of Deaths with Unknown Race',
                'column': 'unknown_race_percentage_of_deaths',
                'data': convert_to_float
            },
            {
                'field': '% of Deaths with Known Ethnicity',
                'column': 'known_ethnicity_percentage_of_deaths',
                'data': convert_to_float
            },
            {
                'field': '% of Deaths with Unknown Ethnicity',
                'column': 'unknown_ethnicity_percentage_of_deaths',
                'data': convert_to_float
            }
        ]
        self.folder_path = '/'.join([constants.temp_dir, 'COVID-19-Data/Race Ethnicity COVID-19 Data/Cases and Deaths'])
        self.git = Git(constants.temp_dir)

    def fetch(self):
        if not os.path.isdir(constants.temp_dir):
            os.mkdir(constants.temp_dir)

        if not os.path.isdir(self.folder_path):
            self.git.clone(GIT_REPO_URL, depth=1)

        all_files = os.listdir(self.folder_path)
        all_files_length = len(all_files)
        index = 0

        self.raw_data = []
        while index < all_files_length:
            filename = all_files[index]
            if matches_death_by_race(filename):
                with open('/'.join([self.folder_path, filename]), newline='', encoding='utf-8') as csvfile:
                    self.raw_data.append({'filename': filename, 'data': csv.DictReader(io.StringIO(csvfile.read()))})

            index += 1

    def has_data(self):
        return self.raw_data is not None and len(self.raw_data) > 0

    def save(self):
        mysql_database = database.Database()
        mysql_database.connect()

        if mysql_database.is_connected():
            mysql_database.start_transaction()

        records = []
        for raw_record_data in self.raw_data:
            for record_data in raw_record_data['data']:
                record_data['date'] = convert_filename_to_date(raw_record_data['filename'])
                if '' in record_data:

                    if record_data[''] in constants.state_abbrev_map:
                        record_data['Location'] = record_data['']

                records.append(record_data)

        record_count = len(records)
        records_processed = 0

        for record in records:
            records_processed += 1
            columns = []
            values = []

            if 'Location' in record and record['Location'] in constants.state_abbrev_map:

                for field in self.fields:
                    if field['field'] in record:

                        if 'column' in field:
                            columns.append(field['column'])
                        elif 'field' in field:
                            columns.append(field['field'])

                        # Populating the values array
                        if 'data' in field:
                            if isinstance(field['data'], types.FunctionType):
                                values.append(field['data'].__call__(record[field['field']]))

                        elif 'field' in field:
                            values.append(record[field['field']])

                    elif 'default' in field:
                        if 'column' in field:
                            columns.append(field['column'])
                        elif 'field' in field:
                            columns.append(field['field'])

                        # Populating the values array
                        values.append(field['default'])

                mysql_database.insert(self.table_name, columns, values)

            utils.progress(records_processed, record_count)

        mysql_database.commit()


def matches_vaccinations_by_race(filename):
    return VACCINATIONS_BY_RE_REGEX.match(filename) is not None


class VaccinationsByRace:

    def __init__(self):
        self.table_name = 'vaccinations_by_race_ethnicity'
        self.raw_data = None
        self.fields = [
            {'field': 'date', 'column': 'date', 'data': utils.ensure_iso_date},
            {'field': 'Location', 'column': 'state'},
            {
                'field': 'Race Categories Include Hispanic Individuals',
                'column': 'hispanic_included',
                'data': utils.bool_to_int,
                'default': 0
            },
            {
                'field': 'White % of Vaccinations',
                'column': 'white_percentage_of_vaccinations',
                'data': convert_to_float
            },
            {
                'field': 'Black % of Vaccinations',
                'column': 'black_percentage_of_vaccinations',
                'data': convert_to_float
            },
            {
                'field': 'Hispanic % of Vaccinations',
                'column': 'hispanic_percentage_of_vaccinations',
                'data': convert_to_float
            },
            {
                'field': 'Asian % of Vaccinations',
                'column': 'asian_percentage_of_vaccinations',
                'data': convert_to_float
            },
            {
                'field': 'American Indian or Alaska Native % of Vaccinations',
                'column': 'american_indian_percentage_of_vaccinations',
                'data': convert_to_float
            },
            {
                'field': 'American Indian or Alaska Native % of Vaccinations',
                'column': 'alaska_native_percentage_of_vaccinations',
                'data': convert_to_float
            },
            {
                'field': 'Native Hawaiian or Other Pacific Islander % of Vaccinations',
                'column': 'native_hawaiian_percentage_of_vaccinations',
                'data': convert_to_float
            },
            {
                'field': 'Native Hawaiian or Other Pacific Islander % of Vaccinations',
                'column': 'pacific_islander_percentage_of_vaccinations',
                'data': convert_to_float
            },
            {
                'field': 'Other % of Vaccinations',
                'column': 'other_percentage_of_vaccinations',
                'data': convert_to_float
            },
            {
                'field': '% of Vaccinations with Known Race',
                'column': 'known_race_percentage_of_vaccinations',
                'data': convert_to_float
            },
            {
                'field': '% of Vaccinations with Unknown Race',
                'column': 'unknown_race_percentage_of_vaccinations',
                'data': convert_to_float
            },
            {
                'field': '% of Vaccinations with Known Ethnicity',
                'column': 'known_ethnicity_percentage_of_vaccinations',
                'data': convert_to_float
            },
            {
                'field': '% of Vaccinations with Unknown Ethnicity',
                'column': 'unknown_ethnicity_percentage_of_vaccinations',
                'data': convert_to_float
            }
        ]
        self.folder_path = '/'.join([constants.temp_dir, 'COVID-19-Data/Race Ethnicity COVID-19 Data/Vaccines'])
        self.git = Git(constants.temp_dir)

    def fetch(self):
        if not os.path.isdir(constants.temp_dir):
            os.mkdir(constants.temp_dir)

        if not os.path.isdir(self.folder_path):
            self.git.clone(GIT_REPO_URL, depth=1)

        all_files = os.listdir(self.folder_path)
        all_files_length = len(all_files)
        index = 0

        self.raw_data = []
        while index < all_files_length:
            filename = all_files[index]
            if matches_vaccinations_by_race(filename):
                with open('/'.join([self.folder_path, filename]), newline='') as csvfile:
                    self.raw_data.append({'filename': filename, 'data': csv.DictReader(io.StringIO(csvfile.read()))})

            index += 1

    def has_data(self):
        return self.raw_data is not None and len(self.raw_data) > 0

    def save(self):
        mysql_database = database.Database()
        mysql_database.connect()

        if mysql_database.is_connected():
            mysql_database.start_transaction()

        records = []
        for raw_record_data in self.raw_data:
            for record_data in raw_record_data['data']:
                record_data['date'] = convert_filename_to_date(raw_record_data['filename'])
                if '' in record_data:

                    if record_data[''] in constants.state_abbrev_map:
                        record_data['Location'] = record_data['']

                elif 'Location' not in record_data:
                    location_key = None
                    for key in record_data.keys():
                        if key.endswith('Location'):
                            location_key = key

                    record_data['Location'] = record_data[location_key] if location_key is not None else location_key

                records.append(record_data)

        record_count = len(records)
        records_processed = 0

        for record in records:
            records_processed += 1
            columns = []
            values = []

            if 'Location' in record and record['Location'] in constants.state_abbrev_map:

                for field in self.fields:
                    if field['field'] in record:

                        if 'column' in field:
                            columns.append(field['column'])
                        elif 'field' in field:
                            columns.append(field['field'])

                        # Populating the values array
                        if 'data' in field:
                            if isinstance(field['data'], types.FunctionType):
                                values.append(field['data'].__call__(record[field['field']]))

                        elif 'field' in field:
                            values.append(record[field['field']])

                    elif 'default' in field:
                        if 'column' in field:
                            columns.append(field['column'])
                        elif 'field' in field:
                            columns.append(field['field'])

                        # Populating the values array
                        values.append(field['default'])

                mysql_database.insert(self.table_name, columns, values)

            utils.progress(records_processed, record_count)

        mysql_database.commit()
