from common import constants, utils
from resource import census
from data import database

import datetime
import requests
import types
import json
import csv
import io

SAT_WEEKDAY_INDEX = 5
STATE_TREND_URL = 'https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=us_trend_by_{}'
VACCINE_TREND_URL = 'https://data.cdc.gov/api/views/unsk-b7fc/rows.csv?accessType=DOWNLOAD'
URL = 'https://gis.cdc.gov/grasp/covid19_3_api/PostPhase03DataTool'
HEADERS = {'Content-Type': 'application/json'}
DATA = {'appversion': 'Public', 'key': 'datadownload', 'injson': []}


def parse_cumulative_rate(response_object):
    return utils.ensure_float(response_object['cumulative-rate'])


def parse_weekly_rate(response_object):
    return utils.ensure_float(response_object['weekly-rate'])


def get_mmwr_date(response_object):
    adjusted = False
    date = datetime.date(int(response_object['mmwr-year']), 1, 1)
    if date.weekday() != SAT_WEEKDAY_INDEX:
        date = date + datetime.timedelta(days=SAT_WEEKDAY_INDEX - date.weekday())
        adjusted = True

    date = date + datetime.timedelta(days=(int(response_object['mmwr-week']) - (1 if adjusted else 0)) * 7)

    return '{}-{}-{}'.format(date.year, date.month, date.day)


class Hospitalizations:

    def __init__(self):
        self.table_name = 'cdc_hospitalizations'
        self.raw_data = None
        self.fields = [
            {'field': 'catchment'},
            {'field': 'network'},
            {'field': 'mmwr-year', 'column': 'mmwr_year'},
            {'field': 'mmwr-week', 'column': 'mmwr_week'},
            {'field': 'age_category'},
            {'field': 'sex_category'},
            {'field': 'race_category'},
            {'field': parse_cumulative_rate, 'column': 'cumulative_rate'},
            {'field': parse_weekly_rate, 'column': 'weekly_rate'},
            {'field': get_mmwr_date, 'column': 'mmwr_date'}
        ]

    def fetch(self):
        request = requests.request('POST', URL, json=DATA, headers=HEADERS)
        self.raw_data = json.loads(request.content.decode('utf-8'))

    def has_data(self):
        return self.raw_data is not None

    def save(self):
        mysql_database = database.Database()
        mysql_database.connect()

        if mysql_database.is_connected():
            mysql_database.start_transaction()

            records = self.raw_data['datadownload']
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
                        if isinstance(field['field'], str):
                            columns.append(field['field'])

                    # Populating the values array
                    if field.__contains__('field'):
                        if isinstance(field['field'], str):
                            values.append(record[field['field']])
                        elif isinstance(field['field'], types.FunctionType):
                            values.append(field['field'].__call__(record))

                mysql_database.insert(self.table_name, columns, values)

                records_processed += 1
                utils.log("\rProgress: {}% - Records processed: {} of {}"
                          .format(records_processed // progress_threshold, records_processed, record_count),
                          newline=records_processed == record_count)

            mysql_database.commit()


def accumulate_tests(record, record_key, cache):
    cumulative_tests_key = 'cumulative_tests'
    cache_key = record['state']
    if cache_key not in cache:
        cache[cache_key] = {}

    if cumulative_tests_key not in cache[cache_key]:
        cache[cache_key][cumulative_tests_key] = 0

    new_test_results_reported = record[record_key] if record[record_key] is not None else 0
    cache[cache_key][cumulative_tests_key] += new_test_results_reported
    return cache[cache_key][cumulative_tests_key]


def get_new_cases_seven_day_avg(record, record_key, cache):
    namespace_key = 'cases_seven_day_avg'
    cache_key = record['state']
    if cache_key not in cache:
        cache[cache_key] = {}

    if namespace_key not in cache[cache_key]:
        cache[cache_key][namespace_key] = {}
        cache[cache_key][namespace_key]['last_seven_records'] = []
        cache[cache_key][namespace_key]['last_seven_records_sum_of_cases'] = 0

    cases_seven_day_avg = cache[cache_key][namespace_key]
    last_seven_records = cases_seven_day_avg['last_seven_records']
    if len(last_seven_records) == 7:
        removed_record = last_seven_records.pop(0)
        cases_seven_day_avg['last_seven_records_sum_of_cases'] -= removed_record \
            if isinstance(removed_record, int) else 0

    last_seven_records.append(record[record_key])
    cases_seven_day_avg['last_seven_records_sum_of_cases'] += record[record_key]

    return cases_seven_day_avg['last_seven_records_sum_of_cases'] / 7 if len(last_seven_records) == 7 else 0


def records_percent_change_last_n_days(record, record_key, cache, record_type, n):
    namespace_key = '{}_percent_change_{}_day'.format(record_type, n)
    # Need to keep track of n + 1 the number of records to
    # calculate the percentage change of the first day of
    # the new iterative range and the last day of the
    # previous iterative range
    record_collection_key = 'last_n_plus_one_records'
    cache_key = record['state']
    if cache_key not in cache:
        cache[cache_key] = {}

    if namespace_key not in cache[cache_key]:
        cache[cache_key][namespace_key] = {}
        cache[cache_key][namespace_key][record_collection_key] = []

    last_n_plus_one_records = cache[cache_key][namespace_key][record_collection_key]
    if len(last_n_plus_one_records) == n + 1:
        last_n_plus_one_records.pop(0)

    last_n_plus_one_records.append(record[record_key])

    record_percent_change_last_n_days = 0
    if len(last_n_plus_one_records) == n + 1:
        first_record_value = last_n_plus_one_records[0]
        last_record_value = last_n_plus_one_records[n]
        if None not in [first_record_value, last_record_value] and first_record_value > 0:
            record_percent_change_last_n_days = (last_record_value - first_record_value) / first_record_value

    return record_percent_change_last_n_days


def cases_percent_change_last_seven_days(record, record_key, cache):
    return records_percent_change_last_n_days(record, record_key, cache, 'cases', 7)


def cases_percent_change_last_fourteen_days(record, record_key, cache):
    return records_percent_change_last_n_days(record, record_key, cache, 'cases', 14)


def get_new_deaths_seven_day_avg(record, record_key, cache):
    namespace_key = 'deaths_seven_day_avg'
    cache_key = record['state']
    if cache_key not in cache:
        cache[cache_key] = {}

    if namespace_key not in cache[cache_key]:
        cache[cache_key][namespace_key] = {}
        cache[cache_key][namespace_key]['last_seven_records'] = []
        cache[cache_key][namespace_key]['last_seven_records_sum_of_deaths'] = 0

    deaths_seven_day_avg = cache[cache_key][namespace_key]
    last_seven_records = deaths_seven_day_avg['last_seven_records']
    if len(last_seven_records) == 7:
        removed_record = last_seven_records.pop(0)
        deaths_seven_day_avg['last_seven_records_sum_of_deaths'] -= removed_record \
            if isinstance(removed_record, int) else 0

    last_seven_records.append(record[record_key])
    deaths_seven_day_avg['last_seven_records_sum_of_deaths'] += record[record_key] \
        if isinstance(record[record_key], int) else 0

    return deaths_seven_day_avg['last_seven_records_sum_of_deaths'] / 7 if len(last_seven_records) == 7 else 0


def deaths_percent_change_last_seven_days(record, record_key, cache):
    return records_percent_change_last_n_days(record, record_key, cache, 'deaths', 7)


def deaths_percent_change_last_fourteen_days(record, record_key, cache):
    return records_percent_change_last_n_days(record, record_key, cache, 'deaths', 14)


def get_new_tests_seven_day_avg(record, record_key, cache):
    namespace_key = 'tests_seven_day_avg'
    cache_key = record['state']
    if cache_key not in cache:
        cache[cache_key] = {}

    if namespace_key not in cache[cache_key]:
        cache[cache_key][namespace_key] = {}
        cache[cache_key][namespace_key]['last_seven_records'] = []
        cache[cache_key][namespace_key]['last_seven_records_sum_of_tests'] = 0

    tests_seven_day_avg = cache[cache_key][namespace_key]
    last_seven_records = tests_seven_day_avg['last_seven_records']
    if len(last_seven_records) == 7:
        removed_record = last_seven_records.pop(0)
        tests_seven_day_avg['last_seven_records_sum_of_tests'] -= removed_record \
            if isinstance(removed_record, int) else 0

    last_seven_records.append(record[record_key])
    tests_seven_day_avg['last_seven_records_sum_of_tests'] += record[record_key] \
        if isinstance(record[record_key], int) else 0

    return tests_seven_day_avg['last_seven_records_sum_of_tests'] / 7 if len(last_seven_records) == 7 else 0


def tests_percent_change_last_seven_days(record, record_cache, cache):
    return records_percent_change_last_n_days(record, record_cache, cache, 'tests', 7)


def tests_percent_change_last_fourteen_days(record, record_cache, cache):
    return records_percent_change_last_n_days(record, record_cache, cache, 'tests', 14)


def get_positivity_rate(*value):
    record, record_key = value[0], value[1]
    return record['New_case'] / record[record_key] if record[record_key] is not None and record[record_key] > 0 else 0


def ensure_int(*values):
    return utils.ensure_int(values[0][values[1]])


def ensure_iso_date(*values):
    return utils.ensure_iso_date(values[0][values[1]])


def skip_record(record):
    return record['state'] == 'Guam' or record['state'] == 'Virgin Islands'


def nil(*value):
    return 0


class StateTrends:

    def __init__(self):
        self.table_name = 'state_trend_data'
        self.population_estimates = None
        self.vaccines_state_trend = None
        self.raw_data = None
        self.fields = [
            {'field': 'state'},
            {'field': 'date', 'data': ensure_iso_date},
            {'field': 'tot_cases', 'column': 'cases'},
            {'field': 'tot_deaths', 'column': 'deaths'},
            {'field': 'new_test_results_reported', 'column': 'tests', 'data': accumulate_tests},
            {'field': 'New_case', 'column': 'cases_change'},
            {'field': 'new_death', 'column': 'deaths_change'},
            {'field': 'new_test_results_reported', 'column': 'tests_change', 'data': ensure_int},
            {'field': 'New_case', 'column': 'cases_7_day_mean', 'data': get_new_cases_seven_day_avg},
            {'field': 'new_death', 'column': 'deaths_7_day_mean', 'data': get_new_deaths_seven_day_avg},
            {'field': 'new_test_results_reported', 'column': 'tests_7_day_mean', 'data': get_new_tests_seven_day_avg},
            {'field': 'new_test_results_reported', 'column': 'positivity_rate', 'data': get_positivity_rate},
            {'field': 'New_case', 'column': 'cases_per_million', 'data': self.get_cases_per_million},
            {'field': 'new_death', 'column': 'deaths_per_million', 'data': self.get_deaths_per_million},
            {'field': 'new_test_results_reported', 'column': 'tests_per_million', 'data': self.get_tests_per_million},

            {'field': 'New_case', 'column': 'pct_change_weekly_cases_7', 'data': cases_percent_change_last_seven_days},
            {
                'field': 'New_case',
                'column': 'pct_change_weekly_cases_14',
                'data': cases_percent_change_last_fourteen_days
            },
            {
                'field': 'new_death',
                'column': 'pct_change_weekly_deaths_7',
                'data': deaths_percent_change_last_seven_days
            },
            {
                'field': 'new_death',
                'column': 'pct_change_weekly_deaths_14',
                'data': deaths_percent_change_last_fourteen_days
            },
            {
                'field': 'new_test_results_reported',
                'column': 'pct_change_weekly_tests_7',
                'data': tests_percent_change_last_seven_days
            },
            {
                'field': 'new_test_results_reported',
                'column': 'pct_change_weekly_tests_14',
                'data': tests_percent_change_last_fourteen_days
            },
            {'field': 'percent_positive_7_day', 'column': 'positivity_rate_7_day_mean', 'data': nil},
            {'field': 'percent_positive_7_day', 'column': 'positivity_rate_7_day_plus_mean', 'data': nil},
            {'field': 'new_test_results_reported', 'column': 'pct_change_positivity_rate_7', 'data': nil},
            {'field': 'new_test_results_reported', 'column': 'pct_change_positivity_rate_14', 'data': nil},

            {'field': 'tot_cases', 'column': 'population', 'data': self.get_population},
            {'field': 'state', 'column': 'vaccines_distributed', 'data': self.get_vaccines_distributed},
            {'field': 'state', 'column': 'vaccines_administered', 'data': self.get_vaccines_administered},
            {'field': 'state', 'column': 'vaccines_one_dose', 'data': self.get_vaccines_administered_one_dose},
            {'field': 'state', 'column': 'vaccines_two_dose', 'data': self.get_vaccines_administered_two_dose},
            {'field': 'tot_cases', 'column': 'hotspot', 'data': nil}
        ]

    def get_vaccine_data_by_key(self, record, state, vaccine_key):
        iso_date = utils.ensure_iso_date(record['date'])
        state_abbrev = constants.state_abbrev_map[state]
        return self.vaccines_state_trend[iso_date][state_abbrev][vaccine_key] \
            if iso_date in self.vaccines_state_trend and state_abbrev in self.vaccines_state_trend[iso_date] else 0

    def get_vaccines_distributed(self, record, record_key, cache):
        return self.get_vaccine_data_by_key(record, record[record_key], 'Distributed')

    def get_vaccines_administered(self, record, record_key, cache):
        return self.get_vaccine_data_by_key(record, record[record_key], 'Administered')

    def get_vaccines_administered_one_dose(self, record, record_key, cache):
        return self.get_vaccine_data_by_key(record, record[record_key], 'Administered_Dose1')

    def get_vaccines_administered_two_dose(self, record, record_key, cache):
        return self.get_vaccine_data_by_key(record, record[record_key], 'Administered_Dose2')

    def get_population(self, *values):
        record = values[0]
        iso_date = utils.ensure_iso_date(record['date'])
        return self.population_estimates[iso_date][record['state']]

    def get_value_per_million(self, record, record_key):
        one_million = 1000000
        iso_date = utils.ensure_iso_date(record['date'])
        state_population = self.population_estimates[iso_date][record['state']]
        new_value_per_million = 0
        if record[record_key] is not None and record[record_key] > 0:
            new_values_per_state_population = state_population / record[record_key]
            new_value_per_million = one_million / new_values_per_state_population

        return new_value_per_million

    def get_cases_per_million(self, record, record_key, other):
        return self.get_value_per_million(record, record_key)

    def get_deaths_per_million(self, record, record_key, other):
        return self.get_value_per_million(record, record_key)

    def get_tests_per_million(self, record, record_key, other):
        return self.get_value_per_million(record, record_key)

    def fetch(self):
        self.raw_data = []
        for state in constants.state_abbrev_list:
            request = requests.request('GET', STATE_TREND_URL.format(state), headers=HEADERS)
            response_content = json.loads(request.content.decode('utf-8'))
            self.raw_data.extend(response_content['us_trend_by_Geography'])

        census_population_estimates = census.PopulationEstimates()
        census_population_estimates.fetch()
        if census_population_estimates.has_data():
            self.population_estimates = census_population_estimates.get_data()

        request = requests.request('GET', VACCINE_TREND_URL)
        request_content = request.content.decode('utf-8')
        vaccines_raw_data = csv.DictReader(io.StringIO(request_content))
        self.vaccines_state_trend = {}
        for vaccine_data in vaccines_raw_data:
            iso_date = utils.ensure_iso_date(vaccine_data['Date'])
            if iso_date not in self.vaccines_state_trend:
                self.vaccines_state_trend[iso_date] = {}

            state = vaccine_data['Location']
            self.vaccines_state_trend[iso_date][state] = {
                'Administered': vaccine_data['Administered'],
                'Distributed': vaccine_data['Distributed'],
                'Administered_Dose1': vaccine_data['Administered_Dose1_Recip'],
                'Administered_Dose2': vaccine_data['Series_Complete_Yes']
            }

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

            # Used to cache values for additional calculations
            record_cache = {}

            for record in records:
                columns = []
                values = []

                if skip_record(record):
                    records_processed += 1
                    utils.log("\rProgress: {} - Records processed: {} of {}"
                              .format(utils.percentage(records_processed, record_count), records_processed, record_count),
                              newline=records_processed == record_count)
                    continue

                for field in self.fields:
                    if 'column' in field:
                        columns.append(field['column'])
                    elif 'field' in field:
                        columns.append(field['field'])

                    # Populating the values array
                    if 'data' in field:
                        if isinstance(field['data'], types.FunctionType):
                            values.append(field['data'].__call__(record, field['field'], record_cache))
                        elif isinstance(field['data'], types.MethodType):
                            values.append(field['data'].__call__(record, field['field'], record_cache))
                        elif isinstance(field['data'], str):
                            values.append(field['data'])

                    elif 'field' in field:
                        if isinstance(field['field'], str):
                            values.append(record[field['field']])
                        elif isinstance(field['field'], types.FunctionType):
                            values.append(field['field'].__call__(record))

                mysql_database.insert(self.table_name, columns, values)

                records_processed += 1
                utils.log("\rProgress: {} - Records processed: {} of {}"
                          .format(utils.percentage(records_processed, record_count), records_processed, record_count),
                          newline=records_processed == record_count)

            mysql_database.commit()
