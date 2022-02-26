from common import utils
from data import database
from git.cmd import Git

import requests
import types
import csv
import io

GIT_REPO_URL = 'https://github.com/'
URL = 'https://raw.githubusercontent.com/KFFData/COVID-19-Data/kff_master/State%20Trend%20Data/State_Trend_Data.csv'


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

    def fetch(self):
        request = requests.request('GET', URL)
        request_content = request.content.decode('utf-8')
        self.raw_data = csv.DictReader(io.StringIO(request_content))

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


class CasesByRace:

    def __init__(self):
        self.table = ''
        self.raw_data = None
        self.fields = [
            {'field': 'date', 'column': 'date', 'data': utils.ensure_iso_date},
            {'field': 'Location', 'column': 'state'},
            {'field': 'Race Categories Include Hispanic Individuals', 'column': 'hispanic_included'},
            {'field': 'White % of Cases', 'column': 'white_percentage_of_cases'},
            {'field': 'White % of Total Population', 'column': 'white_percentage_of_population'},
            {'field': 'Black % of Cases', 'column': 'black_percentage_of_cases'},
            {'field': 'Black % of Total Population', 'column': 'black_percentage_total_population'},
            {'field': 'Hispanic % of Cases', 'column': 'hispanic_percentage_of_cases'},
            {'field': 'Hispanic % of Total Population', 'column': 'hispanic_percentage_of_population'},
            {'field': 'Asian % of Cases', 'column': 'asian_percentage_of_cases'},
            {'field': 'Asian % of Total Population', 'column': 'asian_percentage_of_population'},
            {'field': 'American Indian or Alaska Native % of Cases', 'column': 'american_indian_percentage_of_cases'},
            {
                'field': 'American Indian or Alaska Native % of Total Population',
                'column': 'american_indian_percentage_or_population'
            },
            {'field': 'American Indian or Alaska Native % of Cases', 'column': 'alaska_native_percentage_of_cases'},
            {
                'field': 'American Indian or Alaska Native % of Total Population',
                'column': 'alaska_native_percentage_of_population'
            },
            {
                'field': 'Native Hawaiian or Other Pacific Islander % of Cases',
                'column': 'native_hawaiian_percentage_of_cases'
            },
            {
                'field': 'Native Hawaiian or Other Pacific Islander % of Total Population',
                'column': 'native_hawaiian_percentage_of_population'
            },
            {
                'field': 'Native Hawaiian or Other Pacific Islander % of Cases',
                'column': 'pacific_islander_percentage_of_cases'
            },
            {
                'field': 'Native Hawaiian or Other Pacific Islander % of Total Population',
                'column': 'pacific_islander_percentage_of_population'
            },
            {'field': 'Other % of Cases', 'column': 'other_percentage_of_cases'},
            {'field': 'Other % of Total Population', 'column': 'other_percentage_of_population'},
            {'field': '% of Cases with Known Race', 'column': 'known_race_percentage_of_cases'},
            {'field': '% of Cases with Unknown Race', 'column': 'unknown_race_percentage_of_cases'},
            {'field': '% of Cases with Known Ethnicity', 'column': 'known_ethnicity_percentage_of_cases'},
            {'field': '% of Cases with Missing Ethnicity', 'column': 'unknown_ethnicity_percentage_of_cases'}
        ]

    def fetch(self):
        pass
