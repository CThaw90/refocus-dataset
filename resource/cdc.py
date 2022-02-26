from common import utils
from data import database

import datetime
import requests
import types
import json

SAT_WEEKDAY_INDEX = 5
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
