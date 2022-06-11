from common import utils
from data import database

import requests
import zipfile
import types
import csv
import io

URL = 'https://www.gstatic.com/covid19/mobility/Region_Mobility_Report_CSVs.zip'
FILENAME_SET = {
    '2020_US_Region_Mobility_Report.csv',
    '2021_US_Region_Mobility_Report.csv',
    '2022_US_Region_Mobility_Report.csv',
    '2023_US_Region_Mobility_Report.csv'
}


def ensure_int_or_none(value):
    try:
        int(value)
    except ValueError:
        value = None
    except TypeError:
        value = None
    return value


class MobilityReport:

    def __init__(self):
        self.table_name = 'google_mobility'
        self.raw_data = None
        self.fields = [
            {'field': 'sub_region_1', 'column': 'state'},
            {'field': 'sub_region_2', 'column': 'county'},
            {'field': 'date', 'data': utils.ensure_iso_date},
            {
                'field': 'retail_and_recreation_percent_change_from_baseline',
                'column': 'retail_and_recreation_change',
                'data': ensure_int_or_none
            },
            {
                'field': 'grocery_and_pharmacy_percent_change_from_baseline',
                'column': 'grocery_and_pharmacy_change',
                'data': ensure_int_or_none
            },
            {'field': 'parks_percent_change_from_baseline', 'column': 'parks_change', 'data': ensure_int_or_none},
            {
                'field': 'transit_stations_percent_change_from_baseline',
                'column': 'transit_stations_change',
                'data': ensure_int_or_none
            },
            {
                'field': 'workplaces_percent_change_from_baseline',
                'column': 'workplaces_change',
                'data': ensure_int_or_none
            },
            {
                'field': 'residential_percent_change_from_baseline',
                'column': 'residential_change',
                'data': ensure_int_or_none
            }
        ]

    def fetch(self):
        self.raw_data = {}
        request = requests.request('GET', URL)
        zipfile_object = zipfile.ZipFile(io.BytesIO(request.content), mode='r')
        for file in zipfile_object.filelist:
            if file.filename in FILENAME_SET:
                csv_file_content = zipfile_object.open(file.filename)
                self.raw_data[file.filename] = csv.DictReader(io.StringIO(csv_file_content.read().decode('utf-8')))

    def has_data(self):
        return self.raw_data is not None

    def save(self):
        mysql_database = database.Database()
        mysql_database.connect()

        if mysql_database.is_connected():
            mysql_database.start_transaction()

            records = []
            for filename in FILENAME_SET:
                records += list(self.raw_data[filename]) if filename in self.raw_data else []

            record_count = len(records)
            records_processed = 0

            for record in records:
                records_processed += 1
                columns = []
                values = []

                if len(record['sub_region_1']) > 0 and len(record['sub_region_2']) > 0:

                    for field in self.fields:
                        if field.__contains__('column'):
                            columns.append(field['column'])
                        elif field.__contains__('field'):
                            columns.append(field['field'])

                        # Populating the values array
                        if field.__contains__('data'):
                            if isinstance(field['data'], types.FunctionType):
                                values.append(field['data'].__call__(record[field['field']]))

                        elif field.__contains__('field'):
                            values.append(record[field['field']])

                    mysql_database.insert(self.table_name, columns, values)

                utils.progress(records_processed, record_count)

            mysql_database.commit()
