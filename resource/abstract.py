from common import utils
from data import database

import types


class Resource:

    def __init__(self):
        self.table_name = ''
        self.raw_data = None
        self.skipping = False
        self.fields = []

    def skip_record(self, record):
        return self.skipping and record is not None

    def has_data(self):
        return self.raw_data is not None

    def save(self, record_cache=None):
        mysql_database = database.Database()
        mysql_database.connect()

        if mysql_database.is_connected():
            mysql_database.start_transaction()

            records = list(self.raw_data)
            record_count = len(records)
            records_processed = 0

            # Used to cache values for additional calculations
            if record_cache is None:
                record_cache = {}

            for record in records:
                columns = []
                values = []

                if self.skip_record(record):
                    records_processed += 1
                    utils.progress(records_processed, record_count)
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
                utils.progress(records_processed, record_count)

            mysql_database.commit()
