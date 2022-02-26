from common import utils

import os
import mysql.connector

SQL_MAX_LENGTH = 20000


def missing_env_var(env_var):
    utils.log('Missing environment variable {}', env_var)


def getenv(variable):
    return os.getenv(variable) if os.getenv(variable) is not None else None


def escape_quotes(value):
    return '`' + value + '`'


def value_or_empty(value, prepended=''):
    return '{} {}'.format(prepended, value) if value is not None else ''


def generate_columns_string(columns):
    columns_string = ''
    delimiter = ''
    for column in columns:
        columns_string += delimiter
        columns_string += escape_quotes(column)
        delimiter = ', '

    return '(' + columns_string + ')'


def generate_values_placeholders(length):
    values_placeholders = ''
    delimiter = ''
    iterator = 0
    while iterator < length:
        values_placeholders += delimiter
        values_placeholders += '%s'
        iterator += 1
        delimiter = ', '

    return '(' + values_placeholders + ')'


def generate_values_string(values):
    values_string = ''
    delimiter = ''
    for value in values:
        values_string += delimiter
        if isinstance(value, str):
            values_string += '\'' + value.replace('\'', '\\\'') + '\''
        else:
            values_string += str(value)

        delimiter = ', '

    return '(' + values_string + ')'


class Database:

    def __init__(self, debug=False, enable_cache=False):
        self.enable_cache = enable_cache
        self.cache = {}

        self.hostname = getenv('DB_HOST')
        self.username = getenv('DB_USER')
        self.password = getenv('DB_PASS')
        self.name = getenv('DB_NAME')
        self.port = getenv('DB_PORT')
        self.connection = None
        self.cursor = None
        self.debug = debug
        self.reset_cache()

    def connect(self):
        if self.hostname is None:
            missing_env_var('DATA_HOST')
        elif self.username is None:
            missing_env_var('DATA_USER')
        elif self.password is None:
            missing_env_var('DATA_PASS')
        elif self.name is None:
            missing_env_var('DATA_NAME')
        elif self.port is None:
            missing_env_var('DATA_PORT')
        elif self.is_connected():
            utils.log('There is already an active connection to the database')
        else:
            self.connection = mysql.connector.connect(
                user=self.username, password=self.password,
                host=self.hostname, database=self.name, port=self.port
            )

    def is_connected(self):
        return self.connection is not None

    def reset_cache(self):
        self.cache = {'table': None, 'sql': '', 'columns': [], 'values': []}

    def transaction_active(self):
        return self.cursor is not None

    def start_transaction(self):
        if not self.is_connected():
            utils.log('There is no active connection to a database')
        elif self.transaction_active():
            utils.log('There is already an active transaction')
        else:
            self.cursor = self.connection.cursor()

    def commit(self):
        if not self.is_connected():
            utils.log('There is no active connection to a database')
        elif not self.transaction_active():
            utils.log('There is no active transaction')
        else:
            if len(self.cache['sql']) > 0:
                self.cursor.execute(self.cache['sql'], self.cache['values'])
                self.reset_cache()

            self.connection.commit()
            self.cursor.close()
            self.cursor = None
            self.reset_cache()

    def insert(self, table_name, columns, values, debug=False):
        auto_transact = False
        assert len(columns) == len(values), 'columns length must match values length'
        if self.cursor is None:
            auto_transact = True
            self.start_transaction()

        if not auto_transact:
            if self.cache['table'] == table_name:
                if utils.array_equals(columns, self.cache['columns']) and len(self.cache['sql']) < SQL_MAX_LENGTH:
                    self.cache['values'] = self.cache['values'] + values
                    self.cache['sql'] += ', {}'.format(generate_values_placeholders(len(values)))
                else:
                    self.cursor.execute(self.cache['sql'], self.cache['values'])
                    self.reset_cache()
                    self.insert(table_name, columns, values, debug)
            else:
                self.cache = {
                    'table': table_name,
                    'columns': columns,
                    'values': values,
                    'sql': 'INSERT INTO {} {} VALUES {}'.format(
                        table_name,
                        generate_columns_string(columns),
                        generate_values_placeholders(len(values))
                    )
                }

        elif self.cache['table'] is not None:
            self.cursor.execute(self.cache['sql'], self.cache['values'])
            self.reset_cache()
            self.insert(table_name, columns, values, debug)

        else:
            insertion_statement = 'INSERT INTO {} {} VALUES {}'.format(
                table_name,
                generate_columns_string(columns),
                generate_values_placeholders(len(values))
            )
            self.cursor.execute(insertion_statement, values)

        if debug:
            utils.log(
                'INSERT INTO {} {} VALUES {}'.format(
                    table_name,
                    generate_columns_string(columns),
                    generate_values_string(values)
                )
            )

        # self.cursor.execute(insertion_statement, values)

        if auto_transact:
            self.commit()

    def select(self, table_name, fields=None, where=None, limit=None):
        query = 'SELECT {} FROM {} {} {}'.format(
            '{}', table_name,
            '{}'.format(value_or_empty(where, 'where')),
            '{}'.format(value_or_empty(limit, 'limit')))

        fields = [] if fields is None else fields
        if len(fields) == 0:
            query = query.format('*')
        else:
            query = query.format(utils.stringify(fields))

        if self.cursor is None:
            self.cursor = self.connection.cursor()
            self.cursor.execute(query)
        else:
            utils.log('Cannot select while a transaction is currently in progress')
            return None

        results = self.cursor.fetchall()
        self.cursor = None

        return results

    def close(self):
        if self.is_connected():
            self.connection.close()
            self.connection = None

    def __del__(self):
        self.close()
