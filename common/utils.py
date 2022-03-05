import dateparser
import math
import sys


def ensure_float(value):
    try:
        float(value)
    except ValueError:
        value = 0.0
    except TypeError:
        value = 0.0
    return value if value != 'Inf' else 0.0


def ensure_int(value):
    try:
        int(value)
    except ValueError:
        value = 0
    except TypeError:
        value = 0
    return value


def bool_to_int(value):
    return 0 if value is None or value is False or value.lower() in (None, 'no', 'false', '') else 1


def ensure_iso_date(value):
    parsed_date = dateparser.parse(value, settings={'TIMEZONE': 'EST'})
    return parsed_date.isoformat() if parsed_date is not None else None


def array_equals(list_one, list_two):
    length = len(list_one)
    equals = length == len(list_two)
    index = 0
    while index < length and equals:
        equals = list_one[index] == list_two[index]
        index += 1

    return equals


def log(message, newline=True):
    sys.stdout.write("{}{}".format(message, "\n" if newline else ""))


def stringify(array):
    string = ''
    delimiter = ''
    for a in array:
        string += delimiter
        string += a
        delimiter = ','

    return string


def get_value(obj, key):
    return obj[key]


def array_map_by_key(array, key):
    results = []
    for data in array:
        results.append(get_value(data, key))
    return results


def escape_quotes(value):
    return "'{}'".format(value.replace('\'', '\\\''))


def percentage(processed, amount):
    quotient = processed / amount
    return '{}%'.format(math.floor(quotient * 100))
