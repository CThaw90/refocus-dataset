from common import utils
from resource import apha, cdc, kff, wapo, elab, census, google

import math
import time
import sys
import re


def populate_module_set(values, module_set):
    for value in values:
        module_set.add(value)


def validate_module_list(module_list, module_set):
    for m in module_list:
        if m not in module_set:
            print('{} is not a module within the module set'.format(m))
            sys.exit(1)



if __name__ == '__main__':

    include_modules_arg_pattern = re.compile('^--include-modules.*')
    exclude_modules_arg_pattern = re.compile('^--exclude-modules.*')
    joined_arg_pattern = re.compile('.*-modules=(.*)')

    list_modules_flag = False
    include_modules_flag = False
    exclude_modules_flag = False

    included_modules_set = set()
    excluded_modules_set = set()
    all_modules_set = set()

    modules = [
        {'id': 'census_county_geo_codes', 'module': census.CountyGeoCodes},
        {'id': 'cdc_hospitalizations', 'module': cdc.Hospitalizations},
        {'id': 'kff_state_trends', 'module': kff.StateTrends},
        {'id': 'kff_cases_by_race', 'module': kff.CasesByRace},
        {'id': 'kff_deaths_by_race', 'module': kff.DeathsByRace},
        {'id': 'kff_vaccinations_by_race', 'module': kff.VaccinationsByRace},
        {'id': 'wapo_police_shootings', 'module': wapo.PoliceShootings},
        {'id': 'apha_map_racism_declarations', 'module': apha.RacismDeclarations},
        {'id': 'elab_weekly_evictions', 'module': elab.WeeklyEvictions},
        {'id': 'google_mobility_report', 'module': google.MobilityReport}
    ]

    populate_module_set(utils.array_map_by_key(modules, 'id'), all_modules_set)

    for arg in sys.argv:
        if include_modules_arg_pattern.match(arg) is not None:
            match_result = joined_arg_pattern.match(arg)
            if match_result is not None:
                module_argument_list = match_result.groups()[0].split(',')
                validate_module_list(module_argument_list, all_modules_set)
                populate_module_set(module_argument_list, included_modules_set)
            else:
                include_modules_flag = True
        elif exclude_modules_arg_pattern.match(arg) is not None:
            match_result = joined_arg_pattern.match(arg)
            if match_result is not None:
                module_argument_list = match_result.groups()[0].split(',')
                validate_module_list(module_argument_list, all_modules_set)
                populate_module_set(module_argument_list, excluded_modules_set)
            else:
                exclude_modules_flag = True
        elif include_modules_flag:
            module_argument_list = arg.split(',')
            validate_module_list(module_argument_list, all_modules_set)
            populate_module_set(module_argument_list, included_modules_set)
            include_modules_flag = False
        elif exclude_modules_flag:
            module_argument_list = arg.split(',')
            validate_module_list(module_argument_list, all_modules_set)
            populate_module_set(module_argument_list, excluded_modules_set)
            exclude_modules_flag = False

    start_time = time.perf_counter()
    time_perf_counters = []

    for module in modules:
        if (len(included_modules_set) == 0 or module['id'] in included_modules_set) and \
                (len(excluded_modules_set) == 0 or module['id'] not in excluded_modules_set):
            utils.log('Starting {}...'.format(module['id']))
            module_start_time = time.perf_counter()
            instantiated_module = module['module']()
            instantiated_module.fetch()
            if instantiated_module.has_data():
                instantiated_module.save()
            module_end_time = time.perf_counter()
            time_perf_counters.append({'id': module['id'], 'time': math.ceil(module_end_time - module_start_time)})

    for counter in time_perf_counters:
        utils.log('{} finished in {} seconds'.format(counter['id'], counter['time']))

    end_time = time.perf_counter()

    utils.log('Application finished in {} seconds'.format(math.ceil(end_time - start_time)))
