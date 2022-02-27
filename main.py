from common import utils
from resource import apha, cdc, kff, wapo, elab, census, google

import math
import time

if __name__ == '__main__':

    start_time = time.perf_counter()

    census_county_geo_codes_start_time = time.perf_counter()
    census_county_geo_codes = census.CountyGeoCodes()
    utils.log(". Starting Census county Geo Codes....")
    census_county_geo_codes.fetch()
    if census_county_geo_codes.has_data():
        census_county_geo_codes.save()
    census_county_geo_codes_end_time = time.perf_counter()

    cdc_hospitalizations_start_time = time.perf_counter()
    utils.log('. Starting CDC Hospitalizations....')
    cdc_hospitalizations = cdc.Hospitalizations()
    cdc_hospitalizations.fetch()
    if cdc_hospitalizations.has_data():
         cdc_hospitalizations.save()
    cdc_hospitalizations_end_time = time.perf_counter()

    utils.log('. Starting KFF state trends....')
    kff_state_trends_start_time = time.perf_counter()
    kff_state_trends = kff.StateTrends()
    kff_state_trends.fetch()
    if kff_state_trends.has_data():
        kff_state_trends.save()
    kff_state_trends_end_time = time.perf_counter()

    utils.log('. Starting KFF cases by race...')
    kff_cases_by_race_start_time = time.perf_counter()
    kff_cases_by_race = kff.CasesByRace()
    kff_cases_by_race.fetch()
    if kff_cases_by_race.has_data():
        kff_cases_by_race.save()
    kff_cases_by_race_end_time = time.perf_counter()

    utils.log('. Starting KFF deaths by race...')
    kff_deaths_by_race_start_time = time.perf_counter()
    kff_deaths_by_race = kff.DeathsByRace()
    kff_deaths_by_race.fetch()
    if kff_deaths_by_race.has_data():
        kff_deaths_by_race.save()
    kff_deaths_by_race_end_time = time.perf_counter()

    utils.log('. Starting KFF vaccinations by race...')
    kff_vaccinations_by_race_start_time = time.perf_counter()
    kff_vaccinations_by_race = kff.VaccinationsByRace()
    kff_vaccinations_by_race.fetch()
    if kff_vaccinations_by_race.has_data():
        kff_vaccinations_by_race.save()
    kff_vaccinations_by_race_end_time = time.perf_counter()

    utils.log('. WaPo Police Shootings....')
    wapo_police_shootings_start_time = time.perf_counter()
    wapo_police_shootings = wapo.PoliceShootings()
    wapo_police_shootings.fetch()
    if wapo_police_shootings.has_data():
        wapo_police_shootings.save()
    wapo_police_shootings_end_time = time.perf_counter()

    utils.log('. Apha Map racism declarations....')
    apha_map_racism_declarations_start_time = time.perf_counter()
    apha_map_racism_declarations = apha.RacismDeclarations()
    apha_map_racism_declarations.fetch()
    if apha_map_racism_declarations.has_data():
        apha_map_racism_declarations.save()
    apha_map_racism_declarations_end_time = time.perf_counter()

    utils.log('. ELab Weekly Evictions....')
    elab_weekly_evictions_start_time = time.perf_counter()
    elab_weekly_evictions = elab.WeeklyEvictions()
    elab_weekly_evictions.fetch()
    if elab_weekly_evictions.has_data():
        elab_weekly_evictions.save()
    elab_weekly_evictions_end_time = time.perf_counter()

    utils.log('. Google Mobility data...')
    google_mobility_report_start_time = time.perf_counter()
    google_mobility_report = google.MobilityReport()
    google_mobility_report.fetch()
    if google_mobility_report.has_data():
        google_mobility_report.save()
    google_mobility_report_end_time = time.perf_counter()
    end_time = time.perf_counter()

    utils.log(
        'Census county geo codes finished in {} seconds'
        .format(math.ceil(census_county_geo_codes_end_time - census_county_geo_codes_start_time))
    )
    utils.log(
        'CDC Hospitalizations finished in {} seconds'
        .format(math.ceil(cdc_hospitalizations_end_time - cdc_hospitalizations_start_time))
    )
    utils.log(
        'KFF State Trends finished in {} seconds'
        .format(math.ceil(kff_state_trends_end_time - kff_state_trends_start_time))
    )
    utils.log(
        'KFF Cases by Race finished in {} seconds'
        .format(math.ceil(kff_cases_by_race_end_time - kff_cases_by_race_start_time))
    )
    utils.log(
        'KFF Deaths by Race finished in {} seconds'
        .format(math.ceil(kff_deaths_by_race_end_time - kff_deaths_by_race_start_time))
    )
    utils.log(
        'KFF Vaccinations by Race finished in {} seconds'
        .format(math.ceil(kff_vaccinations_by_race_end_time - kff_vaccinations_by_race_start_time))
    )
    utils.log(
         'Wapo Police shootings finished in {} seconds'
         .format(math.ceil(wapo_police_shootings_end_time - wapo_police_shootings_start_time))
    )
    utils.log(
        'Apha Map racism declarations finished in {} seconds'
        .format(math.ceil(apha_map_racism_declarations_end_time - apha_map_racism_declarations_start_time))
    )
    utils.log(
        'Elab Weekly Evictions finished in {} seconds'
        .format(math.ceil(elab_weekly_evictions_end_time - elab_weekly_evictions_start_time))
    )
    utils.log(
        'Google Mobility report finished in {} seconds'
        .format(math.ceil(google_mobility_report_end_time - google_mobility_report_start_time))
    )
    utils.log('Application finished in {} seconds'.format(math.ceil(end_time - start_time)))
