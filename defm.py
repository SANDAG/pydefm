import inspect, os
import pandas as pd
import time
from db import extract
from db import log
from forecast import compute
from forecast import util

"""
Demographic and Economic Forecasting Model
a simulation using rate versions in model_config.yml input config file
rate versions refer to original data source

"""

# Housekeeping stuff

# measure script time
start_time = time.time()

# change to current directory to find .yml input config file
full_path = os.path.abspath(inspect.getfile(inspect.currentframe()))
os.chdir(os.path.dirname(full_path))

# set console display to show MultiIndex for every row
pd.set_option('display.multi_sparse', False)

# rate versions to result database & return primary key for table
# db_run_id = log.new_run('model_summary.db')
db_run_id = log.new_run()

years = util.yaml_to_dict('model_config.yml', 'years')

# Load rates for all years: SQL query to pandas DataFrame
#   columns:  'age', 'race_ethn', 'sex' (cohort), 'rate', 'year'
#   pivot migration DataFrame w 4 rates: domestic in & out, foreign in & out

birth_rates = extract.create_df('birth', 'rate_table')
death_rates = extract.create_df('death', 'rate_table')
mig_rates = extract.create_df('migration', 'rate_table', pivot=True)


# Load base population: SQL query to pandas DataFrame
#   columns:  'age', 'race_ethn', 'sex' (cohort),
#   'gq.type', 'mildep', 'persons', 'households'

population = extract.create_df('population', 'population_table')

# special case ratios
ins_ratio = extract.create_df('ins', 'rate_table')

oth_ratio = extract.create_df('oth', 'rate_table')


# base population to result database with year
# log.insert_run('base_defm.db', db_run_id, population,'base_population'
y0 = years['y1'] - 1
population['yr'] = y0
log.insert_run('defm.db', db_run_id, population, 'population')
population = population.drop(['yr'], 1)


# years to be used in model

population_summary = []  # initialize list for population by year

# iterate over all years
for index, yr in enumerate(range(years['y1'],years['yf'] + 1)):

    print ('{} {}'.format(index, yr))

    # MIGRATION

    # rates for simulated yr joined with population DataFrame
    yr_mig = compute.rates_for_yr(population, mig_rates, yr)

    # in & out migrating population calculated
    mig = compute.net_mig(yr_mig, db_run_id, yr)

    # non-migrating population: population - out mig
    non_mig = compute.non_mig(mig, db_run_id, yr)

    # BIRTH

    # rates for simulated yr joined with population DataFrame
    yr_birth = compute.rates_for_yr(non_mig, birth_rates, yr)

    births_per_cohort = compute.births_all(yr_birth, db_run_id, yr)

    # sum newborn population across cohorts
    newborns = compute.births_sum(births_per_cohort, db_run_id, yr)

    # DEATHS

    # rates for simulated yr joined with population DataFrame
    yr_death = compute.rates_for_yr(non_mig, death_rates, yr)

    survived_pop = compute.deaths(yr_death, db_run_id, yr)

    # age population by one year
    aged_pop = compute.age_the_pop(survived_pop)

    # PREDICTED POPULATION

    # Update population by adding in-migrating population and newborns
    pop = compute.new_pop(newborns, aged_pop)

    # Special cases base_population.gq_type in ("INS","OTH")

    # INS
    # apply cohort-specific ratio (INS pop / HP pop)
    yr_ins = compute.rates_for_yr(pop, ins_ratio, yr)  # ratios
    # Update population for special case for INS
    pop_ins = compute.case_ins_oth(pop, yr_ins, 'INS')

    # OTH
    # apply cohort-specific ratio (OTH pop / HP pop)
    yr_oth = compute.rates_for_yr(pop_ins, oth_ratio, yr)  # ratios
    # Update population for special case for OTH
    population = compute.case_ins_oth(pop_ins, yr_ins, 'OTH')

    # add column for simulated yr for result database
    pop_by_year = population.copy()
    pop_by_year['yr'] = yr
    # log.insert_run('population.db', db_run_id, pop_by_year,
    #                   'population_' + str(yr))
    log.insert_run('defm.db', db_run_id, pop_by_year,
                   'population')
    # create summary for result db
    population_summary.append({'Year': yr,
                               'Population': population['persons'].sum()})
# end iteration over all years

# database logging of results population by year
summary_df = pd.DataFrame(population_summary)
log.insert_run('defm.db', db_run_id, summary_df, 'summary')

# script time to run
print("--- %s seconds ---" % (time.time() - start_time))
