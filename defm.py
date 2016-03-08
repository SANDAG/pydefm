import pandas as pd
from db import extract
from forecast import compute
from forecast import util
import inspect, os
from db import log
import time


# Housekeeping stuff

# measure script time
start_time = time.time()

# change to current directory to find .yml input config file
full_path = os.path.abspath(inspect.getfile(inspect.currentframe()))
os.chdir(os.path.dirname(full_path))

# set console display to show MultiIndex for every row
pd.set_option('display.multi_sparse', False)


# Start model run using rate versions in model_config.yml
#   note: rate versions refer to original data source

# record rate versions in result database
db_run_id = log.new_run('model_summary.db') # primary key for this run


# Load base population and rates.

# SQL query to pandas DataFrame (returns rates for all years)
#      columns:  'age', 'race_ethn', 'sex' (cohort), 'rate', 'year'
mig_rates = extract.create_df('migration', 'rate_table')
# migration rates: domestic in, domestic out, foreign in, foreign out
mig_rates = util.apply_pivot(mig_rates)  # pivot df so 4 mig rates in cols
birth_rates = extract.create_df('birth', 'rate_table')
death_rates = extract.create_df('death', 'rate_table')

# pandas DataFrame from SQL query (base population)
#      columns:  'age', 'race_ethn', 'sex' (cohort),
#       'gq.type', 'mildep','persons', 'households'
population = extract.create_df('population', 'population_table')

# base population to result database
log.insert_run('base_population.db', db_run_id, population,'base_population')

# years to be used in model
years = util.yaml_to_dict('model_config.yml', 'years')

population_summary = []  # initialize population by year list

# iterate over all years
for index, yr in enumerate(range(years['y1'],years['yf'] + 1)):

    print ('{} {}'.format(index, yr))

    # MIGRATION
    # Apply rates for in-migration & out-migration to base population

    # get rates for simulated yr in loop
    # join DataFrame with population
    yr_mig = compute.rates_for_yr(population, mig_rates, yr)

    # in & out migrating population
    net_mig_pop = compute.net_mig(yr_mig, db_run_id, yr)

    # non-migrating population
    non_mig = compute.non_mig(net_mig_pop, db_run_id, yr)

    # BIRTH
    # Apply rates for birth to base population
    yr_birth = compute.rates_for_yr(non_mig, birth_rates, yr)  # simulated yr
    births_per_cohort = compute.births_all(yr_birth, db_run_id, yr)  # newborn population
    # sum newborn population across cohorts
    births = compute.births_sum(births_per_cohort, db_run_id, yr)

    # DEATH
    # Apply rates for death to base population
    yr_death = compute.rates_for_yr(non_mig,death_rates,yr)  # simulated yr
    survived_pop = compute.deaths(yr_death, db_run_id, yr)  # deceased population
    # age population by one year
    aged_pop = compute.age_the_pop(survived_pop)
    # PREDICTED POPULATION
    # Update base population by adding in-migrating population and newborns
    population = compute.new_pop(births,aged_pop)
    pop_by_year = population.copy()
    pop_by_year['yr'] = yr
    log.insert_run('population.db',db_run_id,pop_by_year,'population_' + str(yr))
    population_summary.append({'Year': yr, 'Population': population['persons'].sum()})

# database logging of results
summary_df = pd.DataFrame(population_summary)
log.insert_run('model_summary.db',db_run_id,summary_df,'summary')

print("--- %s seconds ---" % (time.time() - start_time)) # time to execute