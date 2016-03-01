import pandas as pd
from db import extract
import numpy as np
from forecast import compute
from forecast import util
import inspect, os
from db import log


# change to script directory - to find model_config.yml file
os.chdir(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))

# set console display to show MultiIndex for every row
pd.set_option('display.multi_sparse', False)

# load rates and base population to pandas DataFrame
mig_rates = extract.create_df('migration', 'rate_table')
mig_rates = util.apply_pivot(mig_rates) # pivot so 4 mig rates in columns
birth_rates = extract.create_df('birth', 'rate_table')
death_rates = extract.create_df('death', 'rate_table')
population = extract.create_df('population', 'population_table')

# years to be used in model
years = util.yaml_to_dict('model_config.yml', 'years')

# iterate over all years
for index, yr in enumerate(range(years['y1'],years['yf'] + 1)):
    print ('{} {}'.format(index, yr))
    # MIGRATION
    # Apply rates for in-migration & out-migration to base population
    yr_mig = compute.rates_for_yr(population,mig_rates,yr)  # simulated yr
    net_mig_pop = compute.net_mig(yr_mig)  # in & out migrating population
    # Update base population by subtracting out-migrating population
    non_mig = compute.non_mig(net_mig_pop)  # non-migrating population
    # BIRTH
    # Apply rates for birth to base population
    yr_birth = compute.rates_for_yr(non_mig,birth_rates,yr)  # simulated yr
    births_per_cohort = compute.births_all(yr_birth)  # newborn population
    # sum newborn population across cohorts
    births = compute.births_sum(births_per_cohort)
    # DEATH
    # Apply rates for death to base population
    yr_death = compute.rates_for_yr(non_mig,death_rates,yr)  # simulated yr
    survived_pop = compute.deaths(yr_death)  # deceased population
    # age population by one year
    aged_pop = compute.age_the_pop(survived_pop)
    # PREDICTED POPULATION
    # Update base population by adding in-migrating population and newborns
    population = compute.new_pop(births,aged_pop)

# database logging of results
population['yr'] = yr
log.insert_run(population)

