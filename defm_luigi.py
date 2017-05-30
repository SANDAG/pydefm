import luigi
import inspect, os
import pandas as pd
import time
from db import extract
from db import log
from forecast import compute
from forecast import util
import shutil


class Population(luigi.Task):
    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        # yr = pd.Series([2015])
        # yr.to_hdf('temp/data.h5', 'year',  mode='w')
        # print yr[0]
        pop = extract.create_df('population', 'population_table')
        pop.to_hdf('temp/data.h5', 'pop', format='table', mode='a')


class MigrationRates(luigi.Task):
    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        mig_rates = extract.create_df('migration', 'rate_table', pivot=True)
        mig_rates.to_hdf('temp/data.h5', 'mig_rates', format='table', mode='a')


class DeathRates(luigi.Task):
    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        death_rates = extract.create_df('death', 'rate_table')
        death_rates.to_hdf('temp/data.h5', 'death_rates', format='table', mode='a')


class BirthRates(luigi.Task):
    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        birth_rates = extract.create_df('birth', 'rate_table')
        birth_rates.to_hdf('temp/data.h5', 'birth_rates', format='table', mode='a')


class MigrationPopulationOut(luigi.Task):

    def requires(self):
        return {'population': Population(),
                'migration_rates': MigrationRates()
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        mig_rates = pd.read_hdf('temp/data.h5', 'mig_rates')
        pop = pd.read_hdf('temp/data.h5', 'pop')
        yr_mig = compute.rates_for_yr(pop, mig_rates, 2015)
        mig_out = compute.net_mig(yr_mig, 1, 2015)
        mig_out = mig_out[['type', 'mildep', 'mig_Dout', 'mig_Fout']]

        mig_out.to_hdf('temp/data.h5', 'mig_out', format='table', mode='a')


class MigrationPopulationIn(luigi.Task):

    def requires(self):
        return {'population': Population(),
                'migration_rates': MigrationRates()
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        mig_rates = pd.read_hdf('temp/data.h5', 'mig_rates')
        pop = pd.read_hdf('temp/data.h5', 'pop')
        yr_mig = compute.rates_for_yr(pop, mig_rates, 2015)
        mig_in = compute.net_mig(yr_mig, 1, 2015)
        mig_in = mig_in[['type', 'mildep', 'mig_Din', 'mig_Fin']]
        mig_in.to_hdf('temp/data.h5', 'mig_in', format='table', mode='a')


class DeadPopulation(luigi.Task):

    def requires(self):
        return {'population': Population(),
                'death_rates': DeathRates()
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        death_rates = pd.read_hdf('temp/data.h5', 'death_rates')
        death_rates = death_rates[(death_rates['yr'] == 2015)]
        pop = pd.read_hdf('temp/data.h5', 'pop')
        pop = pop.join(death_rates)
        pop['deaths'] = (pop['persons'] * pop['death_rate']).round()
        pop = pop[['type', 'mildep', 'deaths']]
        pop.to_hdf('temp/data.h5', 'dead_pop', format='table', mode='a')


class NewBornPopulation(luigi.Task):

    def requires(self):
        return {'population': Population(),
                'birth_rates': BirthRates()
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        birth_rates = pd.read_hdf('temp/data.h5', 'birth_rates')
        pop = pd.read_hdf('temp/data.h5', 'pop')
        birth_rates = compute.rates_for_yr(pop, birth_rates, 2015)
        birth_rates = birth_rates[(birth_rates['yr'] == 2015)]
        births_per_cohort = compute.births_all(birth_rates, 1, 2015)

        # sum newborn population across cohorts
        newborn = compute.births_sum(births_per_cohort, 1, 2015)
        newborn.to_hdf('temp/data.h5', 'new_born', format='table', mode='a')


class FinalPopInterim(luigi.Task):

    def requires(self):
        return {'mig_out': MigrationPopulationOut(),
                'mig_in': MigrationPopulationIn(),
                'deaths': DeadPopulation(),
                'newborn': NewBornPopulation()
                }

    def run(self):
        mig_in = pd.read_hdf('temp/data.h5', 'mig_in')
        mig_out = pd.read_hdf('temp/data.h5', 'mig_out')
        deaths = pd.read_hdf('temp/data.h5', 'dead_pop')
        new_born = pd.read_hdf('temp/data.h5', 'new_born')


if __name__ == '__main__':

    shutil.rmtree('temp')
    os.makedirs('temp')

    luigi.run(main_task_cls=FinalPopInterim)