import luigi
import inspect, os
import pandas as pd
import time
from db import extract
from db import log
from forecast import compute
from forecast import util
import shutil
import luigi.contrib.hadoop
from pathlib import Path


class Population(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        # yr = pd.Series([self.year])
        # yr.to_hdf('temp/data.h5', 'year',  mode='w')
        # print yr[0]
        my_file = Path('temp/data.h5')
        if my_file.is_file():
            print'File exists'
        else:
            pop = extract.create_df('population', 'population_table')
            pop.to_hdf('temp/data.h5', 'pop', format='table', mode='a')


class InMigrationRates(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        mig_rates = extract.create_df('migration', 'rate_table', pivot=True)
        mig_rates = mig_rates[['yr', 'DIN', 'FIN']]
        mig_rates.to_hdf('temp/data.h5', 'in_mig_rates', format='table', mode='a')


class OutMigrationRates(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        mig_rates = extract.create_df('migration', 'rate_table', pivot=True)
        mig_rates = mig_rates[['yr', 'DOUT', 'FOUT']]
        mig_rates.to_hdf('temp/data.h5', 'out_mig_rates', format='table', mode='a')


class DeathRates(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return Population(self.year)

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        death_rates = extract.create_df('death', 'rate_table')
        death_rates.to_hdf('temp/data.h5', 'death_rates', format='table', mode='a')


class BirthRates(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        birth_rates = extract.create_df('birth', 'rate_table')
        birth_rates.to_hdf('temp/data.h5', 'birth_rates', format='table', mode='a')


class MigrationPopulationOut(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return {
                'migration_rates': OutMigrationRates(self.year)
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        mig_rates = pd.read_hdf('temp/data.h5', 'out_mig_rates')
        pop = pd.read_hdf('temp/data.h5', 'pop')
        pop = compute.rates_for_yr(pop, mig_rates, self.year)
        pop = pop[(pop['type'] == 'HP') & (pop['mildep'] == 'N')]
        # pop.loc[pop['type'].isin(['COL', 'INS', 'MIL', 'OTH']), ['DOUT', 'FOUT']] = 0
        # pop.loc[pop['mildep'].isin(['Y']), ['DOUT', 'DOUT']] = 0
        pop['mig_Dout'] = (pop['persons'] * pop['DOUT']).round()
        pop['mig_Fout'] = (pop['persons'] * pop['FOUT']).round()
        pop = pop[['mig_Dout', 'mig_Fout']]

        pop.to_hdf('temp/data.h5', 'mig_out', format='table', mode='a')


class MigrationPopulationIn(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return {
                'migration_rates': InMigrationRates(self.year)
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        mig_rates = pd.read_hdf('temp/data.h5', 'in_mig_rates')
        pop = pd.read_hdf('temp/data.h5', 'pop')
        pop = compute.rates_for_yr(pop, mig_rates, self.year)
        pop = pop[(pop['type'] == 'HP') & (pop['mildep'] == 'N')]
        # pop.loc[pop['type'].isin(['COL', 'INS', 'MIL', 'OTH']), ['DIN', 'DIN']] = 0
        # pop.loc[pop['mildep'].isin(['Y']), ['DIN', 'DIN']] = 0
        pop['mig_Din'] = (pop['persons'] * pop['DIN']).round()
        pop['mig_Fin'] = (pop['persons'] * pop['FIN']).round()

        pop = pop[['mig_Din', 'mig_Fin']]

        pop.to_hdf('temp/data.h5', 'mig_in', format='table', mode='a')


class NonMigratingPopulation(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return {'migration_rates': MigrationPopulationOut(self.year)
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        out_pop = pd.read_hdf('temp/data.h5', 'mig_out')
        pop = pd.read_hdf('temp/data.h5', 'pop')
        pop = pop.join(out_pop)
        pop.loc[pop['type'].isin(['COL', 'INS', 'MIL', 'OTH']), ['mig_Dout', 'mig_Fout']] = 0
        pop.loc[pop['mildep'].isin(['Y']), ['mig_Dout', 'mig_Fout']] = 0
        pop['non_mig_pop'] = (pop['persons'] - pop['mig_Dout'] - pop['mig_Dout']).round()
        pop.to_hdf('temp/data.h5', 'non_mig_pop', format='table', mode='a')


class DeadPopulation(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return {'non_mig_pop': NonMigratingPopulation(self.year),
                'death_rates': DeathRates(self.year)
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        death_rates = pd.read_hdf('temp/data.h5', 'death_rates')
        death_rates = death_rates[(death_rates['yr'] == self.year)]
        pop = pd.read_hdf('temp/data.h5', 'non_mig_pop')
        pop = pop.join(death_rates)
        pop = pop[(pop['type'] == 'HP') & (pop['mildep'] == 'N')]
        pop['deaths'] = (pop['non_mig_pop'] * pop['death_rate']).round()
        # do we apply death rates to mil pop?
        pop = pop[['deaths']]
        pop.to_hdf('temp/data.h5', 'dead_pop', format='table', mode='a')


class NonMigratingSurvivedPop(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return {'non_mig_pop': NonMigratingPopulation(self.year),
                'dead_pop': DeadPopulation(self.year)
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        deaths = pd.read_hdf('temp/data.h5', 'dead_pop')
        non_mig_pop = pd.read_hdf('temp/data.h5', 'non_mig_pop')
        non_mig_pop = non_mig_pop.join(deaths, how='left')
        # sum newborn population across cohorts
        non_mig_pop.loc[non_mig_pop['type'].isin(['COL', 'INS', 'MIL', 'OTH']), ['deaths']] = 0
        non_mig_pop.loc[non_mig_pop['mildep'].isin(['Y']), ['deaths']] = 0
        non_mig_pop['non_mig_survived_pop'] = (non_mig_pop['non_mig_pop'] - non_mig_pop['deaths']).round()

        non_mig_pop.to_hdf('temp/data.h5', 'non_mig_survived_pop', format='table', mode='a')


class NewBornPopulation(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return {'population': NonMigratingSurvivedPop(self.year),
                'birth_rates': BirthRates(self.year)
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        birth_rates = pd.read_hdf('temp/data.h5', 'birth_rates')
        pop = pd.read_hdf('temp/data.h5', 'non_mig_survived_pop')
        pop = pop[(pop['type'] == 'HP') & (pop['mildep'] == 'N')]
        birth_rates = compute.rates_for_yr(pop, birth_rates, self.year)
        birth_rates = birth_rates[(birth_rates['yr'] == self.year)]
        births_per_cohort = compute.births_all(birth_rates, 1, self.year)

        # sum newborn population across cohorts
        newborn = compute.births_sum(births_per_cohort, 1, self.year)
        newborn.to_hdf('temp/data.h5', 'new_born', format='table', mode='a')


class AgedPop(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return {'non_mig_survived_pop': NonMigratingSurvivedPop(self.year)
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        non_mig_survived_pop = pd.read_hdf('temp/data.h5', 'non_mig_survived_pop')
        non_mig_survived_pop['increment'] = 1
        # sum newborn population across cohorts
        non_mig_survived_pop = non_mig_survived_pop.reset_index(level=['age', 'race_ethn', 'sex'])

        non_mig_survived_pop.loc[non_mig_survived_pop['type'].isin(['COL', 'INS', 'MIL', 'OTH']), ['increment']] = 0
        non_mig_survived_pop.loc[non_mig_survived_pop['mildep'].isin(['Y']), ['increment']] = 0

        temp = non_mig_survived_pop[(non_mig_survived_pop['increment'] == 1) & (non_mig_survived_pop['age'] == 0)]
        temp['non_mig_survived_pop'] = 0
        non_mig_survived_pop['age'] = non_mig_survived_pop['age'] + non_mig_survived_pop['increment']
        non_mig_survived_pop = non_mig_survived_pop.append(temp)

        non_mig_survived_pop = non_mig_survived_pop.set_index(['age', 'race_ethn', 'sex'])

        non_mig_survived_pop.to_hdf('temp/data.h5', 'aged_pop', format='table', mode='a')


class NewPopulation(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return {'new_born': NewBornPopulation(self.year),
                'in_mig_pop': MigrationPopulationIn(self.year)
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        new_born = pd.read_hdf('temp/data.h5', 'new_born')
        mig_in = pd.read_hdf('temp/data.h5', 'mig_in')
        # sum newborn population across cohorts
        new_born['new_born'] = new_born['persons']
        new_pop = mig_in.join(new_born)
        new_pop = new_pop.fillna(0)

        new_pop['new_pop'] = new_pop['mig_Din'] + new_pop['mig_Fin'] + new_pop['new_born']
        new_pop = new_pop[['new_pop']]

        new_pop.to_hdf('temp/data.h5', 'new_pop', format='table', mode='a')


class FinalPopulation(luigi.Task):
    year = luigi.Parameter()

    @property
    def priority(self):
        return 10000 - self.year

    def requires(self):
        return {'aged_pop': AgedPop(self.year),
                'new_pop': NewPopulation(self.year)
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        aged_pop = pd.read_hdf('temp/data.h5', 'aged_pop')
        new_pop = pd.read_hdf('temp/data.h5', 'new_pop')

        pop = aged_pop.join(new_pop)
        pop.loc[pop['type'].isin(['COL', 'INS', 'MIL', 'OTH']), ['new_pop']] = 0
        pop.loc[pop['mildep'].isin(['Y']), ['new_pop']] = 0
        pop['persons'] = pop['non_mig_survived_pop'] + pop['new_pop']

        pop = pop[['type', 'mildep', 'persons', 'households']]
        pop.to_hdf('temp/data.h5', 'pop', format='table', mode='a')


class Iter(luigi.contrib.hadoop.JobTask):

    def requires(self):
        return [FinalPopulation(y) for y in range(2015, 2017)]

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        print 'complete'

if __name__ == '__main__':

    shutil.rmtree('temp')
    os.makedirs('temp')
    luigi.run(main_task_cls=Iter)
