import luigi
import os
import pandas as pd
from db import extract
from db import log
from forecast import compute
import shutil
import luigi.contrib.hadoop
from pathlib import Path
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pydefm.compute as cp
import numpy as np
from forecast import util
from pydefm import utils


class Population(luigi.Task):
    year = luigi.Parameter()
    dem_id = luigi.Parameter()
    econ_id = luigi.Parameter()

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        # create file only first year, exists in subsequent years
        my_file = Path('temp/data.h5')
        if my_file.is_file():
            print'File exists'
        else: # only first year
            db_run_id = log.new_run(dem_id=self.dem_id, econ_id=self.econ_id)
            run_id = pd.Series([db_run_id])
            run_id.to_hdf('temp/data.h5', 'run_id',  mode='a')

            dem_sim_rates = extract.create_df('dem_sim_rates', 'dem_sim_rates_table',
                                              rate_id=self.dem_id, index=None)

            dem_sim_rates.to_hdf('temp/data.h5', 'dem_sim_rates',  mode='a')

            pop = extract.create_df('population', 'population_table', rate_id=dem_sim_rates.base_population_id[0])
            pop.to_hdf('temp/data.h5', 'pop', mode='a')

            # Create function here and test
            # to get ratio of INS and OTH to HHP to keep constant
            rates = cp.compute_ins_oth_rate(pop)

            rates.to_hdf('temp/data.h5', 'ins_oth_rates', mode='a')

            engine = create_engine(get_connection_string("model_config.yml", 'output_database'))
            population_summary = []
            population_summary.append({'Year': self.year - 1,
                                       'Run_id': run_id[0],
                                       'Population': pop['persons'].sum(),
                                       'mig_out': 0,
                                       'mig_in': 0,
                                       'deaths_hhp_non_mil': 0,
                                       'new_born': 0})

            summary_df = pd.DataFrame(population_summary)
            summary_df.to_sql(name='population_summary', con=engine, schema='defm', if_exists='append', index=False)
            pop['yr'] = self.year - 1
            pop['run_id'] = db_run_id
            pop.to_sql(name='population', con=engine, schema='defm', if_exists='append', index=True)


class InMigrationRates(luigi.Task):
    year = luigi.Parameter()
    dem_id = luigi.Parameter()
    econ_id = luigi.Parameter()

    def requires(self):
        return Population(year=self.year, dem_id=self.dem_id, econ_id=self.econ_id)

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        dem_sim_rates = pd.read_hdf('temp/data.h5', 'dem_sim_rates')
        mig_rates = extract.create_df('migration', 'migration_rate_table', rate_id=dem_sim_rates.migration_rate_id[0])
        mig_rates = mig_rates[['yr', 'DIN', 'FIN']]
        mig_rates.to_hdf('temp/data.h5', 'in_mig_rates', mode='a')


class OutMigrationRates(luigi.Task):
    year = luigi.Parameter()
    dem_id = luigi.Parameter()
    econ_id = luigi.Parameter()

    def requires(self):
        return Population(year=self.year, dem_id=self.dem_id, econ_id=self.econ_id)

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        dem_sim_rates = pd.read_hdf('temp/data.h5', 'dem_sim_rates')
        mig_rates = extract.create_df('migration', 'migration_rate_table', rate_id=dem_sim_rates.migration_rate_id[0])
        mig_rates = mig_rates[['yr', 'DOUT', 'FOUT']]
        mig_rates.to_hdf('temp/data.h5', 'out_mig_rates', mode='a')


class DeathRates(luigi.Task):
    year = luigi.Parameter()
    dem_id = luigi.Parameter()
    econ_id = luigi.Parameter()

    def requires(self):
        return Population(year=self.year, dem_id=self.dem_id, econ_id=self.econ_id)

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        dem_sim_rates = pd.read_hdf('temp/data.h5', 'dem_sim_rates')
        death_rates = extract.create_df('death', 'death_rate_table', rate_id=dem_sim_rates.death_rate_id[0])
        death_rates.to_hdf('temp/data.h5', 'death_rates', mode='a')


class BirthRates(luigi.Task):
    year = luigi.Parameter()
    dem_id = luigi.Parameter()
    econ_id = luigi.Parameter()

    def requires(self):
        return Population(year=self.year, dem_id=self.dem_id, econ_id=self.econ_id)

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        dem_sim_rates = pd.read_hdf('temp/data.h5', 'dem_sim_rates')
        birth_rates = extract.create_df('birth', 'birth_rate_table', rate_id=dem_sim_rates.birth_rate_id[0])
        birth_rates.to_hdf('temp/data.h5', 'birth_rates', mode='a')


class MigrationPopulationOut(luigi.Task):
    year = luigi.Parameter()
    dem_id = luigi.Parameter()
    econ_id = luigi.Parameter()

    def requires(self):
        return {
                'migration_rates': OutMigrationRates(year=self.year, dem_id=self.dem_id, econ_id=self.econ_id)
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        mig_rates = pd.read_hdf('temp/data.h5', 'out_mig_rates')
        pop = pd.read_hdf('temp/data.h5', 'pop')
        pop = utils.rates_for_yr(pop, mig_rates, self.year)

        pop = cp.out_migrating_population(pop)

        pop.to_hdf('temp/data.h5', 'mig_out', mode='a')


class MigrationPopulationIn(luigi.Task):
    year = luigi.Parameter()
    dem_id = luigi.Parameter()
    econ_id = luigi.Parameter()

    def requires(self):
        return {
                'migration_rates': InMigrationRates(year=self.year, dem_id=self.dem_id, econ_id=self.econ_id)
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        mig_rates = pd.read_hdf('temp/data.h5', 'in_mig_rates')
        pop = pd.read_hdf('temp/data.h5', 'pop')
        pop = utils.rates_for_yr(pop, mig_rates, self.year)

        pop = cp.in_migrating_population(pop)

        pop.to_hdf('temp/data.h5', 'mig_in', mode='a')


class NonMigratingPopulation(luigi.Task):
    year = luigi.Parameter()
    dem_id = luigi.Parameter()
    econ_id = luigi.Parameter()

    def requires(self):
        return {'migration_pop': MigrationPopulationOut(year=self.year, dem_id=self.dem_id, econ_id=self.econ_id)
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        out_pop = pd.read_hdf('temp/data.h5', 'mig_out')
        pop = pd.read_hdf('temp/data.h5', 'pop')

        pop = cp.non_migrating_population(pop, out_pop)
        pop.to_hdf('temp/data.h5', 'non_mig_pop', mode='a')


class DeadPopulation(luigi.Task):
    year = luigi.Parameter()
    dem_id = luigi.Parameter()
    econ_id = luigi.Parameter()

    def requires(self):
        return {'non_mig_pop': NonMigratingPopulation(year=self.year, dem_id=self.dem_id, econ_id=self.econ_id),
                'death_rates': DeathRates(year=self.year, dem_id=self.dem_id, econ_id=self.econ_id)
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        death_rates = pd.read_hdf('temp/data.h5', 'death_rates')
        # move to function call
        death_rates = death_rates[(death_rates['yr'] == self.year)]
        pop = pd.read_hdf('temp/data.h5', 'non_mig_pop')
        pop = pop.join(death_rates, how='left')

        # do we apply death rates to mil pop?
        pop = cp.dead_population(pop)
        pop.to_hdf('temp/data.h5', 'dead_pop', mode='a')


class NonMigratingSurvivedPop(luigi.Task):
    year = luigi.Parameter()
    dem_id = luigi.Parameter()
    econ_id = luigi.Parameter()

    def requires(self):
        return {'non_mig_pop': NonMigratingPopulation(year=self.year, dem_id=self.dem_id, econ_id=self.econ_id),
                'dead_pop': DeadPopulation(year=self.year, dem_id=self.dem_id, econ_id=self.econ_id)
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        deaths = pd.read_hdf('temp/data.h5', 'dead_pop')
        non_mig_pop = pd.read_hdf('temp/data.h5', 'non_mig_pop')
        non_mig_pop = cp.non_migrating_survived_pop(non_mig_pop, deaths)
        non_mig_pop.to_hdf('temp/data.h5', 'non_mig_survived_pop', mode='a')


class NewBornPopulation(luigi.Task):
    year = luigi.Parameter()
    dem_id = luigi.Parameter()
    econ_id = luigi.Parameter()

    def requires(self):
        return {'non_mig_population': NonMigratingPopulation(year=self.year, dem_id=self.dem_id, econ_id=self.econ_id),
                'birth_rates': BirthRates(year=self.year, dem_id=self.dem_id, econ_id=self.econ_id)
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        birth_rates = pd.read_hdf('temp/data.h5', 'birth_rates')
        pop = pd.read_hdf('temp/data.h5', 'non_mig_pop')
        pop = pop[(pop['type'] == 'HHP') & (pop['mildep'] == 'N')]
        birth_rates = utils.rates_for_yr(pop, birth_rates, self.year)
        birth_rates = birth_rates[(birth_rates['yr'] == self.year)]

        rate_versions = util.yaml_to_dict('model_config.yml', 'rate_versions')

        random_numbers = extract.create_df('random_numbers', 'random_numbers_table', rate_id=rate_versions['random_numbers'])

        random_numbers = random_numbers[(random_numbers['yr'] == self.year)]
        random_numbers = random_numbers[['random_number']]
        births_per_cohort = cp.births_all(birth_rates, pop_col='non_mig_pop', rand_df=random_numbers)

        death_rates = pd.read_hdf('temp/data.h5', 'death_rates')
        death_rates = death_rates[(death_rates['yr'] == self.year)]
        # sum newborn population across cohorts
        newborn = cp.births_sum(births_per_cohort, self.year)

        newborn = newborn.join(death_rates)
        newborn['new_deaths'] = (newborn['new_born'] * newborn['death_rate']).round()
        newborn['new_born_survived'] = (newborn['new_born'] - newborn['new_deaths']).round()

        dead_pop = pd.read_hdf('temp/data.h5', 'dead_pop')
        dead_pop = dead_pop.join(newborn['new_deaths'])

        dead_pop = dead_pop.fillna(0)
        dead_pop['deaths_hhp_non_mil'] = (dead_pop['deaths_hhp_non_mil'] + dead_pop['new_deaths']).round()

        dead_pop = dead_pop.drop(['new_deaths'], 1)

        dead_pop.to_hdf('temp/data.h5', 'dead_pop', mode='a')

        newborn = newborn.drop(['new_deaths', 'death_rate'], 1)
        newborn.to_hdf('temp/data.h5', 'new_born', mode='a')


class AgedPop(luigi.Task):
    year = luigi.Parameter()
    dem_id = luigi.Parameter()
    econ_id = luigi.Parameter()

    def requires(self):
        return {'non_mig_survived_pop': NonMigratingSurvivedPop(year=self.year, dem_id=self.dem_id, econ_id=self.econ_id)
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        non_mig_survived_pop = pd.read_hdf('temp/data.h5', 'non_mig_survived_pop')
        non_mig_survived_pop = cp.aged_pop(non_mig_survived_pop)
        non_mig_survived_pop.to_hdf('temp/data.h5', 'aged_pop', mode='a')


class NewPopulation(luigi.Task):
    year = luigi.Parameter()
    dem_id = luigi.Parameter()
    econ_id = luigi.Parameter()

    def requires(self):
        return {'new_born': NewBornPopulation(year=self.year, dem_id=self.dem_id, econ_id=self.econ_id),
                'in_mig_pop': MigrationPopulationIn(year=self.year, dem_id=self.dem_id, econ_id=self.econ_id)
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        new_born = pd.read_hdf('temp/data.h5', 'new_born')
        mig_in = pd.read_hdf('temp/data.h5', 'mig_in')
        # sum newborn population across cohorts
        # new_born['new_born'] = new_born['persons']
        new_pop = mig_in.join(new_born)
        new_pop = new_pop.fillna(0)

        new_pop = cp.new_population(new_pop)

        new_pop.to_hdf('temp/data.h5', 'new_pop', mode='a')


class FinalPopulation(luigi.Task):
    year = luigi.Parameter()
    dem_id = luigi.Parameter()
    econ_id = luigi.Parameter()

    def requires(self):
        return {'aged_pop': AgedPop(year=self.year, dem_id=self.dem_id, econ_id=self.econ_id),
                'new_pop': NewPopulation(year=self.year, dem_id=self.dem_id, econ_id=self.econ_id)
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        aged_pop = pd.read_hdf('temp/data.h5', 'aged_pop')
        new_pop = pd.read_hdf('temp/data.h5', 'new_pop')
        rates = pd.read_hdf('temp/data.h5', 'ins_oth_rates')

        pop = aged_pop.join(new_pop)
        pop = pop.fillna(0)

        pop.loc[pop['type'].isin(['COL', 'INS', 'MIL', 'OTH']), ['new_pop']] = 0
        pop.loc[pop['mildep'].isin(['Y']), ['new_pop']] = 0

        pop = pop.reset_index(drop=False)

        rates = rates.reset_index(drop=False)

        pop = pop.set_index(['age', 'race_ethn', 'sex', 'mildep', 'type'])
        rates = rates.set_index(['age', 'race_ethn', 'sex', 'mildep', 'type'])

        pop = pop.join(rates)
        pop = pop.reset_index(drop=False)
        pop = pop.set_index(['age', 'race_ethn', 'sex'])

        pop = cp.final_population(pop)

        dem_sim_rates = pd.read_hdf('temp/data.h5', 'dem_sim_rates')

        householder = extract.create_df('householder', 'householder_table', rate_id=dem_sim_rates.householder_rate_id[0])
        householder = householder[(householder['yr'] == self.year)]
        householder = householder.drop(['yr'], 1)

        pop = pop.join(householder)
        pop = pop.fillna(0)
        pop['households'] = (pop['persons'] * pop['householder_rate']).round()
        pop = pop.drop(['householder_rate'], 1)

        pop.to_hdf('temp/data.h5', 'pop', mode='a')


class ExportTables(luigi.Task):
    year = luigi.Parameter()
    dem_id = luigi.Parameter()
    econ_id = luigi.Parameter()

    @property
    def priority(self):
        return 10000 - self.year

    def requires(self):
        return FinalPopulation(year=self.year, dem_id=self.dem_id, econ_id=self.econ_id)

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        engine = create_engine(get_connection_string("model_config.yml", 'output_database'))
        run_table = pd.read_hdf('temp/data.h5', 'run_id')
        run_id = run_table[0]
        population_summary = []  # initialize list for population by year
        pop = pd.read_hdf('temp/data.h5', 'pop')
        mig_out = pd.read_hdf('temp/data.h5', 'mig_out')
        mig_in = pd.read_hdf('temp/data.h5', 'mig_in')
        dead_pop = pd.read_hdf('temp/data.h5', 'dead_pop')
        new_born = pd.read_hdf('temp/data.h5', 'new_born')
        population_summary.append({'Year': self.year,
                                   'Run_id': run_id,
                                   'Population': pop['persons'].sum(),
                                   'mig_out': mig_out['mig_Dout'].sum() + mig_out['mig_Fout'].sum(),
                                   'mig_in': mig_in['mig_Din'].sum() + mig_in['mig_Fin'].sum(),
                                   'deaths_hhp_non_mil': dead_pop['deaths_hhp_non_mil'].sum(),
                                   'new_born': new_born['new_born'].sum()})

        for table in [pop, mig_out, mig_in, dead_pop, new_born]:
            # df = pd.read_hdf('temp/data.h5', table)
            table = table.assign(yr=self.year)
            table = table.assign(run_id=run_id)

        pop.to_sql(name='population', con=engine, schema='defm', if_exists='append', index=True)
        mig_out.to_sql(name='mig_out', con=engine, schema='defm', if_exists='append', index=True)
        mig_in.to_sql(name='mig_in', con=engine, schema='defm', if_exists='append', index=True)
        dead_pop.to_sql(name='dead_pop', con=engine, schema='defm', if_exists='append', index=True)
        new_born.to_sql(name='new_born', con=engine, schema='defm', if_exists='append', index=True)

        summary_df = pd.DataFrame(population_summary)
        summary_df.to_sql(name='population_summary', con=engine, schema='defm', if_exists='append', index=False)


class Iter(luigi.contrib.hadoop.JobTask):
    start = luigi.Parameter()
    end = luigi.Parameter()
    dem = luigi.Parameter()
    econ = luigi.Parameter()

    def requires(self):
        return [ExportTables(year=y, dem_id=int(self.dem), econ_id=int(self.econ)) for y in range(int(self.start), int(self.end) + 1)]

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        print 'complete'


if __name__ == '__main__':

    os.makedirs('temp')

    luigi.run(main_task_cls=Iter, cmdline_args=['--start=2011', '--end=2012', '--dem=1005'])
    shutil.rmtree('temp')
