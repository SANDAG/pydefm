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


class MigrationPopulationOut(luigi.Task):

    def requires(self):
        return {'population': Population(),
                'migration_rates': MigrationRates()
                }

    def run(self):
        mig_rates = pd.read_hdf('temp/data.h5', 'mig_rates')
        pop = pd.read_hdf('temp/data.h5', 'pop')
        # pop = pop.set_index(['age', 'race_ethn', 'sex'])
        # mig_rates = mig_rates.set_index(['age', 'race_ethn', 'sex'])
        yr_mig = compute.rates_for_yr(pop, mig_rates, 2015)
        mig_out = compute.net_mig(yr_mig, 1, 2015)
        mig_out = mig_out[['mig_Dout','mig_Fout']]
        mig_out.to_hdf('temp/data.h5', 'mig_out', format='table', mode='a')


if __name__ == '__main__':

    shutil.rmtree('temp')
    os.makedirs('temp')

    luigi.run(main_task_cls=MigrationPopulationOut)