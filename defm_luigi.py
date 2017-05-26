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
        print mig_out.head()

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


class MigrationPopulationNet(luigi.Task):
    def requires(self):
        return {'mig_out': MigrationPopulationOut(),
                'mig_in': MigrationPopulationIn()
                }

    def run(self):
        mig_in = pd.read_hdf('temp/data.h5', 'mig_in')
        mig_out = pd.read_hdf('temp/data.h5', 'mig_out')


if __name__ == '__main__':

    shutil.rmtree('temp')
    os.makedirs('temp')

    luigi.run(main_task_cls=MigrationPopulationNet)