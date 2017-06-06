import luigi
import inspect, os
import pandas as pd
import time
from db import extract
from db import log
from db import sql
from forecast import compute
from forecast import util
import shutil
import luigi.contrib.hadoop
from pathlib import Path
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import defm_luigi as dl


class IncPopulation(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):

        my_file = Path('temp/data.h5')
        if my_file.is_file():
            print'File exists'
        else:
            db_run_id = log.new_run(name='inc_run_log')
            run_id = pd.Series([db_run_id])
            run_id.to_hdf('temp/data.h5', 'run_id',  mode='a')
            engine = create_engine(get_connection_string("model_config.yml", 'output_database'))

            rate_versions = util.yaml_to_dict('model_config.yml', 'rate_versions')
            tables = util.yaml_to_dict('model_config.yml', 'db_tables')
            in_query = getattr(sql, 'inc_pop') % (tables['inc_pop_table'], rate_versions['inc_pop'])

            pop = pd.read_sql(in_query, engine, index_col=['age', 'race_ethn', 'sex'])

            pop.to_hdf('temp/data.h5', 'pop', mode='a')


class IncomeTypeRates(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return IncPopulation(self.year)

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        # inc_type_rates = extract.create_df('', '')
        # inc_type_rates.to_hdf('temp/data.h5', 'inc_type_rates', mode='a')
        print 'IncomeTypeRates'


class AvgIncomeType(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        # avg_inc_type = extract.create_df('', '')
        # avg_inc_type.to_hdf('temp/data.h5', 'avg_inc_type', mode='a')
        print 'AvgIncomeType'


class NonSelfEmployedPop(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return {'pop': IncPopulation(self.year),
                'inc_type_rates': IncomeTypeRates(self.year)}

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):

        pop = pd.read_hdf('temp/data.h5', 'pop')
        # inc_type_rates = pd.read_hdf('temp/data.h5', 'inc_type_rates')
        # pop = pop.join(inc_type_rates)
        print 'NonSelfEmployedPop'


class NonSelfEmployedIncome(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return {'non_self_employed_pop': NonSelfEmployedPop(self.year),
                'avg_income_type': AvgIncomeType(self.year)}

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        print 'NonSelfEmployedIncome'


class SelfEmployedIncome(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return NonSelfEmployedPop(self.year)

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        print 'SelfEmployedIncome'


class TotalIncome(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return {'non_self_employed_inc': NonSelfEmployedIncome(self.year),
                'self_employed_inc': SelfEmployedIncome(self.year)}

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        print 'TotalIncome'


class IncIter(luigi.contrib.hadoop.JobTask):

    def requires(self):
        return [TotalIncome(y) for y in range(2015, 2016)]

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        print 'complete'

if __name__ == '__main__':
    shutil.rmtree('temp')
    os.makedirs('temp')
    luigi.run(main_task_cls=IncIter)
