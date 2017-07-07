import luigi
import os
import pandas as pd
from db import extract
from db import sql
from forecast import util
import shutil
import luigi.contrib.hadoop
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
from pysandag import database
from db import log


class IncPopulation(luigi.Task):

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):

            engine = create_engine(get_connection_string("model_config.yml", 'output_database'))
            db_connection_string = database.get_connection_string('model_config.yml', 'in_db')
            sql_in_engine = create_engine(db_connection_string)

            in_query = getattr(sql, 'max_run_id')
            db_run_id = pd.read_sql(in_query, engine, index_col=None)
            # db_run_id = log.new_run(name='inc_run_log', run_id=db_run_id['max'].iloc[0])

            run_id = pd.Series([db_run_id['max'].iloc[0]])
            run_id.to_hdf('temp/data.h5', 'run_id',  mode='a')

            rate_versions = util.yaml_to_dict('model_config.yml', 'rate_versions')
            tables = util.yaml_to_dict('model_config.yml', 'db_tables')

            in_query = getattr(sql, 'inc_pop') % (tables['inc_pop_table'], run_id[0])
            in_query2 = getattr(sql, 'inc_mil_hh_pop') % (tables['population_table'], rate_versions['population'])

            pop = pd.read_sql(in_query, engine, index_col=['age', 'race_ethn', 'sex', 'mildep'])
            pop_mil = pd.read_sql(in_query2, sql_in_engine, index_col=['age', 'race_ethn', 'sex', 'mildep'])

            pop = pop.join(pop_mil)
            pop['persons'] = (pop['persons'] - pop['mil_mildep'])
            pop = pop.reset_index(drop=False)
            pop = pop[pop['age'] >= 18]

            pop['age_cat'] = ''

            pop.loc[pop['age'].isin(list(range(18, 25))), ['age_cat']] = '18_24'
            pop.loc[pop['age'].isin(list(range(25, 35))), ['age_cat']] = '25_34'
            pop.loc[pop['age'].isin(list(range(35, 45))), ['age_cat']] = '35_44'
            pop.loc[pop['age'].isin(list(range(45, 55))), ['age_cat']] = '45_54'
            pop.loc[pop['age'].isin(list(range(55, 60))), ['age_cat']] = '55_59'
            pop.loc[pop['age'].isin(list(range(60, 65))), ['age_cat']] = '60_64'
            pop.loc[pop['age'].isin(list(range(65, 75))), ['age_cat']] = '65_74'
            pop.loc[pop['age'].isin(list(range(75, 103))), ['age_cat']] = '75_99'

            pop = pd.DataFrame(pop['persons'].groupby([pop['yr'], pop['age_cat']]).sum())

            pop.to_hdf('temp/data.h5', 'pop', mode='a')


class IncomeByType(luigi.Task):

    @property
    def priority(self):
        return 3

    def requires(self):
        return IncPopulation()

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        engine = create_engine(get_connection_string("model_config.yml", 'output_database'))

        pop = pd.read_hdf('temp/data.h5', 'pop')
        inc_type_rates = extract.create_df('inc_shares', 'inc_shares_table', index=['yr', 'age_cat'])

        inc_type_rates = inc_type_rates.join(pop)
        inc_type_rates['totals'] = (inc_type_rates['income'] * inc_type_rates['persons'] * inc_type_rates['share'])
        inc_type_rates = inc_type_rates.reset_index(drop=False)

        inc_type_rates['multiplier'] = 0

        aigr_table = extract.create_df('aigr', 'aigr_table', index=None)

        inc_type_rates.loc[(inc_type_rates['yr'] > 2014) & (inc_type_rates['income_type'] != 'semp'),
                           ['multiplier']] = (aigr_table.aigr[0] * (inc_type_rates['yr'] - 2014))

        # pow(1.01, mil_wages.index.get_level_values('yr') - 2014)

        inc_type_rates['totals'] = (inc_type_rates['totals'] + inc_type_rates['totals'] * inc_type_rates['multiplier'])

        inc_type_rates = pd.DataFrame(inc_type_rates['totals'].groupby([inc_type_rates['yr'], inc_type_rates['income_type']]).sum())

        inc_type_rates = inc_type_rates.reset_index(drop=False)
        inc_type_rates = pd.pivot_table(inc_type_rates, values='totals',
                            index=['yr'],
                            columns=['income_type'])
        # inc_type_rates.to_hdf('temp/data.h5', 'inc_type_rates', mode='a')

        inc_type_rates.rename(columns={'intp': 'Interest'}, inplace=True)
        inc_type_rates.rename(columns={'oip': 'Other'}, inplace=True)
        inc_type_rates.rename(columns={'pap': 'Public_Assistance'}, inplace=True)
        inc_type_rates.rename(columns={'retp': 'Retirement'}, inplace=True)
        inc_type_rates.rename(columns={'ssip': 'Supplemental_Social_Security'}, inplace=True)
        inc_type_rates.rename(columns={'ssp': 'Social_Security'}, inplace=True)
        inc_type_rates.rename(columns={'semp': 'Selfemp_Income'}, inplace=True)

        inc_type_rates = inc_type_rates[['Interest', 'Other', 'Public_Assistance', 'Retirement',
                                         'Supplemental_Social_Security', 'Social_Security', 'Selfemp_Income']]

        inc_type_rates.to_hdf('temp/data.h5', 'ue_income')
        run_table = pd.read_hdf('temp/data.h5', 'run_id')
        run_id = run_table[0]

        inc_type_rates['run_id'] = run_id
        inc_type_rates.to_sql(name='non_wage_income', con=engine, schema='defm', if_exists='append', index=True)


if __name__ == '__main__':
    os.makedirs('temp')
    luigi.run(main_task_cls=IncomeByType)
    shutil.rmtree('temp')
