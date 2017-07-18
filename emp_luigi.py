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


class EmpPopulation(luigi.Task):
    econ_id = luigi.Parameter()
    dem_id = luigi.Parameter()

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
            # db_run_id = log.new_run(name='emp_run_log', run_id=db_run_id['max'].iloc[0])

            run_id = pd.Series([db_run_id['max'].iloc[0]])
            run_id.to_hdf('temp/data.h5', 'run_id',  mode='a')

            tables = util.yaml_to_dict('model_config.yml', 'db_tables')

            dem_sim_rates = extract.create_df('dem_sim_rates', 'dem_sim_rates_table',
                                              rate_id=self.dem_id, index=None)
            dem_sim_rates.to_hdf('temp/data.h5', 'dem_sim_rates', mode='a')

            econ_sim_rates = extract.create_df('econ_sim_rates', 'econ_sim_rates_table',
                                               rate_id=self.econ_id, index=None)
            econ_sim_rates.to_hdf('temp/data.h5', 'econ_sim_rates', mode='a')

            in_query = getattr(sql, 'inc_pop') % (tables['inc_pop_table'], run_id[0])
            in_query2 = getattr(sql, 'inc_mil_hh_pop') % (tables['population_table'], dem_sim_rates.base_population_id[0])

            pop = pd.read_sql(in_query, engine, index_col=['age', 'race_ethn', 'sex', 'mildep'])
            pop_mil = pd.read_sql(in_query2, sql_in_engine, index_col=['age', 'race_ethn', 'sex', 'mildep'])

            pop = pop.join(pop_mil)
            pop['persons'] = (pop['persons'] - pop['mil_mildep'])
            pop = pop.reset_index(drop=False)

            pop['age_cat'] = ''
            pop.loc[pop['age'].isin(list(range(0, 5))), ['age_cat']] = '00_04'
            pop.loc[pop['age'].isin(list(range(5, 10))), ['age_cat']] = '05_09'
            pop.loc[pop['age'].isin(list(range(10, 15))), ['age_cat']] = '10_14'
            pop.loc[pop['age'].isin(list(range(15, 18))), ['age_cat']] = '15_17'
            pop.loc[pop['age'].isin(list(range(18, 20))), ['age_cat']] = '18_19'
            pop.loc[pop['age'].isin(list(range(20, 21))), ['age_cat']] = '20_20'
            pop.loc[pop['age'].isin(list(range(21, 22))), ['age_cat']] = '21_21'
            pop.loc[pop['age'].isin(list(range(22, 25))), ['age_cat']] = '22_24'
            pop.loc[pop['age'].isin(list(range(25, 30))), ['age_cat']] = '25_29'
            pop.loc[pop['age'].isin(list(range(30, 35))), ['age_cat']] = '30_34'
            pop.loc[pop['age'].isin(list(range(35, 40))), ['age_cat']] = '35_39'
            pop.loc[pop['age'].isin(list(range(40, 45))), ['age_cat']] = '40_44'
            pop.loc[pop['age'].isin(list(range(45, 50))), ['age_cat']] = '45_49'
            pop.loc[pop['age'].isin(list(range(50, 55))), ['age_cat']] = '50_54'
            pop.loc[pop['age'].isin(list(range(55, 60))), ['age_cat']] = '55_59'
            pop.loc[pop['age'].isin(list(range(60, 62))), ['age_cat']] = '60_61'
            pop.loc[pop['age'].isin(list(range(62, 65))), ['age_cat']] = '62_64'
            pop.loc[pop['age'].isin(list(range(65, 67))), ['age_cat']] = '65_66'
            pop.loc[pop['age'].isin(list(range(67, 70))), ['age_cat']] = '67_69'
            pop.loc[pop['age'].isin(list(range(70, 75))), ['age_cat']] = '70_74'
            pop.loc[pop['age'].isin(list(range(75, 80))), ['age_cat']] = '75_79'
            pop.loc[pop['age'].isin(list(range(80, 85))), ['age_cat']] = '80_84'
            pop.loc[pop['age'].isin(list(range(85, 103))), ['age_cat']] = '85_99'

            pop = pd.DataFrame(pop['persons'].groupby([pop['yr'], pop['age_cat'], pop['sex'], pop['race_ethn']]).sum())
            pop.to_hdf('temp/data.h5', 'pop', mode='a')


class MilPopulation(luigi.Task):
    econ_id = luigi.Parameter()
    dem_id = luigi.Parameter()

    def requires(self):
        return EmpPopulation(econ_id=self.econ_id, dem_id=self.dem_id)

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):

            engine = create_engine(get_connection_string("model_config.yml", 'output_database'))
            db_connection_string = database.get_connection_string('model_config.yml', 'in_db')
            sql_in_engine = create_engine(db_connection_string)

            in_query = getattr(sql, 'max_run_id')
            db_run_id = pd.read_sql(in_query, engine, index_col=None)

            run_id = pd.Series([db_run_id['max'].iloc[0]])
            run_id.to_hdf('temp/data.h5', 'run_id',  mode='a')

            tables = util.yaml_to_dict('model_config.yml', 'db_tables')

            dem_sim_rates = pd.read_hdf('temp/data.h5', 'dem_sim_rates')

            in_query = getattr(sql, 'inc_mil_gc_pop') % (tables['inc_pop_table'], run_id[0])
            in_query2 = getattr(sql, 'inc_mil_hh_pop') % (tables['population_table'], dem_sim_rates.base_population_id[0])

            pop = pd.read_sql(in_query, engine, index_col=['age', 'race_ethn', 'sex'])
            pop_mil = pd.read_sql(in_query2, sql_in_engine, index_col=['age', 'race_ethn', 'sex'])

            pop_mil = pop_mil.loc[pop_mil['mildep'] == 'Y']
            pop = pop.join(pop_mil)

            pop.rename(columns={'persons': 'mil_gc_pop'}, inplace=True)
            pop.rename(columns={'mil_mildep': 'mil_hh_pop'}, inplace=True)

            pop = pop.reset_index(drop=False)

            pop = pd.DataFrame(pop[['mil_gc_pop', 'mil_hh_pop']].groupby([pop['yr']]).sum())
            pop.to_hdf('temp/data.h5', 'mil_pop', mode='a')


class LaborForceParticipationRates(luigi.Task):
    econ_id = luigi.Parameter()
    dem_id = luigi.Parameter()

    def requires(self):
        return EmpPopulation(econ_id=self.econ_id, dem_id=self.dem_id)

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        econ_sim_rates = pd.read_hdf('temp/data.h5', 'econ_sim_rates')
        lfpr = extract.create_df('lfp_rates', 'lfp_rates_table', rate_id=econ_sim_rates.lfpr_id[0], index=['yr', 'age_cat', 'sex', 'race_ethn'])
        lfpr.to_hdf('temp/data.h5', 'lfpr', mode='a')


class LaborForce(luigi.Task):
    econ_id = luigi.Parameter()
    dem_id = luigi.Parameter()

    def requires(self):
        return LaborForceParticipationRates(econ_id=self.econ_id, dem_id=self.dem_id)

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        pop = pd.read_hdf('temp/data.h5', 'pop')
        lfpr = pd.read_hdf('temp/data.h5', 'lfpr')
        labor_force = pop.join(lfpr)
        labor_force['labor_force'] = (labor_force['persons'] * labor_force['lfpr']).round()

        labor_force = labor_force.iloc[~labor_force.index.get_level_values('age_cat').isin(['00_04', '05_09', '10_14'])]
        labor_force.to_hdf('temp/data.h5', 'labor_force', mode='a')


class CohortUrRate(luigi.Task):
    econ_id = luigi.Parameter()
    dem_id = luigi.Parameter()

    def requires(self):
        return LaborForce(econ_id=self.econ_id, dem_id=self.dem_id)

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        econ_sim_rates = pd.read_hdf('temp/data.h5', 'econ_sim_rates')

        cohort_ur = extract.create_df('cohort_ur', 'cohort_ur_table', rate_id=econ_sim_rates.ur1_id[0], index=['yr', 'age_cat', 'sex', 'race_ethn'])
        cohort_ur.to_hdf('temp/data.h5', 'cohort_ur', mode='a')
        yearly_ur = extract.create_df('yearly_ur', 'yearly_ur_table', rate_id=econ_sim_rates.ur2_id[0], index=['yr'])
        yearly_ur.to_hdf('temp/data.h5', 'yearly_ur', mode='a')


class WorkForce(luigi.Task):
    econ_id = luigi.Parameter()
    dem_id = luigi.Parameter()

    def requires(self):
        return CohortUrRate(econ_id=self.econ_id, dem_id=self.dem_id)

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        labor_force = pd.read_hdf('temp/data.h5', 'labor_force')
        cohort_ur = pd.read_hdf('temp/data.h5', 'cohort_ur')
        yearly_ur = pd.read_hdf('temp/data.h5', 'yearly_ur')

        work_force = labor_force.join(cohort_ur)
        work_force['unemployed'] = (work_force['labor_force'] * work_force['ur2']).round()

        computed_ur = work_force.reset_index(drop=False)

        computed_ur = pd.DataFrame(computed_ur[['labor_force', 'unemployed']].groupby([computed_ur['yr']]).sum())
        computed_ur['computed_ur'] = (computed_ur['unemployed'] / computed_ur['labor_force'])
        computed_ur = computed_ur.join(yearly_ur)
        computed_ur['adjustment'] = (computed_ur['ur1'] / computed_ur['computed_ur'])

        work_force = work_force.join(computed_ur['adjustment'])
        work_force['unemployed'] = (work_force['unemployed'] * work_force['adjustment']).round()
        work_force['work_force'] = (work_force['labor_force'] - work_force['unemployed'])
        work_force.to_hdf('temp/data.h5', 'work_force', mode='a')

        # Code to check if after adjustment ur matches target
        '''
        computed_ur = work_force.reset_index(drop=False)

        computed_ur = pd.DataFrame(computed_ur[['labor_force', 'unemployed']].groupby([computed_ur['yr']]).sum())
        computed_ur['computed_ur'] = (computed_ur['unemployed'] / computed_ur['labor_force'])
        print computed_ur
        '''


class LocalWorkForce(luigi.Task):
    econ_id = luigi.Parameter()
    dem_id = luigi.Parameter()

    def requires(self):
        return WorkForce(econ_id=self.econ_id, dem_id=self.dem_id)

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        econ_sim_rates = pd.read_hdf('temp/data.h5', 'econ_sim_rates')

        out_commuting = extract.create_df('out_commuting', 'out_commuting_table', rate_id=econ_sim_rates.oc_id[0], index=['yr'])
        work_force = pd.read_hdf('temp/data.h5', 'work_force')
        work_force = work_force.reset_index(drop=False)
        work_force = pd.DataFrame(work_force[['labor_force', 'unemployed', 'work_force']].groupby([work_force['yr']]).sum())
        work_force = work_force.join(out_commuting)
        work_force['work_force_outside'] = (work_force['work_force'] * work_force['wtlh_lh']).round()
        work_force['work_force_local'] = (work_force['work_force'] - work_force['work_force_outside']).round()
        work_force.to_hdf('temp/data.h5', 'work_force_local', mode='a')


class Jobs(luigi.Task):
    econ_id = luigi.Parameter()
    dem_id = luigi.Parameter()

    def requires(self):
        return LocalWorkForce(econ_id=self.econ_id, dem_id=self.dem_id)

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        econ_sim_rates = pd.read_hdf('temp/data.h5', 'econ_sim_rates')
        local_jobs = extract.create_df('local_jobs', 'local_jobs_table', rate_id=econ_sim_rates.lj_id[0], index=['yr'])
        in_commuting = extract.create_df('in_commuting', 'in_commuting_table',rate_id=econ_sim_rates.ic_id[0], index=['yr'])

        work_force_local = pd.read_hdf('temp/data.h5', 'work_force_local')
        work_force_local = work_force_local.join(local_jobs)
        work_force_local['jobs_local'] = (work_force_local['work_force_local'] * work_force_local['jlw']).round()
        work_force_local = work_force_local.join(in_commuting)
        work_force_local['jobs_total'] = (work_force_local['jobs_local'] * work_force_local['wh_whlh']).round()
        work_force_local['jobs_external'] = (work_force_local['jobs_total'] - work_force_local['jobs_local']).round()

        # pull information from here
        work_force_local.to_hdf('temp/data.h5', 'jobs', mode='a')


class SectoralPay(luigi.Task):
    econ_id = luigi.Parameter()
    dem_id = luigi.Parameter()

    def requires(self):
        return Jobs(econ_id=self.econ_id, dem_id=self.dem_id)

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        engine = create_engine(get_connection_string("model_config.yml", 'output_database'))
        econ_sim_rates = pd.read_hdf('temp/data.h5', 'econ_sim_rates')

        sectoral_share = extract.create_df('sectoral_share', 'sectoral_share_table', rate_id=econ_sim_rates.ss_id[0], index=['yr', 'sandag_sector'])
        sectoral_pay = extract.create_df('sectoral_pay', 'sectoral_pay_table', rate_id=econ_sim_rates.sp_id[0], index=['yr', 'sandag_sector'])

        jobs = pd.read_hdf('temp/data.h5', 'jobs')

        jobs = jobs[['jobs_total']]
        jobs = jobs.join(sectoral_share, how='right')
        jobs['sector_jobs'] = (jobs['jobs_total'] * jobs['share']).round()
        jobs = jobs.drop(['jobs_total'], 1)

        jobs = jobs.join(sectoral_pay)
        jobs['tot_ann_job_pay'] = (jobs['sector_jobs'] * jobs['annual_pay']).round()

        jobs.to_hdf('temp/data.h5', 'sectoral', mode='a')
        run_table = pd.read_hdf('temp/data.h5', 'run_id')
        run_id = run_table[0]
        jobs['run_id'] = run_id

        jobs.to_sql(name='sectors', con=engine, schema='defm', if_exists='append', index=True)


class MilPay(luigi.Task):
    econ_id = luigi.Parameter()
    dem_id = luigi.Parameter()

    def requires(self):
        return MilPopulation(econ_id=self.econ_id, dem_id=self.dem_id)

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        econ_sim_rates = pd.read_hdf('temp/data.h5', 'econ_sim_rates')

        mil_pay = extract.create_df('mil_pay', 'mil_pay_table', rate_id=econ_sim_rates.mp_id[0], index=['yr'])
        mil_pay.to_hdf('temp/data.h5', 'mil_pay', mode='a')


class HouseHoldWages(luigi.Task):
    econ_id = luigi.Parameter()
    dem_id = luigi.Parameter()

    def requires(self):
        return SectoralPay(econ_id=self.econ_id, dem_id=self.dem_id)

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):

        jobs = pd.read_hdf('temp/data.h5', 'jobs')
        sectoral_wages = pd.read_hdf('temp/data.h5', 'sectoral')
        sectoral_wages = sectoral_wages.reset_index(drop=False)
        sectoral_wages = pd.DataFrame(sectoral_wages[['sector_jobs', 'tot_ann_job_pay']].groupby([sectoral_wages['yr']]).sum())
        sectoral_wages['avg_wage'] = (sectoral_wages['tot_ann_job_pay'] / sectoral_wages['sector_jobs'] )
        jobs = jobs.join(sectoral_wages['avg_wage'])
        jobs['jobs_total_wages'] = (jobs['jobs_total'] * jobs['avg_wage']).round()
        jobs['jobs_local_wages'] = (jobs['jobs_local'] * jobs['avg_wage']).round()
        jobs['jobs_external_wages'] = (jobs['jobs_external'] * jobs['avg_wage']).round()
        jobs['wf_outside_wages'] = (jobs['work_force_outside'] * jobs['avg_wage']).round()
        jobs['avg_wage'] = (jobs['avg_wage']).round()

        jobs.to_hdf('temp/data.h5', 'hh_income', mode='a')


class MilWages(luigi.Task):
    econ_id = luigi.Parameter()
    dem_id = luigi.Parameter()

    def requires(self):
        return MilPay(econ_id=self.econ_id, dem_id=self.dem_id)

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        econ_sim_rates = pd.read_hdf('temp/data.h5', 'econ_sim_rates')

        mil_pop = pd.read_hdf('temp/data.h5', 'mil_pop')
        mil_wages = pd.read_hdf('temp/data.h5', 'mil_pay')
        mil_wages = mil_wages.join(mil_pop)

        mil_wages['mil_gc_wages'] = (mil_wages['mil_gc_pop'] * mil_wages['annual_pay_gq']).round()
        mil_wages['mil_hh_wages'] = (mil_wages['mil_hh_pop'] * mil_wages['annual_pay_hp']).round()

        mil_wages['multiplier'] = 0

        aigrm_table = extract.create_df('aigrm', 'aigrm_table', rate_id=econ_sim_rates.aigrm_id[0],index=None)

        mil_wages.loc[mil_wages.index.get_level_values('yr') > 2014, ['multiplier']] = (aigrm_table.aigrm[0] *
                                                                                        (mil_wages.index.get_level_values('yr') - 2014))
        # pow(1.01, mil_wages.index.get_level_values('yr') - 2014)

        mil_wages['mil_gc_wages'] = (mil_wages['mil_gc_wages'] + mil_wages['mil_gc_wages'] * mil_wages['multiplier'])
        mil_wages['mil_hh_wages'] = (mil_wages['mil_hh_wages'] + mil_wages['mil_hh_wages'] * mil_wages['multiplier'])

        mil_wages['military_income'] = (mil_wages['mil_gc_wages'] + mil_wages['mil_hh_wages']).round()

        mil_wages.to_hdf('temp/data.h5', 'mil_income', mode='a')


class PersonalIncome(luigi.Task):
    econ = luigi.Parameter()
    dem = luigi.Parameter()

    @property
    def priority(self):
        return 2

    def requires(self):
        return {'hh_wage': HouseHoldWages(econ_id=self.econ, dem_id=self.dem),
                'mil_wages': MilWages(econ_id=self.econ, dem_id=self.dem)
                }

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        engine = create_engine(get_connection_string("model_config.yml", 'output_database'))
        econ_sim_rates = pd.read_hdf('temp/data.h5', 'econ_sim_rates')

        trs_rates = extract.create_df('trs_rates', 'trs_rates_table', rate_id=econ_sim_rates.trs_id[0], index=['yr'])

        hh_income = pd.read_hdf('temp/data.h5', 'hh_income')
        mil_income = pd.read_hdf('temp/data.h5', 'mil_income')
        ue_income = pd.read_hdf('temp/data.h5', 'ue_income')
        inc = hh_income.join(mil_income)
        inc = inc.join(ue_income)
        inc = inc.join(trs_rates)

        inc['unearned_income'] = (inc['Interest'] + inc['Other'] + inc['Public_Assistance'] + inc['Retirement'] +
                                  inc['Supplemental_Social_Security'] + inc['Social_Security']).round()

        inc['personal_income'] = (inc['jobs_local_wages'] + inc['wf_outside_wages'] + inc['unearned_income'] +
                                  inc['Selfemp_Income'] + inc['military_income']).round()

        inc['taxable_retail_sales'] = (inc['personal_income'] * inc['trs_pct']).round()

        inc = inc[['labor_force', 'unemployed', 'work_force', 'work_force_outside',
                    'work_force_local', 'jobs_local', 'jobs_total', 'jobs_external',
                    'avg_wage', 'jobs_total_wages', 'jobs_local_wages', 'jobs_external_wages',
                    'wf_outside_wages', 'military_income', 'unearned_income', 'Selfemp_Income', 'personal_income',
                    'taxable_retail_sales']]

        run_table = pd.read_hdf('temp/data.h5', 'run_id')
        run_id = run_table[0]
        inc['run_id'] = run_id

        inc.to_sql(name='emp_summary', con=engine, schema='defm', if_exists='append', index=True)


if __name__ == '__main__':
    os.makedirs('temp')
    luigi.run(main_task_cls=PersonalIncome)
    shutil.rmtree('temp')


