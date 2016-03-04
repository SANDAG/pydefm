"""
Calculate population changes based on migration, birth, and death rates
"""

import numpy as np
import pandas as pd
from db import log


def rates_for_yr(pop,rates,year):
    """
    Filter specific rates for a given year and join with population
    by cohort

    Parameters
    ----------
    pandas DataFrame : population
    pandas DataFrame : rates, to be filtered by year
    int: year for simulation

    Returns
    -------
    pandas DataFrame : rates for a given year and population for each cohort

    """
    rates_yr = rates[rates['yr']== year]
    pop_w_rates = pop.join(rates_yr)
    return pop_w_rates


def net_mig(df, db_id, sim_year):
    """
    Calculate net migration by applying rates to population

    Parameters
    ----------
    pandas DataFrame : With population and migration rates for current yr

    Returns
    -------
    pandas DataFrame : In and out migrating population per cohort for a given year

    """
    df['mig_out_dom'] = (df['persons'] * df['DOUT']).round()
    df['mig_out_for'] = (df['persons'] * df['FOUT']).round()
    df['mig_in_dom'] = (df['persons'] * df['DIN']).round()
    df['mig_in_for'] = (df['persons'] * df['FIN']).round()
    df['mig_out_net'] = df['mig_out_dom'] + df['mig_out_for']
    df['mig_in_net'] = df['mig_in_dom'] + df['mig_in_for']
    log.insert_run('migration.db',db_id,df,'migration_' + str(sim_year))

    return df


def non_mig(df):
    """
    Calculate non-migration population by subtracting net out migrating from population

    Parameters
    ----------
    df : pandas.DataFrame
        Net migration population for current yr
        Should contain the columns for population
        and migration out

    Returns
    -------
    df : pandas DataFrame
        Non-migrating population per cohort for a given year

    """
    df['non_mig_pop'] = np.where(((df['type']=='HP') & (df['mildep'] == 'Y')), df['persons'], df['persons'] - df['mig_out_net'])
    df = df[['type','mildep','non_mig_pop','households','mig_in_net']]
    return df


def deaths(df):
    """
     Calculate deaths by applying death rates to non-migrating population for given year

    Parameters
    ----------
    pandas DataFrame : With population and death rates for current yr

    Returns
    -------
    pandas DataFrame : Survived population per cohort for a given year

    """
    df['deaths'] = (df['non_mig_pop'] * df['death_rate']).round()
    df['survived'] = df['non_mig_pop'] - df['deaths']
    df = df.drop(['deaths','yr','death_rate','non_mig_pop'], 1)
    return df


def age_the_pop(df):
    """
    Age population by one year.  Get rid of population greater than 100

    Parameters
    ----------
    pandas DataFrame : With survived population

    Returns
    -------
    pandas DataFrame : population aged by one year

    """
    df = df.reset_index(drop=False)
    df['age'] = df['age'] + 1
    df = df[df.age < 101]
    pop = df.set_index(['age','race_ethn','sex'])
    return pop


def births_all(df, db_id, sim_year):
    """
    Calculate births for given year
    Predict male or female birth by random number
    Add random number (0 or 0.5) before rounding (converting to int which truncates float)
    Parameters
    ----------
    pandas DataFrame : population and birth rates

    Returns
    -------
    pandas DataFrame : male and female births by cohort (race_ethn and age)

    """
    df['births_rounded'] = np.round(df['non_mig_pop'] * df['birth_rate']).fillna(0.0).astype(int)
    df['births_m_float'] = df['births_rounded'] * 0.51  # float, 51% male
    np.random.seed(2010)
    df['randomNumCol'] = 0.5 * np.random.randint(2,size = df.shape[0])  # 0 or 0.5
    df['births_m'] = (df['births_m_float'] + df['randomNumCol']).astype(int)
    df['births_f'] = df['births_rounded'] - df['births_m']
    df2 = df.copy()
    df2 =df2[df2.yr.notnull()] # remove rows with no birth rate
    df2 = df2.drop('mig_in_net', 1)
    log.insert_run('births_all.db',db_id,df2,'births_all_' + str(sim_year))
    return df


def births_sum(df,db_id,sim_year):
    """
    Sum births over all the ages in a given cohort
    Set birth age to zero and reset DataFrame index

    Parameters
    ----------
    pandas DataFrame : male and female births for each cohort and non-migrating population

    Returns
    -------
    pandas DataFrame : births summed across age for each cohort

    """
    df = df.reset_index(drop=False)
    male_births = pd.DataFrame({'persons' : df['births_m'].groupby([df['yr'],df['race_ethn'],df['type'],df['mildep']]).sum()}).reset_index()
    male_births['sex'] = 'M'
    male_births['age'] = 0
    male_births = male_births.set_index(['age','race_ethn','sex'])
    female_births = pd.DataFrame({'persons' : df['births_f'].groupby([df['yr'],df['race_ethn'],df['type'],df['mildep']]).sum()}).reset_index()
    female_births['sex'] = 'F'
    female_births['age'] = 0
    female_births = female_births.set_index(['age','race_ethn','sex'])
    births_age0 = pd.concat([male_births, female_births], axis=0)
    births_age0['households'] = 0 # need to fix this.  temp IGNORE
    log.insert_run('births_sum.db',db_id,births_age0,'births_sum_' + str(sim_year))
    births_age0 = births_age0.drop('yr', 1)
    return births_age0


def new_pop(newborn,aged):
    """
    Update base population by adding in-migrating population and newborns


    Parameters
    ----------
    pandas DataFrame : newborns for simulated year
    pandas DataFrame : aged population for simulated year


    Returns
    -------
    pandas DataFrame :  Newborn Population + Survived Population

    """
    aged['persons'] = aged['survived'] + aged['mig_in_net']
    aged = aged.drop(['survived','mig_in_net'], 1)
    pop = pd.concat([newborn,aged])
    return pop
