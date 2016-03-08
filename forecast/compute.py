"""
Calculate population changes based on migration, birth, and death rates
"""

import numpy as np
import pandas as pd
from db import log


def rates_for_yr(population, rates_all_years, sim_year):
    """
    Filter specific rates for a given year and join with population
    by cohort

    Parameters
    ----------
    population : pandas.DataFrame
        population for simulated year, starting with base population
    rates_all_years : pandas DataFrame
        rates, to be filtered by year
    sim_year : int
        year being simulated

    Returns
    -------
    pop_w_rates : pandas DataFrame
        rates for a given year and population for each cohort

    """
    rates_yr = rates_all_years[rates_all_years['yr']== sim_year]
    pop_w_rates = population.join(rates_yr)
    return pop_w_rates


def net_mig(df, db_id, sim_year):
    """
    Calculate net migration by applying rates to population

    Parameters
    ----------
    df : pandas.DataFrame
        with population and migration rates for current yr
    db_id : int
        primary key for current simulation
    sim_year : int
        year being simulated

    Returns
    -------
    df : pandas DataFrame
        In and out migrating population per cohort for a given year
            population x  migration rate, where rates are:
                domestic in (DIN), domestic out (DOUT),
                foreign in (FIN), and foreign out (FOUT)

    """
    # for special cases, no migration thus set rates to zero

    # when group quarters = "HP" and mildep = "Y"
    df.loc[((df.type == 'HP') & (df.mildep == 'Y')),['DIN','DOUT','FIN','FOUT']] = 0

    # when group quarters equal COL, INS, MIL, or OTH
    df.loc[df['type'].isin(['COL','INS','MIL','OTH']),['DIN','DOUT','FIN','FOUT']] = 0

    # calculate net migration
    df['mig_Dout'] = (df['persons'] * df['DOUT']).round()
    df['mig_Fout'] = (df['persons'] * df['FOUT']).round()
    df['mig_Din'] = (df['persons'] * df['DIN']).round()
    df['mig_Fin'] = (df['persons'] * df['FIN']).round()
    df['mig_out_net'] = df['mig_Dout'] + df['mig_Fout']
    df['mig_in_net'] = df['mig_Din'] + df['mig_Fin']

    # record net migration in result database
    log.insert_run('net_migration.db',db_id,df,'migration_' + str(sim_year))

    return df


def non_mig(df, db_id, sim_year):
    """
    Calculate non-migration population by subtracting net out migrating from population

    Parameters
    ----------
    df : pandas.DataFrame
        with population for current yr
        and population migrating in & out
    db_id : int
        primary key for current simulation
    sim_year : int
        year being simulated

    Returns
    -------
    df : pandas DataFrame
        non-migrating population per cohort for a given year

    """
    df['non_mig_pop'] = df['persons'] - df['mig_out_net']

    log.insert_run('non_migrating_pop.db',db_id,df,'non_migrating_' + str(sim_year))
    # drop year column in order to join with birth and death rates
    # which have a year column
    # df = df.drop(['yr'], 1)
    df = df[['type','mildep','non_mig_pop','households','mig_in_net']]
    return df


def deaths(df, db_id, sim_year):
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
    log.insert_run('deaths.db',db_id,df,'survived_' + str(sim_year))

    # special case for military: deaths not carried over into next year
    # otherwise: subtract deaths from non-migrating population
    df['survived'] = np.where(
        ((df['type'] == 'HP') & (df['mildep'] == 'Y')),  # special case
                                 df['non_mig_pop'],  # no deaths
                                  df['non_mig_pop'] - df['deaths'])  # else

    # special case for group quarters:
    # deaths not carried over into next year
    df['survived'] = np.where(
        df['type'].isin(['COL','INS','MIL','OTH']),  # special case
                                 df['non_mig_pop'],  # no deaths carried over
                                  df['survived'])

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
    df.loc[((df["type"] =='HP') & (df["mildep"] =='Y')) , 'age'] = df['age'] - 1
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
    # set birth rate to zero for special case
    # when group quarters in ("COL","INS","MIL","OTH")
    df.loc[df['type'].isin(['COL','INS','MIL','OTH']),['birth_rate']] = 0

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
    births_age0['households'] = 0  # need to fix this.  temp IGNORE
    log.insert_run('births_sum.db',db_id,births_age0,'births_sum_' + str(sim_year))
    births_age0 = births_age0.drop('yr', 1)
    # keep rows in which either type != 'HP' OR mildep != 'Y'
    # which results in dropping rows  where type = 'HP' AND mildep = 'Y'
    births_age0 = births_age0[((births_age0.type != 'HP') | (births_age0.mildep != 'Y'))]

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
    #aged['persons'] = aged['survived'] + aged['mig_in_net']
    aged['persons'] = np.where(
        ((aged['type'] == 'HP') & (aged['mildep'] == 'Y')), # special case
                                 aged['survived'],  # no in migration
                                 aged['survived'] + aged['mig_in_net'])  # else
    aged = aged.drop(['survived','mig_in_net'], 1)
    pop = pd.concat([newborn,aged])
    return pop
