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
    population : pandas DataFrame
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
    rates_yr = rates_all_years[rates_all_years['yr'] == sim_year]
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
    # SPECIAL CASE: no migration, set rates to zero

    # when group quarters = "HP" and mildep = "Y"
    df.loc[((df.type == 'HP') & (df.mildep == 'Y')),
           ['DIN', 'DOUT', 'FIN', 'FOUT']] = 0

    # when group quarters equal COL, INS, MIL, or OTH
    df.loc[df['type'].isin(['COL', 'INS', 'MIL', 'OTH']),
           ['DIN', 'DOUT', 'FIN', 'FOUT']] = 0

    # calculate net migration
    df['mig_Dout'] = (df['persons'] * df['DOUT']).round()
    df['mig_Fout'] = (df['persons'] * df['FOUT']).round()
    df['mig_Din'] = (df['persons'] * df['DIN']).round()
    df['mig_Fin'] = (df['persons'] * df['FIN']).round()
    df['mig_out_net'] = df['mig_Dout'] + df['mig_Fout']
    df['mig_in_net'] = df['mig_Din'] + df['mig_Fin']

    return df


def non_mig(nm_df, db_id, sim_year):
    """
    Calculate non-migration population by subtracting net out migrating from population

    Parameters
    ----------
    nm_df : pandas.DataFrame
        with population for current yr
        and population migrating in & out
    db_id : int
        primary key for current simulation
    sim_year : int
        year being simulated

    Returns
    -------
    nm_df : pandas DataFrame
        non-migrating population per cohort for a given year

    """
    nm_df['non_mig_pop'] = nm_df['persons'] - nm_df['mig_out_net']

    # record non migration population in result database
    log.insert_run('non_migrating.db', db_id, nm_df,
                   'non_migrating_' + str(sim_year))

    # drop year column in order to join w birth and death rates
    # drop other unnecessary columns
    nm_df = nm_df[['type', 'mildep', 'non_mig_pop', 'households', 'mig_in_net']]
    return nm_df


def births_all(b_df, db_id, sim_year):
    """
    Calculate births for given year based on rates.
    Predict male births as 51% of all births & female births as 49%.
    Result is nearest integer (floor) after +0 or +0.5 (randomly generated)

    Parameters
    ----------
    b_df : pandas.DataFrame
        with population for current yr and birth rates
    db_id : int
        primary key for current simulation
    sim_year : int
        year being simulated

    Returns
    -------
    b_df : pandas DataFrame
        male and female births by cohort (race_ethn and age)

    """
    # SPECIAL CASE: no births, set rates to zero

    # when group quarters in ("COL","INS","MIL","OTH")
    b_df.loc[b_df['type'].isin(['COL','INS','MIL','OTH']), ['birth_rate']] = 0

    # total births =  population * birth rate (fill blanks w zero)
    b_df['births_rounded'] = np.round(
        b_df['non_mig_pop'] * b_df['birth_rate']).fillna(0.0).astype(int)

    # male births 51%
    b_df['births_m_float'] = b_df['births_rounded'] * 0.51

    # 0 or 0.5 generated randomly by multiplying 0 or 1 by 0.5
    np.random.seed(2010)
    b_df['randomNumCol'] = 0.5 * np.random.randint(2, size=b_df.shape[0])

    # Add random 0 or 0.5
    # Convert to int which truncates float (floor)
    b_df['births_m'] = b_df['births_m_float'] + b_df['randomNumCol']
    b_df['births_m'] = b_df['births_m'].astype(int)

    # female births
    b_df['births_f'] = b_df['births_rounded'] - b_df['births_m']

    # remove rows w no birth rate
    # use yr column since yr column in original birth rates DataFrame
    b_df_notnull = b_df[b_df.yr.notnull()].copy()

    log.insert_run('births.db', db_id, b_df_notnull,
                   'births_' + str(sim_year))

    return b_df_notnull


def births_sum(df,db_id,sim_year):
    """
    Sum births over all the ages in a given cohort
    Set birth age to zero and reset DataFrame index

    Parameters
    ----------
    df : pandas DataFrame
        male and female births for each cohort and non-migrating population
    db_id : int
        primary key for current simulation
    sim_year : int
        year being simulated

    Returns
    -------
    births_age0 : pandas DataFrame
        births summed across age for each cohort

    """
    df = df.reset_index(drop=False)

    df = df[['yr', 'race_ethn', 'mildep','type','births_m','births_f']]

    births_grouped = df.groupby(['yr', 'race_ethn', 'mildep',
                              'type'], as_index=False).sum()

    male_births = births_grouped.copy()
    male_births.rename(columns={'births_m': 'persons'}, inplace=True)
    male_births['sex'] = 'M'
    male_births['age'] = 0
    male_births = male_births.set_index(['age','race_ethn','sex'])
    male_births = male_births.drop('births_f',1)

    female_births = births_grouped.copy()
    female_births.rename(columns={'births_f': 'persons'}, inplace=True)
    female_births['sex'] = 'F'
    female_births['age'] = 0
    female_births = female_births.set_index(['age','race_ethn','sex'])
    female_births = female_births.drop('births_m',1)

    births_mf = pd.concat([male_births, female_births], axis=0)

    births_mf['households'] = 0  # temp ignore households

    log.insert_run('newborns.db', db_id, births_mf, 'newborns_' +
                   str(sim_year))

    births_mf = births_mf.drop('yr', 1)

    # SPECIAL CASE: when births are not carried over
    # keep rows in which either type != 'HP' OR mildep != 'Y'
    # which results in dropping rows  where type = 'HP' AND mildep = 'Y'
    births_mf = births_mf[((births_mf.type != 'HP') | (births_mf.mildep != 'Y'))]
    # no births for this special case
    births_mf = births_mf[-births_mf['type'].isin(['COL','MIL','INS','OTH'])]
    return births_mf


def deaths(df, db_id, sim_year):
    """
     Calculate deaths by applying death rates to non-migrating population

    Parameters
    ----------
    df : pandas DataFrame
        population and death rates for current yr
    db_id : int
        primary key for current simulation
    sim_year : int
        year being simulated

    Returns
    -------
    df : pandas DataFrame
        survived population per cohort for a given year

    """
    df['deaths'] = (df['non_mig_pop'] * df['death_rate']).round()
    # report out deaths
    log.insert_run('deaths.db', db_id, df, 'survived_' + str(sim_year))

    # SPECIAL CASES
    # deaths not carried over into next year
    df['survived'] = np.where(
        ((df['type'] == 'HP') & (df['mildep'] == 'Y')) |
        df['type'].isin(['COL','INS','MIL','OTH']),
        df['non_mig_pop'],  # special case
        df['non_mig_pop'] - df['deaths'])  # else

    # drop other unnecessary columns
    df = df.drop(['deaths', 'yr', 'death_rate', 'non_mig_pop'], 1)
    return df


def age_the_pop(df):
    """
    Age population by one year.  Get rid of population greater than 100

    Parameters
    ----------
    df : pandas DataFrame
        survived population

    Returns
    -------
    pop : pandas DataFrame
        population aged by one year

    """
    df = df.reset_index(drop=False)

    # age the population
    df['aged'] = df['age'] + 1

    # SPECIAL CASES
    # next year's population is carried over from the base unchanged
    df.loc[((df["type"] == 'HP') & (df["mildep"] == 'Y')), 'aged'] = df['age']
    df.loc[(df['type'].isin(['COL', 'MIL'])), 'aged'] = df['age']
    df = df[-df['type'].isin(['INS','OTH'])]

    # fix later
    df = df[df.aged < 101]  # need to fix w death rate = 1 when age > 100

    df = df.drop(['age'], 1)
    df.rename(columns={'aged': 'age'}, inplace=True)
    pop = df.set_index(['age', 'race_ethn', 'sex'])
    return pop


def new_pop(newborn,aged):
    """
    Update base population by adding in-migrating population and newborns

    Parameters
    ----------
    newborn : pandas DataFrame
        newborns for simulated year
    aged : pandas DataFrame
        aged population for simulated year

    Returns
    -------
    pop : pandas DataFrame
        Newborn Population + Survived Population

    """
    aged['persons'] = np.where(
        ((aged['type'] == 'HP') & (aged['mildep'] == 'Y')), # special case
                                 aged['survived'],  # no in migration
                                 aged['survived'] + aged['mig_in_net'])  # else
    aged = aged.drop(['survived','mig_in_net'], 1)
    pop = pd.concat([newborn,aged])
    return pop


def case_ins_oth(pop, ratios, gq_type):
    """
    Apply ratios to determine INS and OTH population (special cases)

    Parameters
    ----------
    pop : pandas DataFrame
        population
    ratios : pandas DataFrame
        cohort-specific ratio: "INS pop / HP pop" or "OTH pop / HP pop"
    gq_type : string
        INS or OTH
    Returns
    -------
    special_pop : pandas DataFrame
        Population + Special Case Population

    """

    ratios_hp = (ratios[ratios['type'] == 'HP']).copy()
    ratios_hp['pop_special_case'] = (ratios_hp['case_ratio'] * ratios_hp['persons']).round()
    ratios_hp['type'] = gq_type
    ratios_hp = ratios_hp.drop(['persons','case_ratio','yr'], 1)
    ratios_hp.rename(columns={'pop_special_case': 'persons'}, inplace=True)
    ratios_hp = ratios_hp.reset_index(drop=False)
    sum_mildep_yn = ratios_hp.groupby(['age', 'race_ethn', 'sex', 'type',
                                       'run_id'], as_index=False).sum()
    sum_mildep_yn = sum_mildep_yn.set_index(['age','race_ethn','sex'])
    sum_mildep_yn['mildep'] = 'N'
    special_pop = pd.concat([pop, sum_mildep_yn])

    return special_pop
