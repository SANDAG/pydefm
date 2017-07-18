"""
Calculate population changes based on migration, birth, and death rates
"""

import numpy as np
import pandas as pd
from db import log


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

    net_mig_db = df[(df.DIN != 0) | (df.DOUT != 0) | (df.FIN != 0) | (df.FOUT != 0)].copy()
    log.insert_run('defm.db', db_id, net_mig_db, 'net_migration')

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
    # drop  unnecessary columns
    nm_df = nm_df[['type', 'mildep','households','persons','mig_out_net','non_mig_pop','mig_in_net','yr']]

    # record non migration population in result database
    # remove rows that have zero population
    nm_db = nm_df[nm_df.non_mig_pop != 0].copy()
    nm_db = nm_db.drop(['mig_in_net'],1)

    log.insert_run('defm.db', db_id, nm_db, 'non_migrating')

    # drop year column in order to join w birth and death rates
    nm_df = nm_df.drop(['yr','persons','mig_out_net'],1)

    return nm_df


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
    # deaths_out = df[df.deaths != 0]
    # report out deaths
    # log.insert_run('deaths.db', db_id, df, 'survived_' + str(sim_year))
    deaths_out = df[df.deaths != 0].copy()
    deaths_out = deaths_out.drop(['mig_in_net'], 1)

    log.insert_run('defm.db', db_id, deaths_out, 'deaths_by_cohort_by_age')

    deaths_out = deaths_out.reset_index(drop=False)

    deaths_out = deaths_out.drop(['non_mig_pop', 'death_rate', 'age','households'], 1)

    deaths_grouped = deaths_out.groupby(['yr', 'race_ethn', 'mildep', 'sex',
                              'type'], as_index=False).sum()

    # log.insert_run('defm.db', db_id, deaths_grouped, 'deaths_sum_by_age')

    # log.insert_run('defm.db', db_id, df, 'deaths')

    # SPECIAL CASES
    # deaths not carried over into next year
    df['survived'] = np.where(
        ((df['type'] == 'HP') & (df['mildep'] == 'Y')) |
        df['type'].isin(['COL','INS','MIL','OTH']),
        df['non_mig_pop'],  # special case
        df['non_mig_pop'] - df['deaths'])  # else

    # drop other unnecessary columns
    survived_out = df[df.non_mig_pop != 0].copy()
    survived_out = survived_out.drop(['mig_in_net'], 1)
    log.insert_run('defm.db', db_id, survived_out, 'survived')

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
    # sum_mildep_yn = ratios_hp.groupby(['age', 'race_ethn', 'sex', 'type',
    #                                      'run_id'], as_index=False).sum()
    sum_mildep_yn = ratios_hp.groupby(['age', 'race_ethn', 'sex', 'type',],
                                      as_index=False).sum()
    sum_mildep_yn = sum_mildep_yn.set_index(['age','race_ethn','sex'])
    sum_mildep_yn['mildep'] = 'N'
    special_pop = pd.concat([pop, sum_mildep_yn])

    return special_pop
