import numpy as np
import pandas as pd


# non-migrating population = base population - out migration
# in migrating population = in migrating rates applied to base population
# dead population =  death rates applied to non-migrating population
# non-migrating survived population = non-migrating population - deaths
# newborns = birth rates applied to non-migrating population
# aged pop = non-migrating survived population is aged
# new pop = newborns + in migrating population
# final pop = new pop + aged pop


def in_migrating_population(pop):
    pop = pop[(pop['type'] == 'HHP') & (pop['mildep'] == 'N')]
    pop = pop.fillna(0)
    pop['mig_Din'] = (pop['persons'] * pop['DIN']).round()
    pop['mig_Fin'] = (pop['persons'] * pop['FIN']).round()
    pop = pop[['mig_Din', 'mig_Fin']]
    return pop


def out_migrating_population(pop):
    pop = pop[(pop['type'] == 'HHP') & (pop['mildep'] == 'N')]

    pop = pop.fillna(0)
    pop['mig_Dout'] = (pop['persons'] * pop['DOUT']).round()
    pop['mig_Fout'] = (pop['persons'] * pop['FOUT']).round()
    pop = pop[['mig_Dout', 'mig_Fout']]
    return pop


def non_migrating_population(pop, out_pop):
    pop = pop.join(out_pop, how='left')
    pop.loc[pop['type'].isin(['COL', 'INS', 'MIL', 'OTH']),
            ['mig_Dout', 'mig_Fout']] = 0
    pop.loc[pop['mildep'].isin(['Y']), ['mig_Dout', 'mig_Fout']] = 0
    pop['non_mig_pop'] = (pop['persons'] - pop['mig_Dout'] -
                          pop['mig_Fout']).round()
    return pop


def dead_population(pop):
    pop = pop[(pop['type'] == 'HHP') & (pop['mildep'] == 'N')]
    pop = pop.fillna(1) # for age 101, no rates
    pop['deaths'] = (pop['non_mig_pop'] * pop['death_rate']).round()
    return pop[['deaths']]

'''
def new_born_population(pop):
    pop['new_born'] = (pop['non_mig_pop'] * pop['death_rate']).round()
    return pop[['deaths']]
'''


def non_migrating_survived_pop(pop, deaths):
    pop = pop.join(deaths, how='left')
    pop.loc[pop['type'].isin(['COL', 'INS', 'MIL', 'OTH']), ['deaths']] = 0
    pop.loc[pop['mildep'].isin(['Y']), ['deaths']] = 0
    pop['non_mig_survived_pop'] = (pop['non_mig_pop'] - pop['deaths']).round()
    return pop


def aged_pop(non_mig_survived_pop):
    non_mig_survived_pop['increment'] = 1
    # sum newborn population across cohorts
    non_mig_survived_pop = non_mig_survived_pop.\
        reset_index(level=['age', 'race_ethn', 'sex'])

    non_mig_survived_pop.loc[non_mig_survived_pop['type'].isin(['COL',
                                                                'INS',
                                                                'MIL',
                                                                'OTH']),
                             ['increment']] = 0
    non_mig_survived_pop.loc[non_mig_survived_pop['mildep'].isin(['Y']),
                             ['increment']] = 0

    temp = non_mig_survived_pop[(non_mig_survived_pop['increment'] == 1) &
                                (non_mig_survived_pop['age'] == 0)]
    temp['non_mig_survived_pop'] = 0
    non_mig_survived_pop['age'] = non_mig_survived_pop['age'] + \
        non_mig_survived_pop['increment']
    non_mig_survived_pop = non_mig_survived_pop.append(temp)

    temp_2 = non_mig_survived_pop[(non_mig_survived_pop['age'] >= 101)]
    temp_2_p = pd.DataFrame(temp_2['non_mig_survived_pop'].groupby(
        [temp_2['race_ethn'], temp_2['sex'],
         temp_2['type'], temp_2['mildep']]).sum())
    temp_2_h = pd.DataFrame(
        temp_2['households'].groupby([temp_2['race_ethn'],
                                      temp_2['sex'],
                                      temp_2['type'],
                                      temp_2['mildep']]).sum())
    temp_2_p = temp_2_p.join(temp_2_h)
    temp_2_p['age'] = 101
    temp_2_p = temp_2_p.reset_index(drop=False)

    non_mig_survived_pop = non_mig_survived_pop[
        ['age', 'race_ethn', 'sex', 'type',
         'mildep', 'non_mig_survived_pop', 'households']]
    non_mig_survived_pop = non_mig_survived_pop[non_mig_survived_pop.age < 101]
    non_mig_survived_pop = non_mig_survived_pop.append(temp_2_p)
    non_mig_survived_pop.fillna(0)

    non_mig_survived_pop = non_mig_survived_pop.\
        set_index(['age', 'race_ethn', 'sex'])
    return non_mig_survived_pop


def new_population(new_pop):
    new_pop['new_pop'] = new_pop['mig_Din'] +\
                         new_pop['mig_Fin'] + new_pop['new_born_survived']
    return new_pop[['new_pop']]


def final_population(pop):
    pop['persons1'] = pop['non_mig_survived_pop'] + pop['new_pop']

    pop2 = pop[(pop['type'] == 'HHP')]
    pop2 = pop2.reset_index(drop=False)

    pop2 = pd.DataFrame(pop2['persons1'].groupby(
        [pop2['age'], pop2['race_ethn'], pop2['sex']]).sum())
    pop2.rename(columns={'persons1': 'persons_sum1'}, inplace=True)
    pop = pop.join(pop2)

    pop['persons'] = np.where(pop['type'].
                              isin(['INS', 'OTH']),
                              (pop['persons_sum1'] * pop['rates']).round(),
                              pop['persons1'])
    return pop[['type', 'mildep', 'persons', 'households']]


def births_all(b_df, rand_df=None, pop_col='persons'):
    """
    Calculate births for given year based on rates.
    Predict male births as 51% of all births & female births as 49%.
    Result is nearest integer (floor) after +0 or +0.5 (randomly generated)

    Parameters
    ----------
    b_df : pandas.DataFrame
        with population for current yr and birth rates
    rand_df : pandas.DataFrame
        with random numbers
    pop_col : string
        column name from which births to be calculated
    Returns
    -------
    b_df : pandas DataFrame
        male and female births by cohort (race_ethn and age)

    """

    # total births =  population * birth rate (fill blanks w zero)
    b_df['births_rounded'] = (b_df[pop_col] *
                              b_df['birth_rate']).round()
    # b_df = b_df.round({'births_rounded': 0})

    # male births 51%
    b_df['births_m_float'] = b_df['births_rounded'] * 0.51

    b_df = b_df.join(rand_df)

    # Add random 0 or 0.5
    # Convert to int which truncates float (floor)
    b_df['births_m'] = b_df['births_m_float'] + b_df['random_number']
    b_df['births_m'] = b_df['births_m'].astype('int64')

    # female births
    b_df['births_f'] = (b_df['births_rounded'] - b_df['births_m']).round()

    return b_df


def births_sum(df, sim_year):
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

    df = df[['yr', 'race_ethn', 'mildep', 'type', 'births_m', 'births_f']]

    births_grouped = df.groupby(['yr', 'race_ethn', 'mildep', 'type'],
                                as_index=False).sum()

    male_births = births_grouped.copy()
    male_births.rename(columns={'births_m': 'persons'}, inplace=True)
    male_births['sex'] = 'M'
    male_births['age'] = 0
    male_births = male_births.set_index(['age', 'race_ethn', 'sex'])
    male_births = male_births.drop('births_f', 1)

    female_births = births_grouped.copy()
    female_births.rename(columns={'births_f': 'persons'}, inplace=True)
    female_births['sex'] = 'F'
    female_births['age'] = 0
    female_births = female_births.set_index(['age', 'race_ethn', 'sex'])
    female_births = female_births.drop('births_m', 1)

    births_mf = pd.concat([male_births, female_births], axis=0)

    births_mf['households'] = 0  # temp ignore households

    # no births for this special case
    births_mf = births_mf[-births_mf['type'].isin(['COL', 'MIL', 'INS',
                                                   'OTH'])]

    newborns = births_mf[births_mf.persons != 0].copy()
    newborns.rename(columns={'persons': 'newborns'}, inplace=True)
    newborns = newborns.drop('households', 1)

    births_mf = births_mf.drop('yr', 1)

    # SPECIAL CASE:
    # Births are estimated & reported out, but are not carried over into the
    # next year ( base_population.type="HP" and base_population.mildep="Y")
    # keep rows in which either type != 'HP' OR mildep != 'Y'
    # which results in dropping rows  where type = 'HP' AND mildep = 'Y'
    births_mf = births_mf[((births_mf.type != 'HP') |
                           (births_mf.mildep != 'Y'))]
    births_mf.rename(columns={'persons': 'new_born'}, inplace=True)

    return births_mf
