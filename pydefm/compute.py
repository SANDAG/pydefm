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
    pop['mig_Din'] = (pop['persons'] * pop['DIN']).round()
    pop['mig_Fin'] = (pop['persons'] * pop['FIN']).round()
    pop = pop[['mig_Din', 'mig_Fin']]
    return pop


def out_migrating_population(pop):
        pop['mig_Dout'] = (pop['persons'] * pop['DOUT']).round()
        pop['mig_Fout'] = (pop['persons'] * pop['FOUT']).round()
        pop = pop[['mig_Dout', 'mig_Fout']]
        return pop


def non_migrating_population(pop):
    pop['non_mig_pop'] = (pop['persons'] - pop['mig_Dout'] - pop['mig_Fout']).round()
    return pop


def dead_population(pop):
    pop['deaths'] = (pop['non_mig_pop'] * pop['death_rate']).round()
    return pop[['deaths']]

'''
def new_born_population(pop):
    pop['deaths'] = (pop['non_mig_pop'] * pop['death_rate']).round()
    return pop[['deaths']]
'''


def non_migrating_survived_pop(pop):
    pop['non_mig_survived_pop'] = (pop['non_mig_pop'] - pop['deaths']).round()
    return pop


def aged_pop(non_mig_survived_pop):
    non_mig_survived_pop['increment'] = 1
    # sum newborn population across cohorts
    non_mig_survived_pop = non_mig_survived_pop.reset_index(level=['age', 'race_ethn', 'sex'])

    non_mig_survived_pop.loc[non_mig_survived_pop['type'].isin(['COL', 'INS', 'MIL', 'OTH']), ['increment']] = 0
    non_mig_survived_pop.loc[non_mig_survived_pop['mildep'].isin(['Y']), ['increment']] = 0

    temp = non_mig_survived_pop[(non_mig_survived_pop['increment'] == 1) & (non_mig_survived_pop['age'] == 0)]
    temp['non_mig_survived_pop'] = 0
    non_mig_survived_pop['age'] = non_mig_survived_pop['age'] + non_mig_survived_pop['increment']
    non_mig_survived_pop = non_mig_survived_pop.append(temp)

    temp_2 = non_mig_survived_pop[(non_mig_survived_pop['age'] >= 101)]
    temp_2_p = pd.DataFrame(temp_2['non_mig_survived_pop'].groupby(
        [temp_2['race_ethn'], temp_2['sex'], temp_2['type'], temp_2['mildep']]).sum())
    temp_2_h = pd.DataFrame(
        temp_2['households'].groupby([temp_2['race_ethn'], temp_2['sex'], temp_2['type'], temp_2['mildep']]).sum())
    temp_2_p = temp_2_p.join(temp_2_h)
    temp_2_p['age'] = 101
    temp_2_p = temp_2_p.reset_index(drop=False)

    non_mig_survived_pop = non_mig_survived_pop[
        ['age', 'race_ethn', 'sex', 'type', 'mildep', 'non_mig_survived_pop', 'households']]
    non_mig_survived_pop = non_mig_survived_pop[non_mig_survived_pop.age < 101]
    non_mig_survived_pop = non_mig_survived_pop.append(temp_2_p)
    non_mig_survived_pop.fillna(0)

    non_mig_survived_pop = non_mig_survived_pop.set_index(['age', 'race_ethn', 'sex'])
    return non_mig_survived_pop


def new_population(new_pop):
    new_pop['new_pop'] = new_pop['mig_Din'] + new_pop['mig_Fin'] + new_pop['new_born']
    return new_pop[['new_pop']]


def final_population(pop):
    pop['persons'] = pop['non_mig_survived_pop'] + pop['new_pop']

    return pop[['type', 'mildep', 'persons', 'households']]