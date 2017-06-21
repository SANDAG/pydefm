

def migrating_pop(population, migration_rates):

    return population.join(migration_rates, how='left', sort=False)


def non_migrating_pop(non_mig_pop):

    non_mig_pop['mig_Dout'] = (non_mig_pop['persons'] * non_mig_pop['DOUT']).round()
    non_mig_pop['mig_Fout'] = (non_mig_pop['persons'] * non_mig_pop['FOUT']).round()
    non_mig_pop['non_mig_pop'] = (non_mig_pop['persons'] - non_mig_pop['mig_Dout'] - non_mig_pop['mig_Fout']).round()
    return non_mig_pop
