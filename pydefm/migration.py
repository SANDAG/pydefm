
def migrating_pop(population, migration_rates):

    return population.join(migration_rates, how='left', sort=False)