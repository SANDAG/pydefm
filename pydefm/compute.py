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




def non_migrating_population(pop):

    pop['mig_Dout'] = (pop['persons'] * pop['DOUT']).round()
    pop['mig_Fout'] = (pop['persons'] * pop['FOUT']).round()
    pop['pop'] = (pop['persons'] - pop['mig_Dout'] - pop['mig_Fout']).round()
    return pop


