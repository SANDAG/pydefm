import pytest
import pandas as pd
import pandas.util.testing as tm
from pydefm import utils



@pytest.fixture
def pop():
    return pd.read_csv('tests/data/population/population.csv',index_col=[0,1,2])

@pytest.fixture
def mig_rates():
    return pd.read_csv('tests/data/migration/migration_rates.csv',index_col=[0,1,2])

@pytest.fixture
def pop_w_mig_rates():
    return pd.read_csv('tests/data/population/population_w_migration_rates.csv',index_col=[0,1,2])

@pytest.mark.parametrize("rates,population_w_rates", [
    (pd.read_csv('tests/data/migration/migration_rates.csv',index_col=[0,1,2]), pd.read_csv('tests/data/population/population_w_migration_rates.csv',index_col=[0,1,2])),
    (pd.read_csv('tests/data/deaths/death_rates.csv',index_col=[0,1,2]), pd.read_csv('tests/data/population/population_w_death_rates.csv',index_col=[0,1,2]))
])
def test_rates_for_yr(pop, rates, population_w_rates):
    year = 2026
    result = utils.rates_for_yr(pop, rates,year)
    tm.assert_frame_equal(result, population_w_rates)


# def test_rates_for_yr(pop, mig_rates, pop_w_mig_rates):
#     year = 2026
#     result = utils.rates_for_yr(pop, mig_rates,year)
#     tm.assert_frame_equal(result, pop_w_mig_rates)