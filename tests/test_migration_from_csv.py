import pytest
import pandas as pd
import pandas.util.testing as tm
from pydefm import migration
from pydefm import compute
import numpy as np


@pytest.fixture
def pop():
    return pd.read_csv('tests/data/population.csv',index_col=[0,1,2])


@pytest.fixture
def m_rates():
    return pd.read_csv('tests/data/migration_rates.csv',index_col=[0,1,2])


@pytest.fixture
def joined_pop_rates():
    return pd.read_csv('tests/data/population_w_migration_rates.csv',index_col=[0,1,2])


@pytest.mark.parametrize(
    "expected",
    [pd.read_csv('tests/data/population_w_migration_rates.csv',index_col=[0,1,2])])
def test_migration(pop, m_rates, expected):
    result =  migration.migrating_pop(pop, m_rates)
    tm.assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    "expected",
    [pd.read_csv('tests/data/non_migrating_pop.csv',index_col=[0,1,2],dtype = {'mig_Dout':  np.float64, 'non_mig_pop': np.float64})])
def test_non_migrating_pop(joined_pop_rates, expected):
    result = migration.non_migrating_pop(joined_pop_rates)
    tm.assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    "expected",
    [pd.read_csv('tests/data/in_migrating_pop.csv',index_col=[0,1,2],dtype = {'mig_Din':  np.float64, 'mig_Fin': np.float64})])
def test_in_migrating_population(joined_pop_rates, expected):
    result = compute.in_migrating_population(joined_pop_rates)
    tm.assert_frame_equal(result, expected)