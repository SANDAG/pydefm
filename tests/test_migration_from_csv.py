import pytest
import pandas as pd
import pandas.util.testing as tm
from pydefm import migration
import numpy as np


@pytest.fixture
def pop():
    return pd.read_csv('tests/data/population.csv')


@pytest.fixture
def m_rates():
    return pd.read_csv('tests/data/migration_rates.csv')


@pytest.fixture
def joined_pop_rates():
    return pd.read_csv('tests/data/population_w_migration_rates.csv')


@pytest.mark.parametrize(
    "expected",
    [pd.read_csv('tests/data/population_w_migration_rates.csv')])
def test_migration(pop, m_rates, expected):
    pop = pop.set_index(['age', 'race_ethn', 'sex'])
    m_rates = m_rates.set_index(['age', 'race_ethn', 'sex'])
    expected = expected.set_index(['age', 'race_ethn', 'sex'])
    result =  migration.migrating_pop(pop, m_rates)
    tm.assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    "expected",
    [pd.read_csv('tests/data/non_migrating_pop.csv',dtype = {'mig_Dout':  np.float64, 'non_mig_pop': np.float64})])
def test_non_migrating_pop(joined_pop_rates, expected):
    result = migration.non_migrating_pop(joined_pop_rates)
    tm.assert_frame_equal(result, expected)