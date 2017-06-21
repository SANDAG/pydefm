import pytest
import numpy as np
from pandas import DataFrame
import pandas.util.testing as tm
from pydefm import migration
from pandas import MultiIndex

@pytest.fixture
def pop():
    return DataFrame({'age': [10, 32, 72],
                      'race_ethn': ['H', 'W', 'B'],
                      'sex': ['M', 'F', 'M'],
                      'persons': [859, 73, 4493]})


@pytest.fixture
def m_rates():
    return DataFrame({'age': [10, 32, 72],
                      'race_ethn': ['H', 'W', 'B'],
                      'sex': ['M', 'F', 'F'],
                      'DOUT': [0.027663, 0.057815, 0.012424],
                      'FOUT': [0.000000, 0.000000, 0.000000]})


@pytest.mark.parametrize(
    "expected",
    [(DataFrame([[859,0.027663,0.000000],
                 [73, 0.057815,0.000000],
                 [4493, np.nan,np.nan]],
                columns=['persons', 'DOUT','FOUT'],
                index=MultiIndex(levels = [
                    [10, 32, 72],
                    ['H', 'W', 'B'],
                    ['M', 'F', 'M']],
                    labels=[[0, 1, 2], [0, 1, 2],[0, 1, 2]],
                    names=['age', 'race_ethn','sex'])))])
def test_migration(pop, m_rates, expected):
    pop = pop.set_index(['age', 'race_ethn', 'sex'])
    m_rates = m_rates.set_index(['age', 'race_ethn', 'sex'])
    result =  migration.migrating_pop(pop, m_rates)
    tm.assert_frame_equal(result, expected)
