import pytest
import numpy as np
from pandas import DataFrame
import pandas.util.testing as tm
from pydefm import migration

@pytest.fixture
def pop():
    return DataFrame({'a': [20, 10, 0]}, index=[2, 1, 0])


@pytest.fixture
def m_rates():
    return DataFrame({'b': [300, 100, 200]}, index=[3, 1, 2])


@pytest.mark.parametrize(
    "expected",
    [(DataFrame({'a': [20, 10, 0],
                                'b': [200, 100, np.nan]},
                               index=[2, 1, 0]))])

def test_migration(pop, m_rates, expected):

    result =  migration.migrating_pop(pop, m_rates)
    tm.assert_frame_equal(result, expected)
