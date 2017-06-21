import pytest
from pandas import DataFrame
import pandas.util.testing as tm
from pydefm import migration

@pytest.fixture
def pop():
    return DataFrame({'persons': [859, 73, 4493]}, index=[2, 1, 0])


@pytest.fixture
def m_rates():
    return DataFrame({'DOUT': [0.027663, 0.057815, 0.012424],'FOUT': [0.000000, 0.000000, 0.000000]}, index=[3, 1, 2])


@pytest.mark.parametrize(
    "expected",
    [(DataFrame([[859,0.012424,0.000000],
                 [73, 0.057815,0.000000],
                 [4493, np.nan,np.nan]],
                columns=['persons', 'DOUT','FOUT'], index=[2, 1, 0]))])
def test_migration(pop, m_rates, expected):
    result =  migration.migrating_pop(pop, m_rates)
    tm.assert_frame_equal(result, expected)
