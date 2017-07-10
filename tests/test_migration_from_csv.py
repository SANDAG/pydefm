import pytest
import pandas as pd
import pandas.util.testing as tm
from pydefm import migration
from pydefm import compute
import numpy as np


@pytest.fixture
def pop():
    return pd.read_csv('tests/data/migration/population.csv',index_col=[0,1,2])


# @pytest.fixture
# def m_rates():
#     return pd.read_csv('tests/data/input/migration_rates.csv',index_col=[0,1,2])


@pytest.fixture
def pop_w_mig_rates():
    return pd.read_csv('tests/data/migration/population_w_migration_rates.csv',index_col=[0,1,2])

@pytest.fixture
def pop_w_death_rates():
    return pd.read_csv('tests/data/deaths/population_w_death_rates.csv',index_col=[0,1,2])

@pytest.fixture
def pop_in():
    return pd.read_csv('tests/data/migration/population_in_migrating.csv',index_col=[0,1,2],dtype = {'mig_Din':  np.float64, 'mig_Fin': np.float64})

@pytest.fixture
def pop_out():
    return pd.read_csv('tests/data/migration/population_out_migrating.csv',index_col=[0,1,2],dtype = {'mig_Dout':  np.float64})

@pytest.fixture
def pop_dead():
    return pd.read_csv('tests/data/deaths/population_deaths.csv',index_col=[0,1,2],dtype = {'deaths':  np.float64})

@pytest.fixture
def pop_non_mig():
    return pd.read_csv('tests/data/migration/non_migrating_population.csv',index_col=[0,1,2],dtype = {'mig_Dout':  np.float64,'mig_Fout':  np.float64,'non_mig_pop': np.float64})


@pytest.fixture
def survived_pop():
    return pd.read_csv('tests/data/deaths/non_migrating_survived_population.csv',index_col=[0,1,2],dtype = {'mig_Dout':  np.float64,
                                            'mig_Fout':  np.float64,
                                            'non_mig_pop': np.float64,
                                            'mig_Fout':  np.float64,
                                            'non_mig_pop': np.float64,
                                            'deaths':  np.float64,
                                            'non_mig_survived_pop': np.float64})

@pytest.fixture
def non_mig_survived_aged_pop():
    return pd.read_csv('tests/data/aged/non_migrating_survived_aged_population.csv',index_col=[0,1,2],dtype = {'non_mig_survived_pop': np.float64})


# @pytest.fixture
# def pop_out():
#     return pd.read_csv('tests/data/pop_Dout_Fout.csv',index_col=[0,1,2])


# @pytest.mark.parametrize(
#     "expected",
#     [pd.read_csv('tests/data/output/population_w_migration_rates.csv',index_col=[0,1,2])])
# def test_migration(pop, m_rates, expected):
#     result =  migration.migrating_pop(pop, m_rates)
#     tm.assert_frame_equal(result, expected)


# @pytest.mark.parametrize(
#     "expected",
#     [pd.read_csv('tests/data/migrating_pop/non_migrating_pop.csv',index_col=[0,1,2],dtype = {'mig_Dout':  np.float64, 'non_mig_pop': np.float64})])
# def test_non_migrating_pop(joined_pop_rates, expected):
#     result = migration.non_migrating_pop(joined_pop_rates)
#     tm.assert_frame_equal(result, expected)
#


def test_in_migrating_population(pop_w_mig_rates, pop_in):
    result = compute.in_migrating_population(pop_w_mig_rates)
    result.to_csv('tests/data/result_out_mig_changes.csv')
    tm.assert_frame_equal(result, pop_in)


def test_out_migrating_population(pop_w_mig_rates, pop_out):
    result = compute.out_migrating_population(pop_w_mig_rates)
    tm.assert_frame_equal(result, pop_out)


def test_non_migrating_population(pop,pop_out,pop_non_mig):
    result = compute.non_migrating_population(pop,pop_out)
    tm.assert_frame_equal(result, pop_non_mig)


def test_dead_population(pop_w_death_rates, pop_dead):
    result = compute.dead_population(pop_w_death_rates)
    tm.assert_frame_equal(result, pop_dead)


@pytest.mark.parametrize(
    "expected",
    [pd.read_csv('tests/data/deaths/non_migrating_survived_population.csv',
                 index_col=[0,1,2],dtype = {'mig_Dout':  np.float64,
                                            'mig_Fout':  np.float64,
                                            'non_mig_pop': np.float64,
                                            'mig_Fout':  np.float64,
                                            'non_mig_pop': np.float64,
                                            'deaths':  np.float64,
                                            'non_mig_survived_pop': np.float64})])
def test_non_migrating_survived_pop(pop_non_mig,pop_dead,expected):
    result = compute.non_migrating_survived_pop(pop_non_mig,pop_dead)
    tm.assert_frame_equal(result, expected)


def test_aged_pop(survived_pop, non_mig_survived_aged_pop):
    result = compute.aged_pop(survived_pop)
    result.to_csv('tests/data/aged_result.csv')
    result.dtypes
    tm.assert_frame_equal(result, non_mig_survived_aged_pop)
