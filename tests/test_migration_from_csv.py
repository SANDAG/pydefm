import pytest
import pandas as pd
import pandas.util.testing as tm
from pydefm import migration
from pydefm import compute
import numpy as np


@pytest.fixture
def pop():
    return pd.read_csv('tests/data/population/population.csv',index_col=[0,1,2])


@pytest.fixture
def pop_w_mig_rates():
    return pd.read_csv('tests/data/population/population_w_migration_rates.csv',index_col=[0,1,2])

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
    return pd.read_csv('tests/data/population/population_minus_out_migration.csv',index_col=[0,1,2],dtype = {'mig_Dout':  np.float64,'mig_Fout':  np.float64,'non_mig_pop': np.float64})


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


@pytest.fixture
def newborns_and_in_migration():
    return pd.read_csv('tests/data/newborns/new_borns_and_in_migration.csv',index_col=[0,1,2])


@pytest.fixture
def new_pop():
    return pd.read_csv('tests/data/newborns/new_population.csv',index_col=[0,1,2])

@pytest.fixture
def non_mig_and_new():
    return pd.read_csv('tests/data/population/non_mig_population_and_new_pop.csv',index_col=[0,1,2])

@pytest.fixture
def final_pop():
    return pd.read_csv('tests/data/population/final_population.csv',index_col=[0,1,2],dtype = {'persons': np.float64})


@pytest.fixture
def random_numbers():
    return pd.read_csv('tests/data/newborns/random_numbers_2026.csv',index_col=[0,1,2])

@pytest.fixture
def births():
    return pd.read_csv('tests/data/newborns/births.csv',index_col=[0,1,2],dtype = {
                                            'births_rounded':  np.float64,
                                            'births_f': np.float64})

@pytest.fixture
def pop_w_birth_rates():
    return pd.read_csv('tests/data/population/non_migrating_pop_w_birth_rates.csv',index_col=[0,1,2])



def test_in_migrating_population(pop_w_mig_rates, pop_in):
    result = compute.in_migrating_population(pop_w_mig_rates)
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
    tm.assert_frame_equal(result, non_mig_survived_aged_pop)


def test_new_pop(newborns_and_in_migration, new_pop):
    result = compute.new_population(newborns_and_in_migration)
    tm.assert_frame_equal(result, new_pop)

def test_final_population(non_mig_and_new, final_pop):
    result = compute.final_population(non_mig_and_new)
    tm.assert_frame_equal(result, final_pop)

def test_births_all(pop_w_birth_rates,random_numbers,births):
    result = compute.births_all(pop_w_birth_rates,random_numbers,'non_mig_pop')
    tm.assert_frame_equal(result, births)
