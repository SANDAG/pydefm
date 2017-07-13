import numpy as np
import pandas as pd


def rates_for_yr(population, rates_all_years, sim_year):
    """
    Filter specific rates for a given year and join with population
    by cohort

    Parameters
    ----------
    population : pandas DataFrame
        population for simulated year, starting with base population
    rates_all_years : pandas DataFrame
        rates, to be filtered by year
    sim_year : int
        year being simulated

    Returns
    -------
    pop_w_rates : pandas DataFrame
        rates for a given year and population for each cohort

    """
    rates_yr = rates_all_years[rates_all_years['yr'] == sim_year]
    pop_w_rates = population.join(rates_yr)
    return pop_w_rates
