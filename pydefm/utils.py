#  get rates for the simulation year
def rates_for_yr(rates_all_years, sim_year):
    """
    Filter specific rates for a given year

    Parameters
    ----------
    rates_all_years : pandas DataFrame
        rates, to be filtered by year
    sim_year : int
        year being simulated

    Returns
    -------
    pop_w_rates : pandas DataFrame
        rates for a given year

    """
    rates_yr = rates_all_years[rates_all_years['yr'] == sim_year]
    return rates_yr


# join the rates with the population
def population_w_rates(population, rates_for_yr):
    """
    Join rates for simulation year with population by cohort

    Parameters
    ----------
    population : pandas DataFrame
        population for simulated year, starting with base population

    rates_for_yr : pandas DataFrame
        rates for simulation year by cohort

    Returns
    -------
    pop_w_rates : pandas DataFrame
        rates for a given year and population for each cohort


    """
    return population.join(rates_for_yr, how='left', sort=False)


