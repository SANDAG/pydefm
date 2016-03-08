"""
SQL queries to get input data for model
    variables (%s): table names and rate versions(%s)
    values: declared in "model_config_yml"
Note: rate versions refer to different data sources
"""

# BASE POPULATION (2010)
population = """
SELECT  age,
        race_ethn,
        sex,
        type,
        mildep,
        persons,
        households
FROM    %s
WHERE   bp_version = %s
"""

# MIGRATION RATES
# for DIN(domestic in), DOUT(domestic out), FIN(foreign in), FOUT(foreign out)
migration = """
SELECT  rate_type AS migration,
        yr,
        age,
        race_ethn,
        sex,
        rate
FROM    %s
WHERE   rate_type IN ('DIN','DOUT','FIN','FOUT') AND
        rate_version = %s
"""

# BIRTH RATES
birth = """
SELECT  age,
        race_ethn,
        sex,
        rate as birth_rate,
        yr
FROM    %s
WHERE   rate_type = 'B' AND
        rate_version = %s
"""

# DEATH RATES
death = """
SELECT  yr,
        age,
        race_ethn,
        sex,
        rate as death_rate
FROM    %s
WHERE   rate_type = 'D' AND
        rate_version = %s
"""