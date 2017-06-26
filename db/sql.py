"""
SQL queries to get input data for model
    variables (%s): table names and rate versions(%s)
    values: declared in "model_config_yml"
Note: rate versions refer to different data sources
"""

# BASE POPULATION (2010)
population = """
SELECT  age,
        r as race_ethn,
        sex,
        type,
        COALESCE(mildep, 'N')as mildep,
        persons,
        COALESCE(households, 0) as households
FROM    %s
WHERE   base_population_id = %s
"""

# MIGRATION RATES
# for DIN(domestic in), DOUT(domestic out), FIN(foreign in), FOUT(foreign out)
migration = """
SELECT
        yr,
        age,
        race as race_ethn,
        sex,
        DIN,
        DOUT,
        FIN,
        FOUT
FROM    %s
WHERE   migration_rate_id = %s
"""

# BIRTH RATES
birth = """
SELECT  age,
        race as race_ethn,
        sex = 'F',
        birth_rate,
        yr
FROM    %s
WHERE   birth_rate_id = %s
"""

# DEATH RATES
death = """
SELECT  yr,
        age,
        race as race_ethn,
        sex,
        death_rate
FROM    %s
WHERE   death_rate_id = %s
"""

# SPECIAL CASE: INS
ins = """
SELECT  yr,
        age,
        race_ethn,
        sex,
        rate as case_ratio
FROM    %s
WHERE   rate_type = 'INS' AND
        rate_version = %s
"""


# SPECIAL CASE: OTH
oth = """
SELECT  yr,
        age,
        race_ethn,
        sex,
        rate as case_ratio
FROM    %s
WHERE   rate_type = 'OTH' AND
        rate_version = %s
"""

inc_pop = """
SELECT  age,
        race_ethn,
        sex,
        type,
        mildep,
        persons,
        households,
        yr
FROM    %s
WHERE   run_id = %s and type = 'HHP'
"""


inc_pop_mil = """
SELECT  age,
        r as race_ethn,
        sex,
        COALESCE(mildep, 'N')as mildep,
        persons - not_mil as mil_mildep
FROM    %s
WHERE   base_population_id = %s and type = 'HHP'
"""

inc_shares = """
SELECT yr
      ,age_cat
      ,income_type
      ,income
      ,share
FROM %s
WHERE income_id = %s
"""

lfp_rates = """
SELECT [yr]
      ,[age_cat]
      ,[sex]
      ,[race] as race_ethn
      ,[lfpr]
  FROM %s
  WHERE lfpr_id = %s
"""

cohort_ur = """
SELECT [yr]
      ,[age_cat]
      ,[sex]
      ,[race] as race_ethn
      ,[ur2]
  FROM %s
  WHERE [ur2_id] = %s
  """

yearly_ur = """
SELECT [yr]
      ,[ur1]
  FROM %s
  WHERE ur1_id = %s
  """