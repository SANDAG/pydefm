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

random_numbers = """
SELECT [yr]
      ,[age]
      ,[race] as race_ethn
      ,[sex]
      ,[random_number]
  FROM %s
  WHERE 1 = %s
  """

householder = """
SELECT [yr]
      ,[age]
      ,[race] as race_ethn
      ,[sex]
      ,[householder_rate]
  FROM %s
  WHERE [householder_rate_id] = %s
      """

dem_sim_rates = '''
SELECT [demographic_simulation_id]
      ,[base_population_id]
      ,[migration_rate_id]
      ,[birth_rate_id]
      ,[death_rate_id]
      ,[householder_rate_id]
      ,[start_yr]
      ,[end_yr]
  FROM [isam].[demographic_rates].[demographic_scenarios]
  '''
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

inc_mil_gc_pop = """
SELECT  age,
        race_ethn,
        sex,
        persons,
        yr
FROM    %s
WHERE   run_id = %s and type = 'MIL'
"""

inc_mil_hh_pop = """
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

out_commuting = '''
SELECT [yr]
      ,[wtlh_lh]
  FROM %s
  WHERE out_commuting_id = %s
  '''

in_commuting = '''
SELECT TOP 41 [yr]
      ,[wh_whlh]
  FROM %s
  WHERE in_commuting_id = %s'''

local_jobs = '''
SELECT [yr]
      ,[jlw]
  FROM %s
  WHERE local_jobs_id = %s
  '''

sectoral_share = '''
SELECT [yr]
      ,[sandag_sector]
      ,[share]
  FROM %s
  WHERE sectoral_share_id = %s'''

sectoral_pay = '''
SELECT [yr]
      ,[sandag_sector]
      ,[annual_pay]
  FROM %s
  WHERE sectoral_pay_id = %s'''

mil_pay = '''
SELECT yr
      ,[annual_pay_hp]
      ,[annual_pay_gq]
  FROM %s
  WHERE [military_pay_id] = %s'''


max_run_id = '''
SELECT max(id)
  FROM defm.runs
'''

aigr = '''
SELECT [aigr]
  FROM %s
  WHERE aigr_id = %s'''

aigrm = '''
SELECT [aigrm]
  FROM %s
  WHERE aigrm_id = %s'''