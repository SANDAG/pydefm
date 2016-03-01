import pandas as pd
from sqlalchemy import create_engine
from pysandag import database
from db import sql
from forecast import util


def create_df(data_type,db_table):
    """
    Create pandas DataFrame from database query to select base population
    or rate versions to be used in model.

    Args:
        data_type (string): type of data (e.g. birth, migration, population)
        db_table (string): database table name

    Returns:
        df_sql_result: pandas DataFrame with sql query result
    """

    # connect to database using SQLAlchemy
    db_connection_string = database.get_connection_string('model_config.yml', 'in_db')
    sql_in_engine = create_engine(db_connection_string)

    # retrieve rate versions for current model and database table names to query
    rate_versions = util.yaml_to_dict('model_config.yml', 'rate_versions')
    tables = util.yaml_to_dict('model_config.yml', 'db_tables')

    # build query
    in_query = getattr(sql,data_type) % (tables[db_table],rate_versions[data_type])

    # pandas DataFrame from query
    df_sql_result = pd.read_sql(in_query, sql_in_engine)

    # MultiIndex on cohort attributes
    df_sql_result = df_sql_result.set_index(['age','race_ethn','sex'])

    return df_sql_result
