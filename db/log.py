from sqlalchemy.orm import sessionmaker
from forecast import util
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from pysandag.database import get_connection_string

import os


def new_run(name='runs'):
    Base = declarative_base()
    table_name = name
    class Run(Base):
        __tablename__ = table_name
        __table_args__ = {'schema': 'defm'}
        # define columns for the table
        id = Column(Integer, primary_key=True)
        base_rate_version = Column(Integer)
        birth_rate_version = Column(Integer)
        death_rate_version = Column(Integer)
        migration_rate_version = Column(Integer)
        householder_rate_version = Column(Integer)

    db_dir = 'results/'
    if not os.path.exists(db_dir):

        os.makedirs(db_dir)

    engine = create_engine(get_connection_string("model_config.yml", 'output_database'))

    if not engine.has_table(table_name):
        Base.metadata.create_all(engine)

    db_session = sessionmaker(bind=engine)
    session = db_session()

    # Rate versions from yml file
    rate_versions = util.yaml_to_dict('model_config.yml', 'rate_versions')

    # Insert versions in database
    model_run = Run(
        base_rate_version=rate_versions['population'],
        birth_rate_version=rate_versions['birth'],
        death_rate_version=rate_versions['death'],
        migration_rate_version=rate_versions['migration'],
        householder_rate_version=rate_versions['householder'])

    session.add(model_run)
    session.commit()
    run_id = model_run.id
    return run_id


def insert_run(db_name,model_run_id,df_results,table_name):

    engine = create_engine(get_connection_string("model_config.yml", 'output_database'))

    # Insert prediction in the population table
    df_results['run_id'] = model_run_id # foreign key to run log table
    df_results.to_sql(name=table_name, con=engine, schema='defm', if_exists = 'append', index=True)
    df_results = df_results.drop('run_id', 1) # remove run_id
