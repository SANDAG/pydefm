from sqlalchemy.orm import sessionmaker
from forecast import util
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine


def insert_run(pop_df):
    Base = declarative_base()
    class Run(Base):
        __tablename__ = 'run_log'
        # Here we define columns for the run log
        # Notice that each column is also a normal Python instance attribute.
        id = Column(Integer, primary_key=True)
        base_id = Column(Integer)
        birth_rate_id = Column(Integer)
        death_rate_id = Column(Integer)
        migration_rate_id = Column(Integer)
        householder_rate_id = Column(Integer)
        version_id = Column(Integer)
        notes = Column(String(250))

    engine = create_engine('sqlite:///model_run.db')

    if not engine.has_table('run_log'):
        Base.metadata.create_all(engine)

    db_session = sessionmaker(bind=engine)
    # A DBSession() instance establishes all conversations with the database
    # and represents a "staging zone" for all the objects loaded into the
    # database session object. Any change made against the objects in the
    # session won't be persisted into the database until you call
    # session.commit(). If you're not happy about the changes, you can
    # revert all of them back to the last commit by calling
    # session.rollback()
    session = db_session()

    # Insert a run in the run log table
    rate_versions = util.yaml_to_dict('model_config.yml', 'rate_versions')

    new_run = Run(base_id=rate_versions['population'],
                          birth_rate_id=rate_versions['birth'],
                          death_rate_id=rate_versions['death'],
                          migration_rate_id=rate_versions['migration'],
                          householder_rate_id=rate_versions['householder'])
    session.add(new_run)
    session.commit()
    model_run_id = new_run.id

    # Insert prediction in the population table
    pop_df['run_id'] = model_run_id # foreign key to run log table
    pop_df = pop_df.reset_index(drop=False)
    pop_df.to_sql(name='population', con=engine, if_exists = 'append', index=False)

    session.commit()