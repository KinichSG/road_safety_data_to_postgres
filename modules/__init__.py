from sqlalchemy import create_engine
from sqlalchemy.schema import CreateSchema

# def create_engine_string(connction_vars, dialect='postgresql', dbapi='psycopg2'):
def create_engine_string(connection_vars, dialect='postgresql'):
    # engine_string = '{0}+{1}://{2}:{3}@{4}:{5}/{6}'.format(
    engine_string = '{0}://{1}:{2}@{3}:{4}/{5}'.format(
        dialect,
        # dbapi,
        connection_vars['user'],
        connection_vars['password'],
        connection_vars['host'],
        connection_vars['port'],
        connection_vars['db_name']
    )
    return engine_string

def create_schema(connection_vars, schema_name, dialect='postgresql'):
    engine_string = create_engine_string(connection_vars, dialect)
    engine = create_engine(engine_string)
    with engine.connect() as conn:
        if not conn.dialect.has_schema(conn, schema_name):
            print('si')
            conn.execute(CreateSchema(schema_name))
        conn.commit()