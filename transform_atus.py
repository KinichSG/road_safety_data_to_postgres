import pandas as pd
from sqlalchemy import create_engine, VARCHAR, INT
import psycopg2
from psycopg2 import sql

connection_ori = {
    'user': 'seguridad_vial',
    'password': 'popcorning',
    'host': 'localhost',
    'port': 5432,
    'dbname': 'seguridad_vial'
}
connection_dest = {
    'user': 'seguridad_vial',
    'password': 'popcorning',
    'host': 'localhost',
    'port': 5432,
    'dbname': 'road_safety_mexico'
}
period = '9722'
schema = f'atus_{period}'

def Create_Schema(connection_dest, schema):
    connection = list(connection_dest.items())
    connection = [str(i[0])+'='+str(i[1]) for i in connection]
    connection = ' '.join(connection)

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            query = sql.SQL("""
                            CREATE SCHEMA IF NOT EXISTS {0};
                            """).format(sql.Identifier(schema))
            cur.execute(query=query)
    print(f'Schema {schema} created')

def Create_atus_tables(connection_ori, connection_dest, schema):
    engine_ori = create_engine(
        'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(
            connection_ori['user'],
            connection_ori['password'],
            connection_ori['host'],
            connection_ori['port'],
            connection_ori['dbname']))
    
    df = pd.read_sql_query(
        sql = '''
            SET search_path='{0}';
            SELECT 
                tr_cifra."ID_ENTIDAD",
                tc_entidad."NOM_ENTIDAD",
                tr_cifra."ID_MUNICIPIO",
                tc_municipio."NOM_MUNICIPIO",
                "ANIO",
                SUM("CONDMUERTO")+SUM("PASAMUERTO")+SUM("PEATMUERTO")+SUM("CICLMUERTO")+SUM("OTROMUERTO")+SUM("NEMUERTO") AS "MUERTOS",
                COUNT(*) AS "ACCIDENTES"
            FROM tr_cifra
            LEFT JOIN tc_entidad
            ON tc_entidad."ID_ENTIDAD"=tr_cifra."ID_ENTIDAD"
            LEFT JOIN tc_municipio
            ON tc_municipio."ID_ENTIDAD"=tr_cifra."ID_ENTIDAD" AND tc_municipio."ID_MUNICIPIO"=tr_cifra."ID_MUNICIPIO"
            WHERE "CLASACC"='Fatal'
            GROUP BY tr_cifra."ID_ENTIDAD", tc_entidad."NOM_ENTIDAD", tr_cifra."ID_MUNICIPIO", tc_municipio."NOM_MUNICIPIO", "ANIO"
            ORDER BY tr_cifra."ID_ENTIDAD", tr_cifra."ID_MUNICIPIO", "ANIO" ASC;
            '''.format(schema),
        con = engine_ori)
    
    engine_dest = create_engine(
        'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(
            connection_dest['user'],
            connection_dest['password'],
            connection_dest['host'],
            connection_dest['port'],
            connection_dest['dbname']))

    dic_col = {'MUERTOS':'accifat', 'ACCIDENTES':'victfat'}
    for col in ['MUERTOS', 'ACCIDENTES']:
        df_col = df.drop(columns=col)
        df_col = df_col.pivot_table(index=['ID_ENTIDAD', 'NOM_ENTIDAD', 'ID_MUNICIPIO', 'NOM_MUNICIPIO'], columns='ANIO')
        df_col.columns = df_col.columns.droplevel(0)
        df_col.reset_index(inplace=True)
        dtype = {key:INT() for key in df_col.columns}
        dtype['ID_ENTIDAD'] = VARCHAR(2)
        dtype['NOM_ENTIDAD'] = VARCHAR(150)
        dtype['ID_MUNICIPIO'] = VARCHAR(3)
        dtype['NOM_MUNICIPIO'] = VARCHAR(150)
        df_col.to_sql(name = f'{dic_col[col]}_municipal_{period}',
                      con = engine_dest,
                      schema = schema,
                      if_exists = 'replace',
                      index = False,
                      dtype = dtype)
        print(f'{schema}.{dic_col[col]}_municipal_{period} created')

        df_ent = df_col.groupby(['ID_ENTIDAD', 'NOM_ENTIDAD']).sum(numeric_only=True)
        df_ent.reset_index(inplace=True)
        df_ent.to_sql(name = f'{dic_col[col]}_estatal_{period}',
                      con = engine_dest,
                      schema = schema,
                      if_exists = 'replace',
                      index = False,
                      dtype = dtype)
        print(f'{schema}.{dic_col[col]}_estatal_{period} created')

if __name__ == '__main__':
    Create_Schema(connection_dest, schema)
    Create_atus_tables(connection_ori, connection_dest, schema)