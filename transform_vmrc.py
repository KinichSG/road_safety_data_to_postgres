import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, VARCHAR, INT

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
period = '8022'
schema = f'vmrc_{period}'

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

def Create_vmrc_total(connection_ori, connection_dest, schema):
    engine_ori = create_engine(
        'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(
            connection_ori['user'],
            connection_ori['password'],
            connection_ori['host'],
            connection_ori['port'],
            connection_ori['dbname']))

    df = pd.read_sql_query(
        sql="""
            SET search_path='{0}';
            SELECT
                tr_cifra."ID_ENTIDAD", "NOM_ENTIDAD",
                tr_cifra."ID_MUNICIPIO", "NOM_MUNICIPIO",
                "ANIO",
                "AUTO_OFICIAL", "AUTO_PUBLICO", "AUTO_PARTICULAR",
                "CAM_PAS_OFICIAL", "CAM_PAS_PUBLICO", "CAM_PAS_PARTICULAR",
                "CYC_CARGA_OFICIAL", "CYC_CARGA_PUBLICO", "CYC_CARGA_PARTICULAR",
                "MOTO_OFICIAL", "MOTO_DE_ALQUILER", "MOTO_PARTICULAR"
            FROM tr_cifra
            LEFT JOIN tc_entidad
            ON tc_entidad."ID_ENTIDAD" = tr_cifra."ID_ENTIDAD"
            LEFT JOIN tc_municipio
            ON tc_municipio."ID_MUNICIPIO" = tr_cifra."ID_MUNICIPIO"
            AND tc_municipio."ID_ENTIDAD" = tr_cifra."ID_ENTIDAD";
            """.format(schema),
        con = engine_ori)
    
    engine_dest = create_engine(
        'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(
            connection_dest['user'],
            connection_dest['password'],
            connection_dest['host'],
            connection_dest['port'],
            connection_dest['dbname']))

    df_mun = df.loc[:, ['ID_ENTIDAD', 'NOM_ENTIDAD', 'ID_MUNICIPIO', 'NOM_MUNICIPIO', 'ANIO']]
    df_mun['VEHI_TOT'] = df.loc[:, ~df.columns.isin(df_mun.columns)].sum(numeric_only=True, axis=1)
    df_mun = df_mun.pivot_table(index=['ID_ENTIDAD', 'NOM_ENTIDAD', 'ID_MUNICIPIO', 'NOM_MUNICIPIO'], columns=['ANIO'])
    df_mun.columns = df_mun.columns.droplevel(level=0)
    df_mun.reset_index(inplace=True) 
    dtype = {key:INT() for key in df_mun.columns}
    dtype['ID_ENTIDAD'] = VARCHAR(2)
    dtype['NOM_ENTIDAD'] = VARCHAR(150)
    dtype['ID_MUNICIPIO'] = VARCHAR(3)
    dtype['NOM_MUNICIPIO'] = VARCHAR(150)
    df_mun.to_sql(name=f'vehitot_municipal_{period}',
                  con=engine_dest,
                  schema=schema,
                  if_exists='replace',
                  index=False,
                  dtype=dtype)
    print(f'{schema}.vehitot_municipal created')

    df_ent = df_mun.groupby(['ID_ENTIDAD', 'NOM_ENTIDAD'], as_index=False).sum(numeric_only=True)
    df_ent.to_sql(name=f'vehitot_estatal_{period}',
                  con=engine_dest,
                  schema=schema,
                  if_exists='replace',
                  index=False,
                  dtype=dtype)
    print(f'{schema}.vehitot_estatal created')

if __name__ == '__main__':
    Create_Schema(connection_dest, schema)
    Create_vmrc_total(connection_ori, connection_dest, schema)

# def Create_vmrc_tables(user, password, host, port, dbname, schema_source, schema_destination):
#     engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')

#     df = pd.read_sql_query(
#         sql='''
#             SET search_path='{0}';
#             SELECT
#                 tr_cifra."ID_ENTIDAD", "NOM_ENTIDAD",
#                 tr_cifra."ID_MUNICIPIO", "NOM_MUNICIPIO",
#                 "ANIO",
#                 "AUTO_OFICIAL", "AUTO_PUBLICO", "AUTO_PARTICULAR",
#                 "CAM_PAS_OFICIAL", "CAM_PAS_PUBLICO", "CAM_PAS_PARTICULAR",
#                 "CYC_CARGA_OFICIAL", "CYC_CARGA_PUBLICO", "CYC_CARGA_PARTICULAR",
#                 "MOTO_OFICIAL", "MOTO_DE_ALQUILER", "MOTO_PARTICULAR"
#             FROM tr_cifra
#             LEFT JOIN tc_entidad
#             ON tc_entidad."ID_ENTIDAD" = tr_cifra."ID_ENTIDAD"
#             LEFT JOIN tc_municipio
#             ON tc_municipio."ID_MUNICIPIO" = tr_cifra."ID_MUNICIPIO"
#             AND tc_municipio."ID_ENTIDAD" = tr_cifra."ID_ENTIDAD";
#             '''.format({schema_source}),
#         con = engine)

#     for vehi_clas in ['AUTO', 'CAM_PAS', 'CYC_CARGA', 'MOTO']:
#         val_cols = df.columns[df.columns.str.startswith(vehi_clas)]
#         # Entidades
#         cols_ent = ['ANIO', 'ID_ENTIDAD', 'NOM_ENTIDAD']
#         df_ent = df.loc[:, cols_ent]
#         df_ent[vehi_clas+'_TOT'] = df.loc[:, val_cols].sum(axis=1)
#         df_ent = df_ent.pivot_table(index=['ID_ENTIDAD', 'NOM_ENTIDAD'], columns=['ANIO'])
#         df_ent = df_ent.droplevel(0, axis=1)
#         df_ent.reset_index(inplace=True)
#         dtype_ent = {key:INTEGER() for key in df_ent.columns}
#         dtype_ent['ID_ENTIDAD'] = VARCHAR(2)
#         dtype_ent['NOM_ENTIDAD'] = VARCHAR(150)
#         df_ent.to_sql(name=f'{vehi_clas.lower()}_ent_{period}', con=engine, schema=schema_destination,
#                       if_exists='replace', index=False, dtype=dtype_ent)
#         print(f'{schema_destination}.{vehi_clas.lower()}_ent_{period} created')
#         # Municipios
#         cols_mun = ['ANIO', 'ID_ENTIDAD', 'NOM_ENTIDAD', 'ID_MUNICIPIO', 'NOM_MUNICIPIO']
#         df_mun = df.loc[:, cols_mun]
#         df_mun[vehi_clas+'_TOT'] = df.loc[:, val_cols].sum(axis=1)
#         df_mun = df_mun.pivot_table(index=['ID_ENTIDAD', 'NOM_ENTIDAD', 'ID_MUNICIPIO', 'NOM_MUNICIPIO'], columns=['ANIO'])
#         df_mun = df_mun.droplevel(0, axis=1)
#         df_mun.reset_index(inplace=True)
#         dtype_mun = {key:INTEGER() for key in cols_mun}
#         dtype_mun['ID_ENTIDAD'] = VARCHAR(2)
#         dtype_mun['NOM_ENTIDAD'] = VARCHAR(150)
#         dtype_mun['ID_MUNICIPIO'] = VARCHAR(3)
#         dtype_mun['NOM_MUNICIPIO'] = VARCHAR(150)
#         df_mun.to_sql(name=f'{vehi_clas.lower()}_mun_{period}', con=engine, schema=schema_destination,
#                       if_exists='replace', index=False, dtype=dtype_mun)
#         print(f'{schema_destination}.{vehi_clas.lower()}_mun_{period} created')