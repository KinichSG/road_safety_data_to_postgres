import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, VARCHAR, INTEGER

host = 'localhost'
dbname = 'seguridad_vial'
user = 'seguridad_vial'
port = 5432
password = 'popcorning'
period = '8022'
schema_source = f'vmrc_{period}'
schema_destination = f'vmrc_{period}'

def Create_Schema(user, password, host, port, dbname, schema_destination):
    with psycopg2.connect(user=user, password=password, host=host, port=port, dbname=dbname, options=f'-c search_path={schema_destination}') as conn:
        with conn.cursor() as cur:
            query = sql.SQL("""
                CREATE SCHEMA IF NOT EXISTS {0}
                """).format(sql.Identifier(schema_destination))
            cur.execute(query=query)
    print(f'Schema {schema_destination} created')

def Create_vmrc_tables(user, password, host, port, dbname, schema_source, schema_destination):
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')

    df = pd.read_sql_table(
        table_name='tr_cifra',
        con=engine,
        schema=schema_source)
    df = pd.read_sql_query(
        sql='''
            SET search_path='vmrc_8022';
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
            ''',
        con = engine)

    for vehi_clas in ['AUTO', 'CAM_PAS', 'CYC_CARGA', 'MOTO']:
        val_cols = df.columns[df.columns.str.startswith(vehi_clas)]
        # Entidades
        cols_ent = ['ANIO', 'ID_ENTIDAD', 'NOM_ENTIDAD']
        df_ent = df.loc[:, cols_ent]
        df_ent[vehi_clas+'_TOT'] = df.loc[:, val_cols].sum(axis=1)
        df_ent = df_ent.pivot_table(index=['ID_ENTIDAD', 'NOM_ENTIDAD'], columns=['ANIO'])
        df_ent = df_ent.droplevel(0, axis=1)
        df_ent.reset_index(inplace=True)
        dtype_ent = {key:INTEGER() for key in df_ent.columns}
        dtype_ent['ID_ENTIDAD'] = VARCHAR(2)
        dtype_ent['NOM_ENTIDAD'] = VARCHAR(150)
        df_ent.to_sql(name=f'{vehi_clas.lower()}_ent_{period}', con=engine, schema=schema_destination,
                      if_exists='replace', index=False, dtype=dtype_ent)
        print(f'{schema_destination}.{vehi_clas.lower()}_ent_{period} created')
        # Municipios
        cols_mun = ['ANIO', 'ID_ENTIDAD', 'NOM_ENTIDAD', 'ID_MUNICIPIO', 'NOM_MUNICIPIO']
        df_mun = df.loc[:, cols_mun]
        df_mun[vehi_clas+'_TOT'] = df.loc[:, val_cols].sum(axis=1)
        df_mun = df_mun.pivot_table(index=['ID_ENTIDAD', 'NOM_ENTIDAD', 'ID_MUNICIPIO', 'NOM_MUNICIPIO'], columns=['ANIO'])
        df_mun = df_mun.droplevel(0, axis=1)
        df_mun.reset_index(inplace=True)
        dtype_mun = {key:INTEGER() for key in cols_mun}
        dtype_mun['ID_ENTIDAD'] = VARCHAR(2)
        dtype_mun['NOM_ENTIDAD'] = VARCHAR(150)
        dtype_mun['ID_MUNICIPIO'] = VARCHAR(3)
        dtype_mun['NOM_MUNICIPIO'] = VARCHAR(150)
        df_mun.to_sql(name=f'{vehi_clas.lower()}_mun_{period}', con=engine, schema=schema_destination,
                      if_exists='replace', index=False, dtype=dtype_mun)
        print(f'{schema_destination}.{vehi_clas.lower()}_mun_{period} created')

if __name__ == '__main__':
    Create_Schema(user, password, host, port, dbname, schema_destination)
    Create_vmrc_tables(user, password, host, port, dbname, schema_source, schema_destination)