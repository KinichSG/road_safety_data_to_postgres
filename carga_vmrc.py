import psycopg2
from psycopg2 import sql
from pathlib import Path
import pandas as pd

host = 'localhost'
dbname = 'seguridad_vial'
user = 'seguridad_vial'
port = 5432
password = 'popcorning'
schema_name = 'vmrc_8022'
dir_vmrc_anual_csv = 'vmrc_anual_csv'

def create_schema(host, dbname, user, port, password, schema_name):
    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            print(f'Creating schema: {schema_name}')
            query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {0}").format(sql.Identifier(schema_name)
            )
            cur.execute(query)

def create_tc_entidad(host, dbname, user, port, password, schema_name):
    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            print(f'Creating table: {schema_name}.tc_entidad')
            query = sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS tc_entidad(
                    "ID_ENTIDAD" varchar(2) PRIMARY KEY,
                    "NOM_ENTIDAD" varchar(150)
                );
                """
                )
            cur.execute(query)

def create_tc_municipio(host, dbname, user, port, password, schema_name):
    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            print(f'Creating table: {schema_name}.tc_municipio')
            query = sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS tc_municipio(
                    "ID_ENTIDAD" varchar(2),
                    "ID_MUNICIPIO" varchar(3),
                    "NOM_MUNICIPIO" varchar(150),
                    PRIMARY KEY("ID_ENTIDAD", "ID_MUNICIPIO"),
                    FOREIGN KEY("ID_ENTIDAD") REFERENCES tc_entidad
                );
                """
            )
            cur.execute(query)

def create_tr_cifra(host, dbname, user, port, password, schema_name):
    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            print(f'Creating table: {schema_name}.tr_cifra')
            query = sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS tr_cifra(
                    "PROD_EST" varchar(200),
                    "COBERTURA" varchar(200),
                    "ANIO" int,
                    "ID_ENTIDAD" varchar(2),
                    "ID_MUNICIPIO" varchar(3),
                    "AUTO_OFICIAL" int,
                    "AUTO_PUBLICO" int,
                    "AUTO_PARTICULAR" int,
                    "CAM_PAS_OFICIAL" int,
                    "CAM_PAS_PUBLICO" int,
                    "CAM_PAS_PARTICULAR" int,
                    "CYC_CARGA_OFICIAL" int,
                    "CYC_CARGA_PUBLICO" int,
                    "CYC_CARGA_PARTICULAR" int,
                    "MOTO_OFICIAL" int,
                    "MOTO_DE_ALQUILER" int,
                    "MOTO_PARTICULAR" int,
                    "ESTATUS" varchar(20),
                    PRIMARY KEY("ANIO", "ID_ENTIDAD", "ID_MUNICIPIO"),
                    FOREIGN KEY("ID_ENTIDAD", "ID_MUNICIPIO") REFERENCES tc_municipio
                );
                """
            )
            cur.execute(query)

def load_tc_entidad(host, dbname, user, port, password, schema_name):
    path = Path().absolute().joinpath(f'{dir_vmrc_anual_csv}/catalogos/tc_entidad.csv')
    path = str(path)
    
    df = pd.read_csv(path, dtype=str, index_col=False)
    df.ID_ENTIDAD = df.ID_ENTIDAD.map(lambda x: x[-2:])
    df.to_csv(path, index=False)

    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            query_val = sql.SQL(
                """
                SELECT count(*)
                FROM tc_entidad;
                """
            )
            cur.execute(query_val)
            if cur.fetchall()[0][0] > 0:
                print(f'Table {schema_name}.tc_entidad already exists')
                return None
    
    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            copy_sql = sql.SQL(
                """
                COPY tc_entidad
                FROM STDIN
                DELIMITER ',' CSV HEADER;
                """
            )
            with open(path, 'r') as file:
                cur.copy_expert(sql=copy_sql, file=file)

def load_tc_municipio(host, dbname, user, port, password, schema_name):
    path = Path().absolute().joinpath(f'{dir_vmrc_anual_csv}/catalogos/tc_municipio.csv')
    path = str(path)

    df = pd.read_csv(path, dtype=str, index_col=False, delimiter=',')
    df.ID_ENTIDAD = df.ID_ENTIDAD.map(lambda x: x[-2:])
    df.ID_MUNICIPIO = df.ID_MUNICIPIO.map(lambda x: x[-3:])
    df = pd.concat([df, pd.DataFrame([['17', '036', 'Hueyapan']], columns=df.columns)])
    df.drop_duplicates(inplace=True)
    df.sort_values(['ID_ENTIDAD', 'ID_MUNICIPIO'], inplace=True)
    df.to_csv(path, index=False)

    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            query_val = sql.SQL(
                """
                Select count(*)
                FROM tc_municipio;
                """
            )
            cur.execute(query_val)
            if cur.fetchall()[0][0] > 0:
                print(f'Table {schema_name}.tc_municipio already exists')
                return
    
    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            copy_sql = sql.SQL(
                """
                COPY tc_municipio
                FROM STDIN
                DELIMITER ',' CSV HEADER;
                """
            )
            with open(path, 'r') as file:
                cur.copy_expert(sql=copy_sql, file=file)

def load_tr_cifra(host, dbname, user, port, password, schema_name):
    path = Path().absolute().joinpath(f'{dir_vmrc_anual_csv}/conjunto_de_datos')

    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            query_val = sql.SQL(
                """
                Select count(*)
                FROM tr_cifra;
                """
            )
            cur.execute(query_val)
            if cur.fetchall()[0][0] > 0:
                print(f'Table {schema_name}.tr_cifra already exists')
                return

    for csv in path.iterdir():
        if csv.suffix == '.csv':
            print(f'Correcting table: {str(csv)}')
            df = pd.read_csv(str(csv), dtype=str, index_col=False)
            df.ID_ENTIDAD = df.ID_ENTIDAD.map(lambda x: x[-2:])
            df.ID_MUNICIPIO = df.ID_MUNICIPIO.map(lambda x: x[-3:])
            df.to_csv(str(csv), index=False)
            
    for csv in path.iterdir():
        if csv.suffix == '.csv':
            with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
                with conn.cursor() as cur:
                    copy_sql = sql.SQL(
                        """
                        COPY tr_cifra
                        FROM STDIN
                        DELIMITER ',' CSV HEADER;
                        """
                    )
                    print(f'Copying table: {schema_name}.tr_cifra')
                    with open(csv, 'r') as file:
                        cur.copy_expert(sql=copy_sql, file=file)

if __name__ == '__main__':
    create_schema(host, dbname, user, port, password, schema_name)
    create_tc_entidad(host, dbname, user, port, password, schema_name)
    create_tc_municipio(host, dbname, user, port, password, schema_name)
    create_tr_cifra(host, dbname, user, port, password, schema_name)
    load_tc_entidad(host, dbname, user, port, password, schema_name)
    load_tc_municipio(host, dbname, user, port, password, schema_name)
    load_tr_cifra(host, dbname, user, port, password, schema_name)