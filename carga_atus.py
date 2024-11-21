import psycopg2
from psycopg2 import sql
from pathlib import Path
import pandas as pd

# connection = 'host=localhost dbname=seguridad_vial user=postgres port=5432 password=popcorning'
connection = 'host=localhost dbname=seguridad_vial user=seguridad_vial port=5432 password=popcorning'
host = 'localhost'
dbname = 'seguridad_vial'
user = 'seguridad_vial'
port = 5432
password = 'popcorning'
schema_name = 'atus_9722'
dir_atus_anual_csv = 'atus_anual_csv'

def create_schema(host, dbname, user, port, password, schema_name):
    query = sql.SQL(
        """
        CREATE SCHEMA IF NOT EXISTS {0}
        """.format(schema_name)
    )

    with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            print(f'Creating schema: {schema_name}')
            cur.execute(query)
            # cur.execute('SET search_path TO %s', [schema_name])

def create_tc_entidad(host, dbname, user, port, password, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS tc_entidad(
            "ID_ENTIDAD" varchar(2),
            "NOM_ENTIDAD" varchar(150),
            PRIMARY KEY("ID_ENTIDAD")
        );
        """
    )

    with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            print(f'Creating table: {schema_name}.tc_entidad')
            cur.execute(query)

def create_tc_municipio(host, dbname, user, port, password, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS tc_municipio(
            "ID_ENTIDAD" varchar(2),
            "ID_MUNICIPIO" varchar(3),
            "NOM_MUNICIPIO" varchar(150),
            PRIMARY KEY("ID_ENTIDAD", "ID_MUNICIPIO")
        );
        """
    )

    with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            print(f'Creating table: {schema_name}.tc_municipio')
            cur.execute(query)

def create_tc_periodo_mes(host, dbname, user, port, password, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS tc_periodo_mes(
            MES varchar(2),
            DESCRIPCION_MES varchar(30),
            PRIMARY KEY(MES)
        );
        """
    )

    with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            print(f'Creating table: {schema_name}.tc_periodo_mes')
            cur.execute(query)

def create_tc_hora(host, dbname, user, port, password, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS tc_hora(
            "ID_HORA" int,
            "DESC_HORA" varchar(50),
            PRIMARY KEY("ID_HORA")
        );
        """
    )

    with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            print(f'Creating table: {schema_name}.tc_hora')
            cur.execute(query)

def create_tc_dia(host, dbname, user, port, password, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS tc_dia(
            "ID_DIA" varchar(2),
            "DESC_DIA" varchar(50),
            PRIMARY KEY("ID_DIA")
        );
        """
    )

    with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            print(f'Creating table: {schema_name}.tc_dia')
            cur.execute(query)

def create_tc_minuto(host, dbname, user, port, password, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS tc_minuto(
            "ID_MINUTO" int,
            "DESC_MINUTO" varchar(50),
            PRIMARY KEY("ID_MINUTO")
        );
        """
    )

    with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            print(f'Creating table: {schema_name}.tc_minuto')
            cur.execute(query)

def create_tc_edad(host, dbname, user, port, password, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS tc_edad(
            "ID_EDAD" varchar(2),
            "DESC_EDAD" varchar(50),
            PRIMARY KEY("ID_EDAD")
        );
        """
    )

    with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            print(f'Creating table: {schema_name}.tc_edad')
            cur.execute(query)

def create_tr_cifra(host, dbname, user, port, password, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS tr_cifra(
                "ID_ATUS" serial,
                "COBERTURA" varchar(200),
                "ID_ENTIDAD" varchar(2),
                "ID_MUNICIPIO" varchar(3),
                "ANIO" int,
                "MES" varchar(2),
                "ID_HORA" int,
                "ID_MINUTO" int,
                "ID_DIA" varchar(2),
                "DIASEMANA" varchar(20),
                "URBANA" varchar(50),
                "SUBURBANA" varchar(50),
                "TIPACCID" varchar(100),
                "AUTOMOVIL" int,
                "CAMPASAJ" int,
                "MICROBUS" int,
                "PASCAMION" int,
                "OMNIBUS" int,
                "TRANVIA" int,
                "CAMIONETA" int,
                "CAMION" int,
                "TRACTOR" int,
                "FERROCARRI" int,
                "MOTOCICLET" int,
                "BICICLETA" int,
                "OTROVEHIC" int,
                "CAUSAACCI" varchar(50),
                "CAPAROD" varchar(50),
                "SEXO" varchar(20),
                "ALIENTO" varchar(20),
                "CINTURON" varchar(20),
                "ID_EDAD" varchar(2),
                "CONDMUERTO" int,
                "CONDHERIDO" int,
                "PASAMUERTO" int,
                "PASAHERIDO" int,
                "PEATMUERTO" int,
                "PEATHERIDO" int,
                "CICLMUERTO" int,
                "CICLHERIDO" int,
                "OTROMUERTO" int,
                "OTROHERIDO" int,
                "NEMUERTO" int,
                "NEHERIDO" int,
                "CLASACC" varchar(50),
                "ESTATUS" varchar(20),
                PRIMARY KEY("ID_ATUS"),
                FOREIGN KEY("ID_ENTIDAD") REFERENCES tc_entidad,
                FOREIGN KEY("ID_ENTIDAD", "ID_MUNICIPIO") REFERENCES tc_municipio,
                FOREIGN KEY("MES") REFERENCES tc_periodo_mes,
                FOREIGN KEY("ID_HORA") REFERENCES tc_hora,
                FOREIGN KEY("ID_DIA") REFERENCES tc_dia,
                FOREIGN KEY("ID_MINUTO") REFERENCES tc_minuto,
                FOREIGN KEY("ID_EDAD") REFERENCES tc_edad
        );
        """
    )

    with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            print(f'Creating table: {schema_name}.tr_cifra')
            cur.execute(query)

def load_tc_entidad(host, dbname, user, port, password, schema_name):
    query_val = sql.SQL(
        """
        SELECT count(*)
        FROM tc_entidad;
        """
    )

    with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            cur.execute(query_val)
            if cur.fetchall()[0][0] > 0:
                print(f'The table {schema_name}.tc_entidad is not empty')
                return None
    
    path = Path().absolute().joinpath(f'{dir_atus_anual_csv}/catalogos/tc_entidad.csv')
    path = str(path)

    copy_sql = """
        COPY tc_entidad
        FROM STDIN
        DELIMITER ','
        CSV HEADER;    
        """

    with open(path, 'r') as file:
        with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
            with conn.cursor() as cur:
                cur.copy_expert(sql=copy_sql, file=file)

def load_tc_municipio(host, dbname, user, port, password, schema_name):
    query_val = sql.SQL(
        """
        SELECT count(*)
        FROM tc_municipio;
        """
    )

    with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            cur.execute(query_val)
            if cur.fetchall()[0][0] > 0:
                print(f'The table {schema_name}.tc_municipio is not empty')
                return None
    
    path = Path().absolute().joinpath(f'{dir_atus_anual_csv}/catalogos/tc_municipio.csv')
    path = str(path)

    copy_sql = """
        COPY tc_municipio
        FROM STDIN
        DELIMITER ','
        CSV HEADER;
        """

    with open(path, 'r') as file:
        with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
            with conn.cursor() as cur:
                print(f'Loading table: {schema_name}.tc_municipio')
                cur.copy_expert(sql=copy_sql, file=file)

def load_tc_periodo_mes(host, dbname, user, port, password, schema_name):
    query_val = sql.SQL(
        """
        SELECT count(*)
        FROM tc_periodo_mes;
        """
    )

    with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            cur.execute(query_val)
            if cur.fetchall()[0][0] > 0:
                print(f'The table {schema_name}.tc_periodo_mes is not empty')
                return None
    
    path = Path().absolute().joinpath(f'{dir_atus_anual_csv}/catalogos/tc_periodo_mes.csv')
    path = str(path)

    df = pd.read_csv(path, dtype=str, index_col=False)
    df.MES = df.MES.str.rjust(2, '0')
    df.to_csv(path, index=False)

    copy_sql = sql.SQL(
        """
        COPY tc_periodo_mes
        FROM STDIN
        DELIMITER ','
        CSV HEADER;
        """
    )

    with open(path, 'r') as file:
        with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
            with conn.cursor() as cur:
                print(f'Loading table: {schema_name}.tc_periodo_mes')
                cur.copy_expert(sql=copy_sql, file=file)

def load_tc_hora(host, dbname, user, port, password, schema_name):
    query_val = sql.SQL(
        """
        SELECT count(*)
        FROM tc_hora;
        """
    )

    with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            cur.execute(query_val)
            if cur.fetchall()[0][0] > 0:
                print(f'The table {schema_name}.tc_hora is not empty')
                return None
    
    path = Path().absolute().joinpath(f'{dir_atus_anual_csv}/catalogos/tc_hora.csv')
    path = str(path)

    copy_sql = sql.SQL(
        """
        COPY tc_hora
        FROM STDIN
        DELIMITER ','
        CSV HEADER;
        """
    )
    with open(path, 'r') as file:
        with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
            with conn.cursor() as cur:
                print(f'Loading table: {schema_name}.tc_hora')
                cur.copy_expert(sql=copy_sql, file=file)

def load_tc_dia(host, dbname, user, port, password, schema_name):
    query_val = sql.SQL(
        """
        SELECT count(*)
        FROM tc_dia;
        """
    )

    with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            cur.execute(query_val)
            if cur.fetchall()[0][0] > 0:
                print(f'The table {schema_name}.tc_dia is not empty')
                return None
    
    path = Path().absolute().joinpath(f'{dir_atus_anual_csv}/catalogos/tc_dia.csv')
    path = str(path)

    copy_sql = sql.SQL(
        """
        COPY tc_dia
        FROM STDIN
        DELIMITER ','
        CSV HEADER;
        """
    )

    with open(path, 'r') as file:
        with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
            with conn.cursor() as cur:
                print(f'Loading table: {schema_name}.tc_dia')
                cur.copy_expert(sql=copy_sql, file=file)

def load_tc_minuto(host, dbname, user, port, password, schema_name):
    query_val = sql.SQL(
        """
        SELECT count(*)
        FROM tc_minuto;
        """
    )

    with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            cur.execute(query_val)
            if cur.fetchall()[0][0] > 0:
                print(f'The table {schema_name}.tc_minuto is not empty')
                return None
    
    path = Path().absolute().joinpath(f'{dir_atus_anual_csv}/catalogos/tc_minuto.csv')
    path = str(path)

    copy_sql = sql.SQL(
        """
        COPY tc_minuto
        FROM STDIN
        DELIMITER ','
        CSV HEADER;
        """
    )

    with open(path, 'r') as file:
        with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
            with conn.cursor() as cur:
                print(f'Loading table: {schema_name}.tc_minuto')
                cur.copy_expert(sql=copy_sql, file=file)

def load_tc_edad(host, dbname, user, port, password, schema_name):
    query_val = sql.SQL(
        """
        SELECT count(*)
        FROM tc_edad;
        """
    )

    with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            cur.execute(query_val)
            if cur.fetchall()[0][0] > 0:
                print(f'The table {schema_name}.tc_edad is not empty')
                return None
    
    path = Path().absolute().joinpath(f'{dir_atus_anual_csv}/catalogos/tc_edad.csv')
    path = str(path)

    copy_sql = sql.SQL(
        """
        COPY tc_edad
        FROM STDIN
        DELIMITER ','
        CSV HEADER;
        """
    )

    with open(path, 'r') as file:
        with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
            with conn.cursor() as cur:
                print(f'Loading table: {schema_name}.tc_edad')
                cur.copy_expert(sql=copy_sql, file=file)

def load_tr_cifra(host, dbname, user, port, password, schema_name):
    query_val = sql.SQL(
        """
        SELECT count(*)
        FROM tr_cifra;
        """
    )

    with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            cur.execute(query_val)
            if cur.fetchall()[0][0] > 0:
                print(f'The table {schema_name}.tr_cifra is not empty')
                return None
    
    path = Path().absolute().joinpath(f'{dir_atus_anual_csv}/conjunto_de_datos')

    with psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            for csv in path.iterdir():
                df = pd.read_csv(str(csv), index_col=False)
                df.ID_ENTIDAD = df.ID_ENTIDAD.astype(str).str.rjust(2, '0')
                df.ID_MUNICIPIO = df.ID_MUNICIPIO.astype(str).str.rjust(3, '0')
                df.ID_MUNICIPIO = df.ID_MUNICIPIO.replace({'998': '999'})
                df.MES = df.MES.astype(str).str.rjust(2,'0')
                df.ID_DIA = df.ID_DIA.astype(str).str.rjust(2, '0')
                df.ID_DIA = df.ID_DIA.replace({'00': '32'})
                df.to_csv(str(csv), index=False)
                columns = df.columns.to_list()
                
                copy_sql = sql.SQL(
                    """
                    COPY tr_cifra({0})
                    FROM STDIN
                    DELIMITER ','
                    CSV HEADER;
                    """
                ).format(sql.SQL(', ').join(map(sql.Identifier, columns)))

                with open(str(csv), 'r') as file:
                    print(f'Loading table: {schema_name}.tr_cifra from {str(csv)}')
                    cur.copy_expert(sql=copy_sql, file=file)

if __name__ == '__main__':
    create_schema(host, dbname, user, port, password, schema_name)
    create_tc_entidad(host, dbname, user, port, password, schema_name)
    create_tc_municipio(host, dbname, user, port, password, schema_name)
    create_tc_periodo_mes(host, dbname, user, port, password, schema_name)
    create_tc_hora(host, dbname, user, port, password, schema_name)
    create_tc_dia(host, dbname, user, port, password, schema_name)
    create_tc_minuto(host, dbname, user, port, password, schema_name)
    create_tc_edad(host, dbname, user, port, password, schema_name)
    create_tr_cifra(host, dbname, user, port, password, schema_name)
    load_tc_entidad(host, dbname, user, port, password, schema_name)
    load_tc_municipio(host, dbname, user, port, password, schema_name)
    load_tc_periodo_mes(host, dbname, user, port, password, schema_name)
    load_tc_hora(host, dbname, user, port, password, schema_name)
    load_tc_dia(host, dbname, user, port, password, schema_name)
    load_tc_minuto(host, dbname, user, port, password, schema_name)
    load_tc_edad(host, dbname, user, port, password, schema_name)
    load_tr_cifra(host, dbname, user, port, password, schema_name)