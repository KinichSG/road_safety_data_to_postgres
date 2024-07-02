import psycopg2
from psycopg2 import sql
from pathlib import Path
import pandas as pd

# connection = 'host=localhost dbname=seguridad_vial user=postgres port=5432 password=popcorning'
connection = 'host=localhost dbname=scuil user=scuil port=5432 password=popcorning'
schema_name = 'atus_9722'
dir_atus_anual_csv = 'atus_anual_csv_9722'

def create_schema(connection, schema_name):
    query = sql.SQL(
        """
        CREATE SCHEMA IF NOT EXISTS {0}
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            print(f'Creating schema: {schema_name}')
            cur.execute(query)

def create_tc_entidad(connection, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {0}.tc_entidad(
            ID_ENTIDAD varchar(2),
            NOM_ENTIDAD varchar(150),
            PRIMARY KEY(ID_ENTIDAD)
        );
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            print(f'Creating table: {schema_name}.tc_entidad')
            cur.execute(query)

def create_tc_municipio(connection, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {0}.tc_municipio(
            ID_ENTIDAD varchar(2),
            ID_MUNICIPIO varchar(3),
            NOM_MUNICIPIO varchar(150),
            PRIMARY KEY(ID_ENTIDAD, ID_MUNICIPIO)
        );
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            print(f'Creating table: {schema_name}.tc_municipio')
            cur.execute(query)

def create_tc_periodo_mes(connection, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {0}.tc_periodo_mes(
            MES varchar(2),
            DESCRIPCION_MES varchar(30),
            PRIMARY KEY(MES)
        );
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            print(f'Creating table: {schema_name}.tc_periodo_mes')
            cur.execute(query)

def create_tc_hora(connection, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {0}.tc_hora(
            ID_HORA int,
            DESC_HORA varchar(50),
            PRIMARY KEY(ID_HORA)
        );
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            print(f'Creating table: {schema_name}.tc_hora')
            cur.execute(query)

def create_tc_dia(connection, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {0}.tc_dia(
            ID_DIA varchar(2),
            DESC_DIA varchar(50),
            PRIMARY KEY(ID_DIA)
        );
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            print(f'Creating table: {schema_name}.tc_dia')
            cur.execute(query)

def create_tc_minuto(connection, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {0}.tc_minuto(
            ID_MINUTO int,
            DESC_MINUTO varchar(50),
            PRIMARY KEY(ID_MINUTO)
        );
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            print(f'Creating table: {schema_name}.tc_minuto')
            cur.execute(query)

def create_tc_edad(connection, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {0}.tc_edad(
            ID_EDAD varchar(2),
            DESC_EDAD varchar(50),
            PRIMARY KEY(ID_EDAD)
        );
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            print(f'Creating table: {schema_name}.tc_edad')
            cur.execute(query)

def create_tr_cifra(connection, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {0}.tr_cifra(
                ID_ATUS serial,
                COBERTURA varchar(200),
                ID_ENTIDAD varchar(2),
                ID_MUNICIPIO varchar(3),
                ANIO int,
                MES varchar(2),
                ID_HORA int,
                ID_MINUTO int,
                ID_DIA varchar(2),
                DIASEMANA varchar(20),
                URBANA varchar(50),
                SUBURBANA varchar(50),
                TIPACCID varchar(100),
                AUTOMOVIL int,
                CAMPASAJ int,
                MICROBUS int,
                PASCAMION int,
                OMNIBUS int,
                TRANVIA int,
                CAMIONETA int,
                CAMION int,
                TRACTOR int,
                FERROCARRI int,
                MOTOCICLET int,
                BICICLETA int,
                OTROVEHIC int,
                CAUSAACCI varchar(50),
                CAPAROD varchar(50),
                SEXO varchar(20),
                ALIENTO varchar(20),
                CINTURON varchar(20),
                ID_EDAD varchar(2),
                CONDMUERTO int,
                CONDHERIDO int,
                PASAMUERTO int,
                PASAHERIDO int,
                PEATMUERTO int,
                PEATHERIDO int,
                CICLMUERTO int,
                CICLHERIDO int,
                OTROMUERTO int,
                OTROHERIDO int,
                NEMUERTO int,
                NEHERIDO int,
                CLASACC varchar(50),
                ESTATUS varchar(20),
                PRIMARY KEY(ID_ATUS),
                FOREIGN KEY(ID_ENTIDAD) REFERENCES {0}.tc_entidad,
                FOREIGN KEY(ID_ENTIDAD, ID_MUNICIPIO) REFERENCES {0}.tc_municipio,
                FOREIGN KEY(MES) REFERENCES {0}.tc_periodo_mes,
                FOREIGN KEY(ID_HORA) REFERENCES {0}.tc_hora,
                FOREIGN KEY(ID_DIA) REFERENCES {0}.tc_dia,
                FOREIGN KEY(ID_MINUTO) REFERENCES {0}.tc_minuto,
                FOREIGN KEY(ID_EDAD) REFERENCES {0}.tc_edad
        )""".format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            print(f'Creating table: {schema_name}.tr_cifra')
            cur.execute(query)

def load_tc_entidad(connection, schema_name):
    query_val = sql.SQL(
        """
        SELECT count(*)
        FROM {0}.tc_entidad;
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            cur.execute(query_val)
            if cur.fetchall()[0][0] > 0:
                print(f'The table {schema_name}.tc_entidad is not empty')
                return None
    
    path = Path().absolute().joinpath(f'{dir_atus_anual_csv}/catalogos/tc_entidad.csv')
    path = str(path)

    query = sql.SQL(
        """
        COPY {0}.tc_entidad
        FROM %s
        DELIMITER ','
        CSV HEADER;
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            print(f'Loading table: {schema_name}.tc_entidad')
            cur.execute(query, [path])

def load_tc_municipio(connection, schema_name):
    query_val = sql.SQL(
        """
        SELECT count(*)
        FROM {0}.tc_municipio;
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            cur.execute(query_val)
            if cur.fetchall()[0][0] > 0:
                print(f'The table {schema_name}.tc_municipio is not empty')
                return None
    
    path = Path().absolute().joinpath(f'{dir_atus_anual_csv}/catalogos/tc_municipio.csv')
    path = str(path)

    query = sql.SQL(
        """
        COPY {0}.tc_municipio
        FROM %s
        DELIMITER ','
        CSV HEADER;
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            print(f'Loading table: {schema_name}.tc_municipio')
            cur.execute(query, [path])

def load_tc_periodo_mes(connection, schema_name):
    query_val = sql.SQL(
        """
        SELECT count(*)
        FROM {0}.tc_periodo_mes;
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
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

    query = sql.SQL(
        """
        COPY {0}.tc_periodo_mes
        FROM %s
        DELIMITER ','
        CSV HEADER;
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            print(f'Loading table: {schema_name}.tc_periodo_mes')
            cur.execute(query, [path])

def load_tc_hora(connection, schema_name):
    query_val = sql.SQL(
        """
        SELECT count(*)
        FROM {0}.tc_hora;
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            cur.execute(query_val)
            if cur.fetchall()[0][0] > 0:
                print(f'The table {schema_name}.tc_hora is not empty')
                return None
    
    path = Path().absolute().joinpath(f'{dir_atus_anual_csv}/catalogos/tc_hora.csv')
    path = str(path)

    query = sql.SQL(
        """
        COPY {0}.tc_hora
        FROM %s
        DELIMITER ','
        CSV HEADER;
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            print(f'Loading table: {schema_name}.tc_hora')
            cur.execute(query, [path])

def load_tc_dia(connection, schema_name):
    query_val = sql.SQL(
        """
        SELECT count(*)
        FROM {0}.tc_dia;
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            cur.execute(query_val)
            if cur.fetchall()[0][0] > 0:
                print(f'The table {schema_name}.tc_dia is not empty')
                return None
    
    path = Path().absolute().joinpath(f'{dir_atus_anual_csv}/catalogos/tc_dia.csv')
    path = str(path)

    query = sql.SQL(
        """
        COPY {0}.tc_dia
        FROM %s
        DELIMITER ','
        CSV HEADER;
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            print(f'Loading table: {schema_name}.tc_dia')
            cur.execute(query, [path])

def load_tc_minuto(connection, schema_name):
    query_val = sql.SQL(
        """
        SELECT count(*)
        FROM {0}.tc_minuto;
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            cur.execute(query_val)
            if cur.fetchall()[0][0] > 0:
                print(f'The table {schema_name}.tc_minuto is not empty')
                return None
    
    path = Path().absolute().joinpath(f'{dir_atus_anual_csv}/catalogos/tc_minuto.csv')
    path = str(path)

    query = sql.SQL(
        """
        COPY {0}.tc_minuto
        FROM %s
        DELIMITER ','
        CSV HEADER;
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            print(f'Loading table: {schema_name}.tc_minuto')
            cur.execute(query, [path])

def load_tc_edad(connection, schema_name):
    query_val = sql.SQL(
        """
        SELECT count(*)
        FROM {0}.tc_edad;
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            cur.execute(query_val)
            if cur.fetchall()[0][0] > 0:
                print(f'The table {schema_name}.tc_edad is not empty')
                return None
    
    path = Path().absolute().joinpath(f'{dir_atus_anual_csv}/catalogos/tc_edad.csv')
    path = str(path)

    query = sql.SQL(
        """
        COPY {0}.tc_edad
        FROM %s
        DELIMITER ','
        CSV HEADER;
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            print(f'Loading table: {schema_name}.tc_edad')
            cur.execute(query, [path])

def load_tr_cifra(connection, schema_name):
    query_val = sql.SQL(
        """
        SELECT count(*)
        FROM {0}.tr_cifra;
        """.format(schema_name)
    )

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            cur.execute(query_val)
            if cur.fetchall()[0][0] > 0:
                print(f'The table {schema_name}.tr_cifra is not empty')
                return None
    
    path = Path().absolute().joinpath(f'{dir_atus_anual_csv}/conjunto_de_datos')

    with psycopg2.connect(connection) as conn:
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
                columns = df.columns.str.lower().to_list()
                
                query = sql.SQL(
                    """
                    COPY {0}.tr_cifra(
                        {1}
                    )
                    FROM %s
                    DELIMITER ','
                    CSV HEADER;
                    """
                ).format(sql.Identifier(schema_name),  sql.SQL(', ').join(map(sql.Identifier, columns)))

                print(f'Loading table: {schema_name}.tr_cifra from {str(csv)}')
                cur.execute(query, [str(csv)])

if __name__ == '__main__':
    create_schema(connection, schema_name)
    create_tc_entidad(connection, schema_name)
    create_tc_municipio(connection, schema_name)
    create_tc_periodo_mes(connection, schema_name)
    create_tc_hora(connection, schema_name)
    create_tc_dia(connection, schema_name)
    create_tc_minuto(connection, schema_name)
    create_tc_edad(connection, schema_name)
    create_tr_cifra(connection, schema_name)
    load_tc_entidad(connection, schema_name)
    load_tc_municipio(connection, schema_name)
    load_tc_periodo_mes(connection, schema_name)
    load_tc_hora(connection, schema_name)
    load_tc_dia(connection, schema_name)
    load_tc_minuto(connection, schema_name)
    load_tc_edad(connection, schema_name)
    load_tr_cifra(connection, schema_name)