import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from modules import create_engine_string

def create_tables(path_atus_anual_csv, connection_vars, schema_name, dialect='postgresql'):
    """
    Create tables: tc_periodo_mes, tc_hora, tc_dia, tc_minuto, tc_edad, atus.

    Args:
        path_atus_anual_csv (str): Path of the dir that contains data of ATUS.
        connection_vars (dict): Dict that contains connection information.
        dialect (str): Dialects's name used.
    
    Returns:
        None.
    """

    engine_string = create_engine_string(connection_vars, dialect)
    engine = create_engine(engine_string)
    
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                SET search_path = {0}
                """.format(schema_name)
            )
        )
        conn.execute(
            text(
                """
                DROP TABLE IF EXISTS tc_periodo_mes CASCADE;
                CREATE TABLE IF NOT EXISTS tc_periodo_mes(
                    mes VARCHAR(2) NOT NULL,
                    descripcion_mes VARCHAR(30) NOT NULL,

                    CONSTRAINT pk_periodo_mes
                    PRIMARY KEY (mes),

                    CONSTRAINT mes_dominio
                    CHECK (mes::numeric >= 1 AND mes::numeric <= 32)
                );
                """
            )
        )
        conn.execute(
            text(
                """
                DROP TABLE IF EXISTS tc_hora CASCADE;
                CREATE TABLE IF NOT EXISTS tc_hora(
                    id_hora INT NOT NULL,
                    desc_hora VARCHAR(50) NOT NULL,

                    CONSTRAINT pk_hora
                    PRIMARY KEY (id_hora)

                    CONSTRAINT hora_dominio
                    CHECK (id_hora::numeric >= 0 AND id_hora::numeric <= 23 OR id_hora::numeric = 99)
                );
                """
            )
        )
        conn.execute(
            text(
                """
                DROP TABLE IF EXISTS tc_dia CASCADE;
                CREATE TABLE IF NOT EXISTS tc_dia(
                    id_dia VARCHAR(2) NOT NULL,
                    desc_dia VARCHAR(50) NOT NULL,

                    CONSTRAINT pk_dia
                    PRIMARY KEY (id_dia),

                    CONSTRAINT dia_dominio
                    CHECK (id_dia::numeric >= 0 AND id_dia::numeric <= 32)
                );
                """
            )
        )
        conn.execute(
            text(
                """
                DROP TABLE IF EXISTS tc_minuto CASCADE;
                CREATE TABLE IF NOT EXISTS tc_minuto(
                    id_minuto INT NOT NULL,
                    desc_minuto VARCHAR(50) NOT NULL,

                    CONSTRAINT pk_minuto
                    PRIMARY KEY (id_minuto),

                    CONSTRAINT minuto_dominio
                    CHECK (id_minuto::numeric >= 0 AND id_minuto::numeric <= 59 OR id_minuto::numeric = 99)
                );
                """
            )
        )
        conn.execute(
            text(
                """
                DROP TABLE IF EXISTS tc_edad CASCADE;
                CREATE TABLE IF NOT EXISTS tc_edad(
                    id_edad VARCHAR(2) NOT NULL,
                    desc_edad VARCHAR(50) NOT NULL,

                    CONSTRAINT pk_edad
                    PRIMARY KEY (id_edad)

                    CONSTRAINT edad_dominio
                    CHECK (id_edad::numeric >= 12 AND id_edad::numeric <= 99 OR id_edad::numeric = 0)
                );
                """
            )
        )
        conn.execute(
            text(
                """
                DROP TABLE IF EXISTS atus CASCADE;
                CREATE TABLE IF NOT EXISTS atus(
                        id_atus SERIAL NOT NULL,
                        cobertura VARCHAR(200) NOT NULL,
                        id_entidad VARCHAR(2) NOT NULL,
                        id_municipio VARCHAR(3) NOT NULL,
                        anio INT NOT NULL,
                        mes VARCHAR(2) NOT NULL,
                        id_hora INT NOT NULL,
                        id_minuto INT NOT NULL,
                        id_dia VARCHAR(2) NOT NULL,
                        diasemana VARCHAR(20) NOT NULL,
                        urbana VARCHAR(50) NOT NULL,
                        suburbana VARCHAR(50) NOT NULL,
                        tipaccid VARCHAR(100) NOT NULL,
                        automovil INT NOT NULL,
                        campasaj INT NOT NULL,
                        microbus INT NOT NULL,
                        pascamion INT NOT NULL,
                        omnibus INT NOT NULL,
                        tranvia INT NOT NULL,
                        camioneta INT NOT NULL,
                        camion INT NOT NULL,
                        tractor INT NOT NULL,
                        ferrocarri INT NOT NULL,
                        motociclet INT NOT NULL,
                        bicicleta INT NOT NULL,
                        otrovehic INT NOT NULL,
                        causaacci VARCHAR(50) NOT NULL,
                        caparod VARCHAR(50) NOT NULL,
                        sexo VARCHAR(20) NOT NULL,
                        aliento VARCHAR(20) NOT NULL,
                        cinturon VARCHAR(20) NOT NULL,
                        id_edad VARCHAR(2) NOT NULL,
                        condmuerto INT NOT NULL,
                        condherido INT NOT NULL,
                        pasamuerto INT NOT NULL,
                        pasaherido INT NOT NULL,
                        peatmuerto INT NOT NULL,
                        peatherido INT NOT NULL,
                        ciclmuerto INT NOT NULL,
                        ciclherido INT NOT NULL,
                        otromuerto INT NOT NULL,
                        otroherido INT NOT NULL,
                        nemuerto INT NOT NULL,
                        neherido INT NOT NULL,
                        clasacc VARCHAR(50) NOT NULL,
                        estatus VARCHAR(20) NOT NULL,

                        CONSTRAINT pk_atus
                        PRIMARY KEY (id_atus),

                        CONSTRAINT fk_atus_entidad
                        FOREIGN KEY (id_entidad) REFERENCES tc_entidad(id_entidad)
                        ON DELETE RESTRICT
                        ON UPDATE RESTRICT,

                        CONSTRAINT fk_atus_entidad
                        FOREIGN KEY (id_entidad, id_municipio) REFERENCES tc_municipio(id_entidad, id_municipio)
                        ON DELETE RESTRICT
                        ON UPDATE RESTRICT,

                        CONSTRAINT fk_atus_mes
                        FOREIGN KEY (mes) REFERENCES tc_periodo_mes(mes)
                        ON DELETE RESTRICT
                        ON UPDATE RESTRICT,

                        CONSTRAINT fk_atus_hora
                        FOREIGN KEY (id_hora) REFERENCES tc_hora(id_hora)
                        ON DELETE RESTRICT
                        ON UPDATE RESTRICT,

                        CONSTRAINT fk_atus_dia
                        FOREIGN KEY (id_dia) REFERENCES tc_dia(id_dia)
                        ON DELETE RESTRICT
                        ON UPDATE RESTRICT,

                        CONSTRAINT fk_atus_minuto
                        FOREIGN KEY (id_minuto) REFERENCES tc_minuto(id_minuto)
                        ON DELETE RESTRICT
                        ON UPDATE RESTRICT,

                        CONSTRAINT fk_atus_edad
                        FOREIGN KEY (id_edad) REFERENCES tc_edad(id_edad)
                        ON DELETE RESTRICT
                        ON UPDATE RESTRICT

                        CONSTRAINT no_negative
                        CHECK (
                            automovil >= 0
                            AND campasaj >= 0
                            AND microbus >= 0
                            AND pascamion >= 0
                            AND omnibus >= 0
                            AND tranvia >= 0
                            AND camioneta >= 0
                            AND camio >= 0
                            AND tractor >= 0
                            AND ferrocarri >= 0
                            AND motociclet  >= 0
                            AND bicicleta >= 0
                            AND otrovehic >= 0
                            AND condmuerto >= 0
                            AND condherido >= 0
                            AND pasamuerto >= 0
                            AND pasaherido >= 0
                            AND peatmuerto >= 0
                            AND peatherido >= 0
                            AND ciclmuerto >= 0
                            AND ciclherido >= 0
                            AND otromuerto >= 0
                            AND otroherido >= 0
                            AND nemuerto >= 0
                            AND neherido >= 0
                        )
                );
                """
            )
        )
    
    path = Path().absolute().joinpath(f'{path_atus_anual_csv}/catalogos/tc_periodo_mes.csv')
    path = str(path)
    df = pd.read_csv(path, dtype=str, index_col=False)
    df.columns = df.columns.str.lower()
    df.mes = df.mes.str.rjust(2, '0')
    df.to_sql('tc_periodo_mes', con=engine, schema=schema_name, if_exists='append', index=False)
    
    path = Path().absolute().joinpath(f'{path_atus_anual_csv}/catalogos/tc_hora.csv')
    path = str(path)
    df = pd.read_csv(path, dtype=str, index_col=False)
    df.columns = df.columns.str.lower()
    df.to_sql('tc_hora', con=engine, schema=schema_name, if_exists='append', index=False)
    
    path = Path().absolute().joinpath(f'{path_atus_anual_csv}/catalogos/tc_dia.csv')
    path = str(path)
    df = pd.read_csv(path, dtype=str, index_col=False)
    df.columns = df.columns.str.lower()
    df.to_sql('tc_dia', con=engine, schema=schema_name, if_exists='append', index=False)
    
    path = Path().absolute().joinpath(f'{path_atus_anual_csv}/catalogos/tc_minuto.csv')
    path = str(path)
    df = pd.read_csv(path, dtype=str, index_col=False)
    df.columns = df.columns.str.lower()
    df.to_sql('tc_minuto', con=engine, schema=schema_name, if_exists='append', index=False)
    
    path = Path().absolute().joinpath(f'{path_atus_anual_csv}/catalogos/tc_edad.csv')
    path = str(path)
    df = pd.read_csv(path, dtype=str, index_col=False)
    df.columns = df.columns.str.lower()
    df.to_sql('tc_edad', con=engine, schema=schema_name, if_exists='append', index=False)
    print('start')
    path = Path().absolute().joinpath(f'{path_atus_anual_csv}/conjunto_de_datos')
    for csv in sorted(path.iterdir()):
        print(csv)
        df = pd.read_csv(str(csv), index_col=False)
        df.columns = df.columns.str.lower()
        df.id_entidad = df.id_entidad.astype(str).str.rjust(2, '0')
        df.id_municipio = df.id_municipio.astype(str).str.rjust(3, '0')
        df.id_municipio = df.id_municipio.replace({'998': '999'})
        df.mes = df.mes.astype(str).str.rjust(2,'0')
        df.id_dia = df.id_dia.astype(str).str.rjust(2, '0')
        df.id_dia = df.id_dia.replace({'00': '32'})
        df.to_sql('atus', con=engine, schema=schema_name, if_exists='append', index=False)
        print('done')