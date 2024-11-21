import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from modules import create_engine_string

def create_tables(path_vmrc_anual_csv, connection_vars, schema_name, dialect='postgresql'):
    """
    Create tables: tc_entidad, tc_municipio, vmrc.

    Args:
        path_vmrc_anual_csv (str): Path of the dir that contains data of VMRC.
        connection_vars (dict): Dict that contains connection information.
        dialect (str): Dialects's name used.
    
    Returns:
        None.
    """

    engine_string = create_engine_string(connection_vars, dialect)
    engine = create_engine(engine_string)
    
    with engine.begin() as conn:
        # set schema
        conn.execute(
            text(
                """
                SET search_path = {0}
                """.format(schema_name)
            )
        )
        # create table tc_entidad
        conn.execute(
            text(
                """
                DROP TABLE IF EXISTS tc_entidad CASCADE;

                CREATE TABLE tc_entidad(
                    id_entidad VARCHAR(2) NOT NULL,
                    nom_entidad VARCHAR(150) NOT NULL,

                    CONSTRAINT pk_entidad
                    PRIMARY KEY id_entidad,

                    CONSTRAINT id_entidad_range
                    CHECK (
                        (id_entidad::numeric <= 32 AND id_entidad::numeric >= 1)
                        OR id_entidad::textual = '99'
                    )
                );
                """
            )
        )
        # create table tc_municipio
        conn.execute(
            text(
                """
                DROP TABLE IF EXISTS tc_municipio CASCADE;
                CREATE TABLE IF NOT EXISTS tc_municipio(
                    id_entidad VARCHAR(2) NOT NULL,
                    id_municipio VARCHAR(3) NOT NULL,
                    nom_municipio VARCHAR(150) NOT NULL,

                    CONSTRAINT pk_municipio
                    PRIMARY KEY (id_entidad, id_municipio),

                    CONSTRAINT fk_municipio_entidad
                    FOREIGN KEY (id_entidad) REFERENCES tc_entidad (id_entidad)
                    ON DELETE RESTRICT
                    ON UPDATE RESTRICT,

                    CONSTRAINT id_entidad_range
                    CHECK (
                        (id_entidad::numeric <= 32 AND id_entidad::numeric >= 1)
                        OR id_entidad::textual = '99'
                    ),

                    CONSTRAINT id_municipio_range
                    CHECK (
                        (id_municipio::numeric <= 570 AND id_municipio::numeric >= 0)
                        OR id_entidad::textual = '999'
                    )
                );
                """
            )
        )
        # create table vmrc
        conn.execute(
            text(
                """
                DROP TABLE IF EXISTS vmrc CASCADE;
                CREATE TABLE IF NOT EXISTS vmrc(
                    prod_est VARCHAR(200) NOT NULL,
                    cobertura VARCHAR(200) NOT NULL,
                    anio INT NOT NULL,
                    id_entidad VARCHAR(2) NOT NULL,
                    id_municipio VARCHAR(3) NOT NULL,
                    auto_oficial INT NOT NULL,
                    auto_publico INT NOT NULL,
                    auto_particular INT NOT NULL,
                    cam_pas_oficial INT NOT NULL,
                    cam_pas_publico INT NOT NULL,
                    cam_pas_particular INT NOT NULL,
                    cyc_carga_oficial INT NOT NULL,
                    cyc_carga_publico INT NOT NULL,
                    cyc_carga_particular INT NOT NULL,
                    moto_oficial INT NOT NULL,
                    moto_de_alquiler INT NOT NULL,
                    moto_particular INT NOT NULL,
                    estatus VARCHAR(20) NOT NULL,

                    CONSTRAINT pk_vmrc
                    PRIMARY KEY (anio, id_entidad, id_municipio),

                    CONSTRAINT fk_vmrc_municipio
                    FOREIGN KEY (id_entidad, id_municipio) REFERENCES tc_municipio,
                    ON DELETE RESTRICT
                    ON UPDATE RESTRICT,

                    CONSTRAINT anio_range
                    CHECK (anio::numeric <= 32 AND anio::numeric >= 1),

                    CONSTRAINT auto_oficial_no_negative
                    CHECK (auto_oficial::numeric >= 0),

                    CONSTRAINT auto_publico_no_negative
                    CHECK (auto_publico::numeric >= 0),

                    CONSTRAINT auto_particular_no_negative
                    CHECK (auto_particular::numeric >= 0),

                    CONSTRAINT cam_pas_oficial_no_negative
                    CHECK (cam_pas_oficial::numeric >= 0),

                    CONSTRAINT cam_pas_publico_no_negative
                    CHECK (cam_pas_publico::numeric >= 0),

                    CONSTRAINT cam_pas_particular_no_negative
                    CHECK (cam_pas_particular::numeric >= 0),

                    CONSTRAINT cyc_carga_oficial_no_negative
                    CHECK (cyc_carga_oficial::numeric >= 0),

                    CONSTRAINT cyc_carga_publico_no_negative
                    CHECK (cyc_carga_publico::numeric >= 0),

                    CONSTRAINT cyc_carga_particular_no_negative
                    CHECK (cyc_carga_particular::numeric >= 0),

                    CONSTRAINT moto_oficial_no_negative
                    CHECK (moto_oficial::numeric >= 0),

                    CONSTRAINT moto_de_alquiler_no_negative
                    CHECK (moto_de_alquiler::numeric >= 0),

                    CONSTRAINT moto_particular_no_negative
                    CHECK (moto_particular::numeric >= 0)
                );
                """
            )
        )
    # Insert data in tc_entidad
    path = Path().absolute().joinpath(f'{path_vmrc_anual_csv}/catalogos/tc_entidad.csv')
    path = str(path)
    df = pd.read_csv(path, dtype=str, index_col=False)
    df.columns = df.columns.str.lower()
    df.id_entidad = df.id_entidad.map(lambda x: x[-2:])
    df.to_sql(name='tc_entidad', con=engine, schema=schema_name, if_exists='append', index=False)
    # Insert data in tc_municipio
    path = Path().absolute().joinpath(f'{path_vmrc_anual_csv}/catalogos/tc_municipio.csv')
    path = str(path)
    df = pd.read_csv(path, dtype=str, index_col=False, delimiter=',')
    df.columns = df.columns.str.lower()
    df.id_entidad = df.id_entidad.map(lambda x: x[-2:])
    df.id_municipio = df.id_municipio.map(lambda x: x[-3:])
    df = pd.concat([df, pd.DataFrame([['17', '036', 'Hueyapan']], columns=df.columns)])
    df.drop_duplicates(inplace=True)
    df.sort_values(['id_entidad', 'id_municipio'], inplace=True)
    df.to_sql('tc_municipio', con=engine, schema=schema_name, if_exists='append', index=False)
    # Insert data in vmrc
    path = Path().absolute().joinpath(f'{path_vmrc_anual_csv}/conjunto_de_datos')
    for csv in path.iterdir():
        if csv.suffix == '.csv':
            print(f'Correcting table: {str(csv)}')
            df = pd.read_csv(str(csv), dtype=str, index_col=False)
            df.columns = df.columns.str.lower()
            df.id_entidad = df.id_entidad.map(lambda x: x[-2:])
            df.id_municipio = df.id_municipio.map(lambda x: x[-3:])
            df.to_sql(name='vmrc', con=engine, schema=schema_name, if_exists='append', index=False)