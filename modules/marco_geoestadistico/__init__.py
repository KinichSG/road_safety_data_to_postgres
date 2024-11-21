import pandas as pd
import geopandas as gpd
from pathlib import Path
from sqlalchemy import create_engine, text
from modules import create_engine_string

def create_marco_geoestadistico(path_mg_entidad, path_mg_municipio, connection_vars, schema_name, dialect='postgresql'):

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
                    PRIMARY KEY (id_entidad),

                    CONSTRAINT id_entidad_range
                    CHECK (
                        (id_entidad::numeric <= 32 AND id_entidad::numeric >= 1)
                        OR id_entidad::numeric = 99
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
                        OR id_entidad::numeric = 99
                    ),

                    CONSTRAINT id_municipio_range
                    CHECK (
                        (id_municipio::numeric <= 570 AND id_municipio::numeric >= 0)
                        OR id_municipio::numeric = 999
                    )
                );
                """
            )
        )

    path = Path(path_mg_entidad)
    mg_ent = gpd.read_file(path, columns=['CVE_ENT', 'NOMGEO'])
    mg_ent = mg_ent[['CVE_ENT', 'NOMGEO']]
    mg_ent.columns = ['id_entidad', 'nom_entidad']
    otros = pd.DataFrame([['99', 'Otros estados']], columns=mg_ent.columns)
    mg_ent = pd.concat([mg_ent, otros], axis=0)

    mg_ent.to_sql(name='tc_entidad', con=engine, schema=schema_name, if_exists='append', index=False)

    path = Path(path_mg_municipio)
    mg_mun = gpd.read_file(path, columns=['CVE_ENT', 'CVE_MUN', 'NOMGEO'])
    mg_mun = mg_mun[['CVE_ENT', 'CVE_MUN', 'NOMGEO']]
    mg_mun.columns = ['id_entidad', 'id_municipio', 'nom_municipio']
    # mg_mun.columns = mg_mun.columns.str.lower()
    otros = [[i, '999', 'Otros municipios'] for i in mg_mun['id_entidad'].unique()]#, '999', 'Otros municipios']
    otros = pd.DataFrame(otros, columns=mg_mun.columns)
    otros_grl = pd.DataFrame([['99', '999', 'Otros municipios']], columns=mg_mun.columns)
    mg_mun = pd.concat([mg_mun, otros, otros_grl], axis=0)
    mg_mun = mg_mun.sort_values(['id_entidad', 'id_municipio'])
    mg_mun = mg_mun.reset_index(drop=True)

    mg_mun.to_sql(name='tc_municipio', con=engine, schema=schema_name, if_exists='append', index=False)