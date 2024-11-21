from pathlib import Path
from sqlalchemy import create_engine, text
from modules import create_engine_string
import pandas as pd
import geopandas as gpd
import geoalchemy2
from sqlalchemy import types
from geoalchemy2 import Geography, Geometry

def etl_mg(path_mg_entidad, path_mg_municipio, connection_ori, connection_dest, schema_ori, schema_dest, dialect='postgresql+psycopg2'):
    # engine destination
    engine_string_dest = create_engine_string(connection_dest, dialect)
    engine_dest = create_engine(engine_string_dest)
    # create tables
    with engine_dest.begin() as conn:
        # create schema
        conn.execute(
            text(
                """
                DROP SCHEMA IF EXISTS {0} CASCADE;
                CREATE SCHEMA {0};
                """.format(schema_dest)
            )
        )
        print('schema created')
        # set schema
        conn.execute(
            text(
                """
                SET search_path = {0};
                """.format(schema_dest)
            )
        )
        # create entidad
        conn.execute(
            text(
                """
                CREATE TABLE entidad (
                    id_entidad CHAR(2) NOT NULL,
                    nom_entidad VARCHAR(150) NOT NULL,

                    CONSTRAINT pk_entidad
                    PRIMARY KEY (id_entidad),

                    CONSTRAINT id_entidad_dominio
                    CHECK (id_entidad::numeric >= 1 AND id_entidad::numeric <= 32 OR id_entidad::numeric = 99)
                );
                """
            )
        )
        print('entidad created')
        # create municipio
        conn.execute(
            text(
                """
                CREATE TABLE municipio (
                    id_entidad CHAR(2) NOT NULL,
                    id_municipio CHAR(3) NOT NULL,
                    nom_municipio VARCHAR(150) NOT NULL,

                    CONSTRAINT pk_municipio
                    PRIMARY KEY (id_entidad, id_municipio),

                    CONSTRAINT fk_municipio_entidad
                    FOREIGN KEY (id_entidad) REFERENCES entidad (id_entidad)
                    ON DELETE RESTRICT
                    ON UPDATE RESTRICT,

                    CONSTRAINT id_municipio_dominio
                    CHECK (id_municipio::numeric >= 0 AND id_municipio::numeric <= 570 OR id_municipio::numeric = 999  )
                );
                """
            )
        )
        print('municipio created')
        # # create geo_entidad
        # conn.execute(
        #     text(
        #         """
        #         DROP TABLE IF EXISTS geo_entidad;

        #         CREATE TABLE geo_entidad (
        #             id_entidad CHAR(2) NOT NULL,
        #             geometry GEOMETRY(Multipolygon, 4326) NOT NULL,

        #             CONSTRAINT pk_geo_entidad
        #             PRIMARY KEY (id_entidad),

        #             CONSTRAINT fk_geo_entidad_entidad
        #             FOREIGN KEY (id_entidad) REFERENCES entidad (id_entidad)
        #             ON DELETE RESTRICT
        #             ON UPDATE RESTRICT
        #         );
        #         """
        #     )
        # )
        # # create geo_municipio
        # conn.execute(
        #     text(
        #         """
        #         DROP TABLE IF EXISTS geo_municipio;

        #         CREATE TABLE geo_municipio (
        #             id_entidad CHAR(2) NOT NULL,
        #             id_municipio CHAR(3) NOT NULL,
        #             geometry GEOMETRY(Multipolygon, 4326) NOT NULL,

        #             CONSTRAINT pk_geo_municipio
        #             PRIMARY KEY (id_entidad, id_municipio),

        #             CONSTRAINT fk_geo_municipio_municipio
        #             FOREIGN KEY (id_entidad, id_municipio) REFERENCES (id_entidad, id_municipio)
        #             ON DELETE RESTRICT
        #             ON UPDATE RESTRICT
        #         );
        #         """
        #     )
        # )
    # engine origen
    engine_string_ori = create_engine_string(connection_ori, dialect)
    engine_ori = create_engine(engine_string_ori)
    # read entidades
    entidad = pd.read_sql(
        # sql="SELECT * FROM {0}.tc_entidad WHERE id_entidad != '99'".format(schema_ori),
        sql="SELECT * FROM {0}.tc_entidad".format(schema_ori),
        con=engine_ori
    )
    # read_municipios
    municipio = pd.read_sql(
        # sql="SELECT * FROM {0}.tc_municipio WHERE id_municipio != '999'".format(schema_ori),
        sql="SELECT * FROM {0}.tc_municipio".format(schema_ori),
        con=engine_ori
    )
    # read entidades geometry
    path_entidad = Path(path_mg_entidad)
    gdf_entidad = gpd.read_file(path_entidad)
    gdf_entidad = gdf_entidad[['CVE_ENT', 'geometry']]
    gdf_entidad.columns = ['id_entidad', 'geometry']
    gdf_entidad = gdf_entidad.to_crs(4326)
    # read municipios geometry
    path_municipio = Path(path_mg_municipio)
    gdf_municipio = gpd.read_file(path_municipio)
    gdf_municipio = gdf_municipio[['CVE_ENT', 'CVE_MUN', 'geometry']]
    gdf_municipio.columns = ['id_entidad', 'id_municipio', 'geometry']
    gdf_municipio = gdf_municipio.to_crs(4326)
    # engine destination
    engine_string_dest = create_engine_string(connection_dest, dialect)
    engine_dest = create_engine(engine_string_dest)
    # load entidad
    entidad.to_sql(
        name='entidad',
        con=engine_dest,
        schema=schema_dest,
        if_exists='append',
        index=False
    )
    # load municipio
    municipio.to_sql(
        name='municipio',
        con=engine_dest,
        schema=schema_dest,
        if_exists='append',
        index=False
    )
    # load geo_entidad
    gdf_entidad.to_postgis(
        name='geo_entidad',
        con=engine_dest,
        if_exists='replace',
        schema=schema_dest,
        index=False
    )
    # load geo_municipio
    gdf_municipio.to_postgis(
        name='geo_municipio',
        con=engine_dest,
        if_exists='replace',
        schema=schema_dest,
        index=False
    )
    # create restricts
    with engine_dest.begin() as conn:
        # set schema
        conn.execute(
            text(
                """
                SET search_path = {0};
                """.format(schema_dest)
            )
        )
        # alter geo_entidad
        conn.execute(
            text(
                """
                ALTER TABLE geo_entidad
                ADD CONSTRAINT pk_geo_entidad
                PRIMARY KEY (id_entidad);

                ALTER TABLE geo_entidad
                ADD CONSTRAINT pk_geo_entidad_entidad
                FOREIGN KEY (id_entidad) REFERENCES entidad (id_entidad)
                ON DELETE RESTRICT
                ON UPDATE RESTRICT;
                """
            )
        )
        # alter geo_municipio
        conn.execute(
            text(
                """
                ALTER TABLE geo_municipio
                ADD CONSTRAINT pk_geo_municipio
                PRIMARY KEY (id_entidad, id_municipio);
                
                ALTER TABLE geo_municipio
                ADD CONSTRAINT fk_geo_municipio_municipio
                FOREIGN KEY (id_entidad, id_municipio) REFERENCES municipio (id_entidad, id_municipio)
                ON DELETE RESTRICT
                ON UPDATE RESTRICT;
                """
            )
        )