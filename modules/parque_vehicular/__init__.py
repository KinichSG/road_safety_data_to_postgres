from modules import create_engine_string
from sqlalchemy import create_engine, text, CHAR, NUMERIC
import pandas as pd

def etl_parque_vehicular(connection_ori, connection_dest, schema_ori, schema_dest):
    engine_string_ori = create_engine_string(connection_ori)
    engine_ori = create_engine(engine_string_ori)

    engine_string_dest = create_engine_string(connection_dest)
    engine_dest = create_engine(engine_string_dest)

    sql_query = """
        SELECT  id_entidad,
                id_municipio,
                anio,
                auto_oficial,
                auto_publico,
                auto_particular,
                cam_pas_oficial,
                cam_pas_publico,
                cam_pas_particular,
                cyc_carga_oficial,
                cyc_carga_publico,
                cyc_carga_particular,
                moto_oficial,
                moto_de_alquiler,
                moto_particular
        FROM    {0}.vmrc
        ;
    """.format(schema_ori)
    pv = pd.read_sql_query(
        sql=sql_query,
        con=engine_ori,
        index_col=['id_entidad', 'id_municipio', 'anio']
    )
    pv = pv.sum(axis=1)
    pv = pd.DataFrame(index=pv.index, columns=['vehi_tot'], data=pv.values)
    pv = pv.pivot_table(index=['id_entidad', 'id_municipio'], columns=['anio'])
    pv = pv.droplevel(axis=1, level=0).reset_index(drop=False)

    table_name = 'vehiculos_totales'
    dtype = {col:NUMERIC(7,0) for col in pv.columns}
    dtype['id_entidad'] = CHAR(2)
    dtype['id_municipio'] = CHAR(3)
    pv.to_sql(
        name = table_name,
        con = engine_dest,
        schema = schema_dest,
        if_exists = 'replace',
        index=False,
        dtype=dtype
    )

    with engine_dest.begin() as conn:
        conn.execute(
            text(
                """
                SET search_path = {0};
                
                ALTER TABLE {1}
                ADD CONSTRAINT pk_{1}
                PRIMARY KEY (id_entidad, id_municipio);

                ALTER TABLE {1}
                ADD CONSTRAINT fk_{1}_municipio
                FOREIGN KEY (id_entidad, id_municipio)
                REFERENCES municipio (id_entidad, id_municipio)
                ON DELETE RESTRICT
                ON UPDATE RESTRICT;
                """.format(schema_dest, table_name)
            )
        )