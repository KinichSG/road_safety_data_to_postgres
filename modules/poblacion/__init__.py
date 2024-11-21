from modules import create_engine_string
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from scipy.interpolate import Akima1DInterpolator

def etl_poblacion(connection_ori, connection_dest, schema_ori, schema_dest, last_year):
    def Interpolacion_Akima(serie, inicio=None, fin=last_year):
        """
        """

        if inicio==None:
            inicio = nuevos.loc[nuevos.cvegeomuni_act==serie.name, 'anio_act'].unique()[0] if serie.name in nuevos.cvegeomuni_act.unique() else 1990

        # inicio = min(inicio, 1990)
        # fin = max(fin, last_year)

        años = [i for i in range(inicio, fin+1)]

        # if serie.count() == 1:
        #     y_1 = serie.mean()

        if serie.count() >= 2:
            x = serie.dropna().index
            y = serie.dropna().values

            p = Akima1DInterpolator(x, y)

            x_1 = años
            y_1 = p.__call__(x_1, extrapolate=True)

        else:
            y_1 = serie

        interp = pd.Series(y_1, index=años).round(0)

        return interp
    

    engine_string_dest = create_engine_string(connection_dest)
    engine_dest = create_engine(engine_string_dest)

    # for table_name in ['poblacion_total', 'poblacion_femenina', 'poblacion_masculina']:
    #     with engine_dest.begin() as conn:
    #         conn.execute(
    #             text(
    #                 """
    #                 SET search_path = {0};
                    
    #                 DROP TABLE IF EXISTS pobtot CASCADE;

    #                 CREATE TABLE pobtot (
    #                     id_entidad CHAR(2) NOT NULL,
    #                     id_municipio CHAR(3) NOT NULL

    #                 );
    #                 """.format(schema_dest)
    #             )
    #         )
    
    engine_string_ori = create_engine_string(connection_ori)
    engine_ori = create_engine(engine_string_ori)


    nuevos = pd.read_sql_table('nuevos_municipios', engine_ori, schema_ori)
    nuevos[['anio_act', 'anio_censo', 'bloque']] = nuevos[['anio_act', 'anio_censo', 'bloque']].astype(int)
    nuevos['cvegeomuni_act'] = nuevos['cve_ent_act'] + nuevos['cve_mun_act']
    nuevos['cvegeomuni_ori'] = nuevos['cve_ent_ori'] + nuevos['cve_mun_ori']
    nuevos = nuevos.loc[nuevos.anio_censo<=last_year]

    
    for table_name in ['poblacion_total', 'poblacion_femenina', 'poblacion_masculina']:
        censos = pd.read_sql_table(table_name=table_name, con=engine_ori, schema=schema_ori, index_col=['id_entidad', 'id_municipio'])
        censos['cvegeomuni'] = censos.index.get_level_values(0) + censos.index.get_level_values(1)
        censos = censos.set_index('cvegeomuni', drop=True)
        censos = censos.T
        censos.index = censos.index.astype(int)
        interp = censos.apply(Interpolacion_Akima)
        # k = 20 #33, 34
        lista_bloques_interp = []
        lista_bloques_censos = []
        for b in nuevos.bloque.unique():#[k:k+1]:
            # Datos básicos del bloque.
            bloque = nuevos.loc[nuevos.bloque==b]
            acts = bloque.cvegeomuni_act.unique()
            oris = bloque.cvegeomuni_ori.unique()
            oris = np.delete(oris, [i in acts for i in oris])
            muns = np.append(oris, acts)
            anios_censos = bloque.anio_censo.unique()
            anios_acts = bloque.anio_act.unique()
            # 1)
            # Series de datos censales municipales originales.
            censos_b = censos[muns]
            # Serie de datos censales de la suma de los municipios originales.
            censos_b.insert(
                len(censos_b.columns),
                '+'.join(muns),
                censos_b[muns].sum(axis=1)
            )
            # )2
            for mun in muns:
                a_cen_1 = 1990
                bloque_mun = bloque.loc[(bloque.cvegeomuni_act.isin([mun]))|(bloque.cvegeomuni_ori.isin([mun]))]
                anios_censos_mun = bloque_mun.anio_censo.unique()
                for a_cen_2 in anios_censos_mun:
                    censos_b.loc[a_cen_1:a_cen_2-1, mun+f'_{a_cen_1}{a_cen_2}'] = censos_b.loc[a_cen_1:a_cen_2-1, mun]
                    a_cen_1 = a_cen_2
                    if a_cen_1 == anios_censos_mun[-1]:
                        censos_b.loc[a_cen_1:last_year, mun+f'_{a_cen_1}{last_year}'] = censos_b.loc[a_cen_1:last_year, mun]
            censos_b = censos_b.dropna(axis=1, how='all')
#             # # Serie de datos censales de los municipios generales auxiliar.
#             # for mun in muns:
#             #     censos_b.insert(
#             #         len(censos_b.columns),
#             #         mun+f'_{1990}{last_year}',
#             #         censos_b[mun].values
#             #     )
#             # # División de las series de datos censales municipales en segmentos.
#             # a_cen_1 = 1990
#             # for a_cen_2 in anios_censos[:]:
#             #     bloque_cen = bloque.loc[bloque.anio_censo==a_cen_2]
#             #     acts_cen = bloque_cen.cvegeomuni_act.unique()
#             #     oris_cen = bloque_cen.cvegeomuni_ori.unique()
#             #     oris_cen = np.delete(oris_cen, [i in acts_cen for i in oris_cen])
#             #     muns_cen = np.append(oris_cen, acts_cen)
#             #     censos_b.loc[a_cen_1:a_cen_2-1, oris_cen+f'_{a_cen_1}{a_cen_2}'] = censos_b.loc[a_cen_1:a_cen_2-1, oris_cen].values
#             #     censos_b = censos_b.loc[:, ~(censos_b.columns.str.startswith(tuple(muns_cen)) & censos_b.columns.str.endswith(str(last_year)))]
#             #     censos_b.loc[a_cen_2:, muns_cen+f'_{a_cen_2}{last_year}'] = censos_b.loc[a_cen_2:, muns_cen].values
#             #     a_cen_1 = a_cen_2
            # 3)
            # Interpolaciones de las series anteriores.
            interp_b = censos_b.apply(Interpolacion_Akima)
            # muns_1 = interp_b.columns[interp_b.count()==1]
            # if len(muns_1) >= 1:
            #     interp_1 = interp_b.loc[:, muns_1]
            #     porcentaje = interp_1.div(interp_b['+'.join(muns)], axis=0).mean()
            #     interp_b[muns_1] = interp_b[['+'.join(muns)]*len(muns_1)].mul(porcentaje.values, axis=1).values
            # 4)
            # Series de datos ficticias en las que no se creó el municipio.
            muns_a = muns.copy()
            for a in anios_acts[::-1]:
                bloque_a = bloque.loc[bloque.anio_act==a]
                acts_a = bloque_a.cvegeomuni_act.unique()
                muns_a = np.delete(muns_a, [i in acts_a for i in muns_a])
                if '+'.join(muns_a) not in censos_b.columns:
                    censos_b.insert(
                        len(censos_b.columns),
                        '+'.join(muns_a),
                        censos_b[muns_a].sum(axis=1)
                        )
            # Estimación de valores extendidos de las series segmentadas.
            a_cen_2 = last_year
            for a_cen_1 in anios_censos[::-1]:
                bloque_cen = bloque.loc[bloque.anio_censo==a_cen_1]
                acts_cen = bloque_cen.cvegeomuni_act.unique()
                oris_cen = bloque_cen.cvegeomuni_ori.unique()
                oris_cen = np.delete(oris_cen, [i in acts_cen for i in oris_cen])
                muns_cen = np.append(oris_cen, acts_cen)
                salto = 5 if a_cen_1!=1990 else 10
                censos_b['+'.join(muns_cen)] = censos_b[muns_cen].sum(axis=1)
                porcentajes_muns = (interp_b.loc[a_cen_1-salto, interp_b.columns.str.contains(f'_{a_cen_1}')]
                                    .div(interp_b.loc[a_cen_1-salto, interp_b.columns.str.contains(f'_{a_cen_1}')].sum(), axis=0))
                censos_b.loc[a_cen_1-salto, censos_b.columns.str.contains(f'_{a_cen_1}')] = (censos_b.loc[a_cen_1-salto, ['+'.join(muns_cen)]*len(muns_cen)]
                                                                                            .mul(porcentajes_muns.values).round().values)
                porcentajes_oris = (interp_b.loc[a_cen_1, interp_b.columns.str.endswith(f'{a_cen_1}')]
                                    .div(interp_b.loc[a_cen_1, interp_b.columns.str.endswith(f'{a_cen_1}')].sum(), axis=0))
                censos_b.loc[a_cen_1, censos_b.columns.str.endswith(f'{a_cen_1}')] = (censos_b.loc[a_cen_1, ['+'.join(muns_cen)]*len(oris_cen)]
                                                                                    .mul(porcentajes_oris.values).round().values)
                a_cen_2 = a_cen_1
            # 5)
            # Calcular las interpolaciones finales de los segmentos.
            interp_b = censos_b.apply(Interpolacion_Akima)
            muns_1 = interp_b.columns[interp_b.count()==1]
            if len(muns_1) >= 1:
                interp_1 = interp_b.loc[:, muns_1]
                porcentaje = interp_1.div(interp_b['+'.join(muns)], axis=0).mean()
                interp_b[muns_1] = interp_b[['+'.join(muns)]*len(muns_1)].mul(porcentaje.values, axis=1).values
            # 6)
            # Unión de los segmentos.
            interp_b[muns] = np.nan
            interp_b = interp_b[interp_b.columns.to_series().sort_index()]
            for a_cen in anios_censos[::-1]:
                bloque_cen = bloque.loc[bloque.anio_censo==a_cen]
                acts_cen = bloque_cen.cvegeomuni_act.unique()
                oris_cen = bloque_cen.cvegeomuni_ori.unique()
                oris_cen = np.delete(oris_cen, [i in acts_cen for i in oris_cen])
                muns_cen = np.append(oris_cen, acts_cen)
                anios_acts_cen = bloque_cen.loc[bloque_cen.anio_censo==a_cen, 'anio_act'].unique()
                if len(anios_acts_cen) == 1:
                    a = anios_acts_cen[0]
                    interp_b.loc[a:, muns_cen] = (interp_b.loc[a:, muns_cen]
                                                .mask(interp_b.loc[a:, muns_cen].isna(),
                                                        interp_b.loc[a:, interp_b.columns.str.startswith(tuple(muns_cen)) & interp_b.columns.str.contains(f'_{a_cen}')].values))
                else:
                    for a_1 in anios_acts[::-1]:
                        if a_1 == anios_acts[-1]:
                            print('1)', a_1)
                            interp_b.loc[a_1:, muns_cen] = (interp_b.loc[a_1:, muns_cen]
                                                        .mask(interp_b.loc[a_1:, muns_cen].isna(),
                                                                interp_b.loc[a_1:, interp_b.columns.str.startswith(tuple(muns_cen)) & interp_b.columns.str.contains(f'_{a_cen}')].values))
                            a_2 = a_1
                        else:
                            print('2)', a_1)
                            bloque_a = bloque.loc[bloque.anio_act==a_1]
                            acts_a = bloque_a.cvegeomuni_act.unique()
                            oris_a = bloque_a.cvegeomuni_ori.unique()
                            muns_a = np.append(oris_a, acts_a)
                            for mun_a in muns_a:
                                interp_b.loc[a_1:a_2-1, mun_a] = interp_b.loc[a_1:a_2-1, interp_b.columns.str.startswith(mun_a) & (interp_b.columns.str.contains(f'_{a_cen}') | interp_b.columns.str.endswith(f'{a_cen}'))].mean(axis=1)

            interp_b.loc[:anios_acts[-1]-1, oris_cen] = (interp_b.loc[:anios_acts[-1]-1, oris_cen]
                                                        .mask(interp_b.loc[:anios_acts[-1]-1, oris_cen].isna(),
                                                            interp_b.loc[:anios_acts[-1]-1, interp_b.columns.str.startswith(tuple(oris_cen)) & interp_b.columns.str.contains(f'_{1990}')].values))
            lista_bloques_interp.append(interp_b[mun])
            # lista_bloques_censos.append(censos_b[np.append('+'.join(muns), muns)])
        lista_bloques_interp = pd.concat(lista_bloques_interp, axis=1)
        interp[lista_bloques_interp.columns] = lista_bloques_interp
        interp = interp.round(decimals=0)
        interp = interp.T
        interp['id_entidad'] = interp.index.map(lambda x: x[:2])
        interp['id_municipio'] = interp.index.map(lambda x: x[2:])
        interp = interp.set_index(['id_entidad', 'id_municipio'], drop=True).reset_index(drop=False)
        interp.to_sql(
            name = table_name,
            con = engine_dest,
            schema = schema_dest,
            if_exists = 'replace',
            index=False
        )

    # for table_name in ['poblacion_total', 'poblacion_femenina', 'poblacion_masculina']:
    with engine_dest.begin() as conn:
        conn.execute(
            text(
                """
                SET search_path = {0};
                
                ALTER TABLE poblacion_total
                ADD CONSTRAINT pk_poblacion_total
                PRIMARY KEY (id_entidad, id_municipio);

                ALTER TABLE poblacion_total
                ADD CONSTRAINT fk_poblacion_total_municipio
                FOREIGN KEY (id_entidad, id_municipio)
                REFERENCES municipio (id_entidad, id_municipio)
                ON DELETE RESTRICT
                ON UPDATE RESTRICT;

                ALTER TABLE poblacion_femenina
                ADD CONSTRAINT pk_poblacion_femenina
                PRIMARY KEY (id_entidad, id_municipio);

                ALTER TABLE poblacion_femenina
                ADD CONSTRAINT fk_poblacion_femenina_municipio
                FOREIGN KEY (id_entidad, id_municipio)
                REFERENCES municipio (id_entidad, id_municipio)
                ON DELETE RESTRICT
                ON UPDATE RESTRICT;

                ALTER TABLE poblacion_masculina
                ADD CONSTRAINT pk_poblacion_masculina
                PRIMARY KEY (id_entidad, id_municipio);

                ALTER TABLE poblacion_masculina
                ADD CONSTRAINT fk_poblacion_masculina_municipio
                FOREIGN KEY (id_entidad, id_municipio)
                REFERENCES municipio (id_entidad, id_municipio)
                ON DELETE RESTRICT
                ON UPDATE RESTRICT;
                """.format(schema_dest)
            )
        )