import pandas as pd
import numpy as np
import camelot
from pathlib import Path
from sqlalchemy import create_engine, text
from modules import create_engine_string

def create_nuevos_municipios(path_nuevos_municipios_info, path_info_2000_20XX, connection_vars, schema_name, last_year, dialect='postgresql'):
    def create_bloques(nuevos):
        nuevos['cve_act'] = nuevos['cve_ent_act'] + nuevos['cve_mun_act']
        nuevos['cve_ori'] = nuevos['cve_ent_ori'] + nuevos['cve_mun_ori']
        # Asignación de bloques de municipios.
        list_act = []                                                           # Lista de municipios nuevos
        cves_bloques = []                                                       # Lista de claves de los bloques
        j = 1                                                                   # Contador de bloques
        # Por cada nuevo municipio
        for act in nuevos.cve_act.unique()[:]:
            if act not in list_act:
                bloque = nuevos.loc[nuevos.cve_act.isin([act])]          # Creación del bloque a partir del municipio act.
                while True:
                    acts = bloque.cve_act.unique()                       # Conjunto de municipios nuevos que incluye el bloque.
                    oris = bloque.cve_ori.unique()                       # Conjunto de municipios originales que incluye el bloque.
                    muns = np.append(acts, oris)                                # Conjunto de municipios en el bloque
                    # Bloque para comparar que incluye al conjunto de municipios del bloque.
                    bloque_comp = nuevos.loc[nuevos.cve_ori.isin(muns) | nuevos.cve_act.isin(muns)]
                    acts_comp = bloque_comp.cve_act.unique()             # Conjunto de municipios nuevos que incluye el bloque con el que se compara.
                    oris_comp = bloque_comp.cve_ori.unique()             # Conjunto de municipios originales que incluye el bloque con el que se compara.
                    muns_comp = np.append(acts_comp, oris_comp)                 # Conjunto de municipios en el bloque con el que se compara
                    # Comprobar si hay municipios nuevos creados a partir de los municipios originales que no estén incluidos en el bloque.
                    if len(muns) == len(muns_comp):
                        break               # En caso de ser iguales, ya se tiene el bloque completo y se interrumpe el ciclo.
                    bloque = bloque_comp                                        # De lo contrario, se utiliza ahora el bloque que tiene más municipios.
                # Ya que se tiene el bloque completo, se agregan todos los municipios nuevos que incluye en la lista de municipios nuevos.
                for a in acts:
                    list_act.append(a)                                          # Agrega las claves de los municipios nuevos a la lista.
                    cves_bloques.append(j)#] {len(oris)}:{len(acts)}')     # Agrega la clave del bloque a la lista.
                j = j+1
        # Crea una serie de nuevos municipios y el bloque al que pertenecen.
        bloques = pd.Series(cves_bloques, index=list_act, name='bloque')#pd.DataFrame([cves_bloques], index=['bloque'], columns=list_act).T

        # Agrega la columna de bloque al DataFrame de nuevos municipios.
        nuevos['bloque'] = nuevos.cve_act.map(lambda x: bloques[x])
        # nuevos.insert(len(nuevos.columns)-2,
        #               'bloque',
        #               nuevos.cve_act.map(lambda x: bloques[x]))
        nuevos = nuevos.sort_values(['bloque', 'anio_act', 'cve_act'])
        nuevos = nuevos.drop(columns=['cve_act', 'cve_ori'])
        return nuevos

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
                DROP TABLE IF EXISTS nuevos_municipios;

                CREATE TABLE nuevos_municipios (
                    id_nuevos SERIAL NOT NULL,
                    cve_ent_ori CHAR(2) NOT NULL,
                    cve_mun_ori CHAR(3) NOT NULL,
                    cve_ent_act CHAR(2) NOT NULL,
                    cve_mun_act CHAR(3) NOT NULL,
                    fecha_act TIMESTAMP NOT NULL,
                    anio_act NUMERIC(4, 0) NOT NULL,
                    anio_censo NUMERIC(4, 0) NOT NULL,
                    bloque NUMERIC(2,0) NOT NULL,

                    CONSTRAINT pk_nuevos
                    PRIMARY KEY (id_nuevos),

                    CONSTRAINT fk_nuevos_ori_municipios
                    FOREIGN KEY (cve_ent_ori, cve_mun_ori)
                    REFERENCES tc_municipio (id_entidad, id_municipio)
                    ON DELETE RESTRICT
                    ON UPDATE RESTRICT,

                    CONSTRAINT fk_nuevos_act_municipios
                    FOREIGN KEY (cve_ent_act, cve_mun_act)
                    REFERENCES tc_municipio (id_entidad, id_municipio)
                    ON DELETE RESTRICT
                    ON UPDATE RESTRICT,

                    CONSTRAINT fecha_inicio
                    CHECK (fecha_act >= '1990-01-01' and fecha_act <= :date),

                    CONSTRAINT anio_act_dominio
                    CHECK (anio_act >= 1990 AND anio_act <= :y_act),

                    CONSTRAINT anio_censo_dominio
                    CHECK (anio_censo >= 1990 AND anio_censo <= :y_censo),

                    CONSTRAINT bloque_positive
                    CHECK (bloque::numeric > 0)
                );
                """
            ),
            {'date': f'{last_year}-12-31', 'y_act': last_year, 'y_censo': last_year-(last_year%5)+5}
        )

    path_nuevos_municipios_info = Path(path_nuevos_municipios_info)
    nuevos_info = pd.read_csv(path_nuevos_municipios_info, index_col='anio')
    nuevos_info['drop_rows'] = nuevos_info['drop_rows'].str.split(';')
    nuevos_info['replace'] = nuevos_info['replace'].str.split(';')

    path_info_2000_20XX = Path(path_info_2000_20XX)
    path_info_2000_20XX = Path('nuevos_municipios/nuevos_municipios_00XX.csv')
    info_2000_20XX = pd.read_csv(path_info_2000_20XX)

    df_1990_1995 = nuevos_municipios_1990_2000(1995, nuevos_info)
    df_1995_2000 = nuevos_municipios_1990_2000(2000, nuevos_info)
    df_2000_20XX = nuevos_municipios_2000_20XX(last_year, info_2000_20XX)
    df = pd.concat([df_1990_1995, df_1995_2000, df_2000_20XX], axis=0)
    df.columns = df.columns.str.lower()
    df.insert(loc=0, value=[i for i in range(1, len(df)+1)], column='id_nuevos')
    df = create_bloques(df)
    print(df)
    df.to_sql(name='nuevos_municipios', con=engine, schema=schema_name, if_exists='append', index=False)

    return df

def nuevos_municipios_1990_2000(anio, nuevos_info):
    nuevos_info_anio = nuevos_info.loc[anio]
    url, joint_tol, drop_row, replace, date_sep = nuevos_info_anio.to_list()
    drop_row = [int(i) for i in drop_row]
    print(f'{anio-5}-{anio}: Reading {url}')
    tables = camelot.read_pdf(filepath=url,
                            pages='all',
                            strip_text='\n',
                            joint_tol=joint_tol,
                            )
    df = pd.DataFrame()
    for table in tables:
        df = pd.concat([df, table.df], axis=0)
    print(f'{anio-5}-{anio}: Transforming')
    df = df.reset_index(drop=True)
    df = df.drop(drop_row)
    df = df.replace('', np.nan).ffill()
    df['CVE_ENT_ORI'] = df.iloc[:, 0].str.extract('([0-9]{2})', expand=True)
    df['CVE_ENT_ACT'] = df.iloc[:, 0].str.extract('([0-9]{2})', expand=True)
    df['CVE_MUN_ACT'] = df.iloc[:, 1].str.extract('([0-9]{3,4})', expand=False)
    df = df.loc[df.CVE_MUN_ACT.str.len()==3]
    date = df.iloc[:, 3].copy()
    date = date.str.extract('([0-9]{2}/[0-9]{2}/[0-9]{2,4})', expand=False)
    date = date.str.replace(replace[0], replace[1]).str.rsplit('/', n=1).str.join(date_sep)
    date = pd.to_datetime(date, format='%d/%m/%Y').dt.normalize()
    df.loc[:, 'FECHA_ACT'] = date
    df.loc[:, 'ANIO_ACT'] = df['FECHA_ACT'].dt.year
    df.iloc[:, 4] = df.iloc[:, 4].str.replace('(,\\s|\\sy\\s)', ',', regex=True).str.split(',')
    df = df.explode(column=df.columns[4])
    df.loc[:, 'CVE_MUN_ORI'] = df.iloc[:, 4].str.extract('([0-9]{3})', expand=False)
    df['ANIO_CENSO'] = anio
    df.loc[:, 'ANIO_CENSO'] = df['ANIO_CENSO'].mask(df['ANIO_ACT']<=anio-5, anio-5)
    df = df[['CVE_ENT_ORI', 'CVE_MUN_ORI', 'CVE_ENT_ACT', 'CVE_MUN_ACT', 'FECHA_ACT', 'ANIO_ACT', 'ANIO_CENSO']].dropna()
    print(f'{anio-5}-{anio}: Done')
    return df

def nuevos_municipios_2000_20XX(last_year, df_00XX):
    # print(f'2000-{last_year}: Reading {csv}')
    # df_00XX = pd.read_csv(csv, dtype=str)
    print(f'2000-{last_year}: Transforming')
    df_00XX = df_00XX.loc[df_00XX.DESCRIP=='Nuevo municipio']
    df_00XX = df_00XX.loc[df_00XX.CVE_ENT_ORI.str.isnumeric() & df_00XX.CVE_ENT_ACT.str.isnumeric()]
    df_00XX['CVE_MUN_ORI'] = df_00XX['CVE_MUN_ORI'].astype(str).str.rjust(3, '0')
    df_00XX['CVE_MUN_ACT'] = df_00XX['CVE_MUN_ACT'].astype(int).astype(str).str.rjust(3, '0')
    df_00XX['FECHA_ACT'] = pd.to_datetime(df_00XX.FECHA_ACT, format='%Y-%M-%d').dt.normalize()
    df_00XX['ANIO_ACT'] = df_00XX['FECHA_ACT'].dt.year
    cut_bins = [i for i in range(2000, last_year+10, 5)]
    cut_labels = cut_bins[1:]
    df_00XX['ANIO_CENSO'] = pd.cut(x=df_00XX.ANIO_ACT, bins=cut_bins, labels=cut_labels)
    df_00XX = df_00XX.loc[df_00XX.ANIO_ACT <= last_year]
    df_00XX = df_00XX[['CVE_ENT_ORI', 'CVE_MUN_ORI', 'CVE_ENT_ACT', 'CVE_MUN_ACT', 'FECHA_ACT', 'ANIO_ACT', 'ANIO_CENSO']]
    print(f'2000-{last_year}: Done')
    return df_00XX