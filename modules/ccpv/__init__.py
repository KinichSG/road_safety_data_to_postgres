import pandas as pd
from pathlib import Path
import urllib.request
import zipfile
from sqlalchemy import create_engine, text
from modules import create_engine_string
from scipy.interpolate import Akima1DInterpolator

last_year =1999

path_catalogo_censos = 'catalogo_censos.csv'
path_cat = Path(path_catalogo_censos)

def download_and_extract_censos(path_cat):
    cat = pd.read_csv(path_cat)

    for year in cat.ANIO.unique():
        print(f'{year}:')
        subcat = cat.loc[cat.ANIO == year]
        if year == 2015:
            for url in subcat.URL:
                dir = Path(subcat.loc[subcat.URL == url].iloc[0].DIR)
                dir.parent.mkdir(parents=True, exist_ok=True)
                urllib.request.urlretrieve(url, dir)
                print(f'\tdownloaded from {url}')
        
        else:
            url = subcat.iloc[0].URL
            dir_zip = Path(subcat.iloc[0].DIR_ZIP)
            dir = Path(subcat.iloc[0].DIR)
            dir_zip.parent.mkdir(parents=True, exist_ok=True)
            dir.mkdir(parents=True, exist_ok=True)
            urllib.request.urlretrieve(url, dir_zip)
            print(f'\tdownloaded from {url}')
            with zipfile.ZipFile(dir_zip, 'r') as ext:
                ext.extractall(dir)
            print(f'\textracted in {dir}')



def create_poblacion_inegi(connection_vars, schema_name, dialect='postgresql'):

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
        # create table poblacion_total_inegi
        conn.execute(
            text(
                """
                DROP TABLE IF EXISTS poblacion_total_inegi CASCADE;

                CREATE TABLE poblacion_total_inegi (
                    id_entidad VARCHAR(2) NOT NULL,
                    id_municipio VARCHAR(3) NOT NULL,
                    "1990" NUMERIC(7,0) NULL,
                    "1995" NUMERIC(7,0) NULL,
                    "2000" NUMERIC(7,0) NULL,
                    "2005" NUMERIC(7,0) NULL,
                    "2010" NUMERIC(7,0) NULL,
                    "2015" NUMERIC(7,0) NULL,
                    "2020" NUMERIC(7,0) NULL,

                    CONSTRAINT pk_poblacion_total_inegi
                    PRIMARY KEY (id_entidad, id_municipio),

                    CONSTRAINT fk_poblacion_total_inegi_municipio
                    FOREIGN KEY (id_entidad, id_municipio) REFERENCES tc_municipio (id_entidad, id_municipio)
                    ON DELETE RESTRICT
                    ON UPDATE RESTRICT,

                    CONSTRAINT not_negaive
                    CHECK (
                        "1990" >= 0
                        AND "1995" >= 0
                        AND "2000" >= 0
                        AND "2005" >= 0
                        AND "2010" >= 0
                        AND "2015" >= 0
                        AND "2020" >= 0
                    )
                );
                """
            )
        )
        # create table poblacion_femenina_inegi
        conn.execute(
            text(
                """
                DROP TABLE IF EXISTS poblacion_femenina_inegi CASCADE;

                CREATE TABLE poblacion_femenina_inegi (
                    id_entidad VARCHAR(2) NOT NULL,
                    id_municipio VARCHAR(3) NOT NULL,
                    "1990" NUMERIC(7,0) NULL,
                    "1995" NUMERIC(7,0) NULL,
                    "2000" NUMERIC(7,0) NULL,
                    "2005" NUMERIC(7,0) NULL,
                    "2010" NUMERIC(7,0) NULL,
                    "2015" NUMERIC(7,0) NULL,
                    "2020" NUMERIC(7,0) NULL,

                    CONSTRAINT pk_poblacion_femenina_inegi
                    PRIMARY KEY (id_entidad, id_municipio),

                    CONSTRAINT fk_poblacion_femenina_inegi_municipio
                    FOREIGN KEY (id_entidad, id_municipio) REFERENCES tc_municipio (id_entidad, id_municipio)
                    ON DELETE RESTRICT
                    ON UPDATE RESTRICT,

                    CONSTRAINT not_negaive
                    CHECK (
                        "1990" >= 0
                        AND "1995" >= 0
                        AND "2000" >= 0
                        AND "2005" >= 0
                        AND "2010" >= 0
                        AND "2015" >= 0
                        AND "2020" >= 0
                    )
                );
                """
            )
        )
        # create table poblacion_masculina_inegi
        conn.execute(
            text(
                """
                DROP TABLE IF EXISTS poblacion_masculina_inegi CASCADE;

                CREATE TABLE poblacion_masculina_inegi (
                    id_entidad VARCHAR(2) NOT NULL,
                    id_municipio VARCHAR(3) NOT NULL,
                    "1990" NUMERIC(7,0) NULL,
                    "1995" NUMERIC(7,0) NULL,
                    "2000" NUMERIC(7,0) NULL,
                    "2005" NUMERIC(7,0) NULL,
                    "2010" NUMERIC(7,0) NULL,
                    "2015" NUMERIC(7,0) NULL,
                    "2020" NUMERIC(7,0) NULL,

                    CONSTRAINT pk_poblacion_masculina_inegi
                    PRIMARY KEY (id_entidad, id_municipio),

                    CONSTRAINT fk_poblacion_masculina_inegi_municipio
                    FOREIGN KEY (id_entidad, id_municipio) REFERENCES tc_municipio (id_entidad, id_municipio)
                    ON DELETE RESTRICT
                    ON UPDATE RESTRICT,

                    CONSTRAINT not_negaive
                    CHECK (
                        "1990" >= 0
                        AND "1995" >= 0
                        AND "2000" >= 0
                        AND "2005" >= 0
                        AND "2010" >= 0
                        AND "2015" >= 0
                        AND "2020" >= 0
                    )
                );
                """
            )
        )

    #TRANSFORM
    df = list()
    #2020
    print(2020)
    print(f'\t{'reading censos/2020/iter_00_cpv2020/conjunto_de_datos/conjunto_de_datos_iter_00CSV20.csv'}')
    df_year = pd.read_csv('censos/2020/iter_00_cpv2020/conjunto_de_datos/conjunto_de_datos_iter_00CSV20.csv',
                    usecols=['ENTIDAD', 'NOM_ENT', 'MUN', 'NOM_MUN', 'LOC', 'POBTOT', 'POBFEM', 'POBMAS'],
                    dtype={'ENTIDAD':str, 'NOM_ENT':str, 'MUN':str, 'NOM_MUN':str, 'LOC':str, 'POBTOT':int, 'POBFEM':float, 'POBMAS':float},
                    na_values='*')
    df_year.rename(columns={'ENTIDAD':'ID_ENTIDAD',
                        'NOM_ENT':'NOM_ENTIDAD',
                        'MUN':'ID_MUNICIPIO',
                        'NOM_MUN':'NOM_MUNICIPIO',
                        'LOC':'ID_LOCALIDAD',
                        'POBTOT':'POB_TOT',
                        'POBFEM':'POB_FEM',
                        'POBMAS':'POB_MAS'},
                inplace=True)
    df_year.query("ID_MUNICIPIO != '000' & ID_LOCALIDAD == '0000'", inplace=True)
    df_year.drop(columns='ID_LOCALIDAD', inplace=True)
    df_year['ANIO'] = 2020
    df.append(df_year)
    #2015
    print(2015)
    df_year = list()
    for path_file in sorted(Path('censos/2015').rglob('*.xls')):
        print(f'\treading {path_file}')
        df_i = pd.read_excel(
            io=path_file,
            sheet_name=2,
            header=6)
        df_i.dropna(inplace=True)
        mun_o_del = 'Municipio' if path_file.stem[-2:]!='09' else 'Delegación'
        df_i = df_i.query(f"{mun_o_del} != 'Total' & `Grupos quinquenales de edad` == 'Total' & Estimador == 'Valor'")
        df_i.loc[:, ['ID_ENTIDAD', 'NOM_ENTIDAD']] = df_i['Entidad federativa'].str.split(' ', n=1, expand=True).values
        df_i.loc[:, ['ID_MUNICIPIO', 'NOM_MUNICIPIO']] = df_i[mun_o_del].str.split(' ', n=1, expand=True).values
        df_i = df_i.rename(columns={'Población total':'POB_TOT', 'Mujeres':'POB_FEM', 'Hombres':'POB_MAS'})
        df_i = df_i.loc[:, ['ID_ENTIDAD', 'NOM_ENTIDAD', 'ID_MUNICIPIO', 'NOM_MUNICIPIO', 'POB_TOT', 'POB_FEM', 'POB_MAS']]
        df_year.append(df_i)
    df_year = pd.concat(df_year, axis=0)
    df_year['NOM_MUNICIPIO'] = df_year['NOM_MUNICIPIO'].str.removesuffix(' *')
    df_year['ANIO'] = 2015
    df.append(df_year)
    #2010
    print(2010)
    print(f'\tcensos/2010/iter_00_cpv2010/conjunto_de_datos/iter_00_cpv2010.csv')
    df_year = pd.read_csv('censos/2010/iter_00_cpv2010/conjunto_de_datos/iter_00_cpv2010.csv',
                    usecols=['entidad', 'nom_ent', 'mun', 'nom_mun', 'loc', 'pobtot', 'pobfem', 'pobmas'],
                    dtype={'entidad':str, 'nom_ent':str, 'mun':str, 'nom_mun':str, 'loc':str, 'pobtot':int, 'pobfem':float, 'pobmas':float},
                    na_values='*')
    df_year.rename(columns={'entidad':'ID_ENTIDAD',
                        'nom_ent':'NOM_ENTIDAD',
                        'mun':'ID_MUNICIPIO',
                        'nom_mun':'NOM_MUNICIPIO',
                        'loc':'ID_LOCALIDAD',
                        'pobtot':'POB_TOT',
                        'pobfem':'POB_FEM',
                        'pobmas':'POB_MAS'},
                inplace=True)
    df_year.query("ID_MUNICIPIO != '000' & ID_LOCALIDAD == '0000'", inplace=True)
    df_year.drop(columns='ID_LOCALIDAD', inplace=True)
    df_year['ANIO'] = 2010
    df.append(df_year)
    #2005
    print(2005)
    print(f'\tcensos/2005/cpv2005_iter_00/conjunto_de_datos/cpv2005_iter_00.csv')
    df_year = pd.read_csv('censos/2005/cpv2005_iter_00/conjunto_de_datos/cpv2005_iter_00.csv',
                    usecols=['entidad', 'nom_ent', 'mun', 'nom_mun', 'loc', 'p_total', 'p_fem', 'p_mas'],
                    dtype={'entidad':str, 'nom_ent':str, 'mun':str, 'nom_mun':str, 'loc':str, 'p_total':int, 'p_fem':float, 'p_mas':float},
                    na_values='*')
    df_year.rename(columns={'entidad':'ID_ENTIDAD',
                        'nom_ent':'NOM_ENTIDAD',
                        'mun':'ID_MUNICIPIO',
                        'nom_mun':'NOM_MUNICIPIO',
                        'loc':'ID_LOCALIDAD',
                        'p_total':'POB_TOT',
                        'p_fem':'POB_FEM',
                        'p_mas':'POB_MAS'},
                inplace=True)
    df_year.query("ID_MUNICIPIO != '000' & ID_LOCALIDAD == '0000'", inplace=True)
    df_year.drop(columns='ID_LOCALIDAD', inplace=True)
    df_year['ANIO'] = 2005
    df.append(df_year)
    #2000
    print(2000)
    print(f'\tcensos/2000/cgpv2000_iter_00/conjunto_de_datos/cgpv2000_iter_00.csv')
    df_year = pd.read_csv('censos/2000/cgpv2000_iter_00/conjunto_de_datos/cgpv2000_iter_00.csv',
                    usecols=['entidad', 'nom_ent', 'mun', 'nom_mun', 'loc', 'pobtot', 'pfemeni', 'pmascul'],
                    dtype={'entidad':str, 'nom_ent':str, 'mun':str, 'nom_mun':str, 'loc':str, 'pobtot':int, 'pfemeni':float, 'pmascul':float},
                    na_values='*')
    df_year.rename(columns={'entidad':'ID_ENTIDAD',
                        'nom_ent':'NOM_ENTIDAD',
                        'mun':'ID_MUNICIPIO',
                        'nom_mun':'NOM_MUNICIPIO',
                        'loc':'ID_LOCALIDAD',
                        'pobtot':'POB_TOT',
                        'pfemeni':'POB_FEM',
                        'pmascul':'POB_MAS'},
                inplace=True)
    df_year.query("ID_MUNICIPIO != '000' & ID_LOCALIDAD == '0000'", inplace=True)
    df_year.drop(columns='ID_LOCALIDAD', inplace=True)
    df_year['ANIO'] = 2000
    df.append(df_year)
    #1995
    print(1995)
    print(f'\tcensos/1995/ITER_NALTXT95.txt')
    df_year = pd.read_csv('censos/1995/ITER_NALTXT95.txt',
                    header=None,
                    sep='\\t',
                    usecols=[0, 1, 2, 3, 4, 9, 11, 10],
                    encoding='latin1',
                    engine='python'
                    )
    df_year.rename(columns={0:'ID_ENTIDAD',
                        1:'NOM_ENTIDAD',
                        2:'ID_MUNICIPIO',
                        3:'NOM_MUNICIPIO',
                        4:'ID_LOCALIDAD',
                        9:'POB_TOT',
                        11:'POB_FEM',
                        10:'POB_MAS'},
                inplace=True)
    for col in df_year.columns:
        df_year.loc[:, col] = df_year[col].str.replace('"', '')
    df_year.query("ID_MUNICIPIO != '000' & ID_LOCALIDAD == '0000'", inplace=True)
    df_year.drop(columns='ID_LOCALIDAD', inplace=True)
    df_year = pd.concat([df_year, pd.DataFrame(['07', 'Chiapas', '999', 'Otros municipios', 519686, 260492, 259194], index=df_year.columns).T], axis=0)
    df_year['POB_TOT'] = df_year['POB_TOT'].astype(int)
    df_year['POB_FEM'] = df_year['POB_FEM'].astype(int)
    df_year['POB_MAS'] = df_year['POB_MAS'].astype(int)
    df_year.sort_values(['ID_ENTIDAD', 'ID_MUNICIPIO'], inplace=True)
    df_year['ANIO'] = 1995
    df.append(df_year)
    #1990
    print(1990)
    print(f'\tcensos/1990/ITER_NALTXT90.txt')
    df_year = pd.read_csv('censos/1990/ITER_NALTXT90.txt',
                    sep='\\t',
                    usecols=['entidad', 'nom_ent', 'mun', 'nom_mun', 'loc', 'p_total', 'mujeres', 'hombres'],
                    encoding='latin1',
                    engine='python'
                    )
    df_year.rename(columns={'entidad':'ID_ENTIDAD',
                        'nom_ent':'NOM_ENTIDAD',
                        'mun':'ID_MUNICIPIO',
                        'nom_mun':'NOM_MUNICIPIO',
                        'loc':'ID_LOCALIDAD',
                        'p_total':'POB_TOT',
                        'mujeres':'POB_FEM',
                        'hombres':'POB_MAS'},
                inplace=True)
    for col in df_year.columns:
        df_year.loc[:, col] = df_year[col].str.replace('"', '')
    df_year.query("ID_MUNICIPIO != '000' & ID_LOCALIDAD == '0000'", inplace=True)
    df_year.drop(columns='ID_LOCALIDAD', inplace=True)
    df_year['POB_TOT'] = df_year['POB_TOT'].astype(int)
    df_year['POB_FEM'] = df_year['POB_FEM'].astype(int)
    df_year['POB_MAS'] = df_year['POB_MAS'].astype(int)
    df_year['ANIO'] = 1990
    df.append(df_year)
    # Concat all the DataFrames
    df = pd.concat(df, axis=0)
    df.columns = df.columns.str.lower()
    df_tot = df.pivot_table(index=['id_entidad', 'id_municipio'], columns='anio', values='pob_tot')
    df_fem = df.pivot_table(index=['id_entidad', 'id_municipio'], columns='anio', values='pob_fem')
    df_mas = df.pivot_table(index=['id_entidad', 'id_municipio'], columns='anio', values='pob_mas')

    #LOAD
    df_tot.to_sql(
        name=f'poblacion_total_inegi',
        con=engine,
        schema=schema_name,
        if_exists='append',
        index=True
    )
    df_fem.to_sql(
        name=f'poblacion_femenina_inegi',
        con=engine,
        schema=schema_name,
        if_exists='append',
        index=True
    )
    df_mas.to_sql(
        name=f'poblacion_masculina_inegi',
        con=engine,
        schema=schema_name,
        if_exists='append',
        index=True
    )
    # print(f'{schema}.pobmas_censos_{period} created')



def estimate_chiapas(connection_vars, schema_name, last_year, dialect='postgresql'):
    def Interpolacion_Chiapas_Akima(serie, inicio=1990, fin=last_year):
        """
        Interpola la serie de tiempo de la población de los municipios.

        Args:
            serie (pd.Series): Serie de tiempo de población.
            inicio (int): Primer año del periodo de estimación.
            fin (int): Último año del periodo de estimación.
        Returns:
            interp (pd.Series): Serie de datos de la interpolación.
        """
        
        años = [i for i in range(inicio, fin+1)]                        # Años que abarca la estimación

        # Datos de entrada para la interpolación
        x = serie.dropna().index                                        # Año
        y = serie.dropna().values                                       # Población

        p = Akima1DInterpolator(x, y)                                   # Interpolador Akima.

        # Datos de salida de la interpolación
        x_interp = años                                                 # Año
        y_interp = p.__call__(x_interp, extrapolate=True)               # Población

        interp = pd.Series(y_interp, index=x_interp).round(0)           # Serie de tiempo con población redondeada a números enteros

        return interp
    
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
        # create table poblacion_total
        print('creating poblacion_total')
        conn.execute(
            text(
                """
                DROP TABLE IF EXISTS poblacion_total CASCADE;

                CREATE TABLE poblacion_total (
                    id_entidad CHAR(2) NOT NULL,
                    id_municipio CHAR(3) NOT NULL,
                    "1990" NUMERIC(7,0) NULL,
                    "1995" NUMERIC(7,0) NULL,
                    "2000" NUMERIC(7,0) NULL,
                    "2005" NUMERIC(7,0) NULL,
                    "2010" NUMERIC(7,0) NULL,
                    "2015" NUMERIC(7,0) NULL,
                    "2020" NUMERIC(7,0) NULL,

                    CONSTRAINT pk_poblacion_total
                    PRIMARY KEY (id_entidad, id_municipio),

                    CONSTRAINT fk_poblacion_total_municipio
                    FOREIGN KEY (id_entidad, id_municipio) REFERENCES tc_municipio (id_entidad, id_municipio)
                    ON DELETE RESTRICT
                    ON UPDATE RESTRICT,

                    CONSTRAINT not_negaive
                    CHECK (
                        "1990" >= 0
                        AND "1995" >= 0
                        AND "2000" >= 0
                        AND "2005" >= 0
                        AND "2010" >= 0
                        AND "2015" >= 0
                        AND "2020" >= 0
                    )
                );
                """
            )
        )
        # create table poblacion_femenina
        print('creating poblacion_femenina')
        conn.execute(
            text(
                """
                DROP TABLE IF EXISTS poblacion_femenina CASCADE;

                CREATE TABLE poblacion_femenina (
                    id_entidad CHAR(2) NOT NULL,
                    id_municipio CHAR(3) NOT NULL,
                    "1990" NUMERIC(7,0) NULL,
                    "1995" NUMERIC(7,0) NULL,
                    "2000" NUMERIC(7,0) NULL,
                    "2005" NUMERIC(7,0) NULL,
                    "2010" NUMERIC(7,0) NULL,
                    "2015" NUMERIC(7,0) NULL,
                    "2020" NUMERIC(7,0) NULL,

                    CONSTRAINT pk_poblacion_femenina
                    PRIMARY KEY (id_entidad, id_municipio),

                    CONSTRAINT fk_poblacion_femenina_municipio
                    FOREIGN KEY (id_entidad, id_municipio) REFERENCES tc_municipio (id_entidad, id_municipio)
                    ON DELETE RESTRICT
                    ON UPDATE RESTRICT,

                    CONSTRAINT not_negaive
                    CHECK (
                        "1990" >= 0
                        AND "1995" >= 0
                        AND "2000" >= 0
                        AND "2005" >= 0
                        AND "2010" >= 0
                        AND "2015" >= 0
                        AND "2020" >= 0
                    )
                );
                """
            )
        )
        # create table poblacion_masculina
        print('creating poblacion_masculina')
        conn.execute(
            text(
                """
                DROP TABLE IF EXISTS poblacion_masculina CASCADE;

                CREATE TABLE poblacion_masculina (
                    id_entidad CHAR(2) NOT NULL,
                    id_municipio CHAR(3) NOT NULL,
                    "1990" NUMERIC(7,0) NULL,
                    "1995" NUMERIC(7,0) NULL,
                    "2000" NUMERIC(7,0) NULL,
                    "2005" NUMERIC(7,0) NULL,
                    "2010" NUMERIC(7,0) NULL,
                    "2015" NUMERIC(7,0) NULL,
                    "2020" NUMERIC(7,0) NULL,

                    CONSTRAINT pk_poblacion_masculina
                    PRIMARY KEY (id_entidad, id_municipio),

                    CONSTRAINT fk_poblacion_masculina_municipio
                    FOREIGN KEY (id_entidad, id_municipio) REFERENCES tc_municipio (id_entidad, id_municipio)
                    ON DELETE RESTRICT
                    ON UPDATE RESTRICT,

                    CONSTRAINT not_negaive
                    CHECK (
                        "1990" >= 0
                        AND "1995" >= 0
                        AND "2000" >= 0
                        AND "2005" >= 0
                        AND "2010" >= 0
                        AND "2015" >= 0
                        AND "2020" >= 0
                    )
                );
                """
            )
        )
    # extract data
    print('extracting poblacion_total_inegi')
    df_pobtot = pd.read_sql_table(table_name='poblacion_total_inegi', con=engine, schema=schema_name, index_col=['id_entidad', 'id_municipio'])
    print('extracting poblacion_femenina_inegi')
    df_pobfem = pd.read_sql_table(table_name='poblacion_femenina_inegi', con=engine, schema=schema_name, index_col=['id_entidad', 'id_municipio'])
    print('extracting poblacion_masculina_inegi')
    df_pobmas = pd.read_sql_table(table_name='poblacion_masculina_inegi', con=engine, schema=schema_name, index_col=['id_entidad', 'id_municipio'])

    dfs = [df_pobtot, df_pobfem, df_pobmas]
    names = ['poblacion_total', 'poblacion_femenina', 'poblacion_masculina']
    for i in range(0, 3):
        print(f'loading {names[i]}')
        df = dfs[i]
        muns_na_95 = df.loc[(df["1995"].isna())&(~df["1990"].isna())]
        other_muns = df.loc[df.index.get_level_values(1)=='999', "1995"]                 # DataFrame con población del resto de municipios en Chiapas en 1995.
        interp_95 = muns_na_95.apply(Interpolacion_Chiapas_Akima, axis=1)                       # DataFrame con la interpolación de los municipios sin dato en 1995.
        
        without_other_muns = df.copy()                                                          # Copia del DataFrame de censos con el resto de municipios.
        without_other_muns = without_other_muns.drop('999', axis=0, level=1)    # Retira el resto de municipios.

        porcentajes_95 = interp_95[1995].div(interp_95[1995].sum())                             # Obtiene el porcentaje de población que representa cada municipio respecto a la suma total de las estimaciones.
        
        without_other_muns.loc[interp_95.index, "1995"] = porcentajes_95 * other_muns.iloc[0]          # Asigna proporcionalmente de la población del resto de municipios a los 15 municipios sin datos.

        # without_other_muns.columns = f'{cve_pob}_' + without_other_muns.columns.astype(str)     # Cambia el nombre de las columnas.

        without_other_muns.to_sql(
            name=names[i],
            con=engine,
            schema=schema_name,
            if_exists='append',
            index=True
        )