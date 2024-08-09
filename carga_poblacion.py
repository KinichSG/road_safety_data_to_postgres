import pandas as pd
import urllib.request
import zipfile
import pathlib
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, VARCHAR, INT

path_cat = pathlib.Path('catalogo_censos.csv')
connection_dest = {
    'user': 'seguridad_vial',
    'password': 'popcorning',
    'host': 'localhost',
    'port': 5432,
    'dbname': 'seguridad_vial'
}
period = '9020'
schema = f'poblacion_{period}'

def download_and_extract_censos(path_cat):
    cat = pd.read_csv(path_cat)

    for year in cat.ANIO.unique():
        print(f'{year}:')
        subcat = cat.loc[cat.ANIO == year]
        if year == 2015:
            for url in subcat.URL:
                dir = pathlib.Path(subcat.loc[subcat.URL == url].iloc[0].DIR)
                dir.parent.mkdir(parents=True, exist_ok=True)
                urllib.request.urlretrieve(url, dir)
                print(f'\tdownloaded from {url}')
        
        else:
            url = subcat.iloc[0].URL
            dir_zip = pathlib.Path(subcat.iloc[0].DIR_ZIP)
            dir = pathlib.Path(subcat.iloc[0].DIR)
            dir_zip.parent.mkdir(parents=True, exist_ok=True)
            dir.mkdir(parents=True, exist_ok=True)
            urllib.request.urlretrieve(url, dir_zip)
            print(f'\tdownloaded from {url}')
            with zipfile.ZipFile(dir_zip, 'r') as ext:
                ext.extractall(dir)
            print(f'\textracted in {dir}')

def create_schema(connection_dest, schema):
    connection = list(connection_dest.items())
    connection = [str(i[0])+'='+str(i[1]) for i in connection]
    connection = ' '.join(connection)

    with psycopg2.connect(connection) as conn:
        with conn.cursor() as cur:
            query = sql.SQL("""
                            CREATE SCHEMA IF NOT EXISTS {0};
                            """).format(sql.Identifier(schema))
            cur.execute(query=query)
    print(f'Schema {schema} created')

def transform_and_load(connection_dest, schema, period):
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
    for path_file in sorted(pathlib.Path('censos/2015').rglob('*.xls')):
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
    df = pd.concat(df, axis=0)
    df_tot = df.pivot_table(index=['ID_ENTIDAD', 'ID_MUNICIPIO'], columns='ANIO', values='POB_TOT')
    df_fem = df.pivot_table(index=['ID_ENTIDAD', 'ID_MUNICIPIO'], columns='ANIO', values='POB_FEM')
    df_mas = df.pivot_table(index=['ID_ENTIDAD', 'ID_MUNICIPIO'], columns='ANIO', values='POB_MAS')

    #LOAD
    engine = create_engine(
        'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(
            connection_dest['user'],
            connection_dest['password'],
            connection_dest['host'],
            connection_dest['port'],
            connection_dest['dbname']
        )
    )
    dtype_tot = {col:INT() for col in df_tot.columns}
    dtype_tot['ID_ENTIDAD'] = VARCHAR(2)
    dtype_tot['ID_MUNICIPIO'] = VARCHAR(3)
    df_tot.to_sql(
        name=f'pobtot_censos_{period}',
        con=engine,
        schema=schema,
        if_exists='replace',
        index=True,
        dtype=dtype_tot
    )
    print(f'{schema}.pobtot_censos_{period} created')
    dtype_fem = {col:INT() for col in df_fem.columns}
    dtype_fem['ID_ENTIDAD'] = VARCHAR(2)
    dtype_fem['ID_MUNICIPIO'] = VARCHAR(3)
    df_fem.to_sql(
        name=f'pobfem_censos_{period}',
        con=engine,
        schema=schema,
        if_exists='replace',
        index=True,
        dtype=dtype_fem
    )
    print(f'{schema}.pobfem_censos_{period} created')
    dtype_mas = {col:INT() for col in df_mas.columns}
    dtype_mas['ID_ENTIDAD'] = VARCHAR(2)
    dtype_mas['ID_MUNICIPIO'] = VARCHAR(3)
    df_mas.to_sql(
        name=f'pobmas_censos_{period}',
        con=engine,
        schema=schema,
        if_exists='replace',
        index=True,
        dtype=dtype_mas
    )
    print(f'{schema}.pobmas_censos_{period} created')

if __name__ == '__main__':
    download_and_extract_censos(path_cat)
    create_schema(connection_dest, schema)
    transform_and_load(connection_dest, schema, period)