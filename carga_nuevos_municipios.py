import pandas as pd
import pathlib
import camelot
import numpy as np
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, VARCHAR, INT, DATE

path_cat = pathlib.Path('catalogo_censos.csv')
connection_dest = {
    'user': 'seguridad_vial',
    'password': 'popcorning',
    'host': 'localhost',
    'port': 5432,
    'dbname': 'seguridad_vial'
}
last_year = 2022
schema = f'nuevos_municipios'

urls = {
    1995: 'https://www.inegi.org.mx/contenidos/programas/ccpv/1995/doc/nuevos_municipios_cpv1995.pdf',
    2000: 'https://www.inegi.org.mx/contenidos/programas/ccpv/2000/doc/nuevos_municipios_cgpv2000.pdf',
    2005: 'https://www.inegi.org.mx/contenidos/programas/ccpv/2005/doc/municipios_iter_2005.pdf',
    2010: 'https://www.inegi.org.mx/contenidos/programas/ccpv/2010/doc/municipios_creados_cpv2010.pdf',
    2020: 'https://www.inegi.org.mx/contenidos/programas/ccpv/2020/doc/Censo2020_CPV_nuevos_municipios_a.pdf',
}
joints_tol = {
    1995: 2,
    2000: 20,
    2005: 20,
    2010: 2,
    2020: 2,
}
drop_rows = {
    1995: [0, 1],
    2000: [0, 1],
    2005: [0, 1],
    2010: [0],
    2020: [],
}
replace = {
    1995: ['/9', '/199'],
    2000: ['/9', '/199'],
    2005: ['/', '/'],
    2010: ['/', '/'],
    2020: ['/', '/'],
}
date_seps = {
    1995: '/',
    2000: '/',
    2005: '/20',
    2010: '/20',
    2020: '/',
}

csv_00XX = pathlib.Path('nuevos_municipios/nuevos_municipios_00XX.csv')

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

def load_nuevos_municipios(urls, joints_tol, drop_rows, replace, date_seps, csv_00XX, last_year, connection_dest, schema):
    df_9095 = nuevos_municipios_9000(1995, urls, joints_tol, drop_rows, replace, date_seps)
    df_9500 = nuevos_municipios_9000(2000, urls, joints_tol, drop_rows, replace, date_seps)
    df_00XX = nuevos_municipios_00XX(csv_00XX, last_year)
    df = pd.concat([df_9095, df_9500, df_00XX], axis=0)

    engine = create_engine(
        'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(
            connection_dest['user'],
            connection_dest['password'],
            connection_dest['host'],
            connection_dest['port'],
            connection_dest['dbname']
        )
    )
    dtype = dict()
    dtype['CVE_ENT_ORI'] = VARCHAR(2)
    dtype['CVE_ENT_ACT'] = VARCHAR(2)
    dtype['CVE_MUN_ORI'] = VARCHAR(3)
    dtype['CVE_MUN_ACT'] = VARCHAR(3)
    dtype['FECHA_ACT'] = DATE()
    dtype['ANIO_ACT'] = INT()
    dtype['ANIO_CENSO'] = INT()
    df.to_sql(
        name = f'{schema}_90{str(last_year)[2:]}',
        con = engine,
        schema = schema,
        if_exists = 'replace',
        index = False,
        dtype = dtype
    )
    print(f'{schema}.nuevos_municipios created')

    return df

def nuevos_municipios_9000(year, urls, joints_tol, drop_rows, replace, date_seps):
    url = urls[year]
    joint_tol = joints_tol[year]
    drop_row = drop_rows[year]
    date_sep = date_seps[year]
    replace = replace[year]
    print(f'{year-5}-{year}: Reading {url}')
    tables = camelot.read_pdf(filepath=url,
                            pages='all',
                            strip_text='\n',
                            joint_tol=joint_tol,
                            )
    df = pd.DataFrame()
    for table in tables:
        df = pd.concat([df, table.df], axis=0)
    print(f'{year-5}-{year}: Transforming')
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
    df['ANIO_CENSO'] = year
    df.loc[:, 'ANIO_CENSO'] = df['ANIO_CENSO'].mask(df['ANIO_ACT']<=year-5, year-5)
    df = df[['CVE_ENT_ORI', 'CVE_MUN_ORI', 'CVE_ENT_ACT', 'CVE_MUN_ACT', 'FECHA_ACT', 'ANIO_ACT', 'ANIO_CENSO']].dropna()
    print(f'{year-5}-{year}: Done')
    return df

def nuevos_municipios_00XX(csv, last_year):
    print(f'2000-{last_year}: Reading {csv}')
    df_00XX = pd.read_csv(csv, dtype=str)
    print(f'2000-{last_year}: Transforming')
    df_00XX = df_00XX.loc[df_00XX.DESCRIP=='Nuevo municipio']
    df_00XX = df_00XX.loc[df_00XX.CVE_ENT_ORI.str.isnumeric() & df_00XX.CVE_ENT_ACT.str.isnumeric()]
    df_00XX['FECHA_ACT'] = pd.to_datetime(df_00XX.FECHA_ACT, format='%Y-%M-%d').dt.normalize()
    df_00XX['ANIO_ACT'] = df_00XX['FECHA_ACT'].dt.year
    cut_bins = [i for i in range(2000, last_year+10, 5)]
    cut_labels = cut_bins[1:]
    df_00XX['ANIO_CENSO'] = pd.cut(x=df_00XX.ANIO_ACT, bins=cut_bins, labels=cut_labels)
    df_00XX = df_00XX.loc[df_00XX.ANIO_ACT <= last_year]
    df_00XX = df_00XX[['CVE_ENT_ORI', 'CVE_MUN_ORI', 'CVE_ENT_ACT', 'CVE_MUN_ACT', 'FECHA_ACT', 'ANIO_ACT', 'ANIO_CENSO']]
    print(f'2000-{last_year}: Done')
    return df_00XX

if __name__ == '__main__':
    create_schema(connection_dest, schema)
    load_nuevos_municipios(urls, joints_tol, drop_rows, replace, date_seps, csv_00XX, last_year, connection_dest, schema)