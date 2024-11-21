# import modules
import modules.marco_geoestadistico as mg
import modules.vmrc as vmrc
import modules.atus as atus
import modules.nuevos_municipios as nm
import modules.ccpv as ccpv
import modules.etl_for_dashboard as etl
import modules.poblacion as pob
import modules.parque_vehicular as pv
# from pathlib import Path
# import pandas as pd

connection_vars = {
    'user': 'road_safety',
    'password': 'popcorning',
    'host': 'localhost',
    'port': 5432,
    'db_name': 'road_safety_mex'
}
last_year = 2022
schema_name = f'road_safety_2023'#{last_year}'
path_mg_entidad = 'mg2022_integrado/conjunto_de_datos/00ent.shp'
# path_marco_geoestadistico = f'mg{last_year}_integrado/conjunto_de_datos/00mun.shp'
path_mg_municipio = 'mg2022_integrado/conjunto_de_datos/00mun.shp'
# path_marco_geoestadistico = f'mg{last_year}_integrado/conjunto_de_datos/00mun.shp'
path_vmrc_anual_csv = 'vmrc_anual_csv'
path_atus_anual_csv = 'atus_anual_csv'
path_nuevos_municipios_info = 'nuevos_municipios/nuevos_municipios.csv'
path_info_2000_20XX = 'nuevos_municipios/nuevos_municipios_00XX.csv'
path_catalogo_censos = 'catalogo_censos.csv'
connection_dest = {
    'user': 'road_safety',
    'password': 'popcorning',
    'host': 'localhost',
    'port': 5432,
    'db_name': 'seguridad_vial_mexico'
}
schema_dest = f'seguridad_vial_{last_year}'

# mg.create_marco_geoestadistico(path_mg_entidad, path_mg_municipio, connection_vars, schema_name)
# vmrc.create_tables(path_vmrc_anual_csv, connection_vars, schema_name)
# atus.create_tables(path_atus_anual_csv, connection_vars, schema_name)
# nm.create_nuevos_municipios(path_nuevos_municipios_info, path_info_2000_20XX, connection_vars, schema_name, last_year)
# ccpv.create_poblacion_inegi(connection_vars, schema_name)
# ccpv.estimate_chiapas(connection_vars, schema_name, last_year)
# etl.etl_mg(path_mg_entidad=path_mg_entidad, path_mg_municipio=path_mg_municipio, connection_ori=connection_vars,
#            connection_dest=connection_dest, schema_ori=schema_name, schema_dest=schema_dest)
# pob.etl_poblacion(connection_vars, connection_dest, schema_name, schema_dest, last_year)
# pv.etl_parque_vehicular(connection_vars, connection_dest, schema_name, schema_dest)