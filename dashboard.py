from dash import Dash, html, dcc, callback, Output, Input, get_asset_url
import pandas as pd
from modules import create_engine_string
from sqlalchemy import create_engine
import plotly.graph_objects as go
import geopandas as gpd
import plotly.express as px

last_year = 2022
connection_vars = {
    'user': 'road_safety',
    'password': 'popcorning',
    'host': 'localhost',
    'port': 5432,
    'db_name': 'seguridad_vial_mexico'
}
schema_name = f'seguridad_vial_{last_year}'

engine_string = create_engine_string(connection_vars)
engine = create_engine(engine_string)

# sql_query = """
# SELECT	a.*,
# 		e.nom_entidad,
# 		g.geometry
# FROM	seguridad_vial_2022.geo_entidad AS g JOIN seguridad_vial_2022.entidad AS e
# 		USING (id_entidad)
# 		JOIN (
# 			SELECT	id_entidad,
# 					SUM("1997") AS "1997"
# 			FROM	seguridad_vial_2022.accidentes_fatales
# 			GROUP BY id_entidad
# 			) AS a
# 		USING (id_entidad)
# ;
# """
# gdf = gpd.read_postgis(
#     sql=sql_query,
#     con=engine,
#     geom_col='geometry'
# )
# gdf = gdf.set_index('id_entidad')

# Read tables
acci = {}
for table in ['accidentes_fatales', 'accidentes_no_fatales', 'accidentes_solo_danios']:
    acci[table] = pd.read_sql_table(table_name=table,
                             con=engine,
                             schema=schema_name,
                             index_col=['id_entidad', 'id_municipio']
                             ).sum(axis=0)
acci = pd.concat(acci, axis=1)
vict = {}
for table in ['victimas_fatales']:
    vict[table] = pd.read_sql_table(table_name=table,
                             con=engine,
                             schema=schema_name,
                             index_col=['id_entidad', 'id_municipio']
                             ).sum(axis=0)
vict = pd.concat(vict, axis=1)
resume_data = {
    'acci.png':{'text':'Accidentes totales', 'value':acci},
    'fata.png':{'text':'Víctimas fatales', 'value':vict}
}
# App
app = Dash()
# app.layout = html.Div(
#     children=[
#         html.Img(src=get_asset_url('acci.png'), width=200, height=200, style={'background-color':'black'}),
#         html.Div('Accidentes totales', className='resumeTitle', style={'color':'White', 'font-size':'30px'}),
#         html.Div('{:,.0f}'.format(acci.loc['2022'].sum()), className='resumeNumber', style={'color':'Pink', 'font-size':'40px'})
#     ],
#     # define this in styles.css
#     className='figure-row',
#     style={'background':'Black', 'display':'inline-block', 'width':'24%', 'text-align':'center'}
# )
children_resume = []
for img_path, vars in resume_data.items():
    children_resume.append(
        html.Div(
            children=[
                html.Img(src=get_asset_url(img_path), width=200, height=200, style={'background-color':'black'}),
                html.Div(vars['text'], className='resumeTitle', style={'color':'White', 'font-size':'30px'}),
                html.Div('{:,.0f}'.format(vars['value'].loc['2022'].sum()), className='resumeNumber', style={'color':'Pink', 'font-size':'40px'})
            ],
            className='figure-row',
            style={'background':'Black', 'display':'inline-block', 'width':'24%', 'text-align':'center'}
        )
    )
app.layout = html.Div(
    children=children_resume
)
# app.layout = html.Div(
#     children=[
#         html.Div(
#             children=[
#                 html.Img(src=get_asset_url('acci.png'), width=200, height=200, style={'background-color':'black'}),
#                 html.Div('Accidentes totales', className='resumeTitle', style={'color':'White', 'font-size':'30px'}),
#                 html.Div('{:,.0f}'.format(acci.loc['2022'].sum()), className='resumeNumber', style={'color':'Pink', 'font-size':'40px'})
#             ],
#             # define this in styles.css
#             className='figure-row',
#             style={'background':'Black', 'display':'inline-block', 'width':'24%', 'text-align':'center'}
#         ),
#         html.Div(
#             children=[
#                 html.Img(src=get_asset_url('fata.png'), width=200, height=200, style={'background-color':'black'}),
#                 html.Div('Accidentes totales', className='resumeTitle', style={'color':'White', 'font-size':'30px'}),
#                 html.Div('{:,.0f}'.format(acci.loc['2022'].sum()), className='resumeNumber', style={'color':'Pink', 'font-size':'40px'})
#             ],
#             # define this in styles.css
#             className='figure-row',
#             style={'background':'Black', 'display':'inline-block', 'width':'24%', 'text-align':'center'}
#         ),
#         html.Div(
#             children=[
#                 html.Img(src=get_asset_url('peat.png'), width=200, height=200, style={'background-color':'black'}),
#                 html.Div('Accidentes totales', className='resumeTitle', style={'color':'White', 'font-size':'30px'}),
#                 html.Div('{:,.0f}'.format(acci.loc['2022'].sum()), className='resumeNumber', style={'color':'Pink', 'font-size':'40px'})
#             ],
#             # define this in styles.css
#             className='figure-row',
#             style={'background':'Black', 'display':'inline-block', 'width':'24%', 'text-align':'center'}
#         ),
#         html.Div(
#             children=[
#                 html.Img(src=get_asset_url('cicl.png'), width=200, height=200, style={'background-color':'black'}),
#                 html.Div('Accidentes totales', className='resumeTitle', style={'color':'White', 'font-size':'30px'}),
#                 html.Div('{:,.0f}'.format(acci.loc['2022'].sum()), className='resumeNumber', style={'color':'Pink', 'font-size':'40px'})
#             ],
#             # define this in styles.css
#             className='figure-row',
#             style={'background':'Black', 'display':'inline-block', 'width':'24%', 'text-align':'center'}
#         ),
#         html.Div(
#             children=[
#                 html.Img(src=get_asset_url('alie.png'), width=200, height=200, style={'background-color':'black'}),
#                 html.Div('Accidentes totales', className='resumeTitle', style={'color':'White', 'font-size':'30px'}),
#                 html.Div('{:,.0f}'.format(acci.loc['2022'].sum()), className='resumeNumber', style={'color':'Pink', 'font-size':'40px'})
#             ],
#             # define this in styles.css
#             className='figure-row',
#             style={'background':'Black', 'display':'inline-block', 'width':'24%', 'text-align':'center'}
#         ),
#         html.Div(
#             children=[
#                 html.Img(src=get_asset_url('cint.png'), width=200, height=200, style={'background-color':'black'}),
#                 html.Div('Accidentes totales', className='resumeTitle', style={'color':'White', 'font-size':'30px'}),
#                 html.Div('{:,.0f}'.format(acci.loc['2022'].sum()), className='resumeNumber', style={'color':'Pink', 'font-size':'40px'})
#             ],
#             # define this in styles.css
#             className='figure-row',
#             style={'background':'Black', 'display':'inline-block', 'width':'24%', 'text-align':'center'}
#         ),
#         html.Div(
#             children=[
#                 html.Img(src=get_asset_url('sexo.png'), width=200, height=200, style={'background-color':'black'}),
#                 html.Div('Accidentes totales', className='resumeTitle', style={'color':'White', 'font-size':'30px'}),
#                 html.Div('{:,.0f}'.format(acci.loc['2022'].sum()), className='resumeNumber', style={'color':'Pink', 'font-size':'40px'})
#             ],
#             # define this in styles.css
#             className='figure-row',
#             style={'background':'Black', 'display':'inline-block', 'width':'24%', 'text-align':'center'}
#         ),
#         html.Div(
#             children=[
#                 html.Img(src=get_asset_url('edad.png'), width=200, height=200, style={'background-color':'black'}),
#                 html.Div('Accidentes totales', className='resumeTitle', style={'color':'White', 'font-size':'30px'}),
#                 html.Div('{:,.0f}'.format(acci.loc['2022'].sum()), className='resumeNumber', style={'color':'Pink', 'font-size':'40px'})
#             ],
#             # define this in styles.css
#             className='figure-row',
#             style={'background':'Black', 'display':'inline-block', 'width':'24%', 'text-align':'center'}
#         )
#     ]
# )

# app.layout = html.Div([
#     dcc.Tabs([
#         dcc.Tab(
#             label='Resumen',
#             children=[
#                 html.Div(className='row', children='Seguridad Vial en México', style={'textAlign':'center'}),
#                 html.Div(className='row', children=dcc.Slider(min=1997, max=last_year, step=1, marks={i:str(i) for i in range(1997, last_year+1)}, value=last_year, id='dropdown-anio')),
#                 # html.Div(className='row', children=dcc.Graph(id='graph-acci'), style={'display':'inline-block', 'width':'24%'}),
#                 html.Img(id='graph-acci', src='src/acci.png', alt='mamó')
#             ]
#         ),
#         # dcc.Tab(
#         #     label='Mapa',
#         #     children=[
#         #         html.Div(className='row', children=dcc.Slider(min=1997, max=last_year, step=1, marks={i:str(i) for i in range(1997, last_year+1)}, value=1997, id='dropdown-anio-mapa')),
#         #         html.Div(className='row', children='Mapa', style={'textAlign':'center'}),
#         #         html.Div(className='row', children=dcc.Graph(id='graph-mapa'))
#         #     ]
#         # )
#         ]
#     ),
#     html.Div(id='tabs-with-classes-2')
# ])

# @callback(
#     Output('graph-acci', 'figure'),
#     Input('dropdown-anio', 'value')
# )
# def update_acci(anio):
#     anio = str(anio)
    

# @callback(
#     Output('graph-mapa', 'figure'),
#     Input('dropdown-anio-mapa', 'value')
# )
# def update_mapa(anio):
#     anio=str(anio)
#     fig_mapa = px.choropleth_mapbox(
#         gdf,
#         geojson=gdf.geometry,
#         locations=gdf.index,
#         color=anio,
#         mapbox_style='open-street-map',
#         center={'lat':21, 'lon':-101},
#         zoom=7
#     )
#     return fig_mapa

if __name__ == '__main__':
    app.run(debug=True)