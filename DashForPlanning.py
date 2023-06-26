import dash
#from dash import dash_bootstrap_components as dbc
from dash import dcc
from dash import html
from dash import dash_table
from dash import Input
from dash import Output
import plotly.graph_objects as go
import pandas as pd
import pymssql


es = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=es)


#zapytanie sql-------------------------------------------------------------------
conn = pymssql.connect(server='***', user='***', password='***' , database='***')

df = pd.read_sql_query(

    '''use tls_metrics;

with cte01 as (

SELECT
     autos = COUNT(*)
       ,lot_id = lot.id
FROM
       dbo.logLot lot JOIN
       dbo.logLinkLotAuto link ON link.lot_id = lot.id
       GROUP BY lot.id
)

SELECT
       id = load.id
       ,Load_nr = load.nr_external
       ,Towar = towar.ext_acronym
       ,Route = route.name
       ,Load_client = load_client.acronym
       ,Date_departure = lot.date_departure
       ,Sap_order_index = lot.sap_order_index
       ,Logistyka = load.flag_logistics_visible
       ,Planowanie = load.flag_planning_visible
       ,Data_od = CONVERT(DATE, load.date_from)
       ,Data_do = CONVERT(DATE,load.date_to)
       ,Data_plan = CONVERT(DATE,load.date_loading_plan)
       ,Liczba_aut = C1.autos
       ,Nr_rejestracyjny = srtransp.nr01 + '/'+ srtransp.nr02

FROM
       dbo.logLoad load LEFT JOIN
       dbo.logLot lot ON lot.load_id=  load.id LEFT JOIN
       dbo.logTowar towar ON load.towar_id = towar.id LEFT JOIN
       dbo.logRoute route ON towar.route_id = route.id LEFT JOIN
       dbo.sysKnt load_client ON towar.client_knt_id = load_client.id LEFT JOIN
       cte01 C1 ON C1.lot_id = lot.id LEFT JOIN
       dbo.logSrtransp srtransp ON load.srtransp_id = srtransp.id



where load.flag_active = 1  and towar.flag_active = 1  and load.flag_archive = 0  and load_client.acronym = 'BMW_GROUP_MUENCHEN'  and lot.sap_order_index IS NULL and lot.date_departure IS NULL 

order by route'''

    , conn)

# Zamiana True/False na 1/0

df.Logistyka = df.Logistyka * 1
df.Planowanie = df.Planowanie * 1

# Odfiltrowanie ładunków z zerową ilością aut--------------------------------------
df = df[
    (
       ((df['Logistyka']==1) & (df['Planowanie']==0) & (df['Liczba_aut'].notnull())) |
       ((df['Logistyka']==1) & (df['Planowanie']==1)) |
        ((df['Logistyka']==0) & (df['Planowanie']==1))
    )
       ]

df.rename(columns = {'Load_nr':'Numer ładunku', 'Route':'Trasa', 'Date_departure':'Data wyjazdu' ,'Data_plan':'Data planowana','Liczba_aut':'Liczba aut' ,'Nr_rejestracyjny':'Nr rejestracyjny', 'Data_od': 'Dostępność od'}, inplace = True)

#-----Wykres-------------------------------------------------------------------

fig = go.Figure()
fig.add_trace(go.Bar(
    x=df['Logistyka'],
    y=df['Towar'],
    name='Ładunki',
    orientation='h',
    marker_color='indianred'
))
fig.add_trace(go.Bar(
    x=df['Planowanie'],
    y=df['Towar'],
    name='Planowanie',
    orientation='h',
    marker_color='lightsalmon'
))


# Here we modify the tickangle of the xaxis, resulting in rotated labels.
fig.update_layout(barmode='group', yaxis_title='Towar', margin=dict(l=20, r=20, t=20, b=20), yaxis = dict(tickfont = dict(size=10)))

#Tabela ladunkow i planowania------------------------------------------------

log_df = df[(df['Logistyka']==1) & (df['Liczba aut'].notnull())]
log_df = log_df[['Towar' , 'Numer ładunku' , 'Nr rejestracyjny' , 'Data planowana' , 'Data wyjazdu' ]]

plan_df = df[df['Planowanie']==1] #, ['Towar','route','load_nr', 'srtransp']]
plan_df = plan_df[['Towar' , 'Numer ładunku' , 'Nr rejestracyjny' , 'Dostępność od' , 'Data planowana' ]]


log_table = dash_table.DataTable(
    log_df.to_dict('records'),
    [{"name": i, "id": i} for i in log_df.columns],
    page_action='none',
    style_table={'height': '500px', 'overflowY': 'auto'},
    sort_action='native',
    fill_width=False,
    id = 'log_table'
)

plan_table = dash_table.DataTable(
    plan_df.to_dict('records'),
    [{"name": i, "id": i} for i in plan_df.columns],
    page_action='none',
    style_table={'height': '500px', 'overflowY': 'auto'},
    sort_action='native',
    fill_width=False,
    id = 'plan_table'
)



app.layout = html.Div(children=[
    html.H1(children='BMW'),
    html.Div(children='Zestawienie ładunków i planowania'),
    dcc.Graph(figure=fig),
    html.Br(),
    html.Br(),
    html.Br(),
    dcc.Dropdown(id = 'lista_towarow' , options=pd.unique(df['Towar']), placeholder="Wybierz towar"),
    html.Br(),
    html.Div([

        html.Div(children=[
            html.Div(children='Ładunki'),
            html.Div(id= 'log_table_output', style={"margin": "5px"})], style={'width': '50%','display': 'inline-block'} ),

         html.Div(children=[
             html.Div(children='Planowanie'),
             html.Div(id= 'plan_table_output', style={"margin": "5px"})], style={'width': '50%','display': 'inline-block'})
    ])
])

#------callback tabel------------------------------------------
@app.callback(
   Output(component_id='log_table_output', component_property='children'),
   Input(component_id='lista_towarow', component_property='value')

)

#------callback logistyka------------------------------------------
def funkcja(wartosc):
    log_df_filtered = log_df[log_df['Towar'] == wartosc]
    log_table1 = dash_table.DataTable(
        log_df_filtered.to_dict('records'),
        [{"name": i, "id": i} for i in log_df.columns],
        page_action='none',
        style_table={'height': '500px', 'overflowY': 'auto'},
        sort_action='native',
        fill_width=False
    )
    return log_table1


#------callback planowanie-----------------------------------
@app.callback(
   Output(component_id='plan_table_output', component_property='children'),
   Input(component_id='lista_towarow', component_property='value')

)

def funkcja(wartosc):
    plan_df_filtered = plan_df[plan_df['Towar'] == wartosc]
    plan_table1 = dash_table.DataTable(
        plan_df_filtered.to_dict('records'),
        [{"name": i, "id": i} for i in plan_df.columns],
        page_action='none',
        style_table={'height': '500px', 'overflowY': 'auto'},
        sort_action='native',
        fill_width=False
    )
    return plan_table1


if __name__ == '__main__':
    app.run_server(debug=True)


