from flask import Flask
import dash
from dash import dcc
from dash import html
from dash.dependencies import Output, Input
import plotly.express as px
import pandas as pd
from cassandra.cluster import Cluster


server = Flask(__name__)


app = dash.Dash(__name__, server=server, url_base_pathname='/dashboard/')



def get_sales_data():
    cluster = Cluster(['localhost'], port='9042') 
    session = cluster.connect('banana_ks')
    
    query = "SELECT * FROM sales"
    rows = session.execute(query)
    
    data = []
    for row in rows:
        data.append([row.id, row.total_price, row.create_date])
        
    df = pd.DataFrame(data, columns=['id', 'sale_amount', 'sale_time'])
    return df


sales_data = get_sales_data()


total_sales = sales_data['sale_amount'].sum()
average_sales = sales_data['sale_amount'].mean()


app.layout = html.Div(children=[
    html.H1(children='Sales Dashboard'),

    html.Div([
        html.Div([
            html.H3('Total Sales'),
            html.P(id='total-sales', children=f'{total_sales}')
        ], className='card'),

        html.Div([
            html.H3('Average Sales'),
            html.P(id='average-sales', children=f'{average_sales}')
        ], className='card')
    ], className='card-container'),

    dcc.Graph(
        id='line-chart',
        figure=px.line(sales_data, x='sale_time', y='sale_amount', title='Sales Over Time')
    ),

    dcc.Interval(
        id='interval-component',
        interval=200,  
        n_intervals=0
    )
], className='container')

@app.callback(
    [Output('line-chart', 'figure'),
     Output('total-sales', 'children'),
     Output('average-sales', 'children')],
    [Input('interval-component', 'n_intervals')]
)

def update_metrics(n):
    sales_data = get_sales_data()
    total_sales = sales_data['sale_amount'].sum()
    average_sales = sales_data['sale_amount'].mean()

    line_chart_figure = px.line(sales_data, x='sale_time', y='sale_amount', title='Sales Over Time')

    return line_chart_figure, f'{total_sales}', f'{average_sales}'

app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <link rel="stylesheet" href="/static/css/style.css">
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

if __name__ == '__main__':
    server.run(debug=True)