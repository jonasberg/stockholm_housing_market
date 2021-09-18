import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import numpy as np
from datetime import date

from figures import choropleth, scatterplot, colormap, query_df


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title = "Stockholm Apartment Dashboard"

app.layout = html.Div(id="main-container",
    children=[
        html.Div(id="sidebar",
            children=[
                html.Div(
                    id="filters",
                    children=[
                        html.H3("Filters"),

                        html.P("Listings sold date range:"),
                        dcc.DatePickerRange(
                            id="date-picker-range",
                            start_date=date(2020,1,1),
                            end_date=date.today(),
                            display_format='YYYY-MM-DD'
                        ),

                        html.P("Number of rooms:"),
                        dcc.RangeSlider(
                            id="n-rooms-slider",
                            marks={
                                1:"1",
                                1.5: "1.5",
                                2:"2",
                                2.5:"2.5",
                                3:"3",
                                3.5:"3.5",
                                4:"4",
                                4.5:"4.5",
                                5:"5+",
                            },
                            min=1.0,
                            max=5.0,
                            value=[1.0, 5.0],
                            step=0.5,
                            allowCross=False
                        )
                    ]
                ), 
                html.Div(
                    id="options",
                    children=[
                        html.H3("Options"),

                        html.P("Data:"),
                        dcc.Dropdown(
                            id='data-selection',
                            options=[
                                {'label': 'Price per m²', 'value': 'sqm'},
                                {'label': 'Construction year', 'value': 'year'},
                                {'label': 'Rent per m²', 'value': 'rent'}
                            ],
                            value='sqm',
                            clearable=False
                        ),

                        html.P("Plot type:"),
                        dcc.RadioItems(
                            id='plot-type',
                            options=[
                                {'label': 'Scatterplot', 'value': 'scatter'},
                                {'label': 'Choropleth', 'value': 'choropleth'},
                                {'label': 'Colormap (IN DEVELOPMENT)', 'value': 'colormap'}
                            ],
                            value='scatter'
                        )
                    ]
                )
            ]
        ),
        dcc.Loading(
            parent_className="map-loader-wrapper",
            children=[
                dcc.Graph(
                    id='map'
                )
            ]
        ),
    ]
)

@app.callback(
    Output('map', 'figure'),
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date'),
    Input('n-rooms-slider', 'value'),
    Input('data-selection', 'value'),
    Input('plot-type', 'value')
)
def update_figure(start_date, end_date, n_rooms_range, data_selection, plot_type):
    # Apply filters
    filtered_df = query_df([start_date, end_date], n_rooms_range)

    # Select data for plotting
    if data_selection == "sqm":
        target_col = "listing_sold_price_per_sqm"
        target_col_desc = 'Price per m²'

    elif data_selection == "year":
        target_col = "construction_year"
        target_col_desc = 'Construction year'

    elif data_selection == "rent":
        target_col = "listing_rent_per_sqm"
        target_col_desc = 'Rent per m²'

    # Plot according to user preference
    if plot_type == "scatter":
        fig = scatterplot(
            filtered_df, 
            target_col=target_col, 
            target_col_desc=target_col_desc
        )
    elif plot_type == "choropleth":
        fig = choropleth(
            filtered_df, 
            target_col=target_col, 
            target_col_desc=target_col_desc
        )
    elif plot_type == "colormap":
        fig = colormap(
            filtered_df, 
            target_col=target_col, 
            target_col_desc=target_col_desc
        )

    return fig

if __name__ == '__main__':
    app.run_server(debug=True)