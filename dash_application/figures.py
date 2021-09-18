import datashader as ds
import colorcet as cc
import pandas as pd
import numpy as np
import plotly.express as px
import re    
import json
import os
from data_dir import DATA_DIR

# Initialize
with open("mapbox_access_token.txt") as f:
    mapbox_accesstoken = f.read()

# ------------------ LOAD DATAFRAME -----------------------
DTYPES = {
    'property_id': np.uint32, 
    'property_URL': 'str', 
    'address': 'str',
    'apartment_number': 'str', 
    'object_type': 'str', 
    'latitude': np.float64, 
    'longitude': np.float64,
    'construction_year': np.float64, # IN REALITY INT; BUT EASIER THIS WAY
    'energy_class': 'str', 
    #'descriptive_area_name': 'str',
    'has_solar_panels': 'str', 
    'brf_name': 'str', 
    'brf_url': 'str', 
    'montly_payment': 'str', # TO BE CHANGED 
    'rent': np.float64, # IN REALITY INT; BUT EASIER THIS WAY
    'rooms': np.float64, # MEMORY COULD BE GREATLY REDUCED 
    'sqm': np.float64, # MEMORY COULD BE GREATLY REDUCED 
    'primary_area': 'str', 
    'floor': 'str', 
    'operating_cost': np.float64, # IN REALITY INT; BUT EASIER THIS WAY
    #'estimate_price' # Parsed to int using converters later
    #'estimate_low'  # Parsed to int using converters later
    #'estimate_high'  # Parsed to int using converters later
    'listing_agent': 'str',
    'listing_agency_name': 'str', 
    'listing_agency_URL': 'str', 
    'listing_days_active': np.uint32,
    'listing_sold_date': 'str', 
    'listing_sold_price_type': 'str', 
    #'listing_sold_price'  # Parsed to int using converters later
    #'listing_listed_price' # Parsed to int using converters later
    'polygon_id': 'str',
    'polygon_name': 'str'
    
}

def parse_price_string(x):
    price = re.sub(" (kr)?", "", x)
    if price != "":
        return int(float(price))
    else:
        return None

# Change price to be integer instead of formatted string
CONVERTERS = {
    c : lambda x : parse_price_string(x) for c in [
        "estimate_price",
        "estimate_low",
        "estimate_high",
        "listing_sold_price",
        "listing_listed_price"
    ]
}

# Align area names format somewhat
CONVERTERS["descriptive_area_name"] = lambda x : re.sub(" ?(-|/) ?", " ", x).title()

df = pd.read_csv(os.path.join(DATA_DIR, "listings_data", "listings_data.csv"), 
                 sep=";", 
                 encoding="utf8", 
                 dtype=DTYPES, 
                 converters=CONVERTERS,
                 parse_dates=["listing_sold_date"], 
                 index_col=0, 
                 low_memory=False)

# Add a price per sqm column
df = df.assign(listing_sold_price_per_sqm = (df["listing_sold_price"] / df["sqm"]).round(0))

# Add a rent per sqm column
df = df.assign(listing_rent_per_sqm = (df["rent"] / df["sqm"]).round(0))
# --------------------------------------------------------

def query_df(date_range, n_rooms_range):
    filtered_df = df.copy()
    # Apply date range
    filtered_df = filtered_df[filtered_df["listing_sold_date"] > date_range[0]]
    filtered_df = filtered_df[filtered_df["listing_sold_date"] < date_range[1]]

    # Apply n_rooms range
    filtered_df = filtered_df[filtered_df["rooms"] > n_rooms_range[0]]
    if n_rooms_range[1] < 5: # 5 is displayed as 5+ to the user, i.e. no upper limit
        filtered_df = filtered_df[filtered_df["rooms"] < n_rooms_range[1]]

    return filtered_df

def choropleth(choropleth_df, target_col="listing_sold_price_per_sqm", target_col_desc = 'Price per m²'):
    # Settings to use for plot
    plot_df = (choropleth_df.groupby('polygon_name')[target_col].mean()).reset_index()
    color = "IceFire"
    #range_color = (40, 140)
    
    # Prepare polygon data
    with open(os.path.join(DATA_DIR, "area_polygons", "polygons.geojson"), encoding='utf-8') as f:
        polygon_data = json.load(f)
    for f in polygon_data['features']:
        f['id'] = str(f['properties']['NAMN'])

    fig = px.choropleth_mapbox(
        plot_df,
        geojson=polygon_data,
        locations='polygon_name',
        color=target_col,
        color_continuous_scale=color,
        #range_color=range_color, 
        #mapbox_style="carto-positron",
        mapbox_style="dark",
        center=dict(lat=choropleth_df["latitude"].mean(), lon=choropleth_df["longitude"].mean()), 
        zoom=10,
        opacity=0.5,
        labels={'polygon_name':'Område', target_col:target_col_desc}
    )
    
    fig.update_layout(
        margin=dict(l = 0, r = 0, t = 0, b = 0),
        template="plotly_dark", # For dark colorbar as well
        mapbox_accesstoken=mapbox_accesstoken
    )

    return fig

def scatterplot(scatter_df, target_col="listing_sold_price_per_sqm", 
                target_col_desc = 'Price per m²', colorbar_percentile_range=[1,99]):

    scatter_df = scatter_df[scatter_df[target_col].notna()]

    range_color = [ # Avoid extreme values to get higher constrast
        np.percentile(scatter_df[target_col], colorbar_percentile_range[0]), 
        np.percentile(scatter_df[target_col], colorbar_percentile_range[1])
    ]

    fig = px.scatter_mapbox(
        scatter_df, 
        lat='latitude', 
        lon='longitude',
        color=target_col,
        range_color=range_color,
        zoom=12,
        hover_name=scatter_df.address,
        hover_data={
            target_col_desc: scatter_df[target_col],
            "Sold date": scatter_df["listing_sold_date"].apply(lambda x: x.strftime("%Y-%m-%d")),
            "latitude": False,
            "longitude": False,
            target_col: False
        }
    )

    fig.update_layout(
        mapbox_style="dark",#"stamen-terrain",#"carto-darkmatter",
        margin=dict(l = 0, r = 0, t = 0, b = 0),
        template="plotly_dark", # For dark colorbar as well
        mapbox_accesstoken=mapbox_accesstoken,
    )

    return fig

def colormap(colormap_df, target_col="listing_sold_price_per_sqm", target_col_desc = 'Price per m²'):
    # Make sure grid pixels are close to quadratic
    d_long = colormap_df["longitude"].max() - colormap_df["longitude"].min()
    d_lat = colormap_df["latitude"].max() - colormap_df["latitude"].min()

    canvases = [
        # canvas, minzoom, maxzoom
        (ds.Canvas(plot_width=int(d_long*500), plot_height=int(d_lat*500)),0,24),
        #(ds.Canvas(plot_width=int(d_long*150), plot_height=int(d_lat*150)),0,10),
        #(ds.Canvas(plot_width=int(d_long*500), plot_height=int(d_lat*500)),10,12),
        #(ds.Canvas(plot_width=int(d_long*1000), plot_height=int(d_lat*1000)),12,14),
        #(ds.Canvas(plot_width=int(d_long*2000), plot_height=int(d_lat*2000)),14,24)
    ]

    images = []
    for cvs, minzoom, maxzoom in canvases:
        # Aggregate
        agg = cvs.points(colormap_df, x="longitude", y="latitude", agg=ds.mean(column=target_col))

        coords_lat, coords_long = agg.coords["latitude"].values, agg.coords["longitude"].values
        coordinates = [[coords_long[0], coords_lat[-1]],
                    [coords_long[-1], coords_lat[-1]],
                    [coords_long[-1], coords_lat[0]],
                    [coords_long[0], coords_lat[0]]]
        img = ds.transfer_functions.shade(agg, cmap=cc.bmy).to_pil()
        images.append((img, minzoom, maxzoom))

    fig = px.scatter_mapbox(
        colormap_df, 
        lat='latitude', 
        lon='longitude', 
        zoom=12,
        hover_name=colormap_df.address,
        hover_data={
            target_col_desc: colormap_df[target_col],
            "Sold date": colormap_df["listing_sold_date"].apply(lambda x: x.strftime("%Y-%m-%d")),
            "latitude": False,
            "longitude": False
        }
    )

    fig.update_layout(
        mapbox_style="dark",#"stamen-terrain",#"carto-darkmatter",
        margin=dict(l = 0, r = 0, t = 0, b = 0),
        template="plotly_dark", # For dark colorbar as well
        mapbox_accesstoken=mapbox_accesstoken,
        mapbox_layers=[
        {
            "sourcetype": "image",
            "source": img,
            "coordinates": coordinates,
            "opacity":0.7,
            "maxzoom":maxzoom,
            "minzoom":minzoom
        } for img, minzoom, maxzoom in images]
    )

    return fig