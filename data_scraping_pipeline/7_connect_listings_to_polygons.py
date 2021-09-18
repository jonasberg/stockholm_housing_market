import os
import json
import logging
import pandas as pd
import numpy as np
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from helper_functions import setup_logging
from working_dir import WORKING_DIR

# ----------------------- CONFIG -----------------------
LISTINGS_CSV = os.path.join(WORKING_DIR, "data", "listings_data", "listings_data.csv")
POLYGONS_GEOJSON = os.path.join(WORKING_DIR, "data", "area_polygons", "polygons.geojson")
# ------------------------------------------------------

setup_logging(__file__) # Formats logging and store warnings/exceptions to file

# Make sure listing data is available
assert os.path.isfile(LISTINGS_CSV), "Can't find file 'listings_data.csv'"

df = pd.read_csv(
    LISTINGS_CSV, 
    sep=";", 
    encoding="utf8",
    index_col=0, 
    low_memory=False
)

with open(POLYGONS_GEOJSON, encoding='utf-8') as f:
    polygon_data = json.load(f)

df["polygon_id"] = None
df["polygon_name"] = None

pol = np.array(polygon_data["features"][0]["geometry"]["coordinates"][0])
areas = polygon_data["features"]

for ind, row in df.iterrows():
    lat = row["latitude"]
    long = row["longitude"]
    
    for area in areas:
        polygon = Polygon(area["geometry"]["coordinates"][0])
        point = Point(long, lat)

        if point.within(polygon):
            polygon_id = area["properties"]["NYCKELKOD_"]
            polygon_name = area["properties"]["NAMN"]
            
            df.loc[ind, "polygon_id"] = polygon_id
            df.loc[ind, "polygon_name"] = polygon_name
            break
    if ind % 1000 == 0:
        logging.info(f"Assigned {ind} listings to polygons")


df.to_csv(LISTINGS_CSV, sep=";", encoding="utf8")