""" SCRAPES ALLABRF PAGE
"""

import os
import logging
import json
import pandas as pd
from helper_functions import setup_logging, get_settings, respectful_requesting
from numpy.random import normal
from working_dir import WORKING_DIR
# ----------------------- CONFIG -----------------------
URL_TEMPLATE = r"https://www.allabrf.se/items/summaries?query={area}&page={page}"#&order={order}"
TARGET_DIR = os.path.join(WORKING_DIR, "data", "allabrf_data", "raw")
# ------------------------------------------------------

settings = get_settings()
setup_logging(__file__, debug=settings["debug"]) # Formats logging and store warnings/exceptions to file

def parse_organizations(data):
    fields = [
        'id',
        'name',
        'org_number',
        'county',
        'price_per_m2',
        'fee_per_m2',
        'debt_category',
        'rating_logo'
    ]

    rows = []
    for org in data["organizations"]:
        rows.append([None if field not in org else org[field] for field in fields])

    return pd.DataFrame(rows, columns=["allabrf_"+field for field in fields])

# Create target directory
os.makedirs(TARGET_DIR, exist_ok=True)

# Fetch areas within the greater Stockholm area
areas_csv_path = os.path.join(WORKING_DIR, "data", "property_data", "areas.csv")
areas_df = pd.read_csv(areas_csv_path, delimiter=";", encoding="utf8", index_col=0).drop_duplicates()

index = areas_df["area_type"].apply(lambda x: x in ["municipality", "locality", "suburb"])#, "userDefined"])
areas = areas_df[index]["area_name"].unique()
temp = []
for area in areas:
    temp += area.split("/") # Split multi-categories so that: "Katarina/Mosebacke" -> ['Katarina', 'Mosebacke']
areas = sorted(set(temp))

#orderings = [
#    "_score;desc",
#    "price_per_m2;desc",
#    "price_per_m2;asc",
#    "debt_per_m2;desc",
#    "debt_per_m2;asc",
#    "fee_per_m2;desc",
#    "fee_per_m2;asc",
#    "rating;desc",
#    "rating;asc"
#]

for area in areas:
    # Store data in corresponding folder
    area_folder = os.path.join(TARGET_DIR, area)
    os.makedirs(area_folder, exist_ok=True)

    previously_scraped_pages = os.listdir(area_folder)
    
    # For some reason, only 40 pages are available (pages >40 return same data as page 40).
    # Hence a for loop suffices; but it is possible not all data is scraped this way.
    for page in range(1,41):
        filename = f"page_{page}.csv"
        if filename in previously_scraped_pages:
            logging.info(f"Already scraped page {page} for area {area}. Continuing.")    
            continue

        curr_URL = URL_TEMPLATE.format(area=area, page=page)
        _, data = respectful_requesting(curr_URL)
        
        data = json.loads(data)
        if len(data["organizations"]) == 0:
            break
        
        df = parse_organizations(data)
        df.to_csv(os.path.join(area_folder, filename), sep=";", encoding="utf8")
        
        if settings["debug"]:
            break
    if settings["debug"]:
            break