import os
import logging
import re
import pandas as pd
from helper_functions import setup_logging
from working_dir import WORKING_DIR

# ----------------------- CONFIG -----------------------
ALLABRF_DATA_DIR = os.path.join(WORKING_DIR, "data", "allabrf_data", "raw")
# ------------------------------------------------------

setup_logging(__file__, log_to_file=False) # Formats logging

assert os.path.isdir(ALLABRF_DATA_DIR), "ALLABRF_DATA_DIR does not exist."
areas = os.listdir(ALLABRF_DATA_DIR)

df = pd.DataFrame()
for area in areas:
    area_dir = os.path.join(ALLABRF_DATA_DIR, area)

    # Get a list of all pages that were scraped from this time period
    pages = os.listdir(area_dir)
    pages.sort(key=lambda x: int(re.findall(r"page_(\d+).csv", x)[0]))

    for page in pages:
        page_file_path = os.path.join(area_dir, page)
        temp_df = pd.read_csv(page_file_path, delimiter=";", encoding="utf8", index_col=0)
        df = df.append(temp_df, ignore_index=True)

    logging.debug(f"Area {area} completed")

logging.debug("All BRF data compiled. Writing to allabrf_data.csv")
df = df.drop_duplicates()
df.to_csv(os.path.join(WORKING_DIR, "data", "allabrf_data", "allabrf_data.csv"), sep=";", encoding="utf8")