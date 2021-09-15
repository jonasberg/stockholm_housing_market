import os
import logging
import re
import pandas as pd
from helper_functions import setup_logging
from working_dir import WORKING_DIR

# ----------------------- CONFIG -----------------------
LISTINGS_URL_DIR = os.path.join(WORKING_DIR, "data", "listings_URLs")
# ------------------------------------------------------

setup_logging(__file__)

assert os.path.isdir(LISTINGS_URL_DIR), "LISTINGS_URL_DIR does not exist."
time_periods = os.listdir(LISTINGS_URL_DIR)

df = pd.DataFrame()
for time_period in time_periods:
    time_period_dir = os.path.join(LISTINGS_URL_DIR, time_period)

    # Get a list of all pages that were scraped from this time period
    pages = os.listdir(time_period_dir)
    pages.sort(key=lambda x: int(re.findall(r"page_(\d+).csv", x)[0]))

    for page in pages:
        page_file_path = os.path.join(time_period_dir, page)
        temp_df = pd.read_csv(page_file_path, delimiter=";", encoding="utf8", index_col=0)
        df = df.append(temp_df, ignore_index=True)

    logging.debug(f"Time period {time_period} completed")

logging.debug("All time periods compiled. Writing to listings_URLs.csv")
df = df.sort_values(by=["listing_id"], ignore_index=True)
df.to_csv(os.path.join(WORKING_DIR, "data", "listings_URLs.csv"), sep=";", encoding="utf8")