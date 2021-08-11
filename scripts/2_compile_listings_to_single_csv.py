import os
import logging
import re
import pandas as pd
from logging_helper import setup_logging
from working_dir import WORKING_DIR

# ----------------------- CONFIG -----------------------
LISTINGS_URL_DIR = os.path.join(WORKING_DIR, "listings_per_page")
# ------------------------------------------------------

setup_logging(__file__, log_to_file=False) # Formats logging

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

logging.debug("All time periods compiled. Writing to listings_data.csv")
df = df.sort_values(by=["listing_date_sold"], ignore_index=True)

# Add fields to be filled later
df["listing_agency"] = None
df["listing_agency_URL"] = None
df["listing_agent_id"] = None
df["listing_sold_price_type"] = None
df["listing_days_active"] = None

df.to_csv(os.path.join(WORKING_DIR, "listings_data.csv"), sep=";", encoding="utf8")