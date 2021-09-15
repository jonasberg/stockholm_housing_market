""" 
Scrapes the URLs for all Booli listings in Stockholm between specified time
interval. Stores the URLs first in a folder structure, with a folder for 
each interval and a CSV file for each page of listings during that time. This 
is meant to facilitate to be easily interpretable and thus make it easy to 
successively add more data as it is being generated.
"""

import os
import bs4
import re
import logging
import pandas as pd
from helper_functions import setup_logging, get_settings, respectful_requesting
from datetime import date
from dateutil.relativedelta import relativedelta
from working_dir import WORKING_DIR

# ----------------------- CONFIG -----------------------
URL_TEMPLATE = r"https://www.booli.se/slutpriser/stockholms+lan/2?objectType=Lägenhet&minSoldDate={start_date}&maxSoldDate={end_date}&page={page_number}"
TARGET_DIR = os.path.join(WORKING_DIR, "data", "listings_URLs")
SCRAPE_FROM = date(2021, 7, 1) # Year, month, date
SCRAPE_TO = date(2021, 7, 31)
TIME_INCREMENT = relativedelta(months=1) # Scrape in increments of one month
# ------------------------------------------------------

settings = get_settings()
setup_logging(__file__)

def get_listing_id_and_URL(listing):
    listing_URL = listing["href"]
    listing_id = int(re.findall(r"\d+", listing_URL)[0])

    return (
        listing_id,
        listing_URL
    )

if not os.path.isdir(TARGET_DIR):
    os.mkdir(TARGET_DIR)

start_date = SCRAPE_FROM
while start_date < SCRAPE_TO:
    end_date = start_date + TIME_INCREMENT - relativedelta(days=1) # Remove one day to avoid overlap

    # Do not scrape until a later date than specified by the user
    if end_date > SCRAPE_TO:
        end_date = SCRAPE_TO

    logging.info(f"Scraping from {start_date} to {end_date}")

    # Create dir for output files
    time_interval_dir = os.path.join(TARGET_DIR, f"{start_date} to {end_date}")
    if not os.path.isdir(time_interval_dir):
        os.mkdir(time_interval_dir)

    curr_page = 1
    while True:
        listing_rows = []
        curr_url = URL_TEMPLATE.format(
            page_number=curr_page, 
            start_date=start_date, 
            end_date=end_date
        )
        
        _, data = respectful_requesting(curr_url)
        soup = bs4.BeautifulSoup(data, "html.parser")

        listings = soup.find_all(href=re.compile("/annons/|/bostad/"))
        
        # Break if no more pages
        if len(listings) == 0:
            logging.info(f"Done scraping from {start_date} to {end_date}, {curr_page - 1} pages found")
            break

        for listing in listings:
            try:
                listing_rows.append(get_listing_id_and_URL(listing))

            except Exception as e:
                # If some error occurs due to unexpected data format, simply skip the listing
                logging.exception(e)
                print(listing)
                continue

        df = pd.DataFrame(listing_rows, columns=["listing_id", "listing_URL"])
        df.to_csv(os.path.join(time_interval_dir, f"page_{curr_page}.csv"), sep=";", encoding="utf8")

        curr_page += 1
        if settings["debug"]:
            break
    
    start_date += TIME_INCREMENT
    if settings["debug"]:
        break
