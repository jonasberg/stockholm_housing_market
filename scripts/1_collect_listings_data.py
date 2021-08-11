import os
import bs4
from numpy import string_
import requests
import re
import time
import logging
import pandas as pd
from logging_helper import setup_logging
from datetime import date
from dateutil.relativedelta import relativedelta
from numpy.random import normal
from working_dir import WORKING_DIR

# ----------------------- CONFIG -----------------------
URL_TEMPLATE = r"https://www.booli.se/slutpriser/stockholms+lan/2?objectType=Lägenhet&minSoldDate={start_date}&maxSoldDate={end_date}&page={page_number}"
DEBUG = False
TARGET_DIR = os.path.join(WORKING_DIR, "listings_per_page")
SCRAPE_FROM = date(2012, 1, 1) # Year, month, date
SCRAPE_TO = date(2021, 6, 30)
TIME_INCREMENT = relativedelta(months=1) # Scrape in increments of one month
SECONDS_BETWEEN_REQUESTS = 3
N_SECONDS_PAUSE_AT_ERROR_CODE = 5*60 # How many seconds to pause before resume 
                        # scraping, if error message is returned from booli.
# ------------------------------------------------------

setup_logging(__file__, debug=DEBUG) # Formats logging and store warnings/exceptions to file

def parse_listing(listing):
    # Parsing the listing according to the order of appearance in the HTML document

    # Parent <a>-tag
    listing_URL = listing["href"]
    listing_id = re.findall(r"\d+", listing_URL)[0]
    listing_id = int(listing_id)

    # Three horizontal divs
    horizontal_divs = list(listing.div.children)

    # Fist div
    string_ = horizontal_divs[0].div.contents[0] # e.g. "+5.0 %", "+/-0 %", "-9 %" etc
    listing_price_increase_percent = re.search(r"(\+|-|\+/-)(\d+\.*\d*) %", string_)
    if listing_price_increase_percent is not None:
        if listing_price_increase_percent.group(1) == "-": 
            listing_price_increase_percent = -1 * float(listing_price_increase_percent.group(2))
        else:
            listing_price_increase_percent = float(listing_price_increase_percent.group(2))

    # Second div
    div = horizontal_divs[1]
    listing_address = div.h4.contents[0]
    ps = div.find_all("p")
    rooms_and_sqm = ps[0].contents[0]

    listing_rooms = re.search(r"(\d+)(½?) rum", rooms_and_sqm)
    if listing_rooms is not None:
        rooms = float(listing_rooms.group(1))
        if listing_rooms.group(2) == "½":
            rooms += 0.5
        listing_rooms = rooms
    
    listing_sqm = re.search(r"(\d+)(½?) m²", rooms_and_sqm)
    if listing_sqm is not None:
        sqm = float(listing_sqm.group(1))
        if listing_sqm.group(2) == "½":
            sqm += 0.5
        listing_sqm = sqm

    type_and_area = ps[1].contents[0]
    listing_type = type_and_area.split(", ")[0]
    listing_area = ", ".join(type_and_area.split(", ")[1:])

    # Third div
    div = horizontal_divs[2]
    listing_final_price = div.h4.contents[0]
    listing_final_price = int(re.sub(" (kr)?", "", listing_final_price))

    listing_date_sold = div.find_all("p")[1].contents[0]
    
    # Infer initial price, if possible
    listing_initial_price = None
    if listing_price_increase_percent is not None and listing_final_price is not None:
        listing_initial_price = int(listing_final_price / (1 + listing_price_increase_percent/100))

    return (
        listing_id,
        listing_URL,
        listing_date_sold,
        listing_address,
        listing_area,
        listing_rooms,
        listing_sqm,
        listing_type,
        listing_initial_price,
        listing_final_price,
        listing_price_increase_percent,
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
        s = max(1, normal(SECONDS_BETWEEN_REQUESTS, SECONDS_BETWEEN_REQUESTS/3))
        logging.debug(f"Sleeping for {s} seconds")
        time.sleep(s)

        listing_rows = []
        curr_url = URL_TEMPLATE.format(
            page_number=curr_page, 
            start_date=start_date, 
            end_date=end_date
        )
        
        resp = requests.get(curr_url)
        # If status code other than 200 is returned, the program sleeps for a predetermined amount of seconds
        # before retrying the request. If this occurs multiple times in a row, double the pause length at each
        # failure.
        pause_exponent = 0
        while resp.status_code != 200:
            pause_length = 2**pause_exponent*N_SECONDS_PAUSE_AT_ERROR_CODE
            logging.warning(f"Status code {resp.status_code} returned. Pausing for {pause_length} seconds.")
            time.sleep(pause_length)
            resp = requests.get(curr_url)
            pause_exponent += 1

        soup = bs4.BeautifulSoup(resp.content, "html.parser")

        listings = soup.find_all(href=re.compile("/annons/|/bostad/"))
        
        # Break if no more pages
        if len(listings) == 0:
            logging.info(f"Done scraping from {start_date} to {end_date}, {curr_page - 1} pages found")
            break

        for listing in listings:
            try:
                listing_rows.append(parse_listing(listing))

            except Exception as e:
                # If some error occurs due to unexpected data format, simply skip the listing
                logging.exception(e)
                print(listing)
                continue

        columns = [
            "listing_id",
            "listing_URL",
            "listing_date_sold",
            "listing_address",
            "listing_area",
            "listing_rooms",
            "listing_sqm",
            "listing_type",
            "listing_initial_price",
            "listing_final_price",
            "listing_price_increase_percent"
        ]
        df = pd.DataFrame(listing_rows, columns=columns)
        df.to_csv(os.path.join(time_interval_dir, f"page_{curr_page}.csv"), sep=";", encoding="utf8")

        curr_page += 1
        
        if DEBUG:
            break
    
    start_date += TIME_INCREMENT

    if DEBUG:
        break
