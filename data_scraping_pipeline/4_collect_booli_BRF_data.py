""" PARSES BOOLI BRF PAGE

<TODO>
"""

import os
import re
import logging
import json
import pandas as pd
import numpy as np
from helper_functions import setup_logging, get_settings, respectful_requesting, \
                                missing_field_decorator, append_to_csv
from numpy.random import normal
from working_dir import WORKING_DIR
# ----------------------- CONFIG -----------------------
URL_TEMPLATE = r"https://www.booli.se{brf_URL}"
TARGET_DIR = os.path.join(WORKING_DIR, "data", "brf_data")
LISTINGS_CSV = os.path.join(WORKING_DIR, "data", "listings_data", "listings_data.csv")
# ------------------------------------------------------

settings = get_settings()
setup_logging(__file__) # Formats logging and store warnings/exceptions to file

class ParseBRF(object):
    def __init__(self, brf_URL, apollo_state_json):
        self.brf_id = brf_URL.split("/")[-1]

        temp_key = f'housingCoop({{"housingCoopId":"{self.brf_id}"}})'
        self.brf_data = apollo_state_json["ROOT_QUERY"][temp_key]
        
        self.brf_name = self.brf_data["name"]
        self.brf_org_nr = self.brf_data["orgNumber"]

        if len(self.brf_data["annualReports"]) > 0:
            self.annual_report = self.brf_data["annualReports"][-1]
        else:
            self.annual_report = None

        self.annual_report_year = self.get_annual_report_year()
        self.annual_report_brf_type = self.get_annual_report_brf_type()
        self.annual_report_debt_other = self.get_annual_report_debt_other()
        self.annual_report_debt_real_estate = self.get_annual_report_debt_real_estate()
        self.annual_report_plot_leased = self.get_annual_report_plot_leased()
        self.annual_report_rental_units = self.get_annual_report_leased_units()
        self.annual_report_units = self.get_annual_report_units()
        self.annual_report_savings = self.get_annual_report_savings()
        self.annual_report_commercial_area = self.get_annual_report_commercial_area()
        self.annual_report_living_area = self.get_annual_report_living_area()
        self.annual_report_rental_area = self.get_annual_report_rental_area()
        self.annual_report_total_loan = self.get_annual_report_total_loan()
        self.annual_report_plot_area = self.get_annual_report_plot_area()

    @missing_field_decorator
    def get_annual_report_year(self):
        return self.annual_report["year"]

    @missing_field_decorator
    def get_annual_report_brf_type(self):
        return self.annual_report["housingCoopType"]

    @missing_field_decorator
    def get_annual_report_debt_other(self):
        return self.annual_report["longTermDebtOther"]
    
    @missing_field_decorator
    def get_annual_report_debt_real_estate(self):
        return self.annual_report["longTermRealEstateDebt"]

    @missing_field_decorator
    def get_annual_report_plot_leased(self):
        return self.annual_report["plotIsLeased"]

    @missing_field_decorator
    def get_annual_report_leased_units(self):
        return self.annual_report["numberOfRentalUnits"]["formatted"]

    @missing_field_decorator
    def get_annual_report_units(self):
        return self.annual_report["numberOfUnits"]["formatted"]
    
    @missing_field_decorator
    def get_annual_report_savings(self):
        return self.annual_report["savings"]["formatted"]
    
    @missing_field_decorator
    def get_annual_report_commercial_area(self):
        return self.annual_report["totalCommercialArea"]

    @missing_field_decorator
    def get_annual_report_living_area(self):
        return self.annual_report["totalLivingArea"]

    @missing_field_decorator
    def get_annual_report_rental_area(self):
        return self.annual_report["totalRentalArea"]

    @missing_field_decorator
    def get_annual_report_total_loan(self):
        return self.annual_report["totalLoan"]["formatted"]

    @missing_field_decorator
    def get_annual_report_plot_area(self):
        return self.annual_report["totalPlotArea"]

    def extract_data(self):
        return pd.DataFrame({
            "brf_id" : [self.brf_id],
            "brf_name": [self.brf_name],
            "brf_org_nr": [self.brf_org_nr],
            "brf_annual_report_year": [self.annual_report_year],
            "brf_annual_report_brf_type": [self.annual_report_brf_type],
            "brf_annual_report_debt_other": [self.annual_report_debt_other],
            "brf_annual_report_debt_real_estate": [self.annual_report_debt_real_estate],
            "brf_annual_report_plot_leased": [self.annual_report_plot_leased],
            "brf_annual_report_rental_units": [self.annual_report_rental_units],
            "brf_annual_report_units": [self.annual_report_units],
            "brf_annual_report_savings": [self.annual_report_savings],
            "brf_annual_report_commercial_area": [self.annual_report_commercial_area],
            "brf_annual_report_living_area": [self.annual_report_living_area],
            "brf_annual_report_rental_area": [self.annual_report_rental_area],
            "brf_annual_report_total_loan": [self.annual_report_total_loan],
            "brf_annual_report_plot_area": [self.annual_report_plot_area],
        })

def get_brf_URLs():
    df = pd.read_csv(LISTINGS_CSV, delimiter=";", encoding="utf8", index_col=0)
    col = df["brf_URL"]
    
    return col[col.notna()].unique()

def get_scraped_IDs():
    filepath = os.path.join(TARGET_DIR, "brf_data.csv")
    if os.path.isfile(filepath):
        df = pd.read_csv(filepath, delimiter=";", encoding="utf8", index_col=0)
        return df["brf_id"].values
    else:
        return []

# Make sure listing data is available
assert os.path.isfile(LISTINGS_CSV), "Can't find file 'listings_data.csv'"

# Create output folder, if not already present
if not os.path.isdir(TARGET_DIR):
    os.mkdir(TARGET_DIR)

# Get brf URLs to scrape
brf_URLs = get_brf_URLs()

# Get previously scraped URLs to avoid scraping them again
scraped_IDs = get_scraped_IDs()

# Initialize dataframe
for brf_URL in brf_URLs:
    brf_id = int(brf_URL.split("/")[-1])
    if brf_id in scraped_IDs:
        logging.info(f"Already scraped {brf_URL}, continuing to next listing...")
        continue

    curr_URL = URL_TEMPLATE.format(brf_URL=brf_URL)
    status_code, data = respectful_requesting(curr_URL)
    
    # If a 404 is returned, log a warning and continue to next listing
    if status_code == 404:
        logging.warning(f"Status code 404 returned for {brf_URL}, continuing to next listing...")
        continue

    # Load the apollo state, containing json data for the page
    apollo_state = re.findall(r'<script>window\.__APOLLO_STATE__ = (.+?)</script>', data)
    if len(apollo_state) == 0:
        raise(Exception("Could not find APOLLO_STATE."))
    apollo_state_json = json.loads(apollo_state[0])
    
    try:
        brf_df = ParseBRF(brf_URL, apollo_state_json).extract_data()

    except Exception as e:
        logging.exception(e)
        continue

    # Save to CSV
    append_to_csv(os.path.join(TARGET_DIR, "brf_data.csv"), brf_df)

    if settings["debug"]:
        break