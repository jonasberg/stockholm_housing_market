""" PARSE BOOLI PROPERTY PAGE
This script parses the data available on the booli property pages, which 
contain info about:
    - The property itself
    - Areas and their hiearchy
    - Previous listings of the property
    - Agents and agencies responsible for previous listings / sales
"""

import os
import re
import time
import logging
import json
import pandas as pd
from helper_functions import setup_logging, get_settings, respectful_requesting, \
                                missing_field_decorator, append_to_csv
from numpy.random import normal
from working_dir import WORKING_DIR

# ----------------------- CONFIG -----------------------
URL_TEMPLATE = r"https://www.booli.se{listing_URL}"
TARGET_DIR = os.path.join(WORKING_DIR, "data", "listings_data")
SAVE_TO_FILE_EVERY_N_LISTINGS = 100
# ------------------------------------------------------

settings = get_settings()
setup_logging(__file__)

class ParseProperty(object):
    def __init__(self, listing_id, listing_URL, apollo_state_json):
        self.listing_id = listing_id
        self.listing_URL = listing_URL

        if "bostad" in listing_URL:
            temp_key = f'propertyByResidenceId({{"residenceId":"{listing_id}"}})'
        elif "annons" in listing_URL:
            temp_key = f'propertyByListingId({{"listingId":"{listing_id}"}})'
        else:
            raise Exception("Unknown listing_URL format")

        self.property_data = apollo_state_json["ROOT_QUERY"][temp_key]

        self.latitude = self.get_latitude()
        self.longitude = self.get_longitude()
        self.construction_year = self.get_construction_year()
        self.energy_class = self.get_energy_class()
        self.address = self.get_address()
        self.object_type = self.get_object_type()
        self.apartment_number = self.get_apartment_number()
        self.descriptive_area_name = self.get_descriptive_area_name()
        self.has_solar_panels = self.get_has_solar_panels()
        self.brf_name = self.get_brf_name()
        self.brf_URL = self.get_brf_URL()
        self.montly_payment = self.get_monthly_payment()
        self.rent = self.get_rent()
        self.rooms = self.get_rooms()
        self.sqm = self.get_sqm()
        self.primary_area = self.get_primary_area()
        self.floor = self.get_floor()
        self.operating_cost = self.get_operating_cost()
        self.estimate_price = self.get_estimate()
        self.estimate_low = self.get_estimate_low()
        self.estimate_high = self.get_estimate_high()
        self.areas = self.get_areas()

    @missing_field_decorator
    def get_latitude(self):    
        return self.property_data["latitude"]
    
    @missing_field_decorator
    def get_longitude(self):
        return self.property_data["longitude"]

    @missing_field_decorator
    def get_construction_year(self):
        return self.property_data["constructionYear"]

    @missing_field_decorator
    def get_energy_class(self):
        return self.property_data["energyClass"]["score"]

    @missing_field_decorator    
    def get_address(self):
        return self.property_data["streetAddress"]

    @missing_field_decorator
    def get_object_type(self):
        return self.property_data["objectType"]

    @missing_field_decorator    
    def get_descriptive_area_name(self):
        return self.property_data["descriptiveAreaName"]

    @missing_field_decorator    
    def get_has_solar_panels(self):
        return self.property_data["hasSolarPanels"]

    @missing_field_decorator
    def get_monthly_payment(self):
        return self.property_data["monthlyPayment"]

    @missing_field_decorator
    def get_apartment_number(self):
        return self.property_data["apartmentNumber"]["formatted"]
    @missing_field_decorator
    def get_brf_name(self):
        return self.property_data["housingCoop"]["name"]

    @missing_field_decorator
    def get_brf_URL(self):
        return self.property_data["housingCoop"]["link"]
        
    @missing_field_decorator
    def get_rent(self):
        return self.property_data["rent"]["raw"]
        
    @missing_field_decorator
    def get_rooms(self):
        return self.property_data["rooms"]["raw"]

    @missing_field_decorator
    def get_sqm(self):
        return self.property_data["livingArea"]["raw"]
        
    @missing_field_decorator
    def get_primary_area(self):
        return self.property_data["primaryArea"]["name"]
        
    @missing_field_decorator
    def get_floor(self):
        return self.property_data["floor"]["formatted"]
        
    @missing_field_decorator
    def get_operating_cost(self):
        return self.property_data["operatingCost"]["raw"]
        
    @missing_field_decorator
    def get_estimate(self):
        return self.property_data["estimate"]["price"]["formatted"]
    
    @missing_field_decorator
    def get_estimate_low(self):
        return self.property_data["estimate"]["low"]["formatted"]
    
    @missing_field_decorator
    def get_estimate_high(self):
        return self.property_data["estimate"]["high"]["formatted"]

    def get_areas(self):
        try:
            areas = self.property_data["areas"]
            return [a["__ref"] for a in areas]
        except Exception as e:
            return []

    def extract_data(self):
        data_df = pd.DataFrame({
            "property_id" : [self.listing_id], # Renaming to property, as it is more apt.
            "property_URL" : [self.listing_URL], # Renaming to property, as it is more apt.
            "address" : [self.address],
            "apartment_number" : [self.apartment_number],
            "object_type" : [self.object_type],
            "latitude" : [self.latitude],
            "longitude" : [self.longitude],
            "construction_year" : [self.construction_year],
            "energy_class" : [self.energy_class],
            "descriptive_area_name" : [self.descriptive_area_name],
            "has_solar_panels" : [self.has_solar_panels],
            "brf_name" : [self.brf_name],
            "brf_URL" : [self.brf_URL],
            "montly_payment" : [self.montly_payment],
            "rent" : [self.rent],
            "rooms" : [self.rooms],
            "sqm" : [self.sqm],
            "primary_area" : [self.primary_area],
            "floor" : [self.floor],
            "operating_cost" : [self.operating_cost],
            "estimate_price" : [self.estimate_price], 
            "estimate_low" : [self.estimate_low], 
            "estimate_high" : [self.estimate_high],
        })

        property_to_area_pairs_df = pd.DataFrame({
            "property_id": [self.listing_id] * len(self.areas),
            "area": self.areas
        })

        return (data_df, property_to_area_pairs_df)
        
class ParseAreas(object):
    def __init__(self, apollo_state_json):
        self.apollo_state_json = apollo_state_json
        self.areas = [a for a in self.apollo_state_json.keys() if "Area" in a]
    
    def extract_data(self):
        data = []
        for area in self.areas:
            id = self.apollo_state_json[area]["id"]
            name = self.apollo_state_json[area]["name"]
            path = self.apollo_state_json[area]["path"]
            parent = self.apollo_state_json[area]["parent"]
            area_type = self.apollo_state_json[area]["type"]
            type_ = self.apollo_state_json[area]["__typename"]
            
            data.append([id, name, path, parent, area_type, type_])

        columns = ["area_id", "area_name", "area_path", "area_parent", "area_type", "area_typename"]
        return pd.DataFrame(data, columns=columns)

class ParseListings(object):
    def __init__(self, listing_id, listing_URL, apollo_state_json):
        self.listing_id = listing_id
        self.listing_URL = listing_URL

        if "bostad" in listing_URL:
            temp_key = f'propertyByResidenceId({{"residenceId":"{listing_id}"}})'
        elif "annons" in listing_URL:
            temp_key = f'propertyByListingId({{"listingId":"{listing_id}"}})'
        else:
            raise Exception("Unknown listing_URL format")

        self.property_data = apollo_state_json["ROOT_QUERY"][temp_key]
    
    def extract_data(self):
        listings = self.property_data["salesOfResidence"]

        data = []
        for listing in listings:
            agent = listing["agent"]["__ref"]

            if listing["agency"] is not None:
                agency_name = listing["agency"]["name"]
                agency_URL = listing["agency"]["url"]
            else: 
                agency_name = None
                agency_URL = None

            days_active = listing["daysActive"]
            sold_date = listing["soldDate"]
            sold_price_type = listing["soldPriceType"]
            sold_price = listing["soldPrice"]["formatted"]
            listed_price = listing["listPrice"]["formatted"]

            data.append([
                agent, 
                agency_name,
                agency_URL,
                days_active,
                sold_date,
                sold_price_type,
                sold_price,
                listed_price
            ])
        
        columns = [
            "listing_agent", 
            "listing_agency_name",
            "listing_agency_URL",
            "listing_days_active",
            "listing_sold_date",
            "listing_sold_price_type",
            "listing_sold_price",
            "listing_listed_price" 
        ]

        return pd.DataFrame(data, columns=columns)

class ParseAgents(object):
    def __init__(self, apollo_state_json):
        self.apollo_state_json = apollo_state_json
        self.agents = [a for a in self.apollo_state_json.keys() if "Agent" in a]

    def extract_data(self):
        data = []
        for agent in self.agents:
            agent_json = self.apollo_state_json[agent]

            id = agent_json["id"]
            type_name = agent_json["__typename"]
            recommendations = agent_json["recommendations"]
            email = agent_json["email"]
            name = agent_json["name"]
            rating = agent_json["overallRating"]
            seller_favorite = agent_json["sellerFavorite"]
            premium = agent_json["premium"]
            review_count = agent_json["reviewCount"]
            url = agent_json["url"]
            published_count = agent_json["listingStatistics"]["publishedCount"]

            if agent_json["listingStatistics"]["publishedValue"] is not None:
                published_value = agent_json["listingStatistics"]["publishedValue"]["raw"]
            else:
                published_value = None

            data.append([
                id, 
                type_name, 
                recommendations, 
                email, 
                name,
                rating, 
                seller_favorite, 
                premium, 
                review_count, 
                url,
                published_count,
                published_value
            ])

        columns = [
            "agent_id", 
            "agent_type_name", 
            "agent_recommendations", 
            "agent_email", 
            "agent_name",
            "agent_rating", 
            "agent_seller_favorite", 
            "agent_premium", 
            "agent_review_count", 
            "agent_URL",
            "agent_published_count",
            "agent_published_value"
        ]

        return pd.DataFrame(data, columns=columns)

def get_scraped_listings():
    filepath = os.path.join(TARGET_DIR, "listings_data.csv")
    if os.path.isfile(filepath):
        df = pd.read_csv(filepath, delimiter=";", encoding="utf8", index_col=0)
        return df["property_URL"].values
    else:
        return []

# Make sure listing data is available
listing_URLs_csv_path = os.path.join(WORKING_DIR, "data", "listings_URLs.csv")
assert os.path.isfile(listing_URLs_csv_path), "Can't find 'listings_URLs.csv' in data folder"

# Create output folder, if not already present
if not os.path.isdir(TARGET_DIR):
    os.mkdir(TARGET_DIR)

listing_URLs_df = pd.read_csv(listing_URLs_csv_path, delimiter=";", encoding="utf8", index_col=0)
listing_URLs = listing_URLs_df.sort_values(by="listing_id").drop_duplicates()

# Get previously scraped listings to avoid scraping them again
scraped_listings = get_scraped_listings()

# Initialize dataframes
listings_df = pd.DataFrame()
property_to_area_df = pd.DataFrame()
areas_df = pd.DataFrame()
agents_df = pd.DataFrame()

for _, (listing_id, listing_URL) in listing_URLs.iterrows():
    if listing_URL in scraped_listings:
        logging.info(f"Already scraped {listing_URL}, continuing to next listing...")
        continue

    curr_URL = URL_TEMPLATE.format(listing_URL=listing_URL)
    status_code, data = respectful_requesting(curr_URL)
    
    # If a 404 is returned, log a warning and continue to next listing
    if status_code == 404:
        logging.warning(f"Status code 404 returned for {listing_URL}, continuing to next listing...")
        continue

    # Measure time for processing
    start = time.time()

    # Load the apollo state, containing json data for the page
    apollo_state = re.findall(r'<script>window\.__APOLLO_STATE__ = (.+?)</script>', data)
    if len(apollo_state) == 0:
        raise(Exception("Could not find APOLLO_STATE."))
    apollo_state_json = json.loads(apollo_state[0])
    
    try:
        curr_property_df, curr_property_to_area_df = ParseProperty(listing_id, listing_URL, apollo_state_json).extract_data()
        curr_areas_df = ParseAreas(apollo_state_json).extract_data()
        curr_agents_df = ParseAgents(apollo_state_json).extract_data()
        curr_listings_df = ParseListings(listing_id, listing_URL, apollo_state_json).extract_data()

    except Exception as e:
        logging.exception(e)
        continue

    # Merge listings and property df; replicate property df for multiple sales of same property
    n_sales = len(curr_listings_df)
    curr_property_df = pd.concat([curr_property_df] * n_sales, ignore_index=True)
    curr_listings_df = pd.concat([curr_property_df, curr_listings_df], axis=1)

    # Append to dataframes
    listings_df = pd.concat([listings_df, curr_listings_df], ignore_index=True)
    property_to_area_df = pd.concat([property_to_area_df, curr_property_to_area_df], ignore_index=True)
    areas_df = pd.concat([areas_df, curr_areas_df], ignore_index=True)
    agents_df = pd.concat([agents_df, curr_agents_df], ignore_index=True)

    if len(listings_df) >= SAVE_TO_FILE_EVERY_N_LISTINGS:
        # Save to CSV
        append_to_csv(os.path.join(TARGET_DIR, "listings.csv"), listings_df)
        append_to_csv(os.path.join(TARGET_DIR, "property_to_area.csv"), property_to_area_df)
        append_to_csv(os.path.join(TARGET_DIR, "areas.csv"), areas_df, avoid_duplicates=True, key_column="area_id")
        append_to_csv(os.path.join(TARGET_DIR, "agents.csv"), agents_df, avoid_duplicates=True, key_column="agent_id")
        logging.info(f"Saved {len(listings_df)} scraped listings to file.")

        # Reset dataframes
        listings_df = pd.DataFrame()
        property_to_area_df = pd.DataFrame()
        areas_df = pd.DataFrame()
        agents_df = pd.DataFrame()

    logging.debug("Time elapsed for processing: " + str(time.time() - start))

    if settings["debug"]:
        break