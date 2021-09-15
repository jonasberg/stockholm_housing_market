import logging
import os
import time
import json
import requests
import pandas as pd
from numpy.random import normal
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from working_dir import WORKING_DIR

def get_settings():
    """ Read settings from settings.json file, return it as a parsed json object. """
    with open(os.path.join(WORKING_DIR, "data_scraping_pipeline", "settings.json")) as f:
        return json.load(f)

# Import settings to make it available to subsequent functions
settings = get_settings()

def setup_logging(filename):
    """ Sets up logging, saving log files to the logs folder. """
    # Logging both to console (with level = DEBUG) and to file (with level = WARNING)
    logging.basicConfig(level=logging.DEBUG)
    root_logger = logging.getLogger()
    log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # Update default StreamHandler output format
    root_logger.handlers[0].setFormatter(log_formatter) 

    # File logging. If debug=True, do not log to file to avoid cluttering
    if settings["log_to_file"] and not settings["debug"]:
        logging_dir = os.path.join(WORKING_DIR, "logs")
        if not os.path.isdir(logging_dir):
            os.mkdir(logging_dir)

        log_filename = filename.replace(".py", "__") + \
                    time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime()) + \
                    ".log"
        file_handler = logging.FileHandler(os.path.join(logging_dir, log_filename))
        file_handler.setLevel(logging.WARNING)
        file_handler.setFormatter(log_formatter)
        root_logger.addHandler(file_handler)

def throttle_decorator(f):
    """
    Throttles a function according to the parameters set in settins.json
    """
    def wrapper(*args, latest_request_timestamp=[-1]):
        pause_length = normal(settings["seconds_between_requests"], settings["seconds_between_requests"]/3)
        pause_length = max(1, pause_length)
        time_since_last_request = time.time() - latest_request_timestamp[0]

        if time_since_last_request < pause_length:
            logging.debug(f"Sleeping for {pause_length - time_since_last_request} seconds")
            time.sleep(pause_length - time_since_last_request)

        # Since latest_request_timestamp is initialized as an list, changes will be
        # persistent over function calls. Here the list is only ever filled with 
        # one value, which is the most recent timestamp, and is used as a way to 
        # throttle the function.
        latest_request_timestamp[0] = time.time() 

        return f(*args)
    
    return wrapper
        
@throttle_decorator
def respectful_requesting(url):
    """ 
    Makes a request for the specified url. If status code other than 200 is 
    returned, the program sleeps for a predetermined amount of seconds before 
    retrying the request. If this occurs multiple times in a row, double the 
    pause length at each failure. This is to make the program back off from 
    sending frequent requests for example if the server experiences issues. 
    """

    if settings["use_selenium"]:
        request_func = respectful_requesting_selenium
    else:
        request_func = respectful_requesting_requests

    # Avoid making too frequent subsequent requests if the server responds with a
    # status code not equal to 200. Also, do not retry if response is 404.
    status_code, data = request_func(url)
    pause_exponent = 0
    while status_code not in [200, 404]:
        pause_length = 2**pause_exponent*settings["n_seconds_pause_at_error_code"]
        logging.warning(f"Status code {status_code} returned. Pausing for {pause_length} seconds.")
        time.sleep(pause_length)
        status_code, data = request_func(url)
        pause_exponent += 1

    return status_code, data
    

def respectful_requesting_requests(url):
    """
    The most straightforward approach, but easily detected and blacklisted.
    Circumventing being blacklisted is not encouraged, but possible by using
    selenium instead of the requests library. 
    """
    resp = requests.get(url)
    return (resp.status_code, resp.content)



# Specify selenium desired capabilities that will allow reading HTTP status code from logs
selenium_logging_capabilities = DesiredCapabilities.CHROME.copy()
selenium_logging_capabilities["goog:loggingPrefs"] = {"performance": "WARNING"}

# Initialize driver, if selenium should be used.
selenium_driver = None
if settings["use_selenium"]:
    selenium_driver = webdriver.Chrome(
        settings["selenium_chrome_driver_path"], 
        desired_capabilities=selenium_logging_capabilities
    )
def get_status(logs):
    """ 
    Taken from https://stackoverflow.com/a/63876668, thank you Jarad.
    
    Searches through the log entries for the most recent text/html response 
    received and returns the status code of this request.
    """
    for log in logs:
        if log['message']:
            d = json.loads(log['message'])
            try:
                content_type = 'text/html' in d['message']['params']['response']['headers']['content-type']
                response_received = d['message']['method'] == 'Network.responseReceived'
                if content_type and response_received:
                    return d['message']['params']['response']['status']
            except:
                pass

def respectful_requesting_selenium(url):
    """
    Similar to the function using the requests library, but a lot more 
    complicated and unstable. However, it is more resistant towards 
    blacklisting as it emulates normal browser behaviour. 
    
    (Not that scraping while blacklisted is encouraged)
    """
    selenium_driver.get(url)
    logs = selenium_driver.get_log('performance')
    status_code = get_status(logs)

    return (status_code, selenium_driver.page_source)


def missing_field_decorator(f):
    def wrapper(*args):
        try:
            return f(*args)
        except Exception as e:
            return None
    return wrapper

def append_to_csv(filepath, df, avoid_duplicates=False, key_column=None):
    # If no file exists, simply write to filepath
    if not os.path.isfile(filepath):
        df.to_csv(filepath, sep=";", encoding="utf8")
        return
    
    old_df = pd.read_csv(filepath, delimiter=";", encoding="utf8", index_col=0)

    if avoid_duplicates:
        existing_rows = old_df[key_column].astype("str")
        rows_to_add = ~df[key_column].astype("str").isin(existing_rows)
        combined_df = pd.concat([old_df, df[rows_to_add]], ignore_index=True)
    else:
        combined_df = pd.concat([old_df, df], ignore_index=True)

    combined_df.to_csv(filepath, sep=";", encoding="utf8")
    return