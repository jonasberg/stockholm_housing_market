import logging
import os
import time
from working_dir import WORKING_DIR

def setup_logging(filename, log_to_file=True, debug=False):
    # Logging both to console (with level = DEBUG) and to file (with level = WARNING)
    logging.basicConfig(level=logging.DEBUG)
    root_logger = logging.getLogger()
    log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # Update default StreamHandler output format
    root_logger.handlers[0].setFormatter(log_formatter) 

    # File logging. If debug=True, do not log to file to avoid cluttering
    if log_to_file and not debug:
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
