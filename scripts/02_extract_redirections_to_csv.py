from wikipedia.etl.tools import batch_extract
from wikipedia import config

if __name__ == "__main__":

    batch_extract(config["default"]["lang"], "redirections", "extract_redirections")
