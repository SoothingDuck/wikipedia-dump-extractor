from configparser import ConfigParser
from wikipedia.etl.tools import batch_extract

if __name__ == "__main__":
    config = ConfigParser()
    config.read("config.ini")

    batch_extract(config["default"]["lang"], "nodes", "extract_nodes")
