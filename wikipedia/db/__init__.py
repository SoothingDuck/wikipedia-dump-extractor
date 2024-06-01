import os
import datetime
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from wikipedia import config

################# LOGGER #################################
# create logger
logger = logging.getLogger("wikipedia.db")
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.FileHandler(
    filename=os.path.join(
        config["default"]["data_directory"],
        "log",
        "wikipedia-model-{}.log".format(datetime.date.today().strftime("%Y%m%d")),
    ),
    encoding="utf-8",
)
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter(
    "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s"
)

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

################# ENGINE #################################
# Dump directory to consider
dump_directory = os.path.join(config["default"]["data_directory"], "full")

db_directory = os.path.join(config["default"]["data_directory"], "db")
os.makedirs(os.path.join(db_directory), exist_ok=True)

engine = create_engine(
    "sqlite:///{}/db/wikipedia.sqlite".format(config["default"]["data_directory"]),
    echo=False,
)

################# SESSION #################################
logger.info("Début : Création de la session")
session = sessionmaker(engine)()
logger.info("Fin : Création de la session")
