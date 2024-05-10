"""
    Wikipedia Dump Analysis
    ~~~~~~~~~~~~~~~~~~~~~~~

    This module was created to me being able to parse wikipedia dumps and extract useful information of it.

    There is also a possibility to use available sqlalchemy models in order to store informations in RDBMS.

    Exemple of code for dump extraction (last french dump)::

    ./scripts/01_download_fr_dump.sh

"""

from configparser import ConfigParser

config = ConfigParser()
config.read("config.ini")
