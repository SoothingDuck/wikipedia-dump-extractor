import os
import glob
from wikipedia.etl.dump import Dump
from wikipedia.etl.extract import DumpFileExtractor
from wikipedia import config


def extraction_done(lang, dir_mask):
    """Vérifier le statut de l'extraction"""
    dump_directory = os.path.join(config["default"]["data_directory"], "dump", lang)
    output_directory = os.path.join(dump_directory, dir_mask)

    return os.path.exists(output_directory)


def batch_extract(lang, dir_mask, extract_function):
    """Extraction en masse d'un dump avec la fonction"""
    dump_directory = os.path.join(config["default"]["data_directory"], "dump", lang)
    output_directory = os.path.join(dump_directory, dir_mask)

    try:
        os.mkdir(output_directory)
    except FileExistsError:
        pass

    for dump_filename in sorted(glob.glob(os.path.join(dump_directory, "*xml-p*.bz2"))):
        dump = Dump(dump_filename, lang)

        etl = DumpFileExtractor(dump, output_directory)
        loader = getattr(etl, extract_function)
        loader()


def batch_extract_parallel(lang, dir_mask, extract_function):
    """Extraction en masse d'un dump avec la fonction"""
    dump_directory = os.path.join(config["default"]["data_directory"], "dump", lang)
    output_directory = os.path.join(dump_directory, dir_mask)

    try:
        os.mkdir(output_directory)
    except FileExistsError:
        pass

    for dump_filename in sorted(glob.glob(os.path.join(dump_directory, "*xml-p*.bz2"))):
        dump = Dump(dump_filename, lang)

        etl = DumpFileExtractor(dump, output_directory)
        loader = getattr(etl, extract_function)
        loader()
        # Avec lambda x et directory en input
        # from multiprocessing import Process
