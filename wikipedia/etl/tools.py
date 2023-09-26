import os
import glob
from wikipedia.etl.dump import Dump
from wikipedia.etl.extract import DumpFileExtractor

def batch_extract(lang, dir_mask, extract_function):
    """Extraction en masse d'un dump avec la fonction"""
    dump_directory = os.path.join("DATA", "dump", lang)
    output_directory = os.path.join(dump_directory, dir_mask)

    try:
        os.mkdir(output_directory)
    except FileExistsError:
        pass

    for dump_filename in sorted(glob.glob(os.path.join(dump_directory, "*.bz2"))):

        dump = Dump(dump_filename)

        etl = DumpFileExtractor(dump, output_directory)
        loader = getattr(etl, extract_function)
        loader()


