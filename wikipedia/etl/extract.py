# -*- coding: utf-8 -*-

"""
    wikipedia
    ---------

    This module contains the classes used to represent and extract informations from wikipedia dumps

"""

__author__ = "Yvan Aillet"

import bz2
from lxml import etree
from dateutil.parser import parse
import re

from wikipedia.etl.dump import Article
from abc import ABC, abstractmethod
import os
import csv


class DumpExtractor(ABC):

    def __init__(self, dump):
        self._dump = dump

    @property
    def dump(self):
        return self._dump

    @abstractmethod
    def extract_nodes(self):
        pass

    @abstractmethod
    def extract_redirections(self):
        pass


class DumpFileExtractor(DumpExtractor):
    def __init__(self, dump, directory_name):
        super().__init__(dump)
        self._directory_name = directory_name

    def __iter__(self):
        """
        Iterator function over wikipedia dump extracting articles

        :return: wikipedia Page object extracted in sequential order
        """
        # Selon qu'on est compressé ou non
        if ".bz2" in self.dump.filename:
            f = bz2.BZ2File(self.dump.filename, mode="r")
        else:
            f = open(self.dump.filename, "rb")

        for _, element in etree.iterparse(f, events=("end",)):
            if element.tag == "{http://www.mediawiki.org/xml/export-0.10/}page":
                yield (Article(element))
            else:
                continue
            element.clear()

        f.close()

    def extract_nodes(self):
        # Nodes
        filename_path = os.path.join(self._directory_name, self.dump.node_filename)
        if not os.path.exists(filename_path):
            with open(filename_path, "w", newline="", encoding="utf-8") as csvfile:
                nodewriter = csv.writer(
                    csvfile, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL
                )
                nodewriter.writerow(
                    ["article_id", "article_title", "article_namespace"]
                )

                for article in self:
                    if article.redirect_title is None:
                        nodewriter.writerow(
                            [article.id, article.title.strip(), article.ns]
                        )

    def extract_redirections(self):
        # Redirections
        filename_path = os.path.join(
            self._directory_name, self.dump.redirection_filename
        )
        if not os.path.exists(filename_path):
            with open(filename_path, "w", newline="", encoding="utf-8") as csvfile:
                redirectionwriter = csv.writer(
                    csvfile, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL
                )
                redirectionwriter.writerow(
                    ["article_id", "article_title", "redirection_title"]
                )

                for article in self:
                    if article.redirect_title is not None:
                        redirectionwriter.writerow(
                            [
                                article.id,
                                article.title.strip(),
                                article.redirect_title.strip(),
                            ]
                        )

    def extract_links(self, maxlength_article_title=2000):
        # Links
        filename_path = os.path.join(self._directory_name, self.dump.link_filename)
        if not os.path.exists(filename_path):
            with open(filename_path, "w", newline="", encoding="utf-8") as csvfile:
                linkwriter = csv.writer(
                    csvfile, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL
                )
                linkwriter.writerow(["article_id", "article_title", "link_title"])

                for article in self:
                    if article.redirect_title is None:
                        if len(article.title.strip()) <= maxlength_article_title:
                            for link in article.links:
                                linkwriter.writerow(
                                    [article.id, article.title.strip(), link.strip()]
                                )

    def extract_infoboxes(self):
        # Categories
        filename_path = os.path.join(self._directory_name, self.dump.infobox_filename)
        if not os.path.exists(filename_path):
            with open(filename_path, "w", newline="", encoding="utf-8") as csvfile:
                infoboxwriter = csv.writer(
                    csvfile, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL
                )
                infoboxwriter.writerow(["article_id", "infobox"])

                for article in self:
                    if article.redirect_title is None:
                        for infobox in article.infoboxes:
                            infoboxwriter.writerow(
                                [article.id, infobox.strip()]
                            )

    def extract_categories(self):
        # Categories
        filename_path = os.path.join(self._directory_name, self.dump.category_filename)
        if not os.path.exists(filename_path):
            with open(filename_path, "w", newline="", encoding="utf-8") as csvfile:
                categorywriter = csv.writer(
                    csvfile, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL
                )
                categorywriter.writerow(["article_id", "article_title", "category"])

                for article in self:
                    if article.redirect_title is None:
                        for category in article.categories:
                            categorywriter.writerow(
                                [article.id, article.title.strip(), category.strip()]
                            )

    def extract_portals(self):
        # Portals
        filename_path = os.path.join(self._directory_name, self.dump.portal_filename)
        if not os.path.exists(filename_path):
            with open(filename_path, "w", newline="", encoding="utf-8") as csvfile:
                portalwriter = csv.writer(
                    csvfile, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL
                )
                portalwriter.writerow(["article_id", "article_title", "portal"])

                for article in self:
                    if article.redirect_title is None:
                        for portal in article.portals:
                            portalwriter.writerow(
                                [article.id, article.title.strip(), portal.strip()]
                            )


if __name__ == "__main__":
    import glob
    from wikipedia.etl.dump import Dump

    for filename in glob.glob("DATA/dump/fr/*.bz2"):
        print(filename)

        d = Dump(filename)
        print(d.node_filename)

        break
