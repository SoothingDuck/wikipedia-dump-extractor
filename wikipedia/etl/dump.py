import os
import bz2
from lxml import etree
from dateutil.parser import parse
import re
import requests
from bs4 import BeautifulSoup
import mwparserfromhell


class Site(object):
    """
    Interactions avec le site dumps.wikimedia.org
    """
    def __init__(self, lang, date):
        self._lang = lang
        self._date = date

    @property
    def lang(self):
        return self._lang

    @property
    def date(self):
        return self._date

    @property
    def articles(self):
        r = requests.get(
            "https://dumps.wikimedia.org/{}wiki/{}/".format(self._lang, self._date)
        )

        soup = BeautifulSoup(r.text, "html.parser")

        result = []
        for a in soup.find_all("a"):
            if re.search("pages-articles[0-9]+", a.get("href")):
                result.append(a.get("href").split("/")[-1])

        return result


class Dump(object):
    def __init__(self, filename):
        self._filename = filename
        self._data = self._filename.split("-")

    @property
    def filename(self):
        return self._filename

    def __filename_template(self, type_filename):
        basename = self._filename.split(os.path.sep)[-1]

        prefix = "-".join(basename.split("-")[:4]).replace(".xml", "")
        suffix = basename.split("-")[-1].replace(".bz2", "")

        return "{}.csv".format("-".join([prefix, type_filename, suffix]))

    @property
    def node_filename(self):
        return self.__filename_template("nodes")

    @property
    def redirection_filename(self):
        return self.__filename_template("redirections")

    @property
    def infobox_filename(self):
        return self.__filename_template("infoboxes")

    @property
    def link_filename(self):
        return self.__filename_template("links")

    @property
    def category_filename(self):
        return self.__filename_template("categories")

    @property
    def portal_filename(self):
        return self.__filename_template("portals")

    def __str__(self):
        return "<Dump {}>".format(self.filename)

class Article(object):
    def __init__(self, xml):
        self._xml = xml
        self._title = None
        self._redirect_title = None
        self._id = None
        self._ns = None

        self._text = None

        self._links = None
        self._categories = None
        self._portals = None

    @property
    def xml(self):
        return self._xml

    def __get_tag_or_none(self, tag):
        for child in self.xml:
            # Parsing id
            if child.tag.endswith("}" + tag):
                return child.text

    @property
    def title(self):
        if not self._title:
            self._title = self.__get_tag_or_none("title")
        return self._title

    @property
    def redirect_title(self):
        if not self._redirect_title:
            for child in self.xml:
                # Parsing id
                if child.tag.endswith("}redirect"):
                    self._redirect_title = child.attrib.get("title").strip()
        return self._redirect_title

    @property
    def id(self):
        if not self._id:
            self._id = int(self.__get_tag_or_none("id"))
        return self._id

    @property
    def ns(self):
        if not self._ns:
            self._ns = int(self.__get_tag_or_none("ns"))
        return self._ns

    @property
    def text(self):
        if not self._text:
            for child in self.xml:
                if child.tag.endswith("}revision"):
                    for child_revision in child:
                        if child_revision.tag.endswith("}text"):
                            self._text = child_revision.text
        return self._text

    @property
    def infoboxes(self):

        result = []
        for template in mwparserfromhell.parse(self.text).filter_templates():
            if "Infobox" in template.name:
                tmp = str(template.name).replace("Infobox", "").strip()
                result.append(tmp.lower())
        return(result)

    @property
    def links(self, max_length=1000):
        if self._links is None:
            self._links = set()

            if self.text is not None:
                for m in re.findall(r"\[\[(.*?)\]\]", self.text):
                    if "|" in m:
                        link = m.split("|")[0]
                    else:
                        link = m

                    if not link.startswith("Category:") and not link.startswith(
                        "Portal:"
                    ):
                        if link.strip() != "" and len(link.strip()) <= max_length:
                            self._links.add(link.strip())

        return self._links

    @property
    def categories(self):
        if self._categories is None:
            self._categories = set()

            if self.text is not None:
                for m in re.findall(r"\[\[%s:(.*?)\]\]" % ("Category"), self.text):
                    for category in m.split("|"):
                        if category.strip() not in ["", "*"]:
                            self._categories.add("Category:{}".format(category.strip()))
        return self._categories

    @property
    def portals(self):
        if self._portals is None:
            self._portals = set()

            if self.text is not None:
                # {{Portal|Anarchism|Libertarianism}}
                for m in re.findall(r"\{\{%s\|(.*?)\}\}" % ("Portal"), self.text):
                    for portal in m.split("|"):
                        if portal.strip() != "":
                            self._portals.add("Portal:{}".format(portal.strip()))

                # {{Portal bar|Biography|American Civil War|Illinois|United States|Politics|Law}}
                for m in re.findall(r"\{\{%s\|(.*?)\}\}" % ("Portal bar"), self.text):
                    for portal in m.split("|"):
                        if portal.strip() != "":
                            self._portals.add("Portal:{}".format(portal.strip()))

        return self._portals

    def pprint(self):
        print(etree.tostring(self.xml, pretty_print=True).decode("utf-8"))
