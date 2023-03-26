"""
L'idée est de pouvoir proposer de lancer des tâches simples telles que:

1. Téléchargement des fichiers wikipedia
2. Init : extraction de données légères et mise sous base de données relationnelle
	2.1 : Portail
	2.2 : Categories
	2.3 : Noeuds
	2.4 : Liens Noeuds-Portail
	2.5 : Liens Noeuds-Categories
3. Extraction d'un sous-ensemble de ces derniers par:
	2.1 : Liste de portails
	2.2 : Liste de catégories

3. Chargement d'un sous-ensemble défini précédemment en base de donnée relationnelle
"""

import argparse
import sys

# https://chase-seibert.github.io/blog/2014/03/21/python-multilevel-argparse.html

class FakeGit(object):

    def __init__(self):
        parser = argparse.ArgumentParser(
            description='Doing things with wikipedia',
            usage='''python -m wikipedia <command> [<args>]

The most commonly used git commands are:
   make_download_file     generate download file for latest dump from wikipedia
   update                 update common info with latest dump data
''')
        parser.add_argument('command', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print('Unrecognized command')
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        getattr(self, args.command)()

    def make_download_file(self):
        parser = argparse.ArgumentParser(
            description='make a download file')
        # prefixing the argument with -- means it's optional
        parser.add_argument('filename')
        parser.add_argument('--lang', default="en")
        parser.add_argument('--date', default="20200720")
        # now that we're inside a subcommand, ignore the first
        # TWO argvs, ie the command (git) and the subcommand (commit)
        args = parser.parse_args(sys.argv[2:])

        from wikipedia.dump import Site

        site = Site(args.lang, args.date)

        with open(args.filename, 'w', newline='', encoding='utf-8') as f:
            f.write("#!/bin/bash\n")

            for a in site.articles:
                tmp = """
# {}
curl -o \\
DATA/{} \\
-L -O -C - \\
https://dumps.wikimedia.org/{}wiki/{}/{}
""".format(a, a, site.lang, site.date, a)

                f.write(tmp)


    def fetch(self):
        parser = argparse.ArgumentParser(
            description='Download objects and refs from another repository')
        # NOT prefixing the argument with -- means it's not optional
        parser.add_argument('repository')
        args = parser.parse_args(sys.argv[2:])
        print('Running git fetch, repository=%s' % args.repository)


if __name__ == '__main__':
    FakeGit()

