# -*- coding: utf-8 -*-
"""
Paul's command line interface
"""
import argparse
import sys
import logging
import time

from .kraken import API
from .collector import Colls, Collector
from .db import DBClient
from . import __version__

__author__ = "Florian Wilhelm"
__copyright__ = "Florian Wilhelm"
__license__ = "new-bsd"

_logger = logging.getLogger(__name__)


def collect(args):
    _logger.info("Starting to collect...")
    client = DBClient()
    api = API()
    asset_pairs = list(api.assetpairs()['result'].keys())
    euro_pairs = [x for x in  asset_pairs
                  if 'EU' in x and not x.endswith('.d')]
    rates = {Colls.ticker: 10, Colls.depth: 600}
    collector = Collector(client, api, euro_pairs, rates)
    collector.start()

def interact(args):
    _logger.info("Starting interactive session...")
    from IPython import embed
    api = API()
    asset_pairs = list(api.assetpairs()['result'].keys())
    euro_pairs = [x for x in  asset_pairs
                  if 'EU' in x and not x.endswith('.d')]
    db = DBClient()
    embed()


def parse_args(args):
    """
    Parse command line parameters

    :param args: command line parameters as list of strings
    :return: command line parameters as :obj:`argparse.Namespace`
    """
    parser = argparse.ArgumentParser(
        description="Paul the kraken")
    parser.add_argument(
        '--version',
        action='version',
        version='paul {ver}'.format(ver=__version__))
    parser.add_argument(
        '-v',
        '--verbose',
        dest="loglevel",
        help="set loglevel to INFO",
        action='store_const',
        const=logging.INFO)
    parser.add_argument(
        '-vv',
        '--very-verbose',
        dest="loglevel",
        help="set loglevel to DEBUG",
        action='store_const',
        const=logging.DEBUG)
    subparsers = parser.add_subparsers(dest='command')
    collect_parser = subparsers.add_parser(
        'collect',
        help='collect ticker, trades etc.')
    collect_parser.set_defaults(func=collect)
    interact_parser = subparsers.add_parser(
        'interact',
        help='interactive IPython shell')
    interact_parser.set_defaults(func=interact)
    args = parser.parse_args(args)
    if not args.command:
        parser.print_help()
        print('ERROR: no subcommand specified!')
        sys.exit(1)
    return args


def main(args):
    args = parse_args(args)
    logging.basicConfig(level=args.loglevel, stream=sys.stdout)
    args.func(args)


def run():
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
