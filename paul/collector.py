# -*- coding: utf-8 -*-
"""
Collector storing Kraken's public data in Mongo DB for later analysis
"""
import logging
import asyncio
from datetime import datetime

__author__ = "Florian Wilhelm"
__copyright__ = "Florian Wilhelm"
__license__ = "new-bsd"

_logger = logging.getLogger(__name__)

class Colls(object):
    """Collections used in databases"""
    ticker = 'ticker'
    depth = 'depth'
    spread = 'spread'
    trades = 'trades'
    ohlc = 'ohlc'


class Collector(object):
    def __init__(self, db_client, db_name, kraken_api, pairs, rates):
        self.client = db_client
        self.db_name = db_name
        self.api = kraken_api
        self.rates = rates
        self.pairs = pairs
        self._nerrors = 0

    def _call_api(self, func, *args):
        loop = asyncio.get_event_loop()
        return loop.run_in_executor(None, func, *args)

    async def _poll_ticker(self):
        while True:
            try:
                resp = await self._call_api(self.api.ticker, self.pairs)
                assert not resp['error']
            except AssertionError as e:
                _logger.error(resp['error'])
                self._nerrors += 1
            except Exception as e:
                _logger.exception("General error:")
                self._nerrors += 1
            else:
                dataset = list()
                for k, v in resp['result'].items():
                    v['pair'] = k
                    v['datetime'] = datetime.utcnow()
                    dataset.append(v)
                self.client[self.db_name][Colls.ticker].insert_many(dataset)
            await asyncio.sleep(self.rates['ticker'])

    async def _poll_depth(self):
        

    def start(self):
        loop = asyncio.get_event_loop()
        loop.create_task(self._poll_ticker())
        try:
            loop.run_forever()
        except Exception as e:
            loop.close()
