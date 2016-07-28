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
    def __init__(self, db_client, kraken_api, pairs, rates):
        self.db_client = db_client
        self.api = kraken_api
        self.rates = rates
        self.pairs = pairs
        self._nerrors = 0

    async def _call_api(self, func, *args):
        loop = asyncio.get_event_loop()
        try:
            resp = await loop.run_in_executor(None, func, *args)
            assert not resp['error']
        except AssertionError as e:
            _logger.error(resp['error'])
            self._nerrors += 1
            return None
        except Exception as e:
            _logger.exception("General error:")
            self._nerrors += 1
            return None
        return resp['result']

    async def _poll_ticker(self):
        while True:
            resp = await self._call_api(self.api.ticker, self.pairs)
            if resp:
                self.db_client.insert_ticker(resp)
            await asyncio.sleep(self.rates['ticker'])

    async def _poll_depth(self):
        while True:
            for pair in self.pairs:
                resp = await self._call_api(self.api.depth, pair)
                if resp:
                    self.db_client.insert_depth(resp)
            await asyncio.sleep(self.rates['depth'])

    def start(self):
        _logger.info("Starting event loop...")
        loop = asyncio.get_event_loop()
        loop.create_task(self._poll_ticker())
        loop.create_task(self._poll_depth())
        try:
            loop.run_forever()
        except Exception as e:
            _logger.exception("Exception in event loop:")
            loop.close()
