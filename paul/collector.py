# -*- coding: utf-8 -*-
"""
Collector storing Kraken's public data in Mongo DB for later analysis
"""
import sys
import signal
import logging
import asyncio
from datetime import datetime

__author__ = "Florian Wilhelm"
__copyright__ = "Florian Wilhelm"
__license__ = "gpl3"

_logger = logging.getLogger(__name__)


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
        except asyncio.CancelledError as e:
            _logger.info("Task cancelled...")
            raise
        except Exception as e:
            _logger.exception("General error:")
            self._nerrors += 1
            return None
        return resp['result']

    async def poll_ticker(self):
        while True:
            _logger.debug("Polling ticker...")
            resp = await self._call_api(self.api.ticker, self.pairs)
            if resp:
                self.db_client.insert_ticker(resp)
            await asyncio.sleep(self.rates['ticker'])

    async def poll_depth(self):
        while True:
            _logger.debug("Polling depth...")
            for pair in self.pairs:
                resp = await self._call_api(self.api.depth, pair)
                if resp:
                    self.db_client.insert_depth(resp)
            await asyncio.sleep(self.rates['depth'])

    def signal_handler(self):
        _logger.info("Shutting down...")
        for task in asyncio.Task.all_tasks():
            task.cancel()
        loop = asyncio.get_event_loop()
        loop.stop()

    def start(self):
        _logger.info("Starting event loop...")
        loop = asyncio.get_event_loop()
        if _logger.getEffectiveLevel() < logging.INFO:
            loop.set_debug(True)
        loop.add_signal_handler(signal.SIGINT, self.signal_handler)
        loop.add_signal_handler(signal.SIGTERM, self.signal_handler)
        loop.create_task(self.poll_ticker())
        loop.create_task(self.poll_depth())
        try:
            loop.run_forever()
        except asyncio.CancelledError as e:
            _logger.info("Execution was cancelled.")
            loop.close()
            sys.exit(0)
        except Exception as e:
            _logger.exception("Exception in event loop:")
            loop.close()
