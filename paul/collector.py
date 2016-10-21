# -*- coding: utf-8 -*-
"""
Collector storing Kraken's public data in Mongo DB for later analysis
"""
import signal
import logging
import asyncio

__author__ = "Florian Wilhelm"
__copyright__ = "Florian Wilhelm"
__license__ = "gpl3"

_logger = logging.getLogger(__name__)


def signal_handler():
    _logger.info("Signal handler called...")
    for task in asyncio.Task.all_tasks():
        task.cancel()
    loop = asyncio.get_event_loop()
    loop.stop()


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
            if not resp['error']:
                return resp['result']
            else:
                _logger.error(resp['error'])
                self._nerrors += 1
                return None
        except asyncio.CancelledError:
            _logger.info("Task cancelled...")
            raise
        except Exception:
            _logger.exception("General error:")
            self._nerrors += 1

    async def poll_ticker(self):
        while True:
            _logger.info("Polling ticker...")
            resp = await self._call_api(self.api.ticker, self.pairs)
            if resp:
                self.db_client.insert_ticker(resp)
            await asyncio.sleep(self.rates['ticker'])

    async def poll_depth(self):
        while True:
            _logger.info("Polling depth...")
            for pair in self.pairs:
                resp = await self._call_api(self.api.depth, pair)
                if resp:
                    self.db_client.insert_depth(resp)
            await asyncio.sleep(self.rates['depth'])

    def start(self):
        _logger.info("Starting event loop...")
        loop = asyncio.get_event_loop()
        if _logger.getEffectiveLevel() < logging.INFO:
            loop.set_debug(True)
        loop.add_signal_handler(signal.SIGINT, signal_handler)
        loop.add_signal_handler(signal.SIGTERM, signal_handler)
        loop.create_task(self.poll_ticker())
        loop.create_task(self.poll_depth())
        try:
            loop.run_forever()
        except asyncio.CancelledError:
            _logger.info("Execution was cancelled")
        except Exception:
            _logger.exception("Exception in event loop:")
            raise
        finally:
            _logger.info("Stopping event loop...")
            loop.close()
