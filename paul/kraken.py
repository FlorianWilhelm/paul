# -*- coding: utf-8 -*-
"""
REST API to Kraken Exchange
"""
import urllib
import urllib.parse
import requests
import logging
import time
import hashlib
import hmac
import base64

from .utils import tolist

__author__ = "Florian Wilhelm"
__copyright__ = "Florian Wilhelm"
__license__ = "gpl3"

_logger = logging.getLogger(__name__)


class API(object):
    """Kraken.com crypto currency exchange API.
    """
    def __init__(self, key=None, secret=None):
        self._key = key
        self._secret = secret
        self._uri = 'https://api.kraken.com'
        self._apiversion = 0

    def load_key(self, path):
        with open(path) as fh:
            self._key = fh.readline().strip()
            self._secret = fh.readline().strip()

    def _query(self, urlpath, params=None, headers=None):
        params = {} if params is None else params
        headers = {} if headers is None else headers
        url = self._uri + urlpath
        r = requests.post(url, data=params, headers=headers)
        r.raise_for_status()
        return r.json()

    def query_public(self, method, params=None):
        params = {} if params is None else params
        urlpath = '/{version}/public/{method}'.format(
            version=self._apiversion, method=method)
        return self._query(urlpath, params=params)

    def query_private(self, method, params=None):
        params = {} if params is None else params
        urlpath = '/{version}/private/{method}'.format(
            version=self._apiversion, method=method)
        params['nonce'] = int(1000*time.time())
        # generate signed headers
        postdata = urllib.parse.urlencode(params)
        enc_params = (str(params['nonce']) + postdata).encode()
        message = urlpath.encode() + hashlib.sha256(enc_params).digest()
        signature = hmac.new(base64.b64decode(self._secret),
                             message,
                             hashlib.sha512)
        sigdigest = base64.b64encode(signature.digest())
        headers = {
            'API-Key': self._key,
            'API-Sign': sigdigest.decode()
        }
        return self._query(urlpath, params=params, headers=headers)

    def time(self):
        return self.query_public('Time')

    def assets(self, asset=None, info='info', aclass='currency'):
        params = {'aclass': aclass, 'info': info}
        if asset:
            params['asset'] = tolist(asset)
        return self.query_public('Assets', params)

    def asset_pairs(self, pair=None, info='info'):
        params = {'info': info}
        if pair:
            params['pair'] = tolist(pair)
        return self.query_public('AssetPairs', params)

    def ticker(self, pair):
        params = {'pair': tolist(pair)}
        return self.query_public('Ticker', params)

    def ohlc(self, pair, interval=1, since=None):
        params = {'pair': tolist(pair), 'interval': interval}
        if since:
            params['since'] = since
        return self.query_public('OHLC', params)

    def depth(self, pair, count=None):
        params = {'pair': pair}
        if count is not None:
            params['count'] = count
        return self.query_public('Depth', params)

    def trades(self, pair, since=None):
        params = {'pair': pair}
        if since:
            params['since'] = since
        return self.query_public('Trades', params)

    def spread(self, pair, since=None):
        params = {'pair': pair}
        if since:
            params['since'] = since
        return self.query_public('Spread', params)
