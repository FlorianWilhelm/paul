#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IPython import embed
from paul.kraken import API
from pymongo import MongoClient

api = API()
asset_pairs = list(api.assetpairs()['result'].keys())
client = MongoClient()
embed()
