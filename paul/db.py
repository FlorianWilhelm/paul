# -*- coding: utf-8 -*-
"""
Database related functionality
"""
import logging
import asyncio
from decimal import Decimal
from datetime import datetime

from sqlalchemy import (Table, Column, Integer, String, MetaData, create_engine,
                        DateTime, Numeric)

__author__ = "Florian Wilhelm"
__copyright__ = "Florian Wilhelm"
__license__ = "new-bsd"

_logger = logging.getLogger(__name__)


PRICE_PREC = Numeric(14, 8)
VOL_PREC = Numeric(14, 4)


class Trade(object):
    createtime = 'createtime'
    pair = 'pair'
    volume = 'volume'
    price = 'price'
    order = 'order'
    otype = 'type'
    misc = 'misc'


class Spread(object):
    createtime = 'createtime'
    pair = 'pair'
    bid = 'bid'
    ask = 'ask'


class Ticker(object):
    createtime = 'createtime'
    pair = 'pair'
    ask_price = 'ask_price'
    ask_vol = 'ask_vol'
    bid_price = 'bid_price'
    bid_vol = 'bid_vol'
    last_price = 'last_price'
    last_volume = 'last_vol'
    vol_day = 'vol_day'
    vol_24h = 'vol_24h'
    vwa_price_day = 'vwa_price_day'
    vwa_price_24h = 'vwa_price_24h'
    n_trades_day = 'n_trades_day'
    n_trades_24h = 'n_trades_24h'
    low_day = 'low_day'
    low_24h = 'low_24h'
    high_day = 'high_day'
    high_24h = 'high_24h'
    open_price = 'open_price'


class Depth(object):
    createtime = 'createtime'
    pair = 'pair'
    order = 'order'
    idx = 'index'
    price = 'price'
    vol = 'volume'
    timestamp = 'timestamp'


class DBClient(object):
    def __init__(self):
        self.engine = create_engine('postgresql+psycopg2://localhost:5432/paul')
        self.metadata = self._create_metadata()
        self.trades = self.metadata.tables['trades']
        self.spreads = self.metadata.tables['spreads']
        self.ticker = self.metadata.tables['ticker']
        self.depth = self.metadata.tables['depth']

    def _create_metadata(self):
        metadata = MetaData()
        trades = Table('trades', metadata,
                       Column('id', Integer, primary_key=True),
                       Column(Trade.createtime, DateTime),
                       Column(Trade.pair, String),
                       Column(Trade.volume, VOL_PREC),
                       Column(Trade.price, PRICE_PREC),
                       Column(Trade.order, String),
                       Column(Trade.otype, String),
                       Column(Trade.misc, String))
        spreads = Table('spreads', metadata,
                        Column('id', Integer, primary_key=True),
                        Column(Spread.createtime, DateTime),
                        Column(Spread.pair, String),
                        Column(Spread.bid, PRICE_PREC),
                        Column(Spread.ask, PRICE_PREC))
        ticker = Table('ticker', metadata,
                       Column('id', Integer, primary_key=True),
                       Column(Ticker.createtime, DateTime),
                       Column(Ticker.pair, String),
                       Column(Ticker.ask_price, PRICE_PREC),
                       Column(Ticker.ask_vol, VOL_PREC),
                       Column(Ticker.bid_price, PRICE_PREC),
                       Column(Ticker.bid_vol, VOL_PREC),
                       Column(Ticker.last_price, PRICE_PREC),
                       Column(Ticker.last_volume, VOL_PREC),
                       Column(Ticker.vol_day, VOL_PREC),
                       Column(Ticker.vol_24h, VOL_PREC),
                       Column(Ticker.vwa_price_day, PRICE_PREC),
                       Column(Ticker.vwa_price_24h, PRICE_PREC),
                       Column(Ticker.n_trades_day, Integer),
                       Column(Ticker.n_trades_24h, Integer),
                       Column(Ticker.low_day, PRICE_PREC),
                       Column(Ticker.low_24h, PRICE_PREC),
                       Column(Ticker.high_day, PRICE_PREC),
                       Column(Ticker.high_24h, PRICE_PREC),
                       Column(Ticker.open_price, PRICE_PREC)
                       )
        depth = Table('depth', metadata,
                      Column('id', Integer, primary_key=True),
                      Column(Depth.createtime, DateTime),
                      Column(Depth.pair, String),
                      Column(Depth.order, String),
                      Column(Depth.idx, Integer),
                      Column(Depth.price, PRICE_PREC),
                      Column(Depth.vol, VOL_PREC),
                      Column(Depth.timestamp, DateTime)
                      )
        return metadata

    def create_tables(self):
        self.metadata.create_all(self.engine)

    def insert_trades(self, pair, trades):
        cols = [Trade.price, Trade.volume, Trade.createtime, Trade.order,
                Trade.type, Trade.misc]
        data = [dict(zip(cols, t)) for t in trades]
        for d in data:
            d[Trade.pair] = pair
        conn = self.engine.connect()
        return conn.execute(self.trades.insert(), data)

    def insert_spreads(self, pair, spreads):
        cols = [Spread.createtime, Spread.bid, Spread.ask]
        data = [dict(zip(cols, s)) for s in spreads]
        for d in data:
            d[Spread.pair] = pair
        conn = self.engine.connect()
        return conn.execute(self.spreads.insert(), data)

    def insert_ticker(self, ticker):
        utcnow = datetime.utcnow()
        data = [{Ticker.createtime: utcnow,
                 Ticker.pair: pair,
                 Ticker.ask_price: Decimal(info['a'][0]),
                 Ticker.ask_vol: Decimal(info['a'][2]),
                 Ticker.bid_price: Decimal(info['b'][0]),
                 Ticker.bid_vol: Decimal(info['b'][2]),
                 Ticker.last_price: Decimal(info['c'][0]),
                 Ticker.last_volume: Decimal(info['c'][1]),
                 Ticker.vol_day: Decimal(info['v'][0]),
                 Ticker.vol_24h: Decimal(info['v'][1]),
                 Ticker.vwa_price_day: Decimal(info['p'][0]),
                 Ticker.vwa_price_24h: Decimal(info['p'][1]),
                 Ticker.n_trades_day: int(info['t'][0]),
                 Ticker.n_trades_24h: int(info['t'][1]),
                 Ticker.low_day: Decimal(info['l'][0]),
                 Ticker.low_24h: Decimal(info['l'][1]),
                 Ticker.high_day: Decimal(info['h'][0]),
                 Ticker.high_24h: Decimal(info['h'][1]),
                 Ticker.open_price: Decimal(info['o'])}
                for pair, info in ticker.items()]
        conn = self.engine.connect()
        return conn.execute(self.ticker.insert(), data)

    def insert_depth(self, depth):
        utcnow = datetime.utcnow()
        data = [{Depth.createtime: utcnow,
                 Depth.pair: pair,
                 Depth.order: order_type,
                 Depth.idx: i,
                 Depth.price: Decimal(array[0]),
                 Depth.vol: Decimal(array[1]),
                 Depth.timestamp: datetime.utcfromtimestamp(array[2])}
                 for order_type, orders in trades.items()
                 for pair, trades in depth.items()
                 for i, array in enumerate(orders)]
        conn = self.engine.connect()
        return conn.execute(self.depth.insert(), data)
