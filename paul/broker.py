# -*- coding: utf-8 -*-
"""
Broker making trade decisions
"""
import sys
import signal
import logging
import asyncio
from datetime import datetime

import numpy as np
import numpy.ma as ma
from scipy import integrate

__author__ = "Florian Wilhelm"
__copyright__ = "Florian Wilhelm"
__license__ = "gpl3"

_logger = logging.getLogger(__name__)


def sort_chunks(chunks):
    chunks.sort(endwith=False)
    chunks[:] = np.flipud(chunks)


class BellmannBroker(object):
    def __init__(self, pair, funds, min_bet, horizon, timestep, discount,
                 max_stake=None, buy_fee=0., sell_fee=0.):
        self.pair = pair  # currency pair
        self.buy_fee = 1. + buy_fee / 100  # in percent
        self.sell_fee = 1. - sell_fee / 100  # in percent
        self.funds = funds  # total available money
        self.max_stake = max_stake if max_stake is not None else funds
        self.leftover = funds  # money that can still be spent
        self.discount = discount  # discount rate
        self.horizon = horizon  # in seconds
        self.timestep = timestep  # time step between two actions in seconds

        if horizon % timestep > 1e-6:
            raise RuntimeError("horizon should be divisible by timestep")
        self.max_delta = int(horizon // timestep)  # max count of timesteps
        self.min_bet = min_bet

        if self.max_stake % min_bet > 1e-6:
            raise RuntimeError("stake should be divisible by min_bet")
        n_chunks = self.max_stake // min_bet
        self.chunks = ma.array(np.empty(size=n_chunks), mask=[True]*n_chunks)

    def at_stake(self):
        return ma.count(self.chunks) * self.min_bet

    def max_buy(self):
        return min(ma.count_masked(self.chunks), self.leftover // self.min_bet)

    def max_sell(self):
        return ma.count(self.chunks)

    def curr_depot_value(self, price):
        return self.leftover + self.sell_fee * price * np.sum(self.chunks)

    def chunk_value(self, price):
        return self.min_bet / (self.buy_fee * price)

    def _get_ret_val(self, leftover, chunks, inplace):
        if inplace:
            self.leftover = leftover
            self.chunks = chunks
            return None
        else:
            return leftover, chunks

    def buy_chunks(self, count, price, inplace=False):
        assert 0 < count <= self.max_buy()
        leftover = self.leftover - count * self.min_bet
        chunks = self.chunks if inplace else self.chunks.copy()
        for _ in range(count):
            idx = np.where(ma.getmask(chunks))[0][0]
            chunks[idx] = self.chunk_value(price)
            chunks.mask[idx] = False
        sort_chunks(chunks)
        return self._get_ret_val(leftover, chunks, inplace)

    def sell_chunks(self, count, price, inplace=False):
        assert 0 < count <= self.max_sell()
        leftover = (self.leftover +
                    np.sum(self.chunks[:count]) * self.sell_fee * price)
        chunks = self.chunks if inplace else self.chunks.copy()
        for idx in range(count):
            chunks.mask[idx] = True
        sort_chunks(chunks)
        return self._get_ret_val(leftover, chunks, inplace)

    def get_chunks(self, count, price, inplace=False):
        if count > 0:
            return self.buy_chunks(count, price, inplace)
        elif count < 0:
            return self.sell_chunks(-count, price, inplace)
        else:
            return self._get_ret_val(self.leftover, self.chunks, inplace)

    def value_at_delta(self, delta, pdf, curr_price, count):
        def cost(price):
            leftover, chunks = self.get_chunks(count, curr_price)
            fut_value = leftover + price * self.sell_fee * np.sum(chunks)
            return pdf(price) * self.discount**delta * fut_value

        return integrate.quad(cost, 0., np.infinity)

