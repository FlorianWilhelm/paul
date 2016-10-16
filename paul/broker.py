# -*- coding: utf-8 -*-
"""
Broker making trade decisions
"""

import logging
from operator import itemgetter

import numpy as np
import numpy.ma as ma
from scipy import integrate

from .utils import find_next_extremum

__author__ = "Florian Wilhelm"
__copyright__ = "Florian Wilhelm"
__license__ = "gpl3"

_logger = logging.getLogger(__name__)


def sort_chunks(chunks):
    chunks.sort(endwith=False)
    chunks[:] = np.flipud(chunks)


class SimpleBroker(object):
    def __init__(self, funds, min_bet, horizon, dt, discount, risk=0.25,
                 max_stake=None, ask_fee=0., bid_fee=0., max_loss=0.1):
        self._dt = dt  # time step in seconds
        self.horizon = horizon  # maximal lookahead for decision in seconds
        self._min_bet = min_bet  # minimal chunk of money to invest, e.g. 10€
        self.ask_fee = ask_fee  # as percentage
        self.bid_fee = bid_fee  # as percentage
        self.funds = funds  # total available money
        self.max_stake = max_stake if max_stake is not None else funds
        self._leftover = funds  # money that can still be spent
        self.discount = discount  # discount rate
        self.horizon = horizon  # in seconds
        self._dt = dt  # time step between two actions in seconds
        self.max_loss = max_loss  # maximum loss as percentage
        self.risk = risk  # risk to lose max_loss in horizon
        n_chunks = self.max_stake // min_bet
        self._chunks = ma.array(np.empty(size=n_chunks), mask=[True]*n_chunks)

    @property
    def chunks(self):
        return self._chunks

    @property
    def risk(self):
        return self._risk

    @risk.setter
    def risk(self, risk):
        if not 0 <= risk <= 1:
            raise RuntimeError("risk should be in [0, 1]")
        self._risk = risk

    @property
    def max_loss(self):
        return self._max_loss

    @max_loss.setter
    def max_loss(self, loss):
        if not 0 <= loss <= 1:
            raise RuntimeError("max_loss should be in [0, 1]")
        self._max_loss = loss

    @property
    def leftover(self):
        return self._leftover

    @property
    def min_bet(self):
        return self._min_bet

    @property
    def max_delta(self):
        """Maximal number of time steps dt

        Returns:
            int: number of time steps
        """
        return int(self._horizon // self._dt)

    @property
    def dt(self):
        return self._dt

    @property
    def horizon(self):
        return self._horizon

    @horizon.setter
    def horizon(self, horizon):
        if horizon % self._dt > 1e-6:
            raise RuntimeError("horizon should be divisible by timestep dt")
        self._horizon = horizon

    @property
    def max_stake(self):
        return self._max_stake

    @max_stake.setter
    def max_stake(self, stake):
        if stake % self._min_bet > 1e-6:
            raise RuntimeError("stake should be divisible by min_bet")
        self._max_stake = stake

    @property
    def ask_fee(self):
        return self.ask_fee

    @ask_fee.setter
    def ask_fee(self, fee):
        if not 0 <= fee <= 1:
            raise RuntimeError("fee should be in [0, 1]")
        self._ask_fee = fee

    @property
    def bid_fee(self):
        return self._bid_fee

    @bid_fee.setter
    def bid_fee(self, fee):
        if not 0 <= fee <= 1:
            raise RuntimeError("fee should be in [0, 1]")
        self._bid_fee = fee

    @property
    def at_stake(self):
        return ma.count(self.chunks) * self.min_bet

    @property
    def max_ask(self):
        return min(ma.count_masked(self.chunks), self.leftover // self.min_bet)

    @property
    def max_bid(self):
        return ma.count(self.chunks)

    def curr_depot_value(self, price):
        price *= 1 - self.bid_fee
        return self.leftover + price * np.sum(self.chunks)

    def chunk_value(self, price):
        """Calculates the value of a chunk in the crypto currency"""
        price *= 1 + self.ask_fee
        return self.min_bet / price

    def total_chunks_value(self, price, chunks):
        return price * (1 - self.bid_fee) * np.sum(chunks)

    def _get_ret_val(self, leftover, chunks, inplace):
        if inplace:
            self._leftover = leftover
            self._chunks = chunks
            return None
        else:
            return leftover, chunks

    def ask_chunks(self, count, price, inplace=False):
        assert 0 < count <= self.max_ask
        leftover = self.leftover - count * self.min_bet
        chunks = self.chunks if inplace else self.chunks.copy()
        for _ in range(count):
            idx = np.where(ma.getmask(chunks))[0][0]
            chunks[idx] = self.chunk_value(price)
            chunks.mask[idx] = False
        sort_chunks(chunks)
        return self._get_ret_val(leftover, chunks, inplace)

    def bid_chunks(self, count, price, inplace=False):
        assert 0 < count <= self.max_bid
        leftover = (self.leftover +
                    np.sum(self.chunks[:count]) * (1 - self.bid_fee) * price)
        chunks = self.chunks if inplace else self.chunks.copy()
        for idx in range(count):
            chunks.mask[idx] = True
        sort_chunks(chunks)
        return self._get_ret_val(leftover, chunks, inplace)

    def get_chunks(self, count, price, inplace=False):
        if count > 0:
            return self.ask_chunks(count, price, inplace)
        elif count < 0:
            return self.bid_chunks(-count, price, inplace)
        else:
            return self._get_ret_val(self.leftover, self.chunks, inplace)

    def value_at_delta(self, delta, pdf, curr_price, count):
        def cost(price):
            leftover, chunks = self.get_chunks(count, curr_price)
            fut_value = leftover + self.total_chunks_value(price, chunks)
            return pdf(price) * self.discount**delta * fut_value

        return integrate.quad(cost, 0., np.infinity)

    def loss_risk(self, loss, price, count, cdf):
        chunks = self.get_chunks(count, price)
        curr_chunks_value = self.total_chunks_value(price, chunks)
        min_chunks_value = curr_chunks_value - loss
        min_price = min_chunks_value / (np.sum(chunks) * (1 - self.bid_fee))
        return cdf(min_price)

    def make_order(self, price, dists):
        assert len(dists) == self.max_delta + 1
        diffs = [dist.mean() - price for dist in dists]
        delta, extremum = find_next_extremum(diffs)
        _logger.info("Expected price difference {} at delta {}".format(
            extremum, delta + 1))
        dist = dists[delta]
        max_loss = self.max_loss * self.max_stake
        fut_at_risk = [(count, self.value_at_delta(delta + 1, dist.pdf,
                                                   price, count))
                       for count in range(-self.max_bid, self.max_ask+1)]
        opts = [(count, value) for count, value in fut_at_risk if
                self.loss_risk(max_loss, price, count, dist.cdf) < self.risk]
        order_count, value = max(opts, key=itemgetter(1))
        _logger.info("Expected depot gain of {} with {} chunks".format(
            value - self.curr_depot_value(price), order_count))
        return order_count
