from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import time
from functools import wraps

import requests
import urllib.parse

import backtrader as bt
from backtrader import OrderBase

api_url = "http://merkabah_server:3000/api/"
api_actions = {
    "balance": "balance/{0}/",
    "exchange_info": "market/exchanges/{0}/",
    "create_order": "trading/{0}/create_order/",
    "cancel_order": "trading/{0}/cancel_order/{1}/",
    "fetch_trades": "trading/{0}/fetch_trades/{1}/",
    "fetch_open_orders": "trading/{0}/fetch_open_orders/",
    "fetch_order": "trading/{0}/fetch_order/{1}",
    "fetch_orders": "trading/{0}/fetch_orders/{1}",
    "fetch_ohlcv": "ohlcv/{0}/{1}/{2}/{3}"
}


class MerkabahOrder(OrderBase):
    def __init__(self, owner, data, size, merkabah_order):
        self.owner = owner
        self.data = data
        self.merkabah_order = merkabah_order
        if merkabah_order['side'] == 'buy':
            self.ordtype = self.Buy
        else:
            self.ordtype = self.Sell
        amount = merkabah_order.get('amount')
        if amount:
            self.size = float(amount)
        else:
            self.size = size

        super(MerkabahOrder, self).__init__()


class MerkabahStore(object):
    '''API provider for Merkabah feed and broker classes.'''

    # Supported granularities
    _GRANULARITIES = {
        (bt.TimeFrame.Minutes, 1): '1m',
        (bt.TimeFrame.Minutes, 3): '3m',
        (bt.TimeFrame.Minutes, 5): '5m',
        (bt.TimeFrame.Minutes, 15): '15m',
        (bt.TimeFrame.Minutes, 30): '30m',
        (bt.TimeFrame.Minutes, 60): '1h',
        (bt.TimeFrame.Minutes, 90): '90m',
        (bt.TimeFrame.Minutes, 120): '2h',
        (bt.TimeFrame.Minutes, 240): '4h',
        (bt.TimeFrame.Minutes, 360): '6h',
        (bt.TimeFrame.Minutes, 480): '8h',
        (bt.TimeFrame.Minutes, 720): '12h',
        (bt.TimeFrame.Days, 1): '1d',
        (bt.TimeFrame.Days, 3): '3d',
        (bt.TimeFrame.Weeks, 1): '1w',
        (bt.TimeFrame.Weeks, 2): '2w',
        (bt.TimeFrame.Months, 1): '1M',
        (bt.TimeFrame.Months, 3): '3M',
        (bt.TimeFrame.Months, 6): '6M',
        (bt.TimeFrame.Years, 1): '1y',
    }

    def __init__(self, exchange, retries):
        # elf.exchange = getattr(ccxt, exchange)(config)
        self.exchangeName = exchange
        self.exchange = self.api_get('exchange_info')
        self.retries = retries

    def api_get(action, path_params=[]):
        if not api_actions[action]:
            raise "API Action is not defined !"
        r = requests.get(url=urllib.parse.urljoin(
            api_url,
            api_actions[action].format(self.exchangeName, *path_params))
        )
        # extract data in json format
        return r.json()

    def api_post(action, json_data, path_params=[]):
        if not api_actions[action]:
            raise "API Action is not defined !"
        r = requests.post(url=urllib.parse.urljoin(
            api_url,
            api_actions[action].format(self.exchangeName, *path_params)
        ), json=json_data)
        # extract data in json format
        return r.json()

    def get_granularity(self, timeframe, compression):
        # market data that should check if hasFetchOHLCV
        canFetch = self.exchange.has['fetchOHLCV']
        if not canFetch:
            raise NotImplementedError(
                "'%s' exchange doesn't support fetching OHLCV data" %
                self.exchange.name
            )

        granularity = self._GRANULARITIES.get((timeframe, compression))
        if granularity is None:
            raise ValueError(
                "backtrader CCXT module doesn't support fetching OHLCV "
                "data for time frame %s, comression %s" %
                (bt.TimeFrame.getname(timeframe), compression)
            )

        if self.exchange.timeframes:
            if granularity not in self.exchange.timeframes:
                raise ValueError(
                    "'%s' exchange doesn't support fetching OHLCV data for "
                    "%s time frame" % (self.exchange.name, granularity)
                )

        return granularity

    def retry(method):
        @wraps(method)
        def retry_method(self, *args, **kwargs):
            for i in range(self.retries):
                time.sleep(self.exchange.rateLimit / 1000)
                try:
                    return method(self, *args, **kwargs)
                except (NetworkError, ExchangeError):
                    if i == self.retries - 1:
                        raise

        return retry_method

    @retry
    def getbalance(self):
        return self.api_get('balance')

    @retry
    def getcash(self, currency):
        return self.getbalance()['free'].get(currency, 0.0)

    @retry
    def getvalue(self, currency):
        return self.getbalance['total'].get(currency, 0.0)

    @retry
    def getposition(self, currency):
        return self.getvalue(currency)

    @retry
    def create_order(self, symbol, order_type, side, amount, price, params):
        return self.api_post('create_order', {
            'symbol': symbol,
            'type': order_type,
            'side': side,
            'amount': amount,
            'price': price,
            'params': params
        }, [])

    @retry
    def cancel_order(self, order):
        return self.api_post('cancel_order', [order.merkabah_order['id']])

    @retry
    def fetch_trades(self, symbol):
        return self.api_get('fetch_trades', [symbol])

    @retry
    def fetch_ohlcv(self, symbol, timeframe, since):
        return self.api_get('fetch_ohlcv', [symbol, timeframe, since])

    @retry
    def fetch_open_orders(self):
        return self.api_get('fetch_open_orders')
