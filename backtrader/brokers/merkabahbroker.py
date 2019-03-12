from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from backtrader import BrokerBase, Order
from backtrader.utils.py3 import queue
from backtrader.stores.merkabahstore import MerkabahStore, MerkabahOrder


class MerkabahBroker(BrokerBase):
    ''' Broker implementation for Merkabah trading system.

    This class maps the orders/positions from Merkabah
    to the internal API of ```backtrader```.
    '''

    order_type = {Order.Market: 'market',
                  Order.Limit: 'limit',
                  Order.Stop: 'stop',
                  Order.StopLimit: 'stop limit'}

    def __init__(self, exchange, currency, retries=5):
        super(MerkabahBroker, self).__init__()

        self.store = MerkabahStore(self.exchange, self.retries)
        self.currency = currency
        self.notifs = queue.Queue()  # holds orders which are notified

    def getcash(self):
        return self.store.getcash(self.currency)

    def getvalue(self, datas=None):
        return self.store.getvalue(self.currency)

    def get_notification(self):
        try:
            return self.notifs.get(False)
        except queue.Empty:
            return None

    def notify(self, order):
        self.notifs.put(order)

    def getposition(self, data):
        currency = data.symbol.split('/')[0]
        return self.store.getposition(currency)

    def get_value(self, datas=None, mkt=False, lever=False):
        return self.store.getvalue(self.currency)

    def get_cash(self):
        return self.store.getcash(self.currency)

    def _submit(self, owner, data, exectype, side, amount, price, params):
        order_type = self.order_type.get(exectype)
        _order = self.store.create_order(
            symbol=data.symbol,
            order_type=order_type,
            side=side,
            amount=amount,
            price=price,
            params=params
        )
        order = MerkabahOrder(owner, data, amount, _order)
        self.notify(order)
        return order

    def buy(self, owner, data, size, price=None, plimit=None,
            exectype=None, valid=None, tradeid=0, oco=None,
            trailamount=None, trailpercent=None, **kwargs):
        return self._submit(owner, data, exectype, 'buy', size, price, kwargs)

    def sell(self, owner, data, size, price=None, plimit=None,
             exectype=None, valid=None, tradeid=0, oco=None,
             trailamount=None, trailpercent=None, **kwargs):
        return self._submit(owner, data, exectype, 'sell', size, price, kwargs)

    def cancel(self, order):
        return self.store.cancel_order(order)
     
    def get_orders_open(self, safe=False):
        return self.store.fetch_open_orders()