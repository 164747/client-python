import time
import typing
import datetime
from decimal import Decimal
from enum import Enum

import pytz
from polygon.rest.models import pm
from pydantic import BaseModel, Field


class SocketBase(BaseModel):
    symbol: str = Field(alias='sym')
    event_type: str = Field(alias='ev')


class Trade(SocketBase):
    exchange_id: int = Field(alias='x')
    trade_id: int = Field(alias='i')
    tape: int = Field(alias='z')
    price: float = Field(alias='p')
    size: int = Field(alias='s')
    trade_conditions: typing.List[int] = Field(alias='c', default=None)
    utc: datetime.datetime = Field(alias='t')

    @property
    def age(self) -> datetime.timedelta:
        return datetime.datetime.now(pytz.utc) - self.utc


class Quote(SocketBase):
    bid_exchange_id: int = Field(alias='bx')
    bid_price: float = Field(alias='bp')
    bid_size: int = Field(alias='bs')
    ask_exchange_id: int = Field(alias='ax')
    ask_price: float = Field(alias='ap')
    ask_size: int = Field(alias='as')
    quote_conditions: int = Field(alias='c')
    utc: datetime.datetime = Field(alias='t')

    def __str__(self):
        return f'{self.bid_size}:{self.bid_price} -- {self.ask_size}:{self.ask_price}'

    @property
    def age(self) -> datetime.timedelta:
        return datetime.datetime.now(pytz.utc) - self.utc

    @property
    def payback(self) -> float:
        return (self.bid_price / self.ask_price) ** .5

    @property
    def middle_price(self) -> float:
        return (self.ask_price + self.bid_price) / 2.0


class Bar(SocketBase):
    volume: int = Field(alias='v')
    volume_today: int = Field(alias='av')
    official_open_price: float = Field(alias='op')
    vol_weight_price: float = Field(alias='vw')
    open_price: float = Field(alias='o')
    close_price: float = Field(alias='c')
    high_price: float = Field(alias='h')
    low_price: float = Field(alias='l')
    avg_prive: float = Field(alias='a')
    utc_start: datetime.datetime = Field(alias='s')
    utc_end: datetime.datetime = Field(alias='e')

    @property
    def rest_bar(self) -> pm.Bar:
        return pm.Bar(v=self.volume, o=self.open_price, c=self.close_price, h=self.high_price, l=self.low_price,
                      t=self.utc_start, n=1)


class OrderEventBase(Enum):

    @property
    def is_active(self) -> bool:
        raise NotImplementedError


class OrderEventActive(OrderEventBase):
    NEW = 'new'
    PARTIAL_FILL = 'partial_fill'
    DONE_FOR_DAY = 'done_for_day'
    PENDING_NEW = 'pending_new'
    REPLACE_REJECTED = 'order_replace_rejected'
    CANCEL_REJECTED = 'order_cancel_rejected'
    PENDING_CANCEL = 'pending_cancel'
    PENDING_REPLACE = 'pending_replace'

    @property
    def is_active(self) -> bool:
        return True


class OrderEventInActive(OrderEventBase):
    FILL = 'fill'
    CANCELED = 'canceled'
    EXPIRED = 'expired'
    REPLACED = 'replaced'
    REJECTED = 'rejected'
    STOPPED = 'stopped'
    CALCULATED = 'calculated'
    SUSPENDED = 'suspended'

    @property
    def is_active(self) -> bool:
        return False


class TradeBase(BaseModel):
    event: typing.Union[OrderEventActive, OrderEventInActive]
    price: typing.Optional[Decimal] = None
    utc: typing.Optional[datetime.datetime] = Field(alias='timestamp', default=None)
    position_qty: typing.Optional[int] = None
    order: pm.Order

    @property
    def has_qty(self) -> bool:
        return self.position_qty is not None
