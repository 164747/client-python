from __future__ import annotations

import datetime
import functools
import typing
from typing import List

import pandas as pd
import requests
from pydantic import BaseModel, Field, PrivateAttr, root_validator
import logging
from polygon import RESTClient

logger = logging.getLogger(__name__)

# _T = typing.TypeVar('_T')
_T = typing.ClassVar


class PolygonModel(BaseModel):
    class Meta:
        client: RESTClient = None


    @classmethod
    def api_action_raw(cls: _T, path: str, params: dict = None) -> typing.Union[dict, list, None]:
        c = PolygonModel.Meta.client
        assert c is not None
        url = f"{c.url}{path}"
        logger.info(f'{url}')
        r = requests.Response = c.session.get(url, params=params)
        logger.info(f'{r.url} --> {r.status_code}')
        if r.status_code == 200:
            return r.json()
        else:
            r.raise_for_status()

    @classmethod
    def api_action(cls: _T, path: str, params: dict = None) -> typing.Union[_T, typing.List[_T]]:
        d = cls.api_action_raw(path, params)
        assert d is not None
        if isinstance(d, list):
            # noinspection PyArgumentList
            return [cls(**el) for el in d]
        else:
            # noinspection PyArgumentList
            return cls(**d)

    @classmethod
    def get(cls: _T, *args, **kwargs) -> typing.Union[_T, typing.List[_T]]:
        raise NotImplementedError


StockSymbol = str


class Ticker(PolygonModel):
    symbol: str = Field(alias='ticker')
    name: str
    market: str
    locale: str
    currency: str
    active: bool
    primary_exchange: str = Field(alias='primaryExch')
    type_: str = Field(alias='type', default=None)
    codes: typing.Dict[str, str] = None
    updated: typing.Union[datetime.datetime, datetime.date]
    url: str
    attrs: typing.Dict[str, str] = None

    @classmethod
    def get(cls: _T, *args, **kwargs) -> typing.Union[_T, typing.List[_T]]:
        raise NotImplementedError


class TickerList(PolygonModel):
    page: int
    per_page: int = Field(alias='perPage')
    count: int
    status: str
    tickers: typing.List[Ticker]

    @classmethod
    @functools.lru_cache()
    def get(cls, market: str, search: str = None, active: str = 'true') -> TickerList:
        params = locals()
        params.pop('cls')
        return TickerList.api_action(f"/v2/reference/tickers", params=params)


class TickerDetail(PolygonModel):
    logo: str
    exchange: str
    name: str
    symbol: StockSymbol
    listdate: str
    cik: str
    bloomberg: str
    figi: str = None
    lei: str = None
    sic: float
    country: str
    industry: str
    sector: str
    marketcap: typing.Optional[float] = None
    employees: typing.Optional[float] = None
    phone: str
    ceo: str
    url: str
    description: str = None
    similar: List[StockSymbol]
    tags: List[str]
    updated: str

    @classmethod
    @functools.lru_cache()
    def get(cls, symbol: str, **kwargs) -> TickerDetail:
        return TickerDetail.api_action(f"/v1/meta/symbols/{symbol}/company")


class Bar(BaseModel):
    volume: int = Field(alias='v')
    open: float = Field(alias='o')
    close: float = Field(alias='c')
    high: float = Field(alias='h')
    low: float = Field(alias='l')
    average: typing.Optional[float] = Field(alias='vw', default=None)
    utc_window_start: datetime.datetime = Field(alias='t')
    trades: int = Field(alias='n', default=0)


class TickerWindow(PolygonModel):
    symbol: StockSymbol = Field(alias='ticker')
    status: str
    adjusted: bool
    query_count: int = Field(alias='queryCount')
    results: typing.List[Bar]

    class Meta:
        data_frames: typing.Dict[int, pd.DataFrame] = {}

    @classmethod
    def get(cls: TickerWindow, symbol: StockSymbol, timespan: str, from_: str, to: str, multiplier: int = 1,
            unadjusted: bool = False, sort: str = 'asc') -> TickerWindow:
        return cls.api_action(f"/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_}/{to}",
                              params=dict(sort=sort, unadjusted=unadjusted))

    def consume(self, other: TickerWindow):
        d_orig = {bar.utc_window_start: bar for bar in self.results}
        d_new = {bar.utc_window_start: bar for bar in other.results}
        d_orig.update(d_new)
        self.results = list(d_orig.values())
        self.results.sort(key=lambda x: x.utc_window_start)
        self.query_count = len(self.results)
        self.__set_df()

    def __set_df(self, df: pd.DataFrame = None):
        self.Meta.data_frames[id(self)] = df

    @property
    def df(self) -> pd.DataFrame:
        df = self.Meta.data_frames.get(id(self), None)
        if df is None:
            df = pd.DataFrame.from_dict(self.dict()['results'])
            df = df.set_index('utc_window_start').sort_index()
            self.__set_df(df)
        return df

    def add_bar(self, bar: Bar):
        if len(self.results) == 0 or bar.utc_window_start > self.results[-1].utc_window_start:
            logger.info(f'Adding BAR {bar} to {self.symbol}')
            self.results.append(bar)
            self.__set_df()
        elif bar.utc_window_start == self.results[-1].utc_window_start:
            self.results[-1] = bar
        else:
            raise NotImplementedError


class TickerWindowFetcher(BaseModel):
    max_date: datetime.date = Field(default_factory=datetime.date.today)
    min_date: typing.Optional[datetime.date] = Field(default=None)
    days_back: typing.Optional[int] = None
    timespan: str = 'minute'
    symbol: StockSymbol
    adjusted: bool = True

    @root_validator
    def check_window_start(cls, values):
        k1, k2 = 'min_date', 'days_back'
        md, db = values.get(k1), values.get(k2)
        if (md is None) is (db is None):
            raise ValueError(f'precisely on of {k1} and {k2} must be set!')
        return values

    def get_ticker_window(self, new_start_date: bool = False) -> TickerWindow:
        if new_start_date:
            self.max_date = datetime.date.today()
        max_date = self.max_date
        min_date = self.min_date if self.min_date else max_date - datetime.timedelta(days=self.days_back)
        res = None
        tmp = max_date
        while min_date < tmp:
            tmp = max(min_date, max_date - datetime.timedelta(days=5))
            tw = TickerWindow.get(self.symbol, self.timespan, tmp.isoformat(), max_date.isoformat(),
                                  unadjusted=(not self.adjusted))
            max_date = tmp
            if res is None:
                res = tw
            else:
                res.consume(tw)
        assert res is not None
        return res


class TradeItem(BaseModel):
    original_id: typing.Optional[int] = Field(alias='I', default=None)
    exchange_id: typing.Optional[int] = Field(alias='x', default=None)
    price: float = Field(alias='p')
    correction_indicator: typing.Optional[int] = Field(alias='e', default=None)
    reporting_id: typing.Optional[int] = Field(alias='r', default=None)
    trade_time: datetime.datetime = Field(alias='t')
    quote_time: typing.Optional[datetime.datetime] = Field(alias='y', default=None)
    report_time: typing.Optional[datetime.datetime] = Field(alias='f', default=None)
    sequence_no: typing.Optional[int] = Field(alias='q')
    trade_conditions: typing.Optional[typing.List[int]] = Field(alias='c', default=None)
    size: int = Field(alias='s')


class Trade(PolygonModel):
    symbol: str = Field(alias='ticker')
    results_count: int
    db_latency: int
    success: bool
    results: typing.List[TradeItem]
    _df: typing.Optional[pd.DataFrame] = PrivateAttr(default=None)

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            df = pd.DataFrame.from_dict(self.dict()['results'])
            # df = df.set_index('trade_time').sort_index()
            self._df = df
        return self._df

    @classmethod
    def __get(cls: Trade, symbol: str, date: str, timestamp_min: int = None, timestamp_max: int = None,
              limit: int = 50000, reverse: bool = False) -> typing.Tuple[Trade, typing.Union[int, None]]:
        d= cls.api_action_raw(f'/v2/ticks/stocks/trades/{symbol}/{date}',
                              params=dict(timestamp=timestamp_min, timestampLimit=timestamp_max, limit=limit,
                                          reverse=reverse))
        trade = Trade(**d)

        last = None if len(trade.results) == 0 else d['results'][-1]['t']
        return trade, last

    @property
    def size(self) -> int:
        return len(self.results)

    def consume(self, other: Trade):
        #assert self.results[-1].trade_time > other.results[0].trade_time, (self.results[-1].trade_time, other.results[0].trade_time)
        assert self.results[-1].trade_time == other.results[0].trade_time, (self.results[-1].trade_time, other.results[0].trade_time)
        self.results_count += (other.results_count-1)
        self.results.extend(other.results[1:])
        self._df = None

    @staticmethod
    def __n(dt: typing.Union[datetime.datetime, None]) -> typing.Union[int, None]:
        if dt is None:
            return None
        else:
            return int(dt.timestamp() * 1e6)

    @classmethod
    def get(cls: Trade, symbol: str, date: str, timestamp_min: datetime.datetime = None,
            timestamp_max: datetime.datetime = None, limit: int = 50000, reverse=False) -> Trade:
        use_limit = min(limit, 50000)
        if reverse:
            assert use_limit == limit
        t_min = cls.__n(timestamp_min)
        t_max = cls.__n(timestamp_max)
        tmp_trade,last = Trade.__get(symbol, date, t_min, t_max, use_limit, reverse=reverse)
        trade = tmp_trade
        while isinstance(last, int) and tmp_trade.size == use_limit and use_limit < limit:
            tmp_trade, last = Trade.__get(symbol, date, last, t_max, use_limit, reverse=reverse)
            if len(tmp_trade.results) > 0:
                trade.consume(tmp_trade)
        return trade
