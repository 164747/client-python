from __future__ import annotations

import datetime
import functools
import typing
from typing import List

import pandas as pd
import requests
from pydantic import BaseModel, Field

from polygon import RESTClient

# _T = typing.TypeVar('_T')
_T = typing.ClassVar


class PolygonModel(BaseModel):
    class Meta:
        client: RESTClient = None

    @classmethod
    def _get(cls: _T, path: str, params: dict = None) -> typing.Union[_T, typing.List[_T]]:
        c = PolygonModel.Meta.client
        assert c is not None
        r = requests.Response = c._session.get(f"{c.url}{path}", params=params)
        if r.status_code == 200:
            d: typing.Union[dict, list] = r.json()
            if isinstance(d, list):
                # noinspection PyArgumentList
                return [cls(**el) for el in d]
            else:
                # noinspection PyArgumentList
                return cls(**d)

        else:
            r.raise_for_status()

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
        return TickerList._get(f"/v2/reference/tickers", params=params)


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
    marketcap: float
    employees: float
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
        return TickerDetail._get(f"/v1/meta/symbols/{symbol}/company")


class Bar(BaseModel):
    volume: int = Field(alias='v')
    open: float = Field(alias='o')
    close: float = Field(alias='c')
    high: float = Field(alias='h')
    low: float = Field(alias='l')
    utc_window_start: datetime.datetime = Field(alias='t')
    trades: int = Field(alias='n', default=0)


class TickerWindow(PolygonModel):
    symbol: StockSymbol = Field(alias='ticker')
    status: str
    adjusted: bool
    query_count: int = Field('queryCount')
    results: typing.List[Bar]
    _df : pd.DataFrame = Field(default_factory=pd.DataFrame)

    class __WindowSplitter(BaseModel):
        from_: datetime.date
        to: datetime.date
        timespan: str

        @property
        def split_list(self) -> typing.List[typing.Tuple[str, str]]:
            res = []
            from_ = self.from_
            while from_ <= self.to:
                to = min(self.to, from_ + datetime.timedelta(days=5))
                res.append((from_.isoformat(), to.isoformat()))
                from_ = to + datetime.timedelta(days=1)
            return res

    @classmethod
    def get(cls: TickerWindow, symbol: StockSymbol, timespan: str, from_: str, to: str, multiplier: int = 1,
            unadjusted: bool = False, sort: str = 'asc') -> TickerWindow:
        # noinspection PyTypeChecker
        ws = TickerWindow.__WindowSplitter(**dict(from_=from_, to=to, timespan=timespan))
        res = None
        for (from_, to) in ws.split_list:
            tmp = cls._get(f"/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_}/{to}",
                           params=dict(sort=sort, unadjusted=unadjusted))
            if isinstance(res, TickerWindow):
                res.consume(tmp)
            else:
                res = tmp
        return res

    def consume(self, other: TickerWindow):
        d_orig = {bar.utc_window_start: bar for bar in self.results}
        d_new = {bar.utc_window_start: bar for bar in other.results}
        d_orig.update(d_new)
        self.results = list(d_orig.values())
        self.results.sort(key=lambda x: x.utc_window_start)
        self.query_count = len(self.results)
        _df : pd.DataFrame = Field(default_factory=pd.DataFrame)

    @property
    def df(self) -> pd.DataFrame:
        if len(self._df) == 0:
            df = pd.DataFrame.from_dict(self.dict()['results'])
            self._df = df.set_index('utc_window_start').sort_index()
        return self._df

    def add_bar(self, bar : Bar):
        if len(self.results) == 0 or bar.utc_window_start > self.results[-1].utc_window_start:
            self.results.append(bar)
            self._df = pd.DataFrame()
        elif bar.utc_window_start == self.results[-1].utc_window_start:
            self.results[-1] = bar
        else:
            raise NotImplementedError

class TickerWindowFetcher(BaseModel):
    max_date: datetime.date = Field(default_factory=datetime.date.today)
    days_back: int = 5
    timespan: str = 'minute'
    symbol: StockSymbol
    adjusted: bool = True

    def get_ticker_window(self, new_start_date: bool = True) -> TickerWindow:
        if new_start_date:
            self.max_date = datetime.date.today()
        max_date = self.max_date
        min_date = max_date - datetime.timedelta(days=self.days_back)
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
            print(min_date, max_date, tmp)
        return res
