from __future__ import annotations
import keyword
import typing
import datetime
from typing import List, Dict, Any

from polygon import RESTClient
from polygon.rest import models
from pydantic import BaseModel
from pydantic import BaseModel, Field
import requests
import functools
import pandas as pd
from dateutil.parser import parser

_T = typing.TypeVar('_T')


class PolygonModel(BaseModel):
    class Meta:
        client: RESTClient = None

    @classmethod
    def _get(cls: PolygonModel, path: str, params: dict = None) -> PolygonModel:
        c = PolygonModel.Meta.client
        assert c is not None
        r = requests.Response = c._session.get(f"{c.url}{path}", params=params)
        if r.status_code == 200:
            d: dict = r.json()
            # noinspection PyArgumentList
            return cls(**d)
        else:
            r.raise_for_status()

    @classmethod
    def get(cls: PolygonModel, *args, **kwargs) -> PolygonModel:
        raise NotImplementedError


StockSymbol = str


class TickerDetail(PolygonModel):
    logo: str
    exchange: str
    name: str
    symbol: StockSymbol
    listdate: str
    cik: str
    bloomberg: str
    figi: str = None
    lei: str
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
    trades: int = Field(alias='n')


class TickerWindow(PolygonModel):
    ticker: StockSymbol
    status: str
    adjusted: bool
    query_count: int = Field('queryCount')
    results: typing.List[Bar]

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
    def get(cls: TickerWindow, ticker: StockSymbol, timespan: str, from_: str, to: str, multiplier: int = 1,
            unadjusted: bool = False, sort: str = 'asc') -> TickerWindow:
        # noinspection PyTypeChecker
        ws = TickerWindow.__WindowSplitter(**dict(from_=from_, to=to, timespan=timespan))
        res = None
        for (from_, to) in ws.split_list:
            tmp = cls._get(f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_}/{to}",
                           params=dict(sort=sort, unadjusted=unadjusted))
            if isinstance(res, TickerWindow):
                res.append(tmp)
            else:
                res = tmp
        return res

    def append(self, other: TickerWindow):
        self.query_count += other.query_count
        self.results.extend(other.results)

    @property
    def df(self) -> pd.DataFrame:
        df = pd.DataFrame.from_dict(self.dict()['results'])
        return df.set_index('utc_window_start').sort_index()
