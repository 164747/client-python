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

_T = typing.TypeVar('_T')


class PolygonModel(BaseModel):
    class Meta:
        client: RESTClient = None

    @classmethod
    def _get(cls: PolygonModel, path: str, params:dict=None) -> PolygonModel:
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

    @classmethod
    def get(cls: TickerWindow, ticker: StockSymbol, timespan: str, from_: str, to: str, multiplier: int = 1,
            unadjusted: bool = False, sort: str = 'asc') -> TickerWindow:
        return TickerWindow._get(f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_}/{to}",
                                 params=dict(sort=sort, unadjusted=unadjusted))

    @property
    def df(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict(self.dict()['results'])
