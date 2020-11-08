from __future__ import annotations
import keyword
import typing
from typing import List, Dict, Any

from polygon import RESTClient
from polygon.rest import models
from pydantic import BaseModel
from pydantic import BaseModel, Field
import requests

_T = typing.TypeVar('_T')


class PolygonModel(BaseModel):
    class Meta:
        client: RESTClient = None

    @classmethod
    def _get(cls: PolygonModel, path: str, **kwargs) -> PolygonModel:
        c = PolygonModel.Meta.client
        assert c is not None
        r = requests.Response = c._session.get(f"{c.url}{path}", **kwargs)
        if r.status_code == 200:
            d : dict = r.json()
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
    def get(cls, symbol: str, **kwargs) -> TickerDetail:
        return TickerDetail._get(f"/v1/meta/symbols/{symbol}/company")
