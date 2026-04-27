from datetime import datetime
from typing import Optional

from pydantic import BaseModel, field_validator

class CoinCap(BaseModel):
    id: str
    rank: Optional[int]
    symbol: str
    name: str
    priceUsd: float
    lastIngestedAt: datetime
    
    @field_validator("priceUsd", pre=True)
    def parse_price(cls, v):
        if v in (None, "", "null"):
            return None
        try:
            return float(v)
        except ValueError:
            raise ValueError(f"Invalid priceUsd value: {v}")
    @field_validator("rank", pre=True)
    def parse_rank(cls, v):
        if v in (None, "", "null"):
            return None
        try:
            return int(v)
        except ValueError:
            raise ValueError(f"Invalid rank value: {v}")