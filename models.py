from pydantic import BaseModel
from typing import List

class AllStatResp(BaseModel):
    deposit_user_cnt: int
    deposit_order_cnt: int
    deposit_dollar_sum: float
    withdraw_user_cnt: int
    withdraw_order_cnt: int
    withdraw_dollar_sum: float
    net_dollar_sum: float
    total_user_cnt: int
    total_order_cnt: int
    total_dollar_sum: float


class FirstDepositMonthlyStat(BaseModel):
    date: str
    date1: str
    num: int
    real_num: int
    rate: str
    tjy_num: int
    tyr_rate: str


class FirstDepositStatResp(BaseModel):
    year: int
    list: List[FirstDepositMonthlyStat]
    year_num: int
    year_real_num: int
    year_tjynum: int
    year_rate: str
    year_tjyrate: str
