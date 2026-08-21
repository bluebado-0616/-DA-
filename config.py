import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from datetime import datetime

# ---------- 数据库配置 ----------
DB_FINANCE = dict(
    host='18.166.167.237',
    port=3306,
    user='goldaydata',
    password=os.environ["DB_PASSWORD"],
    db='gold',
    charset='utf8mb4'
)

DB_TRADE = dict(
    host='43.198.60.37',
    port=3306,
    user='goldaydata',
    password=os.environ["DB_PASSWORD"],
    db='traderecord',
    charset='utf8mb4'
)

# 2023-11-01 之前的交易数据使用 mt4_alldata 库中的视图 v_mt4_trades_filtered
DB_TRADE_MT4 = {
    **DB_TRADE,
    "db": "mt4_alldata",
}

def create_engine_from_cfg(cfg: dict):
    encoded_pw = quote_plus(cfg['password'])
    url = f"mysql+pymysql://{cfg['user']}:{encoded_pw}@{cfg['host']}:{cfg['port']}/{cfg['db']}?charset={cfg['charset']}"
    return create_engine(
        url,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_timeout=30,
        pool_use_lifo=True,
        pool_reset_on_return="rollback",
        connect_args={
            "connect_timeout": 10,
            "read_timeout": 120,
            "write_timeout": 120,
            "charset": cfg.get("charset", "utf8mb4"),
        },
    )

engine_finance = create_engine_from_cfg(DB_FINANCE)
engine_trade = create_engine_from_cfg(DB_TRADE)
engine_trade_mt4 = create_engine_from_cfg(DB_TRADE_MT4)

# ---------- 业务常量 ----------
BOUNDARY_DATETIME = datetime(2023, 11, 1, 0, 0, 0)
DAILY_STAT_PAGE_SIZE = 50
TRADING_DISTRIBUTION_MAX_CONCURRENCY = 1
TRADING_DISTRIBUTION_CACHE_TTL_SECONDS = 6 * 60 * 60
