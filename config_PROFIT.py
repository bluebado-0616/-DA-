import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine

# 看板十（平仓盈亏分析）专用库连接，与 config.py 隔离。
DB_TRADE = dict(
    host='43.198.60.37',
    port=3306,
    user='goldaydata',
    password=os.environ["DB_PASSWORD"],
    db='traderecord',
    charset='utf8mb4'
)

# 平仓盈亏看板用的是 js_mt5_deals_view
DB_TRADE_PROFIT = {
    **DB_TRADE,
    "db": "mt4_alldata",
}


def create_engine_from_cfg(cfg: dict):
    encoded_pw = quote_plus(cfg['password'])
    url = f"mysql+pymysql://{cfg['user']}:{encoded_pw}@{cfg['host']}:{cfg['port']}/{cfg['db']}?charset={cfg['charset']}"
    return create_engine(
        url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_timeout=30,
        pool_use_lifo=True,
        pool_reset_on_return="rollback",
        connect_args={
            "connect_timeout": 10,
            "read_timeout": 180,
            "write_timeout": 180,
            "charset": cfg.get("charset", "utf8mb4"),
        },
    )


engine_profit = create_engine_from_cfg(DB_TRADE_PROFIT)
