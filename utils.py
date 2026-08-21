import os
import json
import hashlib
import pickle
import threading
import pandas as pd
from datetime import datetime
from typing import Any, Optional
from config import (
    TRADING_DISTRIBUTION_CACHE_TTL_SECONDS,
    TRADING_DISTRIBUTION_MAX_CONCURRENCY,
    DAILY_STAT_PAGE_SIZE
)

# ====================== 并发保护 Semaphore ======================
# 接口四 和 接口五（交易分布类）共用
trading_distribution_semaphore = threading.BoundedSemaphore(TRADING_DISTRIBUTION_MAX_CONCURRENCY)

# 接口二：日交易人数
daily_stat_semaphore = threading.BoundedSemaphore(3)

# 接口三：月交易人数
year_stat_semaphore = threading.BoundedSemaphore(2)

# 接口六：用户画像
user_persona_semaphore = threading.BoundedSemaphore(1)

# 接口八：入金速度分析
deposit_speed_semaphore = threading.BoundedSemaphore(2)

# 接口九：首次接听类型分析
first_answer_semaphore = threading.BoundedSemaphore(1)

# 接口十：平仓盈亏分析
close_pnl_semaphore = threading.BoundedSemaphore(2)

# ====================== 全局锁 ======================
_cache_lock = threading.Lock()

# ====================== 缓存目录 ======================
def _cache_dir(cache_subdir: str = "trading_distribution") -> str:
    base = os.path.dirname(os.path.abspath(__file__))
    d = os.path.join(base, f".cache_{cache_subdir}")
    os.makedirs(d, exist_ok=True)
    return d

def _cache_key(prefix: str, payload: dict) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"{prefix}_{h}"

def _cache_paths(key: str, cache_subdir: str) -> tuple[str, str]:
    d = _cache_dir(cache_subdir)
    return (os.path.join(d, f"{key}.pkl"), os.path.join(d, f"{key}.json"))

# ====================== 通用缓存函数 ======================
def cache_load_df(prefix: str, payload: dict, cache_subdir: str = "trading_distribution") -> Optional[pd.DataFrame]:
    key = _cache_key(prefix, payload)
    pkl_path, meta_path = _cache_paths(key, cache_subdir)
    try:
        if not (os.path.exists(pkl_path) and os.path.exists(meta_path)):
            return None
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        created = float(meta.get("created_at", 0))
        ttl = float(meta.get("ttl_seconds", TRADING_DISTRIBUTION_CACHE_TTL_SECONDS))
        if created <= 0 or (datetime.now().timestamp() - created) > ttl:
            return None
        return pd.read_pickle(pkl_path)
    except Exception:
        return None


def cache_save_df(prefix: str, payload: dict, df: pd.DataFrame, 
                  ttl_seconds: int = TRADING_DISTRIBUTION_CACHE_TTL_SECONDS,
                  cache_subdir: str = "trading_distribution") -> None:
    key = _cache_key(prefix, payload)
    pkl_path, meta_path = _cache_paths(key, cache_subdir)
    try:
        meta = {"created_at": datetime.now().timestamp(), "ttl_seconds": int(ttl_seconds), "payload": payload}
        df.to_pickle(pkl_path)
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False)
    except Exception:
        pass


def cache_load_obj(prefix: str, payload: dict, cache_subdir: str = "first_answer") -> Optional[Any]:
    key = _cache_key(prefix, payload)
    pkl_path, meta_path = _cache_paths(key, cache_subdir)
    try:
        if not (os.path.exists(pkl_path) and os.path.exists(meta_path)):
            return None
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        created = float(meta.get("created_at", 0))
        ttl = float(meta.get("ttl_seconds", TRADING_DISTRIBUTION_CACHE_TTL_SECONDS))
        if created <= 0 or (datetime.now().timestamp() - created) > ttl:
            return None
        with open(pkl_path, "rb") as f:
            return pickle.load(f)
    except Exception:
        return None


def cache_save_obj(
    prefix: str,
    payload: dict,
    obj: Any,
    ttl_seconds: int = TRADING_DISTRIBUTION_CACHE_TTL_SECONDS,
    cache_subdir: str = "first_answer",
) -> None:
    key = _cache_key(prefix, payload)
    pkl_path, meta_path = _cache_paths(key, cache_subdir)
    try:
        meta = {"created_at": datetime.now().timestamp(), "ttl_seconds": int(ttl_seconds), "payload": payload}
        with open(pkl_path, "wb") as f:
            pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False)
    except Exception:
        pass


from datetime import datetime, timedelta, time, date

# ====================== 交易日转换逻辑 ======================
def is_summer_time(dt: datetime) -> bool:
    """判断给定时间是否处于夏令时（美国标准：3月第二个周日至11月第一个周日）"""
    year = dt.year
    # 3月第二个周日
    march_1st = datetime(year, 3, 1)
    march_2nd_sunday = march_1st + timedelta(days=(6 - march_1st.weekday()) % 7 + 7)
    # 11月第一个周日
    nov_1st = datetime(year, 11, 1)
    nov_1st_sunday = nov_1st + timedelta(days=(6 - nov_1st.weekday()) % 7)
    
    # 切换通常在凌晨 02:00
    start = march_2nd_sunday.replace(hour=2)
    end = nov_1st_sunday.replace(hour=2)
    return start <= dt < end

def get_trading_day(dt: datetime) -> datetime:
    """根据夏令时(05:00)/冬令时(06:00)规则将日历时间转换为交易日"""
    if not isinstance(dt, datetime):
        return dt
    
    is_summer = is_summer_time(dt)
    curr_time = dt.time()
    
    # 根据用户最新规则：
    # 夏令时分界点：05:00:00
    # 冬令时分界点：06:00:00
    cutoff_hour = 5 if is_summer else 6
    
    if curr_time < time(cutoff_hour, 0):
        # 凌晨分界点之前的时间属于前一个交易日
        return (dt - timedelta(days=1)).date()
    
    # 分界点之后的时间属于当天交易日
    return dt.date()


def last_completed_trading_day(now: datetime | None = None) -> date:
    """当前进行中交易日的前一个已结束交易日。周末回退到周五。"""
    now = now or datetime.now()
    current = get_trading_day(now)
    if isinstance(current, datetime):
        current = current.date()
    day = current - timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


# ====================== 提取6位辨别码 ======================
def extract_login_code(series: pd.Series) -> pd.Series:
    def get_code(x):
        x = str(x).strip()
        if x.startswith(('86', '66')):
            return x[2:8]
        if x.startswith(('2000', '2001')):
            return x[4:10]
        if x.startswith('530'):
            return x[3:9]
        return '0'

    codes = series.apply(get_code)
    mask_special = series.astype(str).str.startswith(('168', '568', '180'))
    import numpy as np
    codes = pd.Series(np.where(mask_special, '0', codes), index=series.index)
    codes = codes.where(codes.str.len() == 6, '0')
    return codes