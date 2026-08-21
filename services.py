import pandas as pd
from sqlalchemy import text
from functools import lru_cache
from datetime import datetime, date, timedelta
from config import engine_finance, engine_trade, engine_trade_mt4, BOUNDARY_DATETIME
from config_PROFIT import engine_profit
from utils import extract_login_code
from models import AllStatResp, FirstDepositStatResp, FirstAnswerStatResp
from utils import extract_login_code, get_trading_day, last_completed_trading_day
import json
import time
from copy import deepcopy
from pathlib import Path
import threading

# 配置
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
USER_PERSONA_JSON_PATH = DATA_DIR / "user_persona_stats.json"
TRADING_DIST_JSON_PATH = DATA_DIR / "trading_distribution_2015_2025.json"
VOLUME_DIST_JSON_PATH = DATA_DIR / "volume_distribution_history_2015_2025.json"
ALL_STAT_JSON_PATH = DATA_DIR / "all_stat_daily_2015_2025.json"
YEAR_STAT_JSON_PATH = DATA_DIR / "year_stat_2015_2025.json"
YEAR_STAT_HISTORY_BOUNDARY = 2026
DEPOSIT_SPEED_JSON_PATH = DATA_DIR / "deposit_speed_2015_2025.json"
DEPOSIT_SPEED_HISTORY_BOUNDARY = date(2026, 1, 1)
FIRST_DEPOSIT_JSON_PATH = DATA_DIR / "first_deposit_stat_2015_2025.json"
FIRST_DEPOSIT_HISTORY_BOUNDARY = 2026
FIRST_ANSWER_JSON_PATH = DATA_DIR / "first_answer_stat_2015_2025.json"
FIRST_ANSWER_HISTORY_BOUNDARY = date(2026, 1, 1)

PERSONA_CACHE_TTL = 3600  # 1小时

# 全局锁和内存缓存
_persona_cache_lock = threading.Lock()
_persona_online_cache = {}   # {year: (data, timestamp)}
_volume_distribution_source_cache = {}

# 新增 semaphore
user_persona_semaphore = threading.BoundedSemaphore(1)

def _get_trading_period_end(end_year: int):
    """返回统计截止交易日及为覆盖该交易日所需的查询结束时间。"""
    today = datetime.today()
    if end_year >= today.year:
        yesterday = today - timedelta(days=1)
        return yesterday.date(), today.replace(hour=6, minute=0, second=0, microsecond=0)
    return date(end_year, 12, 31), datetime(end_year + 1, 1, 1, 6, 0, 0)

def clear_volume_distribution_source_cache():
    """释放历史手数批量生成时复用的交易明细。"""
    _volume_distribution_source_cache.clear()

@lru_cache(maxsize=1)
def get_valid_login_codes() -> set:
    try:
        sql = """
            SELECT id, `group`
            FROM js_mt4_account
            WHERE TRIM(UPPER(COALESCE(`group`, ''))) NOT IN ('99', 'G99', 'MANAGER')
        """
        df = pd.read_sql(sql, engine_finance)
        if df.empty:
            return set()
        df["login_code"] = extract_login_code(df["id"])
        return set(df[df["login_code"] != "0"]["login_code"].unique())
    except Exception as e:
        print(f"Failed to load valid login codes: {e}")
        return set()

@lru_cache(maxsize=1)
def get_activated_login_codes() -> set:
    """已激活账户：gold.js_bank_notify 中入金成功(iPayResult=1)的账户。"""
    try:
        sql = """
            SELECT DISTINCT sUserName
            FROM js_bank_notify
            WHERE iPayResult = 1
        """
        df = pd.read_sql(sql, engine_finance)
        if df.empty:
            return set()
        df['login_code'] = extract_login_code(df['sUserName'])
        return set(df[df['login_code'] != '0']['login_code'].unique())
    except Exception as e:
        print(f"Failed to load activated login codes: {e}")
        return set()

def _query_trading_stat_from_db(year: int) -> pd.DataFrame:
    try:
        start_dt = datetime(year, 1, 1, 0, 0, 0)
        today = date.today()
        yesterday = today - timedelta(days=1)
        if year == today.year:
            # 如果是当年，为了包含跨交易日的记录（如今天凌晨属于昨天的交易），
            # 数据库查询范围向后多取 1 天
            end_dt = datetime.combine(today, datetime.max.time())
        else:
            # 否则统计到该年最后一天
            end_dt = datetime(year, 12, 31, 23, 59, 59)

        df_open_list: list[pd.DataFrame] = []
        df_close_list: list[pd.DataFrame] = []

        if start_dt < BOUNDARY_DATETIME:
            pre_start = start_dt
            pre_end = min(end_dt, BOUNDARY_DATETIME - timedelta(seconds=1))
            pre_start_str = pre_start.strftime('%Y-%m-%d %H:%M:%S')
            pre_end_str = pre_end.strftime('%Y-%m-%d %H:%M:%S')

            sql_open_pre = text("""
                SELECT OPEN_TIME AS trade_time, login AS login
                FROM v_mt4_trades_filtered
                WHERE OPEN_TIME >= :start_date AND OPEN_TIME <= :end_date
                AND ((LOGIN >= 86600000 AND LOGIN <= 89999999) OR (LOGIN >= 666000000 AND LOGIN <= 699000000))
                AND cmd IN (0, 1)
            """)
            sql_close_pre = text("""
                SELECT CLOSE_TIME AS trade_time, login AS login
                FROM v_mt4_trades_filtered
                WHERE CLOSE_TIME >= :start_date AND CLOSE_TIME <= :end_date
                AND ((LOGIN >= 86600000 AND LOGIN <= 89999999) OR (LOGIN >= 666000000 AND LOGIN <= 699000000))
                AND cmd IN (0, 1)
            """)

            with engine_trade_mt4.connect() as conn:
                df_open_pre = pd.read_sql(sql_open_pre, conn, params={"start_date": pre_start_str, "end_date": pre_end_str})
                df_close_pre = pd.read_sql(sql_close_pre, conn, params={"start_date": pre_start_str, "end_date": pre_end_str})
                if not df_open_pre.empty:
                    df_open_list.append(df_open_pre)
                if not df_close_pre.empty:
                    df_close_list.append(df_close_pre)

        if end_dt >= BOUNDARY_DATETIME:
            post_start = max(start_dt, BOUNDARY_DATETIME)
            post_end = end_dt
            post_start_str = post_start.strftime('%Y-%m-%d %H:%M:%S')
            post_end_str = post_end.strftime('%Y-%m-%d %H:%M:%S')

            sql_open_post = text("""
                SELECT nearmonth1 AS trade_time, login AS login
                FROM js_day_jyr
                WHERE nearmonth1 >= :start_date AND nearmonth1 <= :end_date
            """)
            sql_close_post = text("""
                SELECT nearmonth AS trade_time, login AS login
                FROM js_day_jyr
                WHERE nearmonth >= :start_date AND nearmonth <= :end_date
            """)

            with engine_trade.connect() as conn:
                df_open_post = pd.read_sql(sql_open_post, conn, params={"start_date": post_start_str, "end_date": post_end_str})
                df_close_post = pd.read_sql(sql_close_post, conn, params={"start_date": post_start_str, "end_date": post_end_str})
                if not df_open_post.empty:
                    df_open_list.append(df_open_post)
                if not df_close_post.empty:
                    df_close_list.append(df_close_post)

        df_open = pd.concat(df_open_list, ignore_index=True) if df_open_list else pd.DataFrame()
        df_close = pd.concat(df_close_list, ignore_index=True) if df_close_list else pd.DataFrame()

        all_users_year = set()
        open_users_year = set()
        close_users_year = set()
        activated_codes = set()

        months = pd.date_range(f"{year}-01-01", periods=12, freq='MS')
        month_strs = months.strftime('%Y-%m').tolist()

        result_dict = {m: {
            "开仓人数": 0, "平仓人数": 0, "开仓+平仓 人数": 0,
            "开仓人数(A)": 0, "平仓人数(A)": 0, "开仓+平仓 人数(A)": 0
        } for m in month_strs}

        if not df_open.empty or not df_close.empty:
            df_open = df_open.assign(type='开仓') if not df_open.empty else pd.DataFrame()
            df_close = df_close.assign(type='平仓') if not df_close.empty else pd.DataFrame()

            df_all = pd.concat([df_open, df_close], ignore_index=True)
            df_all['trade_time'] = pd.to_datetime(df_all['trade_time'], errors='coerce')
            # 关键修改：将日历时间转换为交易日，再按年月统计
            df_all['trading_day'] = df_all['trade_time'].apply(get_trading_day)
            df_all['year_month'] = pd.to_datetime(df_all['trading_day']).dt.strftime('%Y-%m')

            valid_codes = get_valid_login_codes()
            if valid_codes:
                df_all['login_code'] = extract_login_code(df_all['login'])
                df_all = df_all[df_all['login_code'].isin(valid_codes)]

            # 过滤：如果统计的是当年，则只保留交易日截止到昨天的数据
            if year == date.today().year:
                df_all = df_all[pd.to_datetime(df_all['trading_day']).dt.date <= yesterday]

            if not df_all.empty:
                activated_codes = get_activated_login_codes()

                for ym, group in df_all.groupby('year_month'):
                    if ym not in result_dict:
                        continue

                    all_users = set(group['login_code'].unique())
                    open_users = set(group[group['type'] == '开仓']['login_code'].unique())
                    close_users = set(group[group['type'] == '平仓']['login_code'].unique())

                    result_dict[ym].update({
                        "开仓人数": len(open_users),
                        "平仓人数": len(close_users),
                        "开仓+平仓 人数": len(all_users),
                        "开仓人数(A)": len(open_users & activated_codes),
                        "平仓人数(A)": len(close_users & activated_codes),
                        "开仓+平仓 人数(A)": len(all_users & activated_codes),
                    })

                    all_users_year.update(all_users)
                    open_users_year.update(open_users)
                    close_users_year.update(close_users)

        df_result = pd.DataFrame.from_dict(result_dict, orient='index').reset_index().rename(columns={'index': '年月'})

        yearly_row = pd.DataFrame([{
            '年月': '全年',
            '开仓人数': len(open_users_year),
            '平仓人数': len(close_users_year),
            '开仓+平仓 人数': len(all_users_year),
            '开仓人数(A)': len(open_users_year & activated_codes),
            '平仓人数(A)': len(close_users_year & activated_codes),
            '开仓+平仓 人数(A)': len(all_users_year & activated_codes),
        }])

        df_result = pd.concat([df_result, yearly_row], ignore_index=True)
        cols = ['年月', '开仓人数', '平仓人数', '开仓+平仓 人数', '开仓人数(A)', '平仓人数(A)', '开仓+平仓 人数(A)']
        return df_result[cols]

    except Exception as e:
        print(f"Query failed for {year}: {e}")
        return pd.DataFrame()


YEAR_STAT_COLUMNS = [
    '年月', '开仓人数', '平仓人数', '开仓+平仓 人数',
    '开仓人数(A)', '平仓人数(A)', '开仓+平仓 人数(A)',
]


@lru_cache(maxsize=1)
def _load_year_stat_history_df() -> dict[int, pd.DataFrame]:
    if not YEAR_STAT_JSON_PATH.exists():
        return {}
    try:
        with open(YEAR_STAT_JSON_PATH, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        result: dict[int, pd.DataFrame] = {}
        for year_key, rows in payload.get("data", {}).items():
            df = pd.DataFrame(rows)
            if df.empty:
                continue
            for col in YEAR_STAT_COLUMNS:
                if col not in df.columns:
                    df[col] = 0
            result[int(year_key)] = df[YEAR_STAT_COLUMNS].copy()
        return result
    except Exception as e:
        print(f"读取月交易人数历史缓存失败: {e}")
        return {}


def clear_year_stat_history_cache():
    _load_year_stat_history_df.cache_clear()


def query_trading_stat_df(year: int) -> pd.DataFrame:
    """2015-2025 优先读静态 JSON，2026 年起在线查库。"""
    if year < YEAR_STAT_HISTORY_BOUNDARY:
        history = _load_year_stat_history_df()
        cached = history.get(year)
        if cached is not None and not cached.empty:
            return cached.copy()
    return _query_trading_stat_from_db(year)

def _iter_weekday_trading_days(start_date: date, end_date: date):
    """遍历区间内交易日标签为周一至周五的日期（按交易日周六日排除）。"""
    d = start_date
    while d <= end_date:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def _normalize_trading_day_key(value) -> date | None:
    """将 groupby / get_trading_day 的结果统一为 date。"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        ts = pd.Timestamp(value)
        if pd.isna(ts):
            return None
        return ts.date()
    except Exception:
        return None


def query_daily_trading_stat_df(start_date: date, end_date: date) -> pd.DataFrame:
    try:
        # 交易日以夏令时 05:00、冬令时 06:00 为界。为覆盖结束交易日
        # 凌晨分界点前仍归属于前一交易日的记录，查询范围向后多取 1 天。
        start_dt = datetime.combine(start_date, datetime.min.time())
        end_dt = datetime.combine(end_date + timedelta(days=1), datetime.max.time())

        df_open_list: list[pd.DataFrame] = []
        df_close_list: list[pd.DataFrame] = []

        if start_dt < BOUNDARY_DATETIME:
            pre_start = start_dt
            pre_end = min(end_dt, BOUNDARY_DATETIME - timedelta(seconds=1))
            pre_start_str = pre_start.strftime('%Y-%m-%d %H:%M:%S')
            pre_end_str = pre_end.strftime('%Y-%m-%d %H:%M:%S')

            sql_open_pre = text("""
                SELECT OPEN_TIME AS trade_time, login AS login
                FROM v_mt4_trades_filtered
                WHERE OPEN_TIME >= :start_date AND OPEN_TIME <= :end_date
                AND ((LOGIN >= 86600000 AND LOGIN <= 89999999) OR (LOGIN >= 666000000 AND LOGIN <= 699000000))
                AND cmd IN (0, 1)
            """)
            sql_close_pre = text("""
                SELECT CLOSE_TIME AS trade_time, login AS login
                FROM v_mt4_trades_filtered
                WHERE CLOSE_TIME >= :start_date AND CLOSE_TIME <= :end_date
                AND ((LOGIN >= 86600000 AND LOGIN <= 89999999) OR (LOGIN >= 666000000 AND LOGIN <= 699000000))
                AND cmd IN (0, 1)
            """)

            with engine_trade_mt4.connect() as conn:
                df_open_pre = pd.read_sql(sql_open_pre, conn, params={"start_date": pre_start_str, "end_date": pre_end_str})
                df_close_pre = pd.read_sql(sql_close_pre, conn, params={"start_date": pre_start_str, "end_date": pre_end_str})
                if not df_open_pre.empty:
                    df_open_list.append(df_open_pre)
                if not df_close_pre.empty:
                    df_close_list.append(df_close_pre)

        if end_dt >= BOUNDARY_DATETIME:
            post_start = max(start_dt, BOUNDARY_DATETIME)
            post_end = end_dt
            post_start_str = post_start.strftime('%Y-%m-%d %H:%M:%S')
            post_end_str = post_end.strftime('%Y-%m-%d %H:%M:%S')

            sql_open_post = text("""
                SELECT nearmonth1 AS trade_time, login AS login
                FROM js_day_jyr
                WHERE nearmonth1 >= :start_date AND nearmonth1 <= :end_date
            """)
            sql_close_post = text("""
                SELECT nearmonth AS trade_time, login AS login
                FROM js_day_jyr
                WHERE nearmonth >= :start_date AND nearmonth <= :end_date
            """)

            with engine_trade.connect() as conn:
                df_open_post = pd.read_sql(sql_open_post, conn, params={"start_date": post_start_str, "end_date": post_end_str})
                df_close_post = pd.read_sql(sql_close_post, conn, params={"start_date": post_start_str, "end_date": post_end_str})
                if not df_open_post.empty:
                    df_open_list.append(df_open_post)
                if not df_close_post.empty:
                    df_close_list.append(df_close_post)

        df_open = pd.concat(df_open_list, ignore_index=True) if df_open_list else pd.DataFrame()
        df_close = pd.concat(df_close_list, ignore_index=True) if df_close_list else pd.DataFrame()

        empty_row = {
            '开仓人数': 0, '平仓人数': 0, '开仓+平仓 人数': 0,
            '开仓人数(A)': 0, '平仓人数(A)': 0, '开仓+平仓 人数(A)': 0,
        }
        day_stats: dict[date, dict] = {}

        if not df_open.empty or not df_close.empty:
            df_open = df_open.assign(type='开仓') if not df_open.empty else pd.DataFrame()
            df_close = df_close.assign(type='平仓') if not df_close.empty else pd.DataFrame()

            df_all = pd.concat([df_open, df_close], ignore_index=True)
            df_all['trade_time'] = pd.to_datetime(df_all['trade_time'], errors='coerce')
            df_all = df_all.dropna(subset=['trade_time'])
            # 按交易日统计（夏令时 05:00 / 冬令时 06:00），不是自然日。
            df_all['trade_date'] = df_all['trade_time'].apply(get_trading_day)

            valid_codes = get_valid_login_codes()
            if valid_codes:
                df_all['login_code'] = extract_login_code(df_all['login'])
                df_all = df_all[df_all['login_code'].isin(valid_codes)]

            if not df_all.empty:
                activated_codes = get_activated_login_codes()
                for raw_day, group in df_all.groupby('trade_date'):
                    trading_day = _normalize_trading_day_key(raw_day)
                    # 交易日为周六、日的数据不进入图表/表格（自然日周末但归属周五的仍计周五）。
                    if trading_day is None or trading_day.weekday() >= 5:
                        continue
                    open_users = set(group[group['type'] == '开仓']['login_code'].unique())
                    close_users = set(group[group['type'] == '平仓']['login_code'].unique())
                    all_users = open_users | close_users
                    day_stats[trading_day] = {
                        '开仓人数': len(open_users),
                        '平仓人数': len(close_users),
                        '开仓+平仓 人数': len(all_users),
                        '开仓人数(A)': len(open_users & activated_codes),
                        '平仓人数(A)': len(close_users & activated_codes),
                        '开仓+平仓 人数(A)': len(all_users & activated_codes),
                    }

        result_rows = [
            {'日期': d.strftime('%Y-%m-%d'), **(day_stats.get(d, empty_row))}
            for d in _iter_weekday_trading_days(start_date, end_date)
        ]

        df_result = pd.DataFrame(result_rows)
        if df_result.empty:
            return pd.DataFrame(columns=[
                '日期', '开仓人数', '平仓人数', '开仓+平仓 人数',
                '开仓人数(A)', '平仓人数(A)', '开仓+平仓 人数(A)',
            ])
        df_result = df_result.sort_values('日期', ascending=False).reset_index(drop=True)
        cols = ['日期', '开仓人数', '平仓人数', '开仓+平仓 人数', '开仓人数(A)', '平仓人数(A)', '开仓+平仓 人数(A)']
        return df_result[cols]

    except Exception as e:
        print(f"Daily query failed: {e}")
        return pd.DataFrame()

def _query_all_stat_data(start: str, end: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    valid_login_codes = get_valid_login_codes()
    if not valid_login_codes:
        raise RuntimeError("有效交易账号列表为空，无法生成出入金统计")

    dep_sql = """
        SELECT sUserName, sDollar, sTradeTime AS event_time
        FROM js_bank_notify
        WHERE iPayResult = 1
          AND sTradeTime >= %s
          AND sTradeTime < %s
    """
    dep_df = pd.read_sql(dep_sql, con=engine_finance, params=(start, end))
    dep_df['sDollar'] = pd.to_numeric(dep_df['sDollar'], errors='coerce').fillna(0)
    dep_df['login_code'] = extract_login_code(dep_df['sUserName'])

    dep_valid = dep_df[
        (dep_df['login_code'] != '0') &
        (dep_df['login_code'].isin(valid_login_codes))
    ].copy()

    wit_sql = """
        SELECT customer_id, bank_amount, suretime AS event_time
        FROM js_bank_withdrawals
        WHERE type = '1'
          AND suretime >= %s
          AND suretime < %s
    """
    wit_df = pd.read_sql(wit_sql, con=engine_finance, params=(start, end))
    wit_df['bank_amount'] = pd.to_numeric(wit_df['bank_amount'], errors='coerce').fillna(0)
    wit_df['login_code'] = extract_login_code(wit_df['customer_id'])

    wit_valid = wit_df[
        (wit_df['login_code'] != '0') &
        (wit_df['login_code'].isin(valid_login_codes))
    ].copy()

    return dep_valid, wit_valid


def _build_all_stat_response(dep_valid: pd.DataFrame, wit_valid: pd.DataFrame) -> AllStatResp:
    withdraw_dollar = float(wit_valid['bank_amount'].sum())
    withdraw_user_cnt = int(wit_valid['login_code'].nunique())
    withdraw_order_cnt = len(wit_valid)
    deposit_dollar = float(dep_valid['sDollar'].sum())
    deposit_user_cnt = int(dep_valid['login_code'].nunique())
    deposit_order_cnt = len(dep_valid)

    # 合计规则：
    # 1) 入金/出金人数：按六位辨识码去重后再合并去重
    # 2) 其他项：直接做普通总和
    total_user_cnt = int(
        len(set(dep_valid['login_code'].unique()) | set(wit_valid['login_code'].unique()))
    )
    total_order_cnt = int(deposit_order_cnt + withdraw_order_cnt)
    total_dollar_sum = float(deposit_dollar + withdraw_dollar)

    return AllStatResp(
        deposit_user_cnt=deposit_user_cnt,
        deposit_order_cnt=deposit_order_cnt,
        deposit_dollar_sum=deposit_dollar,
        withdraw_user_cnt=withdraw_user_cnt,
        withdraw_order_cnt=withdraw_order_cnt,
        withdraw_dollar_sum=withdraw_dollar,
        net_dollar_sum=deposit_dollar - withdraw_dollar,
        total_user_cnt=total_user_cnt,
        total_order_cnt=total_order_cnt,
        total_dollar_sum=total_dollar_sum,
    )


def _build_all_stat_trend_df(
    dep_valid: pd.DataFrame,
    wit_valid: pd.DataFrame,
    start: str,
    end: str,
) -> pd.DataFrame:
    columns = [
        '日期', '入金人数', '出金人数', '总人数', '入金笔数', '出金笔数', '总笔数',
        '入金金额', '出金金额', '净入金', '总金额'
    ]
    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    dates = pd.date_range(start_date, end_date - timedelta(days=1), freq='D')
    if dates.empty:
        return pd.DataFrame(columns=columns)

    dep = dep_valid.copy()
    wit = wit_valid.copy()
    dep['日期'] = pd.to_datetime(dep['event_time'], errors='coerce').dt.strftime('%Y-%m-%d')
    wit['日期'] = pd.to_datetime(wit['event_time'], errors='coerce').dt.strftime('%Y-%m-%d')
    dep_groups = {day: group for day, group in dep.groupby('日期')}
    wit_groups = {day: group for day, group in wit.groupby('日期')}

    rows = []
    for current_date in dates.strftime('%Y-%m-%d'):
        day_dep = dep_groups.get(current_date, dep.iloc[0:0])
        day_wit = wit_groups.get(current_date, wit.iloc[0:0])
        dep_users = set(day_dep['login_code'].dropna().unique())
        wit_users = set(day_wit['login_code'].dropna().unique())
        deposit_dollar = float(day_dep['sDollar'].sum())
        withdraw_dollar = float(day_wit['bank_amount'].sum())
        deposit_orders = int(len(day_dep))
        withdraw_orders = int(len(day_wit))
        rows.append({
            '日期': current_date,
            '入金人数': len(dep_users),
            '出金人数': len(wit_users),
            '总人数': len(dep_users | wit_users),
            '入金笔数': deposit_orders,
            '出金笔数': withdraw_orders,
            '总笔数': deposit_orders + withdraw_orders,
            '入金金额': round(deposit_dollar, 2),
            '出金金额': round(withdraw_dollar, 2),
            '净入金': round(deposit_dollar - withdraw_dollar, 2),
            '总金额': round(deposit_dollar + withdraw_dollar, 2),
        })
    return pd.DataFrame(rows, columns=columns)


@lru_cache(maxsize=1)
def _load_all_stat_history_df() -> pd.DataFrame:
    if not ALL_STAT_JSON_PATH.exists():
        return pd.DataFrame()
    try:
        with open(ALL_STAT_JSON_PATH, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        rows = []
        for year_rows in payload.get("data", {}).values():
            rows.extend(year_rows)
        df = pd.DataFrame(rows)
        if not df.empty:
            df["日期"] = df["日期"].astype(str)
            df = df.sort_values("日期").reset_index(drop=True)
        return df
    except Exception as e:
        print(f"读取出入金历史缓存失败: {e}")
        return pd.DataFrame()


def clear_all_stat_history_cache():
    _load_all_stat_history_df.cache_clear()


def _build_all_stat_response_from_trend(trend_df: pd.DataFrame) -> AllStatResp:
    if trend_df.empty:
        return AllStatResp(
            deposit_user_cnt=0, deposit_order_cnt=0, deposit_dollar_sum=0.0,
            withdraw_user_cnt=0, withdraw_order_cnt=0, withdraw_dollar_sum=0.0,
            net_dollar_sum=0.0, total_user_cnt=0, total_order_cnt=0,
            total_dollar_sum=0.0,
        )
    return AllStatResp(
        # 历史缓存不保存账号明细；跨日人数为每日去重人数累计。
        deposit_user_cnt=int(trend_df["入金人数"].sum()),
        deposit_order_cnt=int(trend_df["入金笔数"].sum()),
        deposit_dollar_sum=float(trend_df["入金金额"].sum()),
        withdraw_user_cnt=int(trend_df["出金人数"].sum()),
        withdraw_order_cnt=int(trend_df["出金笔数"].sum()),
        withdraw_dollar_sum=float(trend_df["出金金额"].sum()),
        net_dollar_sum=float(trend_df["净入金"].sum()),
        total_user_cnt=int(trend_df["总人数"].sum()),
        total_order_cnt=int(trend_df["总笔数"].sum()),
        total_dollar_sum=float(trend_df["总金额"].sum()),
    )


def query_all_stat(start: str, end: str) -> AllStatResp:
    """接口一自然日汇总；end 保持原有约定，为不包含的结束日期。"""
    summary, _ = query_all_stat_dashboard(start, end)
    return summary


def _query_first_deposit_stat_from_db(year: int) -> FirstDepositStatResp:
    """在线查库：按伦敦金交易日口径返回月度及年度首入金转化统计。"""
    # 1 月 1 日处于冬令时，交易日从当天 06:00 开始。
    yesterday = date.today() - timedelta(days=1)
    start = f"{year}-01-01 06:00:00"
    if year >= date.today().year:
        # 当年当月只统计到昨天，SQL 多取缓冲日覆盖凌晨分界。
        end = datetime.combine(
            yesterday + timedelta(days=2), datetime.min.time()
        ).strftime("%Y-%m-%d %H:%M:%S")
    else:
        # 历史整年：查到下一年 06:00，纳入年末跨交易日数据。
        end = f"{year + 1}-01-01 06:00:00"

    first_deposit_sql = """
        SELECT login_code, MIN(event_time) AS addtime
        FROM (
            SELECT
                CASE
                    WHEN sUserName LIKE '168%%'
                      OR sUserName LIKE '568%%'
                      OR sUserName LIKE '180%%' THEN '0'
                    WHEN LEFT(sUserName, 2) IN ('86', '66') THEN SUBSTRING(sUserName, 3, 6)
                    WHEN LEFT(sUserName, 4) IN ('2000', '2001') THEN SUBSTRING(sUserName, 5, 6)
                    WHEN LEFT(sUserName, 3) = '530' THEN SUBSTRING(sUserName, 4, 6)
                    ELSE '0'
                END AS login_code,
                sTradeTime AS event_time
            FROM js_bank_notify
            WHERE iPayResult = 1
              AND sTradeTime < %s
        ) AS successful_deposits
        WHERE login_code <> '0'
          AND CHAR_LENGTH(login_code) = 6
        GROUP BY login_code
        HAVING addtime >= %s
           AND addtime < %s
    """
    first_deposits = pd.read_sql(
        first_deposit_sql,
        con=engine_finance,
        params=(end, start, end),
    )

    valid_login_codes = get_valid_login_codes()
    if not valid_login_codes:
        raise RuntimeError("有效交易账号列表为空，无法生成首入金统计")

    if not first_deposits.empty:
        first_deposits["login_code"] = first_deposits["login_code"].astype(str)
        first_deposits = first_deposits[
            first_deposits["login_code"].isin(valid_login_codes)
        ].copy()
        first_deposits["addtime"] = pd.to_datetime(
            first_deposits["addtime"], errors="coerce"
        )
        first_deposits = first_deposits.dropna(subset=["addtime"])
        first_deposits["trading_date"] = first_deposits["addtime"].apply(
            get_trading_day
        )
        first_deposits["trading_date"] = first_deposits["trading_date"].apply(
            _normalize_trading_day_key
        )
        first_deposits = first_deposits.dropna(subset=["trading_date"])
        first_deposits = first_deposits[
            first_deposits["trading_date"] <= yesterday
        ]

    real_account_sql = """
        SELECT reg_time
        FROM js_mt4_account
        WHERE reg_time >= %s
          AND reg_time < %s
          AND TRIM(UPPER(COALESCE(`group`, ''))) NOT IN ('99', 'G99')
    """
    real_accounts = pd.read_sql(
        real_account_sql,
        con=engine_finance,
        params=(start, end),
    )
    if real_accounts.empty:
        real_by_month = {}
    else:
        real_accounts["reg_time"] = pd.to_datetime(
            real_accounts["reg_time"], errors="coerce"
        )
        real_accounts = real_accounts.dropna(subset=["reg_time"])
        real_accounts["trading_date"] = real_accounts["reg_time"].apply(
            get_trading_day
        )
        real_accounts["trading_date"] = real_accounts["trading_date"].apply(
            _normalize_trading_day_key
        )
        real_accounts = real_accounts.dropna(subset=["trading_date"])
        real_accounts = real_accounts[real_accounts["trading_date"] <= yesterday]
        real_accounts["month"] = real_accounts["trading_date"].apply(
            lambda value: value.strftime("%Y-%m")
        )
        real_by_month = real_accounts.groupby("month").size().to_dict()

    referral_sql = """
        SELECT login, addtime
        FROM activity.a_jsinferrer
        WHERE TRIM(UPPER(COALESCE(`group`, ''))) NOT IN ('99', 'G99')
        UNION
        SELECT login, addtime
        FROM activity.a_jsinferrer24
        WHERE TRIM(UPPER(COALESCE(`group`, ''))) NOT IN ('99', 'G99')
    """
    referrals = pd.read_sql(
        referral_sql,
        con=engine_finance,
    )
    # 历史上曾被推荐的辨识码（不要求推荐发生在首入金当月）
    if referrals.empty:
        ever_referred_codes: set = set()
    else:
        referrals["login_code"] = extract_login_code(referrals["login"])
        ever_referred_codes = set(
            referrals.loc[referrals["login_code"] != "0", "login_code"].unique()
        )

    if first_deposits.empty:
        first_by_month = {}
        referral_first_by_month = {}
    else:
        first_deposits["month"] = first_deposits["trading_date"].apply(
            lambda value: value.strftime("%Y-%m")
        )
        first_by_month = first_deposits.groupby("month").size().to_dict()
        referral_first_by_month = {
            month: int(group["login_code"].isin(ever_referred_codes).sum())
            for month, group in first_deposits.groupby("month")
        }

    def percentage(numerator: int, denominator: int) -> str:
        if denominator == 0:
            return "0.0%"
        return f"{round(numerator * 100 / denominator, 1):.1f}%"

    monthly_rows = []
    for month_number in range(1, 13):
        month = f"{year}-{month_number:02d}"
        num = int(first_by_month.get(month, 0))
        real_num = int(real_by_month.get(month, 0))
        tjy_num = int(referral_first_by_month.get(month, 0))
        monthly_rows.append({
            "date": f"{month}-01",
            "date1": f"{year}/{month_number:02d}",
            "num": num,
            "real_num": real_num,
            "rate": percentage(num, real_num),
            "tjy_num": tjy_num,
            "tyr_rate": percentage(tjy_num, num),
        })

    year_num = sum(row["num"] for row in monthly_rows)
    year_real_num = sum(row["real_num"] for row in monthly_rows)
    year_tjynum = sum(row["tjy_num"] for row in monthly_rows)
    return FirstDepositStatResp(
        year=year,
        list=monthly_rows,
        year_num=year_num,
        year_real_num=year_real_num,
        year_tjynum=year_tjynum,
        year_rate=percentage(year_num, year_real_num),
        year_tjyrate=percentage(year_tjynum, year_num),
    )


@lru_cache(maxsize=1)
def _load_first_deposit_history() -> dict[int, dict]:
    if not FIRST_DEPOSIT_JSON_PATH.exists():
        return {}
    try:
        with open(FIRST_DEPOSIT_JSON_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f)
        data = payload.get("data", {})
        return {int(year_key): value for year_key, value in data.items()}
    except Exception as e:
        print(f"读取首入金历史缓存失败: {e}")
        return {}


def clear_first_deposit_history_cache():
    _load_first_deposit_history.cache_clear()


def query_first_deposit_stat(year: int) -> FirstDepositStatResp:
    """2015-2025 优先读静态 JSON，2026 年起在线查库。"""
    if year < FIRST_DEPOSIT_HISTORY_BOUNDARY:
        cached = _load_first_deposit_history().get(year)
        if cached:
            return FirstDepositStatResp.model_validate(cached)
    return _query_first_deposit_stat_from_db(year)


def build_first_deposit_history_payload(
    start_year: int = 2015,
    end_year: int = 2025,
) -> dict:
    """批量生成 2015-2025 首入金统计（供 regenerate 脚本写入 JSON）。"""
    data: dict[str, dict] = {}
    for year in range(start_year, end_year + 1):
        print(f"  查询年份: {year}")
        result = _query_first_deposit_stat_from_db(year)
        if len(result.list) != 12:
            raise RuntimeError(f"{year} 首入金缓存不完整：应有 12 个月")
        data[str(year)] = result.model_dump()
        print(f"  完成 {year}: 首入金={result.year_num}, 曾被推荐={result.year_tjynum}")
    return {
        "meta": {
            "logic": "first_deposit_stat_v2_ever_referred",
            "start_year": start_year,
            "end_year": end_year,
            "calendar": "london_gold_trading_day",
            "referral": "lifetime_ever_referred",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        },
        "data": data,
    }


def query_all_stat_dashboard(start: str, end: str) -> tuple[AllStatResp, pd.DataFrame]:
    """合并 2015-2025 静态缓存与 2026 年起的在线数据。"""
    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    if start_date >= end_date:
        empty = pd.DataFrame(columns=[
            '日期', '入金人数', '出金人数', '总人数', '入金笔数', '出金笔数', '总笔数',
            '入金金额', '出金金额', '净入金', '总金额'
        ])
        return _build_all_stat_response_from_trend(empty), empty

    history_boundary = date(2026, 1, 1)
    trend_parts = []
    contains_history = start_date < history_boundary

    if contains_history:
        history_end = min(end_date, history_boundary)
        history_df = _load_all_stat_history_df()
        selected = history_df[
            (history_df["日期"] >= start_date.isoformat()) &
            (history_df["日期"] < history_end.isoformat())
        ].copy() if not history_df.empty else pd.DataFrame()
        expected_days = (history_end - start_date).days
        if len(selected) != expected_days:
            dep, wit = _query_all_stat_data(start_date.isoformat(), history_end.isoformat())
            selected = _build_all_stat_trend_df(
                dep, wit, start_date.isoformat(), history_end.isoformat()
            )
        trend_parts.append(selected)

    online_summary = None
    if end_date > history_boundary:
        online_start = max(start_date, history_boundary)
        dep, wit = _query_all_stat_data(online_start.isoformat(), end_date.isoformat())
        online_summary = _build_all_stat_response(dep, wit)
        trend_parts.append(
            _build_all_stat_trend_df(dep, wit, online_start.isoformat(), end_date.isoformat())
        )

    trend_df = pd.concat(trend_parts, ignore_index=True) if trend_parts else pd.DataFrame()
    if not trend_df.empty:
        trend_df = trend_df.sort_values("日期").reset_index(drop=True)
    summary = (
        _build_all_stat_response_from_trend(trend_df)
        if contains_history else online_summary
    )
    return summary or _build_all_stat_response_from_trend(trend_df), trend_df

@lru_cache(maxsize=1)
def get_account_reg_year_df() -> pd.DataFrame:
    try:
        sql = """
            SELECT id, reg_time, `group`
            FROM js_mt4_account
            WHERE TRIM(UPPER(COALESCE(`group`, ''))) NOT IN ('99', 'G99', 'MANAGER')
        """
        df = pd.read_sql(sql, engine_finance)
        if df.empty:
            return pd.DataFrame(columns=['login_code', 'reg_year'])

        df['login_code'] = extract_login_code(df['id'])
        df = df[df['login_code'] != '0'].copy()
        df['reg_time'] = pd.to_datetime(df['reg_time'], errors='coerce')
        df['reg_year'] = df['reg_time'].dt.year
        df = df.dropna(subset=['reg_year'])
        df['reg_year'] = df['reg_year'].astype(int)
        df = df.sort_values(['login_code', 'reg_year'])
        df = df.drop_duplicates(subset=['login_code'], keep='first')
        return df[['login_code', 'reg_year']]
    except Exception as e:
        print(f"Failed to load account reg_year: {e}")
        return pd.DataFrame(columns=['login_code', 'reg_year'])

def _query_trading_distribution_from_db(
    start_year: int = 2015,
    end_year: int = 2026,
    user_type: str = "开仓人数",
) -> pd.DataFrame:
    try:
        years = list(range(start_year, end_year + 1))
        reg_years = list(range(2015, 2026 + 1))
        type_labels = ["开仓人数", "平仓人数", "开仓+平仓 人数", "开仓人数(A)", "平仓人数(A)", "开仓+平仓 人数(A)"]

        valid_codes = get_valid_login_codes()
        activated_codes = get_activated_login_codes()
        acct_df = get_account_reg_year_df()

        if not acct_df.empty and valid_codes:
            acct_df = acct_df[acct_df["login_code"].isin(valid_codes)].copy()

        range_start_dt = datetime(start_year, 1, 1, 0, 0, 0)
        range_end_dt = datetime(end_year, 12, 31, 23, 59, 59)
        df_all_parts: list[pd.DataFrame] = []

        # 优化：只有当开始时间早于分界点时，才查询 MT4 数据库
        pre_start = range_start_dt
        pre_end = min(range_end_dt, BOUNDARY_DATETIME - timedelta(seconds=1))
        if pre_start < BOUNDARY_DATETIME and pre_start <= pre_end:
            sql_open_pre = text("""
                SELECT OPEN_TIME AS trade_time, LOGIN AS login
                FROM v_mt4_trades_filtered
                WHERE OPEN_TIME >= :start_date AND OPEN_TIME <= :end_date
                  AND ((LOGIN >= 86600000 AND LOGIN <= 89999999) OR (LOGIN >= 666000000 AND LOGIN <= 699000000))
                  AND cmd IN (0, 1)
            """)
            sql_close_pre = text("""
                SELECT CLOSE_TIME AS trade_time, LOGIN AS login
                FROM v_mt4_trades_filtered
                WHERE CLOSE_TIME >= :start_date AND CLOSE_TIME <= :end_date
                  AND ((LOGIN >= 86600000 AND LOGIN <= 89999999) OR (LOGIN >= 666000000 AND LOGIN <= 699000000))
                  AND cmd IN (0, 1)
            """)

            chunk_days = 90
            cur_start = pre_start
            with engine_trade_mt4.connect() as conn:
                while cur_start <= pre_end:
                    cur_end = min(pre_end, cur_start + timedelta(days=chunk_days - 1))
                    cur_start_str = cur_start.strftime("%Y-%m-%d %H:%M:%S")
                    cur_end_str = cur_end.strftime("%Y-%m-%d %H:%M:%S")
                    df_open_pre = pd.read_sql(sql_open_pre, conn, params={"start_date": cur_start_str, "end_date": cur_end_str})
                    df_close_pre = pd.read_sql(sql_close_pre, conn, params={"start_date": cur_start_str, "end_date": cur_end_str})

                    if not df_open_pre.empty:
                        df_open_pre.columns = df_open_pre.columns.str.lower()
                        # 应用交易日逻辑获取真实年份
                        df_open_pre["trading_day"] = pd.to_datetime(df_open_pre["trade_time"]).apply(get_trading_day)
                        df_open_pre["trade_year"] = pd.to_datetime(df_open_pre["trading_day"]).dt.year
                        df_open_pre = df_open_pre[df_open_pre["trade_year"].isin(years)].copy()
                        df_open_pre["type"] = "开仓"
                        df_all_parts.append(df_open_pre[["trade_year", "login", "type"]])
                    if not df_close_pre.empty:
                        df_close_pre.columns = df_close_pre.columns.str.lower()
                        # 应用交易日逻辑获取真实年份
                        df_close_pre["trading_day"] = pd.to_datetime(df_close_pre["trade_time"]).apply(get_trading_day)
                        df_close_pre["trade_year"] = pd.to_datetime(df_close_pre["trading_day"]).dt.year
                        df_close_pre = df_close_pre[df_close_pre["trade_year"].isin(years)].copy()
                        df_close_pre["type"] = "平仓"
                        df_all_parts.append(df_close_pre[["trade_year", "login", "type"]])
                    cur_start = cur_end + timedelta(seconds=1)

        # 优化：只有当结束时间晚于分界点时，才查询新系统数据库
        post_start = max(range_start_dt, BOUNDARY_DATETIME)
        post_end = range_end_dt
        if post_end >= BOUNDARY_DATETIME and post_start <= post_end:
            sql_open_post = text("""
                SELECT nearmonth1 AS trade_time, login AS login
                FROM js_day_jyr
                WHERE nearmonth1 >= :start_date AND nearmonth1 <= :end_date
            """)
            sql_close_post = text("""
                SELECT nearmonth AS trade_time, login AS login
                FROM js_day_jyr
                WHERE nearmonth >= :start_date AND nearmonth <= :end_date
            """)
            with engine_trade.connect() as conn:
                df_open_post = pd.read_sql(sql_open_post, conn, params={"start_date": post_start.strftime("%Y-%m-%d %H:%M:%S"), "end_date": post_end.strftime("%Y-%m-%d %H:%M:%S")})
                df_close_post = pd.read_sql(sql_close_post, conn, params={"start_date": post_start.strftime("%Y-%m-%d %H:%M:%S"), "end_date": post_end.strftime("%Y-%m-%d %H:%M:%S")})

            if not df_open_post.empty:
                df_open_post.columns = df_open_post.columns.str.lower()
                # 应用交易日逻辑获取真实年份
                df_open_post["trading_day"] = pd.to_datetime(df_open_post["trade_time"]).apply(get_trading_day)
                df_open_post["trade_year"] = pd.to_datetime(df_open_post["trading_day"]).dt.year
                df_open_post = df_open_post[df_open_post["trade_year"].isin(years)].copy()
                df_open_post["type"] = "开仓"
                df_all_parts.append(df_open_post[["trade_year", "login", "type"]])
            if not df_close_post.empty:
                df_close_post.columns = df_close_post.columns.str.lower()
                # 应用交易日逻辑获取真实年份
                df_close_post["trading_day"] = pd.to_datetime(df_close_post["trade_time"]).apply(get_trading_day)
                df_close_post["trade_year"] = pd.to_datetime(df_close_post["trading_day"]).dt.year
                df_close_post = df_close_post[df_close_post["trade_year"].isin(years)].copy()
                df_close_post["type"] = "平仓"
                df_all_parts.append(df_close_post[["trade_year", "login", "type"]])

        if not df_all_parts:
            rows = [{"年度": y, "交易人数类型": 0, **{str(ry): 0 for ry in reg_years}} for y in years]
            return pd.DataFrame(rows)[["年度", "交易人数类型"] + [str(y) for y in reg_years]]

        df_all = pd.concat(df_all_parts, ignore_index=True)
        df_all["login_code"] = extract_login_code(df_all["login"])
        df_all = df_all[df_all["login_code"].isin(valid_codes)]

        year_type_users = {y: {lbl: set() for lbl in type_labels} for y in years}
        for y, grp in df_all.groupby("trade_year"):
            y_int = int(y)
            if y_int not in year_type_users: continue
            opens = set(grp[grp["type"] == "开仓"]["login_code"].unique())
            closes = set(grp[grp["type"] == "平仓"]["login_code"].unique())
            all_users = opens | closes
            year_type_users[y_int]["开仓人数"] = opens
            year_type_users[y_int]["平仓人数"] = closes
            year_type_users[y_int]["开仓+平仓 人数"] = all_users
            year_type_users[y_int]["开仓人数(A)"] = opens & activated_codes
            year_type_users[y_int]["平仓人数(A)"] = closes & activated_codes
            year_type_users[y_int]["开仓+平仓 人数(A)"] = all_users & activated_codes

        rows = []
        for year in years:
            users = year_type_users.get(year, {}).get(user_type, set())
            row = {"年度": year, "交易人数类型": len(users)}
            if not users or acct_df.empty:
                for ry in reg_years: row[str(ry)] = 0
            else:
                sub = acct_df[acct_df["login_code"].isin(users)]
                vc = sub["reg_year"].value_counts()
                for ry in reg_years: row[str(ry)] = int(vc.get(ry, 0))
            rows.append(row)

        df_res = pd.DataFrame(rows)
        cols = ["年度", "交易人数类型"] + [str(y) for y in reg_years]
        return df_res[cols]
    except Exception as e:
        print(f"Query trading distribution from DB failed: {e}")
        return pd.DataFrame(columns=["年度", "交易人数类型"] + [str(y) for y in range(2015, 2026 + 1)])

def query_trading_distribution_df(
    start_year: int = 2015,
    end_year: int = 2026,
    user_type: str = "开仓人数",
) -> pd.DataFrame:
    results_df = pd.DataFrame()
    
    # 1. 优先处理 2015-2025 的 JSON 数据
    if start_year <= 2025:
        if TRADING_DIST_JSON_PATH.exists():
            try:
                with open(TRADING_DIST_JSON_PATH, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                
                # 处理嵌套的 "data" 结构
                if isinstance(json_data, dict) and "data" in json_data:
                    type_data = json_data["data"].get(user_type, [])
                    if type_data:
                        full_df = pd.DataFrame(type_data)
                        # 过滤出请求的年份
                        results_df = full_df[(full_df["年度"] >= start_year) & 
                                             (full_df["年度"] <= min(end_year, 2025))]
                elif isinstance(json_data, list):
                    # 如果是平铺的列表结构
                    full_df = pd.DataFrame(json_data)
                    if not full_df.empty:
                        results_df = full_df[(full_df["年度"] >= start_year) & 
                                             (full_df["年度"] <= min(end_year, 2025)) & 
                                             (full_df["交易人数类型"] == user_type)]
            except Exception as e:
                print(f"读取交易分布JSON失败: {e}")
        
        # 只有在 JSON 文件不存在或完全没数据时，才针对 2015-2025 回退到数据库
        if results_df.empty:
            results_df = _query_trading_distribution_from_db(start_year, min(end_year, 2025), user_type)

    # 2. 如果需要 2026 年的数据，仅针对 2026 进行数据库在线查询
    if end_year >= 2026:
        # 强制只查 2026 年
        df_2026 = _query_trading_distribution_from_db(2026, end_year, user_type)
        results_df = pd.concat([results_df, df_2026], ignore_index=True)

    # 统一输出列维度：年度、交易人数、2015~2026
    if not results_df.empty:
        if "交易人数类型" in results_df.columns:
            if "交易人数" in results_df.columns:
                # 兜底：拼接不同来源数据时，优先使用非空值，避免出现 NaN
                results_df["交易人数"] = results_df["交易人数"].combine_first(results_df["交易人数类型"])
                results_df = results_df.drop(columns=["交易人数类型"])
            else:
                results_df = results_df.rename(columns={"交易人数类型": "交易人数"})

        expected_cols = ["年度", "交易人数"] + [str(y) for y in range(2015, 2026 + 1)]
        for col in expected_cols:
            if col not in results_df.columns:
                results_df[col] = 0
        results_df = results_df[expected_cols].sort_values("年度").reset_index(drop=True)

    return results_df

def _build_volume_distribution_result(
    df_all: pd.DataFrame,
    acct_df: pd.DataFrame,
    activated_codes: set,
    years: list,
    reg_years: list,
    type_labels: list,
    volume_type: str,
) -> pd.DataFrame:
    year_type_volumes = {y: {label: 0.0 for label in type_labels} for y in years}
    for year, group in df_all.groupby("trade_year"):
        year_int = int(year)
        if year_int not in year_type_volumes:
            continue
        open_volume = group[group["type"] == "开仓"]["volume"].sum()
        close_volume = group[group["type"] == "平仓"]["volume"].sum()
        activated_group = group[group["login_code"].isin(activated_codes)]
        activated_open_volume = activated_group[activated_group["type"] == "开仓"]["volume"].sum()
        activated_close_volume = activated_group[activated_group["type"] == "平仓"]["volume"].sum()
        year_type_volumes[year_int].update({
            "开仓手数": open_volume,
            "平仓手数": close_volume,
            "开仓+平仓 手数": open_volume + close_volume,
            "开仓手数(A)": activated_open_volume,
            "平仓手数(A)": activated_close_volume,
            "开仓+平仓 手数(A)": activated_open_volume + activated_close_volume,
        })

    rows = []
    for year in years:
        volume = year_type_volumes.get(year, {}).get(volume_type, 0.0)
        row = {"年度": year, "交易手数类型": round(volume, 2)}
        if volume == 0 or acct_df.empty:
            for reg_year in reg_years:
                row[str(reg_year)] = 0.0
        else:
            selected = df_all[df_all["trade_year"] == year].copy()
            if volume_type in ("开仓手数", "开仓手数(A)"):
                selected = selected[selected["type"] == "开仓"]
            elif volume_type in ("平仓手数", "平仓手数(A)"):
                selected = selected[selected["type"] == "平仓"]
            if "(A)" in volume_type:
                selected = selected[selected["login_code"].isin(activated_codes)]
            login_volumes = selected.groupby("login_code")["volume"].sum().reset_index()
            login_volumes = login_volumes.merge(acct_df, on="login_code", how="inner")
            by_reg_year = login_volumes.groupby("reg_year")["volume"].sum()
            for reg_year in reg_years:
                row[str(reg_year)] = round(float(by_reg_year.get(reg_year, 0)), 2)
        rows.append(row)

    columns = ["年度", "交易手数类型"] + [str(year) for year in reg_years]
    return pd.DataFrame(rows)[columns]

def _query_trading_volume_distribution_from_db(
    start_year: int = 2015,
    end_year: int = 2026,
    volume_type: str = "开仓手数",
    reuse_source: bool = False,
) -> pd.DataFrame:
    try:
        years = list(range(start_year, end_year + 1))
        reg_years = list(range(2015, 2026 + 1))
        type_labels = ["开仓手数", "平仓手数", "开仓+平仓 手数", 
                      "开仓手数(A)", "平仓手数(A)", "开仓+平仓 手数(A)"]

        valid_codes = get_valid_login_codes()
        activated_codes = get_activated_login_codes()
        acct_df = get_account_reg_year_df()

        if not acct_df.empty and valid_codes:
            acct_df = acct_df[acct_df["login_code"].isin(valid_codes)].copy()

        cache_key = (start_year, end_year)
        if reuse_source and cache_key in _volume_distribution_source_cache:
            df_all = _volume_distribution_source_cache[cache_key]
            return _build_volume_distribution_result(
                df_all, acct_df, activated_codes, years, reg_years, type_labels, volume_type
            )

        range_start_dt = datetime(start_year, 1, 1, 0, 0, 0)
        target_end_day, range_end_dt = _get_trading_period_end(end_year)
        df_all_parts: list[pd.DataFrame] = []

        # 优化：只有当开始时间早于分界点时，才查询 MT4 数据库
        pre_start = range_start_dt
        pre_end = min(range_end_dt, BOUNDARY_DATETIME - timedelta(seconds=1))
        if pre_start < BOUNDARY_DATETIME and pre_start <= pre_end:
            sql_open_pre = text("""
                SELECT OPEN_TIME AS trade_time, LOGIN AS login, SUM(VOLUME) / 100 AS volume
                FROM v_mt4_trades_filtered
                WHERE OPEN_TIME >= :start_date AND OPEN_TIME <= :end_date
                  AND ((LOGIN >= 86600000 AND LOGIN <= 89999999) OR (LOGIN >= 666000000 AND LOGIN <= 699000000))
                  AND cmd IN (0, 1)
                GROUP BY OPEN_TIME, LOGIN
            """)
            sql_close_pre = text("""
                SELECT CLOSE_TIME AS trade_time, LOGIN AS login, SUM(VOLUME) / 100 AS volume
                FROM v_mt4_trades_filtered
                WHERE CLOSE_TIME >= :start_date AND CLOSE_TIME <= :end_date
                  AND ((LOGIN >= 86600000 AND LOGIN <= 89999999) OR (LOGIN >= 666000000 AND LOGIN <= 699000000))
                  AND cmd IN (0, 1)
                GROUP BY CLOSE_TIME, LOGIN
            """)

            # 按年生成时一次覆盖完整年度及次年元旦交易日尾段。
            chunk_days = 367
            cur_start = pre_start
            with engine_trade_mt4.connect() as conn:
                while cur_start <= pre_end:
                    cur_end = min(pre_end, cur_start + timedelta(days=chunk_days - 1))
                    cur_start_str = cur_start.strftime("%Y-%m-%d %H:%M:%S")
                    cur_end_str = cur_end.strftime("%Y-%m-%d %H:%M:%S")
                    df_open_pre = pd.read_sql(sql_open_pre, conn, params={"start_date": cur_start_str, "end_date": cur_end_str})
                    df_close_pre = pd.read_sql(sql_close_pre, conn, params={"start_date": cur_start_str, "end_date": cur_end_str})

                    if not df_open_pre.empty:
                        df_open_pre.columns = df_open_pre.columns.str.lower()
                        # MT4 查询层已按 SUM(VOLUME) / 100 换算为“手”
                        df_open_pre["volume"] = pd.to_numeric(df_open_pre["volume"], errors='coerce').fillna(0)
                        # 应用交易日逻辑获取真实年份
                        df_open_pre["trading_day"] = pd.to_datetime(df_open_pre["trade_time"]).apply(get_trading_day)
                        df_open_pre["trade_year"] = pd.to_datetime(df_open_pre["trading_day"]).dt.year
                        df_open_pre = df_open_pre[
                            df_open_pre["trade_year"].isin(years)
                            & (pd.to_datetime(df_open_pre["trading_day"]).dt.date <= target_end_day)
                        ].copy()
                        df_open_pre["type"] = "开仓"
                        df_all_parts.append(df_open_pre[["trade_year", "login", "volume", "type"]])
                    if not df_close_pre.empty:
                        df_close_pre.columns = df_close_pre.columns.str.lower()
                        # MT4 查询层已按 SUM(VOLUME) / 100 换算为“手”
                        df_close_pre["volume"] = pd.to_numeric(df_close_pre["volume"], errors='coerce').fillna(0)
                        # 应用交易日逻辑获取真实年份
                        df_close_pre["trading_day"] = pd.to_datetime(df_close_pre["trade_time"]).apply(get_trading_day)
                        df_close_pre["trade_year"] = pd.to_datetime(df_close_pre["trading_day"]).dt.year
                        df_close_pre = df_close_pre[
                            df_close_pre["trade_year"].isin(years)
                            & (pd.to_datetime(df_close_pre["trading_day"]).dt.date <= target_end_day)
                        ].copy()
                        df_close_pre["type"] = "平仓"
                        df_all_parts.append(df_close_pre[["trade_year", "login", "volume", "type"]])
                    cur_start = cur_end + timedelta(seconds=1)

        # 优化：只有当结束时间晚于分界点时，才查询新系统数据库
        post_start = max(range_start_dt, BOUNDARY_DATETIME)
        post_end = range_end_dt
        if post_end >= BOUNDARY_DATETIME and post_start <= post_end:
            sql_open_post = text("""
                SELECT nearmonth1 AS trade_time, login AS login, jyvulmes AS volume
                FROM js_day_jyr
                WHERE nearmonth1 >= :start_date AND nearmonth1 <= :end_date
            """)
            sql_close_post = text("""
                SELECT nearmonth AS trade_time, login AS login, jyvulmes AS volume
                FROM js_day_jyr
                WHERE nearmonth >= :start_date AND nearmonth <= :end_date
            """)
            with engine_trade.connect() as conn:
                df_open_post = pd.read_sql(sql_open_post, conn, params={"start_date": post_start.strftime("%Y-%m-%d %H:%M:%S"), "end_date": post_end.strftime("%Y-%m-%d %H:%M:%S")})
                df_close_post = pd.read_sql(sql_close_post, conn, params={"start_date": post_start.strftime("%Y-%m-%d %H:%M:%S"), "end_date": post_end.strftime("%Y-%m-%d %H:%M:%S")})

            if not df_open_post.empty:
                df_open_post.columns = df_open_post.columns.str.lower()
                # 应用交易日逻辑获取真实年份
                df_open_post["trading_day"] = pd.to_datetime(df_open_post["trade_time"]).apply(get_trading_day)
                df_open_post["trade_year"] = pd.to_datetime(df_open_post["trading_day"]).dt.year
                df_open_post = df_open_post[
                    df_open_post["trade_year"].isin(years)
                    & (pd.to_datetime(df_open_post["trading_day"]).dt.date <= target_end_day)
                ].copy()
                df_open_post["type"] = "开仓"
                df_all_parts.append(df_open_post[["trade_year", "login", "volume", "type"]])
            if not df_close_post.empty:
                df_close_post.columns = df_close_post.columns.str.lower()
                # 应用交易日逻辑获取真实年份
                df_close_post["trading_day"] = pd.to_datetime(df_close_post["trade_time"]).apply(get_trading_day)
                df_close_post["trade_year"] = pd.to_datetime(df_close_post["trading_day"]).dt.year
                df_close_post = df_close_post[
                    df_close_post["trade_year"].isin(years)
                    & (pd.to_datetime(df_close_post["trading_day"]).dt.date <= target_end_day)
                ].copy()
                df_close_post["type"] = "平仓"
                df_all_parts.append(df_close_post[["trade_year", "login", "volume", "type"]])

        if not df_all_parts:
            rows = [{"年度": y, "交易手数类型": 0.0, **{str(ry): 0.0 for ry in reg_years}} for y in years]
            return pd.DataFrame(rows)[["年度", "交易手数类型"] + [str(y) for y in reg_years]]

        df_all = pd.concat(df_all_parts, ignore_index=True)
        df_all["login_code"] = extract_login_code(df_all["login"])
        df_all = df_all[df_all["login_code"].isin(valid_codes)]
        df_all["volume"] = pd.to_numeric(df_all["volume"], errors='coerce').fillna(0)
        if reuse_source:
            _volume_distribution_source_cache.clear()
            _volume_distribution_source_cache[cache_key] = df_all
        return _build_volume_distribution_result(
            df_all, acct_df, activated_codes, years, reg_years, type_labels, volume_type
        )
    except Exception as e:
        print(f"Query trading volume distribution from DB failed: {e}")
        return pd.DataFrame(columns=["年度", "交易手数类型"] + [str(y) for y in range(2015, 2026 + 1)])

def query_trading_volume_distribution_df(
    start_year: int = 2015,
    end_year: int = 2026,
    volume_type: str = "开仓手数",
) -> pd.DataFrame:
    results_df = pd.DataFrame()
    
    # 1. 优先处理 2015-2025 的 JSON 数据
    if start_year <= 2025:
        if VOLUME_DIST_JSON_PATH.exists():
            try:
                with open(VOLUME_DIST_JSON_PATH, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                
                # 处理嵌套的 "data" 结构
                if isinstance(json_data, dict) and "data" in json_data:
                    type_data = json_data["data"].get(volume_type, [])
                    if type_data:
                        full_df = pd.DataFrame(type_data)
                        # 过滤出请求的年份
                        results_df = full_df[(full_df["年度"] >= start_year) & 
                                             (full_df["年度"] <= min(end_year, 2025))]
                elif isinstance(json_data, list):
                    # 如果是平铺的列表结构
                    full_df = pd.DataFrame(json_data)
                    if not full_df.empty:
                        results_df = full_df[(full_df["年度"] >= start_year) & 
                                             (full_df["年度"] <= min(end_year, 2025)) & 
                                             (full_df["交易手数类型"] == volume_type)]
            except Exception as e:
                print(f"读取手数分布JSON失败: {e}")
        
        # 只有在 JSON 文件不存在或完全没数据时，才针对 2015-2025 回退到数据库
        if results_df.empty:
            results_df = _query_trading_volume_distribution_from_db(start_year, min(end_year, 2025), volume_type)

    # 2. 如果需要 2026 年的数据，仅针对 2026 进行数据库在线查询
    if end_year >= 2026:
        # 强制只查 2026 年
        df_2026 = _query_trading_volume_distribution_from_db(2026, end_year, volume_type)
        results_df = pd.concat([results_df, df_2026], ignore_index=True)

    # 统一输出列维度：年度、交易手数、2015~2026
    if not results_df.empty:
        if "交易手数类型" in results_df.columns:
            if "交易手数" in results_df.columns:
                # 兜底：拼接不同来源数据时，优先使用非空值，避免出现 NaN
                results_df["交易手数"] = results_df["交易手数"].combine_first(results_df["交易手数类型"])
                results_df = results_df.drop(columns=["交易手数类型"])
            else:
                results_df = results_df.rename(columns={"交易手数类型": "交易手数"})

        expected_cols = ["年度", "交易手数"] + [str(y) for y in range(2015, 2026 + 1)]
        for col in expected_cols:
            if col not in results_df.columns:
                results_df[col] = 0.0
        results_df = results_df[expected_cols].sort_values("年度").reset_index(drop=True)

    return results_df

# ====================== 用户画像相关函数 ======================

AGE_ORDER = ["18-25岁", "25-35岁", "35-45岁", "45-55岁", "55岁以上", "未知"]

def _categorize_age(age):
    try:
        if age is None or pd.isna(age):
            return "未知"
        age = int(age)
        if 18 <= age < 25: return "18-25岁"
        if 25 <= age < 35: return "25-35岁"
        if 35 <= age < 45: return "35-45岁"
        if 45 <= age < 55: return "45-55岁"
        if age >= 55: return "55岁以上"
        return "未知"
    except (TypeError, ValueError):
        return "未知"

def _current_age_distribution_from_birth_year(items, value_key):
    """基于缓存的出生年份分布动态计算当前年龄，避免重新查询交易明细。"""
    totals = {group: 0.0 for group in AGE_ORDER}
    for item in items or []:
        birth_year = item.get("birth_year")
        try:
            age = datetime.now().year - int(birth_year)
        except (TypeError, ValueError):
            age = None
        totals[_categorize_age(age)] += float(item.get(value_key, 0) or 0)

    total = sum(totals.values())
    result = []
    for group in AGE_ORDER:
        value = totals[group]
        if value_key == "count":
            value = int(value)
        else:
            value = round(value, 2)
        result.append({
            "age_group": group,
            value_key: value,
            "percentage": f"{(value / total * 100):.2f}%" if total > 0 else "0.00%"
        })
    return result

def calculate_persona_distribution(user_set: set, all_persona_df: pd.DataFrame):
    """根据给定用户集计算画像分布（对齐离线脚本逻辑）"""
    empty_result = {
        "age_distribution": [],
        "opening_age_distribution": [],
        "birth_year_distribution": [],
        "gender_distribution": [],
        "region_distribution": [],
        "source_distribution": []
    }
    if not user_set or all_persona_df.empty:
        return empty_result

    df = all_persona_df[all_persona_df['login_code'].isin(user_set)].copy()
    if df.empty:
        return empty_result

    current_year = datetime.now().year
    df["birth_year_num"] = pd.to_numeric(df["birth_year"], errors="coerce")
    df["reg_year_num"] = pd.to_numeric(df["reg_year"], errors="coerce")
    df['age_group'] = (current_year - df["birth_year_num"]).apply(_categorize_age)
    df['opening_age_group'] = (df["reg_year_num"] - df["birth_year_num"]).apply(_categorize_age)
    df["birth_year_key"] = df["birth_year_num"].apply(
        lambda value: str(int(value)) if pd.notna(value) else "未知"
    )
    
    total_count = len(df)
    df_age = (df.groupby('age_group')['id'].count()
              .reindex(AGE_ORDER, fill_value=0)
              .reset_index().rename(columns={'id': 'count'}))
    df_age['percentage'] = df_age['count'].apply(lambda x: f"{(x/total_count*100):.2f}%" if total_count > 0 else "0.00%")
    df_opening_age = (df.groupby('opening_age_group')['id'].count()
                      .reindex(AGE_ORDER, fill_value=0)
                      .reset_index().rename(columns={'opening_age_group': 'age_group', 'id': 'count'}))
    df_opening_age['percentage'] = df_opening_age['count'].apply(
        lambda x: f"{(x/total_count*100):.2f}%" if total_count > 0 else "0.00%"
    )
    df_birth_year = (df.groupby("birth_year_key")["id"].count().reset_index()
                     .rename(columns={"birth_year_key": "birth_year", "id": "count"}))

    # 性别
    def categorize_gender(appellation):
        if not appellation or pd.isna(appellation):
            return "未知"
        appellation = str(appellation).strip()
        if appellation in ["男", "先生", "M", "男士"]:
            return "男"
        if appellation in ["女", "女士", "小姐", "F"]:
            return "女"
        return "未知"

    df['gender_group'] = df['appellation'].apply(categorize_gender)
    gender_order = ["男", "女", "未知"]
    df_gender = (df.groupby('gender_group')['id'].count()
                 .reindex(gender_order, fill_value=0)
                 .reset_index().rename(columns={'gender_group': 'appellation', 'id': 'count'}))
    
    df_gender['percentage'] = df_gender['count'].apply(lambda x: f"{(x/total_count*100):.2f}%" if total_count > 0 else "0.00%")

    # 地域
    region_map = {
        '11':'北京市','12':'天津市','13':'河北省','14':'山西省','15':'内蒙古自治区',
        '21':'辽宁省','22':'吉林省','23':'黑龙江省','31':'上海市','32':'江苏省',
        '33':'浙江省','34':'安徽省','35':'福建省','36':'江西省','37':'山东省',
        '41':'河南省','42':'湖北省','43':'湖南省','44':'广东省','45':'广西壮族自治区',
        '46':'海南省','50':'重庆市','51':'四川省','52':'贵州省','53':'云南省',
        '54':'西藏自治区','61':'陕西省','62':'甘肃省','63':'青海省','64':'宁夏回族自治区',
        '65':'新疆维吾尔自治区','71':'台湾省','81':'香港特别行政区','82':'澳门特别行政区'
    }
    df['region'] = df['province_code'].map(region_map).fillna("未知")
    df_region = (df.groupby('region')['id'].count()
                 .reset_index().rename(columns={'id': 'count'})
                 .sort_values('count', ascending=False))
    df_region['percentage'] = df_region['count'].apply(lambda x: f"{(x/total_count*100):.2f}%" if total_count > 0 else "0.00%")

    # 开户来源分布
    source_map = {0: "PC", 1: "Mobile", 2: "App"}
    df['source_clean'] = pd.to_numeric(df['source'], errors='coerce')
    df['source_name'] = df['source_clean'].map(source_map).fillna("未知")
    source_order = ["PC", "Mobile", "App", "未知"]
    df_source = (df.groupby('source_name')['id'].count()
                 .reindex(source_order, fill_value=0)
                 .reset_index().rename(columns={'source_name': 'source_name', 'id': 'count'}))
    df_source['percentage'] = df_source['count'].apply(lambda x: f"{(x/total_count*100):.2f}%" if total_count > 0 else "0.00%")

    return {
        "age_distribution": df_age.to_dict(orient='records'),
        "opening_age_distribution": df_opening_age.to_dict(orient='records'),
        "birth_year_distribution": df_birth_year.to_dict(orient='records'),
        "gender_distribution": df_gender.to_dict(orient='records'),
        "region_distribution": df_region.to_dict(orient='records'),
        "source_distribution": df_source.to_dict(orient='records')
    }

def calculate_persona_volume_distribution(login_volumes: pd.DataFrame, all_persona_df: pd.DataFrame):
    """按用户画像维度汇总交易手数及贡献比例。"""
    empty_result = {
        "age_distribution": [],
        "opening_age_distribution": [],
        "birth_year_distribution": [],
        "gender_distribution": [],
        "region_distribution": []
    }
    if login_volumes.empty:
        return empty_result

    volumes = login_volumes[["login_code", "volume"]].copy()
    volumes["volume"] = pd.to_numeric(volumes["volume"], errors="coerce").fillna(0)
    volumes = volumes.groupby("login_code", as_index=False)["volume"].sum()
    volumes = volumes[volumes["volume"] > 0]
    if volumes.empty:
        return empty_result

    persona_columns = ["login_code", "appellation", "birth_year", "reg_year", "province_code"]
    if all_persona_df.empty:
        df = volumes.copy()
        for column in persona_columns[1:]:
            df[column] = None
    else:
        persona = all_persona_df[persona_columns].drop_duplicates("login_code")
        df = volumes.merge(persona, on="login_code", how="left")

    def categorize_gender(appellation):
        if not appellation or pd.isna(appellation):
            return "未知"
        appellation = str(appellation).strip()
        if appellation in ["男", "先生", "M", "男士"]:
            return "男"
        if appellation in ["女", "女士", "小姐", "F"]:
            return "女"
        return "未知"

    region_map = {
        '11':'北京市','12':'天津市','13':'河北省','14':'山西省','15':'内蒙古自治区',
        '21':'辽宁省','22':'吉林省','23':'黑龙江省','31':'上海市','32':'江苏省',
        '33':'浙江省','34':'安徽省','35':'福建省','36':'江西省','37':'山东省',
        '41':'河南省','42':'湖北省','43':'湖南省','44':'广东省','45':'广西壮族自治区',
        '46':'海南省','50':'重庆市','51':'四川省','52':'贵州省','53':'云南省',
        '54':'西藏自治区','61':'陕西省','62':'甘肃省','63':'青海省','64':'宁夏回族自治区',
        '65':'新疆维吾尔自治区','71':'台湾省','81':'香港特别行政区','82':'澳门特别行政区'
    }

    current_year = datetime.now().year
    df["birth_year_num"] = pd.to_numeric(df["birth_year"], errors="coerce")
    df["reg_year_num"] = pd.to_numeric(df["reg_year"], errors="coerce")
    df["age_group"] = (current_year - df["birth_year_num"]).apply(_categorize_age)
    df["opening_age_group"] = (df["reg_year_num"] - df["birth_year_num"]).apply(_categorize_age)
    df["birth_year_key"] = df["birth_year_num"].apply(
        lambda value: str(int(value)) if pd.notna(value) else "未知"
    )
    df["gender_group"] = df["appellation"].apply(categorize_gender)
    df["region"] = df["province_code"].map(region_map).fillna("未知")
    total_volume = float(df["volume"].sum())

    def add_percentages(grouped_df):
        grouped_df["volume"] = grouped_df["volume"].round(2)
        grouped_df["percentage"] = grouped_df["volume"].apply(
            lambda value: f"{(value / total_volume * 100):.2f}%" if total_volume > 0 else "0.00%"
        )
        return grouped_df

    df_age = (df.groupby("age_group")["volume"].sum()
              .reindex(AGE_ORDER, fill_value=0)
              .reset_index())
    df_opening_age = (df.groupby("opening_age_group")["volume"].sum()
                      .reindex(AGE_ORDER, fill_value=0)
                      .reset_index().rename(columns={"opening_age_group": "age_group"}))
    df_birth_year = (df.groupby("birth_year_key")["volume"].sum().reset_index()
                     .rename(columns={"birth_year_key": "birth_year"}))
    df_gender = (df.groupby("gender_group")["volume"].sum()
                 .reindex(["男", "女", "未知"], fill_value=0)
                 .reset_index().rename(columns={"gender_group": "appellation"}))
    df_region = (df.groupby("region", as_index=False)["volume"].sum()
                 .sort_values("volume", ascending=False))

    return {
        "age_distribution": add_percentages(df_age).to_dict(orient="records"),
        "opening_age_distribution": add_percentages(df_opening_age).to_dict(orient="records"),
        "birth_year_distribution": add_percentages(df_birth_year).to_dict(orient="records"),
        "gender_distribution": add_percentages(df_gender).to_dict(orient="records"),
        "region_distribution": add_percentages(df_region).to_dict(orient="records")
    }

@lru_cache(maxsize=1)
def get_all_persona_data():
    """获取所有合法用户的画像基础数据"""
    try:
        sql = text("""
            SELECT 
                id, 
                appellation,
                idPassportNo,
                source,
                reg_time,
                SUBSTRING(idPassportNo, 7, 4) AS birth_year,
                YEAR(reg_time) AS reg_year,
                LEFT(idPassportNo, 2) AS province_code
            FROM js_mt4_account
            WHERE SUBSTRING(`ID`, 1, 3) NOT IN ('168', '568')
              AND `GROUP` NOT IN ('99', 'G99', 'MANAGER')
              AND idPassportNo IS NOT NULL
              AND LENGTH(idPassportNo) = 18
              AND idPassportNo REGEXP '^[0-9]{17}[0-9X]$'
        """)
        with engine_finance.connect() as conn:
            df = pd.read_sql(sql, conn)
        
        df['login_code'] = extract_login_code(df['id'])
        return df[df['login_code'] != '0'].copy()
    except Exception as e:
        print(f"获取全量画像基础数据失败: {e}")
        return pd.DataFrame()

def _get_static_persona_cache():
    """读取 2015-2025 的静态 JSON 缓存"""
    if not USER_PERSONA_JSON_PATH.exists():
        return {}
    try:
        with open(USER_PERSONA_JSON_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return {item['year']: item for item in data}
    except Exception as e:
        print(f"读取用户画像静态缓存失败: {e}")
        return {}

def _apply_age_dimension(results, age_type: str):
    """按请求选择开户年龄或动态当前年龄，不触发交易数据库查询。"""
    output = deepcopy(results)
    for year_data in output:
        for group_data in year_data.get("user_groups", {}).values():
            for persona_key, value_key in (("persona", "count"), ("volume_persona", "volume")):
                persona = group_data.get(persona_key, {})
                if age_type == "opening":
                    opening_distribution = persona.get("opening_age_distribution")
                    if opening_distribution is not None:
                        persona["age_distribution"] = opening_distribution
                else:
                    birth_year_distribution = persona.get("birth_year_distribution")
                    if birth_year_distribution is not None:
                        persona["age_distribution"] = _current_age_distribution_from_birth_year(
                            birth_year_distribution, value_key
                        )
    return output

def get_online_year_persona(year: int, valid_codes: set, activated_codes: set, all_persona_df: pd.DataFrame):
    """在线查询单一年份的用户画像及交易手数画像。"""
    with _persona_cache_lock:
        if year in _persona_online_cache:
            data, ts = _persona_online_cache[year]
            if time.time() - ts < PERSONA_CACHE_TTL:
                return data

    try:
        print(f"正在在线查询 {year} 年用户画像...")
        start_dt = datetime(year, 1, 1)
        target_end_day, end_dt = _get_trading_period_end(year)

        # 1. 获取该年份的交易数据，并按账号汇总交易手数
        df_open_list = []
        df_close_list = []

        if start_dt < BOUNDARY_DATETIME:
            pre_start = start_dt
            pre_end = min(end_dt, BOUNDARY_DATETIME - timedelta(seconds=1))
            sql_open_pre = text("""
                SELECT OPEN_TIME AS trade_time, login, SUM(VOLUME) / 100 AS volume
                FROM v_mt4_trades_filtered
                WHERE OPEN_TIME >= :start_date AND OPEN_TIME <= :end_date
                AND ((LOGIN >= 86600000 AND LOGIN <= 89999999) OR (LOGIN >= 666000000 AND LOGIN <= 699000000))
                AND cmd IN (0, 1)
                GROUP BY OPEN_TIME, login
            """)
            sql_close_pre = text("""
                SELECT CLOSE_TIME AS trade_time, login, SUM(VOLUME) / 100 AS volume
                FROM v_mt4_trades_filtered
                WHERE CLOSE_TIME >= :start_date AND CLOSE_TIME <= :end_date
                AND ((LOGIN >= 86600000 AND LOGIN <= 89999999) OR (LOGIN >= 666000000 AND LOGIN <= 699000000))
                AND cmd IN (0, 1)
                GROUP BY CLOSE_TIME, login
            """)
            with engine_trade_mt4.connect() as conn:
                df_open_pre = pd.read_sql(sql_open_pre, conn, params={"start_date": pre_start, "end_date": pre_end})
                df_close_pre = pd.read_sql(sql_close_pre, conn, params={"start_date": pre_start, "end_date": pre_end})
                df_open_pre.columns = df_open_pre.columns.str.lower()
                df_close_pre.columns = df_close_pre.columns.str.lower()
                if not df_open_pre.empty: df_open_list.append(df_open_pre)
                if not df_close_pre.empty: df_close_list.append(df_close_pre)

        if end_dt >= BOUNDARY_DATETIME:
            post_start = max(start_dt, BOUNDARY_DATETIME)
            post_end = end_dt
            sql_open_post = text("""
                SELECT nearmonth1 AS trade_time, login, jyvulmes AS volume
                FROM js_day_jyr
                WHERE nearmonth1 >= :start_date AND nearmonth1 <= :end_date
            """)
            sql_close_post = text("""
                SELECT nearmonth AS trade_time, login, jyvulmes AS volume
                FROM js_day_jyr
                WHERE nearmonth >= :start_date AND nearmonth <= :end_date
            """)
            with engine_trade.connect() as conn:
                df_open_post = pd.read_sql(sql_open_post, conn, params={"start_date": post_start, "end_date": post_end})
                df_close_post = pd.read_sql(sql_close_post, conn, params={"start_date": post_start, "end_date": post_end})
                df_open_post.columns = df_open_post.columns.str.lower()
                df_close_post.columns = df_close_post.columns.str.lower()
                if not df_open_post.empty: df_open_list.append(df_open_post)
                if not df_close_post.empty: df_close_list.append(df_close_post)

        def normalize_year_trades(parts):
            if not parts:
                return pd.DataFrame(columns=["login_code", "volume"])
            trades = pd.concat(parts, ignore_index=True)
            trades.columns = trades.columns.str.lower()
            trades["trade_time"] = pd.to_datetime(trades["trade_time"], errors="coerce")
            trades = trades.dropna(subset=["trade_time"])
            trades["trading_day"] = trades["trade_time"].apply(get_trading_day)
            trades = trades[
                (pd.to_datetime(trades["trading_day"]).dt.year == year)
                & (pd.to_datetime(trades["trading_day"]).dt.date <= target_end_day)
            ].copy()
            trades["login_code"] = extract_login_code(trades["login"])
            trades["volume"] = pd.to_numeric(trades["volume"], errors="coerce").fillna(0)
            return (trades[trades["login_code"].isin(valid_codes)]
                    .groupby("login_code", as_index=False)["volume"].sum())

        df_open_all = normalize_year_trades(df_open_list)
        df_close_all = normalize_year_trades(df_close_list)

        open_users = set(df_open_all["login_code"].unique())
        close_users = set(df_close_all["login_code"].unique())
        
        all_users = open_users | close_users
        act_open = open_users & activated_codes
        act_close = close_users & activated_codes
        act_all = all_users & activated_codes

        df_all = (pd.concat([df_open_all, df_close_all], ignore_index=True)
                  .groupby("login_code", as_index=False)["volume"].sum())
        df_act_open = df_open_all[df_open_all["login_code"].isin(activated_codes)]
        df_act_close = df_close_all[df_close_all["login_code"].isin(activated_codes)]
        df_act_all = df_all[df_all["login_code"].isin(activated_codes)]

        def build_group(users, volume_df):
            total_volume = round(float(volume_df["volume"].sum()), 2) if not volume_df.empty else 0.0
            return {
                "count": len(users),
                "total_volume": total_volume,
                "persona": calculate_persona_distribution(users, all_persona_df),
                "volume_persona": calculate_persona_volume_distribution(volume_df, all_persona_df)
            }

        year_data = {
            "year": int(year),
            "user_groups": {
                "open_users": build_group(open_users, df_open_all),
                "close_users": build_group(close_users, df_close_all),
                "all_users": build_group(all_users, df_all),
                "activated_open_users": build_group(act_open, df_act_open),
                "activated_close_users": build_group(act_close, df_act_close),
                "activated_all_users": build_group(act_all, df_act_all)
            }
        }

        with _persona_cache_lock:
            _persona_online_cache[year] = (year_data, time.time())
        return year_data
    except Exception as e:
        print(f"在线查询 {year} 年画像失败: {e}")
        return None

def query_user_persona(start_year: int = 2015, end_year: int = 2026, age_type: str = "current"):
    """接口六查询：2015-2025 读 JSON，严格仅 2026 走在线查询"""
    static_cache = _get_static_persona_cache()
    results = []

    # 严格控制：只在请求范围包含 2026 时，才准备在线查询依赖
    needs_online_2026 = (start_year <= 2026 <= end_year)

    valid_codes = set()
    activated_codes = set()
    all_persona_df = pd.DataFrame()

    if needs_online_2026:
        valid_codes = get_valid_login_codes()
        activated_codes = get_activated_login_codes()
        all_persona_df = get_all_persona_data()

    for year in range(start_year, end_year + 1):
        if year <= 2025:
            # 用户要求：2015-2025 直接读取 data/user_persona_stats.json
            if year in static_cache:
                results.append(static_cache[year])
        elif year == 2026:
            # 严格仅 2026：使用在线查询（对齐离线脚本的查询逻辑）
            year_data = get_online_year_persona(year, valid_codes, activated_codes, all_persona_df)
            if year_data:
                results.append(year_data)
        else:
            # 2027+ 不做在线查询（按需求严格限制）
            continue

    return _apply_age_dimension(results, age_type)


# ========== 接口八：入金速度分布 ==========
DEPOSIT_SPEED_BUCKETS = [
    ("0-1小时", 0, 1),
    ("1-2小时", 1, 2),
    ("2-3小时", 2, 3),
    ("3-4小时", 3, 4),
    ("4-5小时", 4, 5),
    ("5-6小时", 5, 6),
    ("6-7小时", 6, 7),
    ("7-8小时", 7, 8),
    ("8-9小时", 8, 9),
    ("9-10小时", 9, 10),
    ("10-11小时", 10, 11),
    ("11-12小时", 11, 12),
    ("12-13小时", 12, 13),
    ("13-14小时", 13, 14),
    ("14-15小时", 14, 15),
    ("15-16小时", 15, 16),
    ("16-17小时", 16, 17),
    ("17-18小时", 17, 18),
    ("18-19小时", 18, 19),
    ("19-20小时", 19, 20),
    ("20-21小时", 20, 21),
    ("21-22小时", 21, 22),
    ("22-23小时", 22, 23),
    ("23-24小时", 23, 24),
    ("24-48小时", 24, 48),
    ("48-72小时", 48, 72),
    ("72小时-7天", 72, 24 * 7),
    ("7-15天", 24 * 7, 24 * 15),
    ("15-30天", 24 * 15, 24 * 30),
]
DEPOSIT_SPEED_LABELS = [label for label, _, _ in DEPOSIT_SPEED_BUCKETS] + ["30天以上"]


def _deposit_speed_bucket_label(hours: float) -> str | None:
    """将注册到首入金的小时差映射到展示分桶；负数视为脏数据排除。"""
    if hours is None or pd.isna(hours) or hours < 0:
        return None
    for label, lo, hi in DEPOSIT_SPEED_BUCKETS:
        if lo <= hours < hi:
            return label
    return "30天以上"


def _empty_deposit_speed_rows() -> list[dict]:
    rows = [{"入金速度分布": label, "人数": 0} for label in DEPOSIT_SPEED_LABELS]
    rows.append({"入金速度分布": "合计", "人数": 0})
    return rows


def _empty_deposit_speed_df() -> pd.DataFrame:
    return pd.DataFrame(_empty_deposit_speed_rows())


def _enrich_deposit_speed_share_columns(df: pd.DataFrame) -> pd.DataFrame:
    """在人数右侧增加占比、累计占比（相对合计人数）。"""
    if df is None or df.empty or "人数" not in df.columns:
        return df
    out = df.copy()
    total_row = out[out["入金速度分布"] == "合计"]
    total = int(total_row["人数"].iloc[0]) if not total_row.empty else int(out["人数"].sum())
    shares = []
    cum = 0.0
    for _, row in out.iterrows():
        label = row["入金速度分布"]
        cnt = int(row["人数"] or 0)
        if label == "合计":
            if total <= 0:
                shares.append(("0.0%", "0.0%"))
            else:
                shares.append(("100.0%", "100.0%"))
            continue
        if total <= 0:
            shares.append(("0.0%", "0.0%"))
            continue
        pct = cnt * 100.0 / total
        cum += pct
        shares.append((f"{pct:.1f}%", f"{min(cum, 100.0):.1f}%"))
    out["占比"] = [s[0] for s in shares]
    out["累计占比"] = [s[1] for s in shares]
    # 有人数时，明细最后一行累计占比对齐为 100.0%，避免浮点尾差
    detail_mask = (out["入金速度分布"] != "合计") & (out["人数"].astype(int) > 0)
    detail_idx = out.index[detail_mask].tolist()
    if total > 0 and detail_idx:
        out.loc[detail_idx[-1], "累计占比"] = "100.0%"
    return out[["入金速度分布", "人数", "占比", "累计占比"]]


def _rows_from_bucket_counts(counts: dict) -> list[dict]:
    rows = [
        {"入金速度分布": label, "人数": int(counts.get(label, 0))}
        for label in DEPOSIT_SPEED_LABELS
    ]
    rows.append({"入金速度分布": "合计", "人数": int(sum(r["人数"] for r in rows))})
    return rows


def _sum_deposit_speed_row_lists(row_lists: list[list[dict]]) -> pd.DataFrame:
    totals = {label: 0 for label in DEPOSIT_SPEED_LABELS}
    for rows in row_lists:
        for row in rows:
            label = row.get("入金速度分布")
            if label in totals:
                totals[label] += int(row.get("人数", 0) or 0)
    return pd.DataFrame(_rows_from_bucket_counts(totals))


def _load_first_deposit_times_df() -> pd.DataFrame:
    first_deposit_sql = """
        SELECT login_code, MIN(event_time) AS first_deposit_time
        FROM (
            SELECT
                CASE
                    WHEN sUserName LIKE '168%%'
                      OR sUserName LIKE '568%%'
                      OR sUserName LIKE '180%%' THEN '0'
                    WHEN LEFT(sUserName, 2) IN ('86', '66') THEN SUBSTRING(sUserName, 3, 6)
                    WHEN LEFT(sUserName, 4) IN ('2000', '2001') THEN SUBSTRING(sUserName, 5, 6)
                    WHEN LEFT(sUserName, 3) = '530' THEN SUBSTRING(sUserName, 4, 6)
                    ELSE '0'
                END AS login_code,
                sTradeTime AS event_time
            FROM js_bank_notify
            WHERE iPayResult = 1
        ) AS successful_deposits
        WHERE login_code <> '0'
          AND CHAR_LENGTH(login_code) = 6
        GROUP BY login_code
    """
    deposit_df = pd.read_sql(first_deposit_sql, con=engine_finance)
    if deposit_df.empty:
        return deposit_df
    deposit_df["login_code"] = deposit_df["login_code"].astype(str)
    deposit_df["first_deposit_time"] = pd.to_datetime(
        deposit_df["first_deposit_time"], errors="coerce"
    )
    return deposit_df.dropna(subset=["first_deposit_time"])


def _load_earliest_reg_df(start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    """排除测试组后，按六位辨识码取最早注册时间。"""
    reg_sql = """
        SELECT id, reg_time
        FROM js_mt4_account
        WHERE reg_time >= %s
          AND reg_time < %s
          AND TRIM(UPPER(COALESCE(`group`, ''))) NOT IN ('99', 'G99', 'MANAGER')
    """
    reg_df = pd.read_sql(
        reg_sql,
        con=engine_finance,
        params=(
            start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )
    if reg_df.empty:
        return reg_df
    reg_df["login_code"] = extract_login_code(reg_df["id"])
    reg_df = reg_df[reg_df["login_code"] != "0"].copy()
    if reg_df.empty:
        return reg_df
    reg_df["reg_time"] = pd.to_datetime(reg_df["reg_time"], errors="coerce")
    reg_df = reg_df.dropna(subset=["reg_time"])
    return (
        reg_df.sort_values("reg_time")
        .groupby("login_code", as_index=False)
        .first()[["login_code", "reg_time"]]
    )


def _load_ever_referred_login_codes() -> set:
    """历史上曾出现在推荐表的六位辨识码（排除测试组）。"""
    referral_sql = """
        SELECT login
        FROM activity.a_jsinferrer
        WHERE TRIM(UPPER(COALESCE(`group`, ''))) NOT IN ('99', 'G99')
        UNION
        SELECT login
        FROM activity.a_jsinferrer24
        WHERE TRIM(UPPER(COALESCE(`group`, ''))) NOT IN ('99', 'G99')
    """
    referrals = pd.read_sql(referral_sql, con=engine_finance)
    if referrals.empty:
        return set()
    referrals["login_code"] = extract_login_code(referrals["login"])
    return set(referrals.loc[referrals["login_code"] != "0", "login_code"].unique())


def _normalize_deposit_speed_referred_mode(referred: str | None) -> str:
    """返回 all / exclude。"""
    text = (referred or "all").strip().lower()
    if text in {"exclude", "exclude_referred", "no", "none", "without"}:
        return "exclude"
    return "all"


def _deposit_speed_rows_from_regs(
    reg_df: pd.DataFrame,
    deposit_df: pd.DataFrame | None = None,
    *,
    exclude_referred: bool = False,
    referred_codes: set | None = None,
) -> list[dict]:
    if reg_df is None or reg_df.empty:
        return _empty_deposit_speed_rows()
    if deposit_df is None:
        deposit_df = _load_first_deposit_times_df()
    if deposit_df is None or deposit_df.empty:
        return _empty_deposit_speed_rows()

    work = reg_df
    if exclude_referred:
        codes = referred_codes if referred_codes is not None else _load_ever_referred_login_codes()
        if codes:
            work = work[~work["login_code"].isin(codes)]
        if work.empty:
            return _empty_deposit_speed_rows()

    merged = work.merge(deposit_df, on="login_code", how="inner")
    if merged.empty:
        return _empty_deposit_speed_rows()

    merged["hours"] = (
        merged["first_deposit_time"] - merged["reg_time"]
    ).dt.total_seconds() / 3600.0
    merged["bucket"] = merged["hours"].apply(_deposit_speed_bucket_label)
    merged = merged.dropna(subset=["bucket"])
    return _rows_from_bucket_counts(merged["bucket"].value_counts().to_dict())


def _attach_reg_trading_day(reg_df: pd.DataFrame) -> pd.DataFrame:
    """为最早注册时间附加伦敦金交易日（夏令时 05:00 / 冬令时 06:00）。"""
    if reg_df is None or reg_df.empty:
        return reg_df
    out = reg_df.copy()
    out["trading_day"] = out["reg_time"].apply(get_trading_day)
    out["trading_day"] = pd.to_datetime(out["trading_day"], errors="coerce").dt.date
    return out.dropna(subset=["trading_day"])


def _query_deposit_speed_from_db(
    start_date: date,
    end_date: date,
    referred_mode: str = "all",
) -> pd.DataFrame:
    """在线查库：按最早注册时间的交易日落在区间内的账户统计入金速度。"""
    # 交易日分界在凌晨 05/06 点，SQL 多取前后缓冲日，再按 trading_day 精确裁剪。
    load_start = datetime(2014, 12, 30)
    end_dt = datetime.combine(end_date + timedelta(days=2), datetime.min.time())
    reg_df = _load_earliest_reg_df(load_start, end_dt)
    if reg_df.empty:
        return _empty_deposit_speed_df()
    reg_df = _attach_reg_trading_day(reg_df)
    reg_df = reg_df[
        (reg_df["trading_day"] >= start_date) & (reg_df["trading_day"] <= end_date)
    ]
    mode = _normalize_deposit_speed_referred_mode(referred_mode)
    return pd.DataFrame(
        _deposit_speed_rows_from_regs(
            reg_df[["login_code", "reg_time"]],
            exclude_referred=(mode == "exclude"),
        )
    )


@lru_cache(maxsize=1)
def _load_deposit_speed_history() -> dict[str, dict]:
    """
    返回 { 'YYYY-MM': { 'all': [...], 'exclude': [...] } }。
    兼容旧格式（月份直接是 list）时仅提供 all。
    """
    if not DEPOSIT_SPEED_JSON_PATH.exists():
        return {}
    try:
        with open(DEPOSIT_SPEED_JSON_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f)
        result: dict[str, dict] = {}
        for month_key, value in payload.get("data", {}).items():
            key = str(month_key)
            if isinstance(value, list):
                result[key] = {"all": value, "exclude": _empty_deposit_speed_rows()}
            elif isinstance(value, dict):
                result[key] = {
                    "all": value.get("all") or _empty_deposit_speed_rows(),
                    "exclude": value.get("exclude")
                    or value.get("exclude_referred")
                    or _empty_deposit_speed_rows(),
                }
        return result
    except Exception as e:
        print(f"读取入金速度历史缓存失败: {e}")
        return {}


def clear_deposit_speed_history_cache():
    _load_deposit_speed_history.cache_clear()


def _iter_year_months(start_ym: date, end_ym: date):
    """按月迭代，start_ym/end_ym 使用该月 1 号。"""
    y, m = start_ym.year, start_ym.month
    end_y, end_m = end_ym.year, end_ym.month
    while (y, m) <= (end_y, end_m):
        yield date(y, m, 1)
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1


def _month_end(day: date) -> date:
    if day.month == 12:
        return date(day.year, 12, 31)
    return date(day.year, day.month + 1, 1) - timedelta(days=1)


def query_deposit_speed_df(
    start_date: date,
    end_date: date,
    referred_mode: str = "all",
) -> pd.DataFrame:
    """
    入金速度分布：首入金时间 - 最早注册时间。
    样本按最早注册时间的伦敦金交易日归年/月（夏令时 05:00 / 冬令时 06:00）。
    referred_mode=all 含被推荐人；exclude 为区间样本减去历史上曾被推荐。
    2015-2025 按月读静态 JSON 后汇总；2026 年起在线查库。
    """
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    mode = _normalize_deposit_speed_referred_mode(referred_mode)

    history = _load_deposit_speed_history()
    history_rows: list[list[dict]] = []
    live_start: date | None = None
    live_end: date | None = None

    cursor = date(start_date.year, start_date.month, 1)
    end_month = date(end_date.year, end_date.month, 1)
    for month_start in _iter_year_months(cursor, end_month):
        month_key = month_start.strftime("%Y-%m")
        seg_start = max(start_date, month_start)
        seg_end = min(end_date, _month_end(month_start))
        if seg_start > seg_end:
            continue

        if month_start < DEPOSIT_SPEED_HISTORY_BOUNDARY:
            cached_month = history.get(month_key) or {}
            cached = cached_month.get(mode)
            if cached:
                history_rows.append(cached)
            continue

        live_start = seg_start if live_start is None else min(live_start, seg_start)
        live_end = seg_end if live_end is None else max(live_end, seg_end)

    parts: list[list[dict]] = list(history_rows)
    if live_start is not None and live_end is not None:
        live_df = _query_deposit_speed_from_db(live_start, live_end, referred_mode=mode)
        parts.append(live_df.to_dict(orient="records"))

    if not parts:
        return _empty_deposit_speed_df()
    return _sum_deposit_speed_row_lists(parts)


def build_deposit_speed_history_payload(
    start_year: int = 2015,
    end_year: int = 2025,
) -> dict:
    """批量生成 2015-2025 按月入金速度分布（含全部 / 不含被推荐人两套）。"""
    start_dt = datetime(start_year - 1, 12, 30)
    end_dt = datetime(end_year + 1, 1, 2)
    print("  加载首入金时间...")
    deposit_df = _load_first_deposit_times_df()
    print(f"  首入金账户数: {len(deposit_df)}")
    print("  加载注册账户...")
    reg_df = _load_earliest_reg_df(start_dt, end_dt)
    print(f"  有效注册辨识码: {len(reg_df)}")
    print("  加载历史被推荐辨识码...")
    referred_codes = _load_ever_referred_login_codes()
    print(f"  曾被推荐辨识码: {len(referred_codes)}")

    data: dict[str, dict] = {}
    empty_pair = {
        "all": _empty_deposit_speed_rows(),
        "exclude": _empty_deposit_speed_rows(),
    }
    if reg_df.empty:
        for month_start in _iter_year_months(date(start_year, 1, 1), date(end_year, 12, 1)):
            data[month_start.strftime("%Y-%m")] = empty_pair
    else:
        reg_df = _attach_reg_trading_day(reg_df)
        reg_df["year_month"] = pd.to_datetime(reg_df["trading_day"]).dt.strftime("%Y-%m")
        for month_start in _iter_year_months(date(start_year, 1, 1), date(end_year, 12, 1)):
            month_key = month_start.strftime("%Y-%m")
            month_regs = reg_df[reg_df["year_month"] == month_key][["login_code", "reg_time"]]
            all_rows = _deposit_speed_rows_from_regs(month_regs, deposit_df)
            exclude_rows = _deposit_speed_rows_from_regs(
                month_regs,
                deposit_df,
                exclude_referred=True,
                referred_codes=referred_codes,
            )
            data[month_key] = {"all": all_rows, "exclude": exclude_rows}
            all_total = next(r["人数"] for r in all_rows if r["入金速度分布"] == "合计")
            ex_total = next(r["人数"] for r in exclude_rows if r["入金速度分布"] == "合计")
            print(f"  完成 {month_key}: 全部={all_total}, 不含推荐={ex_total}")

    return {
        "meta": {
            "logic": "deposit_speed_v3_referred_toggle",
            "start_year": start_year,
            "end_year": end_year,
            "grain": "month",
            "reg_time_calendar": "london_gold_trading_day",
            "variants": ["all", "exclude"],
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        },
        "data": data,
    }


# ========== 接口九：首次接听类型分析 ==========
FOLLOW_TYPE_MAP = {
    "1": "开户回访",
    "10": "7天未入金",
    "15": "14天未入金",
}
FIRST_ANSWER_TYPE_ORDER = ["开户回访", "7天未入金", "14天未入金", "其他"]


def _normalize_first_answer_scope(scope: str | None) -> str:
    text = (scope or "all").strip().lower()
    if text in {"deposit", "in", "deposit_customers"}:
        return "deposit"
    return "all"


def _first_answer_pct(numerator: int, denominator: int) -> str:
    if not denominator:
        return "0.0%"
    return f"{round(numerator * 100 / denominator, 1):.1f}%"


def _map_follow_type(follow_why) -> str:
    if follow_why is None or (isinstance(follow_why, float) and pd.isna(follow_why)):
        return "其他"
    return FOLLOW_TYPE_MAP.get(str(follow_why).strip(), "其他")


def _normalize_account(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)


def _iter_months_inclusive(start_date: date, end_date: date) -> list[str]:
    months = []
    for month_start in _iter_year_months(
        date(start_date.year, start_date.month, 1),
        date(end_date.year, end_date.month, 1),
    ):
        months.append(month_start.strftime("%Y-%m"))
    return months


def _load_first_answer_df(
    start_time: str | None = None,
    end_time: str | None = None,
) -> pd.DataFrame:
    """
    首次接听：follow_state 排除未接通类，disposition=ANSWERED，talkduration>=10，
    每个 account 取 MIN(follow_time)。可选再按首次接听时间过滤（左闭右开）。
    """
    sql = """
        SELECT
            x.account AS account,
            x.follow_time AS first_answer_time,
            x.follow_why
        FROM (
            SELECT a.account, a.follow_time, a.follow_why
            FROM js_mt4_follow_real a
            INNER JOIN (
                SELECT account, MIN(follow_time) AS min_time
                FROM js_mt4_follow_real
                WHERE follow_state NOT IN ('2', '5', '6', '8', '19', '21')
                  AND disposition = 'ANSWERED'
                  AND talkduration >= 10
                GROUP BY account
            ) b ON a.account = b.account
               AND a.follow_time = b.min_time
            WHERE a.follow_state NOT IN ('2', '5', '6', '8', '19', '21')
              AND a.disposition = 'ANSWERED'
              AND a.talkduration >= 10
        ) x
    """
    params: dict = {}
    if start_time and end_time:
        sql += " WHERE x.follow_time >= :start_time AND x.follow_time < :end_time"
        params = {"start_time": start_time, "end_time": end_time}
        sql += " ORDER BY x.follow_time, x.account"
        df = pd.read_sql(text(sql), engine_finance, params=params)
        return _finalize_first_answer_df(df)

    cached = _load_all_first_answer_df_cached()
    return cached.copy()


def _finalize_first_answer_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["account", "first_answer_time", "follow_why", "login_code", "接听类型", "月份"])
    df = df.copy()
    df["account"] = _normalize_account(df["account"])
    df["first_answer_time"] = pd.to_datetime(df["first_answer_time"], errors="coerce")
    df = (
        df.sort_values(["account", "first_answer_time"])
        .drop_duplicates("account", keep="first")
        .reset_index(drop=True)
    )
    df["login_code"] = extract_login_code(df["account"])
    df["接听类型"] = df["follow_why"].apply(_map_follow_type)
    df["月份"] = df["first_answer_time"].dt.strftime("%Y-%m")
    return df


@lru_cache(maxsize=1)
def _load_all_first_answer_df_cached() -> pd.DataFrame:
    sql = """
        SELECT
            x.account AS account,
            x.follow_time AS first_answer_time,
            x.follow_why
        FROM (
            SELECT a.account, a.follow_time, a.follow_why
            FROM js_mt4_follow_real a
            INNER JOIN (
                SELECT account, MIN(follow_time) AS min_time
                FROM js_mt4_follow_real
                WHERE follow_state NOT IN ('2', '5', '6', '8', '19', '21')
                  AND disposition = 'ANSWERED'
                  AND talkduration >= 10
                GROUP BY account
            ) b ON a.account = b.account
               AND a.follow_time = b.min_time
            WHERE a.follow_state NOT IN ('2', '5', '6', '8', '19', '21')
              AND a.disposition = 'ANSWERED'
              AND a.talkduration >= 10
        ) x
        ORDER BY x.follow_time, x.account
    """
    df = pd.read_sql(text(sql), engine_finance)
    return _finalize_first_answer_df(df)


def _load_open_accounts_by_trading_month(start_date: date, end_date: date) -> pd.DataFrame:
    """
    期间开户账号：reg_time 按伦敦金交易日归月（夏令时 05:00 / 冬令时 06:00）。
    SQL 多取前后缓冲日，再按 trading_day 精确裁剪到 [start_date, end_date]。
    """
    start_time = datetime.combine(
        start_date - timedelta(days=1), datetime.min.time()
    ).strftime("%Y-%m-%d %H:%M:%S")
    end_time = datetime.combine(
        end_date + timedelta(days=2), datetime.min.time()
    ).strftime("%Y-%m-%d %H:%M:%S")
    sql = text("""
        SELECT id AS account, reg_time
        FROM js_mt4_account
        WHERE TRIM(UPPER(COALESCE(`group`, ''))) NOT IN ('99', 'G99', 'MANAGER')
          AND reg_time >= :start_time
          AND reg_time < :end_time
    """)
    df = pd.read_sql(sql, engine_finance, params={"start_time": start_time, "end_time": end_time})
    empty_cols = ["account", "reg_time", "login_code", "trading_day", "开户月份"]
    if df.empty:
        return pd.DataFrame(columns=empty_cols)

    df["account"] = _normalize_account(df["account"])
    df["reg_time"] = pd.to_datetime(df["reg_time"], errors="coerce")
    df = df.dropna(subset=["reg_time"]).copy()
    df["login_code"] = extract_login_code(df["account"])
    df["trading_day"] = df["reg_time"].apply(get_trading_day)
    df["trading_day"] = df["trading_day"].apply(_normalize_trading_day_key)
    df = df.dropna(subset=["trading_day"])
    df = df[
        (df["trading_day"] >= start_date) & (df["trading_day"] <= end_date)
    ].copy()
    if df.empty:
        return pd.DataFrame(columns=empty_cols)
    df["开户月份"] = df["trading_day"].apply(lambda value: value.strftime("%Y-%m"))
    return df.reset_index(drop=True)


def _load_real_open_monthly(start_date: date, end_date: date) -> pd.DataFrame:
    df = _load_open_accounts_by_trading_month(start_date, end_date)
    if df.empty:
        return pd.DataFrame(columns=["月份", "real_open_count"])
    return (
        df.groupby("开户月份", dropna=False)["account"]
        .nunique()
        .reset_index(name="real_open_count")
        .rename(columns={"开户月份": "月份"})
    )


def _load_deposit_customers(start_date: date, end_date: date) -> pd.DataFrame:
    """期间开户（排除测试组，按交易日归月）且截至统计时已入金；同月同辨别码保留最早开户账号。"""
    df = _load_open_accounts_by_trading_month(start_date, end_date)
    if df.empty:
        return pd.DataFrame(columns=["account", "reg_time", "login_code", "开户月份"])

    df = df[df["login_code"] != "0"].copy()
    activated = get_activated_login_codes()
    df = df[df["login_code"].isin(activated)].copy()
    return (
        df.sort_values(["开户月份", "login_code", "reg_time"])
        .drop_duplicates(["开户月份", "login_code"], keep="first")
        .reset_index(drop=True)
    )


def _match_deposit_first_answer(
    deposit_df: pd.DataFrame,
    first_answer_df: pd.DataFrame,
) -> pd.DataFrame:
    if deposit_df.empty:
        out = deposit_df.copy()
        out["first_answer_time"] = pd.NaT
        out["接听类型"] = pd.NA
        out["月份"] = out["开户月份"] if "开户月份" in out.columns else pd.NA
        return out

    if first_answer_df.empty:
        out = deposit_df.copy()
        out["first_answer_time"] = pd.NaT
        out["接听类型"] = pd.NA
        out["月份"] = out["开户月份"]
        return out

    fa = first_answer_df.copy()
    fa_by_account = fa[["account", "first_answer_time", "接听类型"]].drop_duplicates("account")
    fa_by_code = (
        fa.sort_values("first_answer_time")
        .drop_duplicates("login_code", keep="first")
        [["login_code", "first_answer_time", "接听类型"]]
        .rename(columns={
            "first_answer_time": "first_answer_time_code",
            "接听类型": "接听类型_code",
        })
    )
    out = deposit_df.merge(fa_by_account, on="account", how="left")
    out = out.merge(fa_by_code, on="login_code", how="left")
    out["first_answer_time"] = out["first_answer_time"].fillna(out["first_answer_time_code"])
    out["接听类型"] = out["接听类型"].fillna(out["接听类型_code"])
    out = out.drop(columns=["first_answer_time_code", "接听类型_code"])
    out["月份"] = out["开户月份"]
    return out


def _build_type_counts(df: pd.DataFrame, id_col: str) -> dict[str, int]:
    counts = {name: 0 for name in FIRST_ANSWER_TYPE_ORDER}
    if df.empty or "接听类型" not in df.columns:
        return counts
    work = df[df["接听类型"].notna()].copy()
    if work.empty:
        return counts
    grouped = work.groupby("接听类型", dropna=False)[id_col].nunique()
    for name in FIRST_ANSWER_TYPE_ORDER:
        counts[name] = int(grouped.get(name, 0))
    return counts


def _query_first_answer_stat_from_db(
    start_date: date,
    end_date: date,
    scope: str = "all",
) -> FirstAnswerStatResp:
    """
    首次接听类型分析。
    scope=all：首次接听时间落在区间内。
    scope=deposit：期间开户且已入金客户，匹配其历史首次接听类型（不限接听月份）。
    开户月份按伦敦金交易日（夏令时 05:00 / 冬令时 06:00）归月。
    """
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    scope_key = _normalize_first_answer_scope(scope)
    start_time = datetime.combine(start_date, datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S")
    end_exclusive = end_date + timedelta(days=1)
    end_time = datetime.combine(end_exclusive, datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S")
    months = _iter_months_inclusive(start_date, end_date)
    date_label = f"{start_date.isoformat()} 至 {end_date.isoformat()}"

    real_open_df = _load_real_open_monthly(start_date, end_date)
    real_open_map = dict(zip(real_open_df["月份"], real_open_df["real_open_count"])) if not real_open_df.empty else {}
    real_open_count = int(sum(real_open_map.get(m, 0) for m in months))

    deposit_count = 0
    deposit_map: dict[str, int] = {}
    monthly_source = pd.DataFrame()
    id_col = "account"

    deposit_df = _load_deposit_customers(start_date, end_date)
    if not deposit_df.empty:
        deposit_map = (
            deposit_df.groupby("开户月份")["login_code"]
            .nunique()
            .to_dict()
        )
        deposit_count = int(sum(deposit_map.get(m, 0) for m in months))

    if scope_key == "deposit":
        first_answer_all = _load_first_answer_df()
        monthly_source = _match_deposit_first_answer(deposit_df, first_answer_all)
        id_col = "login_code"
    else:
        monthly_source = _load_first_answer_df(start_time, end_time)
        if not monthly_source.empty:
            monthly_source = monthly_source[monthly_source["月份"].isin(months)].copy()

    type_counts = _build_type_counts(monthly_source, id_col)
    total_answered = sum(type_counts.values())
    if scope_key == "deposit":
        coverage_rate = _first_answer_pct(total_answered, deposit_count)
    else:
        coverage_rate = "100.0%" if total_answered else "0.0%"

    types = [
        {
            "type_name": name,
            "count": type_counts[name],
            "rate": _first_answer_pct(type_counts[name], total_answered),
        }
        for name in FIRST_ANSWER_TYPE_ORDER
    ]

    monthly_rows: list[dict] = []
    for month in months:
        month_df = monthly_source[monthly_source["月份"] == month] if not monthly_source.empty else monthly_source
        month_counts = _build_type_counts(month_df, id_col)
        month_total = sum(month_counts.values())
        month_deposit = int(deposit_map.get(month, 0))
        month_real = int(real_open_map.get(month, 0))
        for name in FIRST_ANSWER_TYPE_ORDER:
            monthly_rows.append({
                "month": month,
                "type_name": name,
                "count": month_counts[name],
                "rate": _first_answer_pct(month_counts[name], month_total),
                "deposit_count": month_deposit,
                "real_open_count": month_real,
            })

    return FirstAnswerStatResp(
        scope=scope_key,
        start=start_date.isoformat(),
        end=end_date.isoformat(),
        date_label=date_label,
        type_order=FIRST_ANSWER_TYPE_ORDER,
        total_answered=total_answered,
        deposit_count=deposit_count,
        real_open_count=real_open_count,
        coverage_rate=coverage_rate,
        types=types,
        monthly=monthly_rows,
    )


def _first_answer_month_rows_from_cached(cached: dict, month_key: str) -> list[dict]:
    rows = []
    for row in cached.get("monthly") or []:
        if row.get("month") == month_key:
            rows.append({
                "month": row["month"],
                "type_name": row["type_name"],
                "count": int(row.get("count") or 0),
                "rate": row.get("rate") or "0.0%",
                "deposit_count": int(row.get("deposit_count") or 0),
                "real_open_count": int(row.get("real_open_count") or 0),
            })
    if rows:
        return rows
    return [
        {
            "month": month_key,
            "type_name": name,
            "count": 0,
            "rate": "0.0%",
            "deposit_count": 0,
            "real_open_count": 0,
        }
        for name in FIRST_ANSWER_TYPE_ORDER
    ]


def _assemble_first_answer_stat(
    scope: str,
    start_date: date,
    end_date: date,
    monthly_rows: list[dict],
) -> FirstAnswerStatResp:
    months = _iter_months_inclusive(start_date, end_date)
    by_month: dict[str, list[dict]] = {month: [] for month in months}
    for row in monthly_rows:
        month = row.get("month")
        if month in by_month:
            by_month[month].append(row)

    filled: list[dict] = []
    type_counts = {name: 0 for name in FIRST_ANSWER_TYPE_ORDER}
    deposit_count = 0
    real_open_count = 0
    for month in months:
        month_rows = by_month[month]
        counts = {name: 0 for name in FIRST_ANSWER_TYPE_ORDER}
        month_deposit = 0
        month_real = 0
        if month_rows:
            month_deposit = int(month_rows[0].get("deposit_count") or 0)
            month_real = int(month_rows[0].get("real_open_count") or 0)
            for row in month_rows:
                name = row.get("type_name")
                if name in counts:
                    counts[name] += int(row.get("count") or 0)
        month_total = sum(counts.values())
        deposit_count += month_deposit
        real_open_count += month_real
        for name in FIRST_ANSWER_TYPE_ORDER:
            type_counts[name] += counts[name]
            filled.append({
                "month": month,
                "type_name": name,
                "count": counts[name],
                "rate": _first_answer_pct(counts[name], month_total),
                "deposit_count": month_deposit,
                "real_open_count": month_real,
            })

    total_answered = sum(type_counts.values())
    if scope == "deposit":
        coverage_rate = _first_answer_pct(total_answered, deposit_count)
    else:
        coverage_rate = "100.0%" if total_answered else "0.0%"
    types = [
        {
            "type_name": name,
            "count": type_counts[name],
            "rate": _first_answer_pct(type_counts[name], total_answered),
        }
        for name in FIRST_ANSWER_TYPE_ORDER
    ]
    return FirstAnswerStatResp(
        scope=scope,
        start=start_date.isoformat(),
        end=end_date.isoformat(),
        date_label=f"{start_date.isoformat()} 至 {end_date.isoformat()}",
        type_order=FIRST_ANSWER_TYPE_ORDER,
        total_answered=total_answered,
        deposit_count=deposit_count,
        real_open_count=real_open_count,
        coverage_rate=coverage_rate,
        types=types,
        monthly=filled,
    )


@lru_cache(maxsize=1)
def _load_first_answer_history() -> dict[str, dict]:
    if not FIRST_ANSWER_JSON_PATH.exists():
        return {}
    try:
        with open(FIRST_ANSWER_JSON_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return {str(year): value for year, value in (payload.get("data") or {}).items()}
    except Exception as e:
        print(f"读取首次接听历史缓存失败: {e}")
        return {}


def clear_first_answer_history_cache():
    _load_first_answer_history.cache_clear()
    _load_all_first_answer_df_cached.cache_clear()


def query_first_answer_stat(
    start_date: date,
    end_date: date,
    scope: str = "all",
) -> FirstAnswerStatResp:
    """2015-2025 优先读静态 JSON，2026 年起在线查库。"""
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    scope_key = _normalize_first_answer_scope(scope)
    months = _iter_months_inclusive(start_date, end_date)
    history = _load_first_answer_history()
    monthly_rows: list[dict] = []
    live_start: date | None = None
    live_end: date | None = None

    for month_key in months:
        month_start = date.fromisoformat(f"{month_key}-01")
        if month_start < FIRST_ANSWER_HISTORY_BOUNDARY:
            cached_year = history.get(str(month_start.year)) or {}
            cached = cached_year.get(scope_key)
            if cached:
                monthly_rows.extend(_first_answer_month_rows_from_cached(cached, month_key))
                continue
        live_start = month_start if live_start is None else min(live_start, month_start)
        live_end = _month_end(month_start) if live_end is None else max(live_end, _month_end(month_start))

    if live_start is not None and live_end is not None:
        live_end = min(live_end, end_date)
        live_start = max(live_start, start_date)
        live_result = _query_first_answer_stat_from_db(live_start, live_end, scope_key)
        monthly_rows.extend([row.model_dump() for row in live_result.monthly])

    return _assemble_first_answer_stat(scope_key, start_date, end_date, monthly_rows)


def build_first_answer_history_payload(
    start_year: int = 2015,
    end_year: int = 2025,
) -> dict:
    """批量生成 2015-2025 首次接听类型分析（供 regenerate 脚本写入 JSON）。"""
    data: dict[str, dict] = {}
    for year in range(start_year, end_year + 1):
        print(f"  查询年份: {year}")
        start_d = date(year, 1, 1)
        end_d = date(year, 12, 31)
        year_payload: dict[str, dict] = {}
        for scope_key in ("all", "deposit"):
            result = _query_first_answer_stat_from_db(start_d, end_d, scope_key)
            month_keys = {row.month for row in result.monthly}
            if len(month_keys) != 12:
                raise RuntimeError(f"{year} {scope_key} 首次接听缓存不完整：应有 12 个月")
            year_payload[scope_key] = result.model_dump()
            print(
                f"    {scope_key}: 接听={result.total_answered}, "
                f"开户={result.real_open_count}, 已入金={result.deposit_count}"
            )
        data[str(year)] = year_payload
    return {
        "meta": {
            "logic": "first_answer_stat_v3_open_deposit_cols",
            "start_year": start_year,
            "end_year": end_year,
            "scopes": ["all", "deposit"],
            "open_calendar": "london_gold_trading_day",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        },
        "data": data,
    }


CLOSE_PNL_BUCKETS = [
    "15分钟或以内",
    ">15分钟, 60分钟或以内",
    ">60分钟, 24小时或以内",
    ">24小时",
]

CLOSE_PNL_METRICS = [
    {"key": "profit_amt", "label": "i) 盈利交易的总金额", "kind": "amount"},
    {"key": "loss_amt", "label": "ii) 亏损交易的总金额", "kind": "amount"},
    {"key": "amt_ratio", "label": "iii) 盈亏金额比例", "kind": "ratio"},
    {"key": "profit_cnt", "label": "i) 盈利交易的笔数", "kind": "count"},
    {"key": "loss_cnt", "label": "ii) 亏损交易的笔数", "kind": "count"},
    {"key": "cnt_ratio", "label": "iii) 盈亏笔数比例", "kind": "ratio"},
]


def _empty_close_pnl_bucket() -> dict:
    return {
        "profit_amt": 0.0,
        "loss_amt": 0.0,
        "amt_ratio": None,
        "profit_cnt": 0,
        "loss_cnt": 0,
        "cnt_ratio": None,
    }


def _hold_minutes_bucket(minutes: int) -> str:
    if minutes <= 15:
        return CLOSE_PNL_BUCKETS[0]
    if minutes <= 60:
        return CLOSE_PNL_BUCKETS[1]
    if minutes <= 24 * 60:
        return CLOSE_PNL_BUCKETS[2]
    return CLOSE_PNL_BUCKETS[3]


def _close_pnl_ratio(numerator: float, denominator: float):
    if not denominator:
        return None
    return round(abs(numerator / denominator), 2)


def _format_close_pnl_value(metric: dict, raw: dict) -> str:
    value = raw[metric["key"]]
    if metric["kind"] == "count":
        return str(int(value))
    if metric["kind"] == "ratio":
        return "-" if value is None else f"{value:.2f}"
    return f"{float(value):.2f}"


def _month_list(start_month: date, end_month: date) -> list[str]:
    months = []
    year, month = start_month.year, start_month.month
    while (year, month) <= (end_month.year, end_month.month):
        months.append(f"{year:04d}-{month:02d}")
        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1
    return months


def query_close_pnl_stat(start_month: date, end_month: date) -> dict:
    """
    看板十：按平仓交易日归月，按持仓时长分档统计盈亏金额/笔数。
    当月未结束时，统计到前一个已结束的伦敦金交易日（不含进行中的交易日）。
    """
    cutoff_day = last_completed_trading_day()
    start_month = date(start_month.year, start_month.month, 1)
    end_month = date(end_month.year, end_month.month, 1)
    if start_month > end_month:
        start_month, end_month = end_month, start_month

    months = _month_list(start_month, end_month)
    query_start = datetime.combine(start_month - timedelta(days=1), datetime.min.time())
    query_end = datetime.combine(_month_end(end_month) + timedelta(days=2), datetime.min.time())

    sql = text("""
        SELECT OPEN_TIME, CLOSE_TIME, PROFIT, SWAPS, COMMISSION_AGENT
        FROM js_mt5_deals_view
        WHERE CLOSE_TIME >= :start_dt
          AND CLOSE_TIME < :end_dt
          AND OPEN_TIME IS NOT NULL
          AND CLOSE_TIME IS NOT NULL
          AND CLOSE_TIME > '2000-01-01'
    """)

    with engine_profit.connect() as conn:
        df = pd.read_sql(sql, conn, params={"start_dt": query_start, "end_dt": query_end})

    month_data = {
        month: {bucket: _empty_close_pnl_bucket() for bucket in CLOSE_PNL_BUCKETS}
        for month in months
    }

    if not df.empty:
        df.columns = df.columns.str.lower()
        df["open_time"] = pd.to_datetime(df["open_time"], errors="coerce")
        df["close_time"] = pd.to_datetime(df["close_time"], errors="coerce")
        df["profit"] = pd.to_numeric(df["profit"], errors="coerce").fillna(0)
        df["swaps"] = pd.to_numeric(df["swaps"], errors="coerce").fillna(0)
        df["commission_agent"] = pd.to_numeric(df["commission_agent"], errors="coerce").fillna(0)
        df = df.dropna(subset=["open_time", "close_time"])
        df["hold_minutes"] = (
            (df["close_time"] - df["open_time"]).dt.total_seconds() // 60
        ).astype("int64")
        df = df[df["hold_minutes"] >= 0].copy()
        df["pnl"] = df["profit"] + df["swaps"] + df["commission_agent"]
        df["trading_day"] = df["close_time"].map(
            lambda ts: get_trading_day(ts.to_pydatetime())
        )
        df["trading_day"] = pd.to_datetime(df["trading_day"], errors="coerce")
        df = df.dropna(subset=["trading_day"])
        df["month"] = df["trading_day"].dt.strftime("%Y-%m")
        df = df[
            df["month"].isin(months)
            & (df["trading_day"].dt.date <= cutoff_day)
        ].copy()
        df["bucket"] = df["hold_minutes"].map(_hold_minutes_bucket)

        if not df.empty:
            for (month, bucket), group in df.groupby(["month", "bucket"]):
                if month not in month_data or bucket not in month_data[month]:
                    continue
                profit_amt = round(float(group.loc[group["pnl"] > 0, "pnl"].sum()), 2)
                loss_amt = round(float(group.loc[group["pnl"] < 0, "pnl"].sum()), 2)
                profit_cnt = int((group["pnl"] > 0).sum())
                loss_cnt = int((group["pnl"] < 0).sum())
                month_data[month][bucket] = {
                    "profit_amt": profit_amt,
                    "loss_amt": loss_amt,
                    "amt_ratio": _close_pnl_ratio(profit_amt, loss_amt),
                    "profit_cnt": profit_cnt,
                    "loss_cnt": loss_cnt,
                    "cnt_ratio": _close_pnl_ratio(profit_cnt, loss_cnt),
                }

    tables = []
    for month in months:
        rows = []
        for metric in CLOSE_PNL_METRICS:
            rows.append({
                "label": metric["label"],
                "cells": [
                    _format_close_pnl_value(metric, month_data[month][bucket])
                    for bucket in CLOSE_PNL_BUCKETS
                ],
            })
        tables.append({"month": month, "rows": rows})

    return {
        "start": start_month.strftime("%Y-%m"),
        "end": end_month.strftime("%Y-%m"),
        "date_label": f"{start_month.strftime('%Y-%m')} 至 {end_month.strftime('%Y-%m')}",
        "buckets": CLOSE_PNL_BUCKETS,
        "metrics": CLOSE_PNL_METRICS,
        "tables": tables,
        "month_data": month_data,
        "cutoff_day": cutoff_day.isoformat(),
    }
