import pandas as pd
from sqlalchemy import text
from functools import lru_cache
from datetime import datetime, date, timedelta
from config import engine_finance, engine_trade, engine_trade_mt4, BOUNDARY_DATETIME
from utils import extract_login_code
from models import AllStatResp, FirstDepositStatResp
from utils import extract_login_code, get_trading_day
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
    try:
        sql = """
            SELECT DISTINCT login
            FROM js_day_jyr
            WHERE rjtime != '1970-01-01 00:00:00' AND rjtime IS NOT NULL
        """
        df = pd.read_sql(sql, engine_trade)
        if df.empty:
            return set()
        df['login_code'] = extract_login_code(df['login'])
        activated = df[df['login_code'] != '0']['login_code'].unique()
        return set(activated)
    except Exception as e:
        print(f"Failed to load activated login codes: {e}")
        return set()

def query_trading_stat_df(year: int) -> pd.DataFrame:
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

def query_daily_trading_stat_df(start_date: date, end_date: date) -> pd.DataFrame:
    try:
        # 为了包含跨午夜的交易，查询范围向后多取 1 天
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

        result_rows = []
        if not df_open.empty or not df_close.empty:
            df_open = df_open.assign(type='开仓') if not df_open.empty else pd.DataFrame()
            df_close = df_close.assign(type='平仓') if not df_close.empty else pd.DataFrame()

            df_all = pd.concat([df_open, df_close], ignore_index=True)
            df_all['trade_time'] = pd.to_datetime(df_all['trade_time'], errors='coerce')
            # 关键修改：日统计现在基于交易日而非自然日
            df_all['trade_date'] = df_all['trade_time'].apply(get_trading_day)

            valid_codes = get_valid_login_codes()
            if valid_codes:
                df_all['login_code'] = extract_login_code(df_all['login'])
                df_all = df_all[df_all['login_code'].isin(valid_codes)]

            if not df_all.empty:
                activated_codes = get_activated_login_codes()
                day_stats = {}
                for d, group in df_all.groupby('trade_date'):
                    open_users = set(group[group['type'] == '开仓']['login_code'].unique())
                    close_users = set(group[group['type'] == '平仓']['login_code'].unique())
                    all_users = open_users | close_users
                    day_stats[d] = {
                        '开仓人数': len(open_users),
                        '平仓人数': len(close_users),
                        '开仓+平仓 人数': len(all_users),
                        '开仓人数(A)': len(open_users & activated_codes),
                        '平仓人数(A)': len(close_users & activated_codes),
                        '开仓+平仓 人数(A)': len(all_users & activated_codes),
                    }

                empty_row = {'开仓人数': 0, '平仓人数': 0, '开仓+平仓 人数': 0, '开仓人数(A)': 0, '平仓人数(A)': 0, '开仓+平仓 人数(A)': 0}
                d = start_date
                while d <= end_date:
                    row = {'日期': d.strftime('%Y-%m-%d'), **(day_stats.get(d, empty_row))}
                    result_rows.append(row)
                    d += timedelta(days=1)
        
        if not result_rows:
            empty_row = {'开仓人数': 0, '平仓人数': 0, '开仓+平仓 人数': 0, '开仓人数(A)': 0, '平仓人数(A)': 0, '开仓+平仓 人数(A)': 0}
            d = start_date
            while d <= end_date:
                result_rows.append({'日期': d.strftime('%Y-%m-%d'), **empty_row})
                d += timedelta(days=1)

        df_result = pd.DataFrame(result_rows)
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


def query_first_deposit_stat(year: int) -> FirstDepositStatResp:
    """按伦敦金交易日口径返回月度及年度首入金转化统计。"""
    # 1 月 1 日处于冬令时，交易日从当天 06:00 开始。
    # 查询到下一年 06:00，可纳入下一自然年凌晨但仍属于本年末交易日的数据。
    start = f"{year}-01-01 06:00:00"
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
        real_accounts["month"] = real_accounts["trading_date"].apply(
            lambda value: value.strftime("%Y-%m")
        )
        real_by_month = real_accounts.groupby("month").size().to_dict()

    referral_sql = """
        SELECT login, addtime
        FROM activity.a_jsinferrer
        WHERE addtime >= %s
          AND addtime < %s
          AND TRIM(UPPER(COALESCE(`group`, ''))) NOT IN ('99', 'G99')
        UNION
        SELECT login, addtime
        FROM activity.a_jsinferrer24
        WHERE addtime >= %s
          AND addtime < %s
          AND TRIM(UPPER(COALESCE(`group`, ''))) NOT IN ('99', 'G99')
    """
    referrals = pd.read_sql(
        referral_sql,
        con=engine_finance,
        params=(start, end, start, end),
    )
    if referrals.empty:
        referral_codes_by_month = {}
    else:
        referrals["addtime"] = pd.to_datetime(
            referrals["addtime"], errors="coerce"
        )
        referrals = referrals.dropna(subset=["addtime"])
        referrals["trading_date"] = referrals["addtime"].apply(get_trading_day)
        referrals["month"] = referrals["trading_date"].apply(
            lambda value: value.strftime("%Y-%m")
        )
        referrals["login_code"] = extract_login_code(referrals["login"])
        referrals = referrals[referrals["login_code"] != "0"]
        referral_codes_by_month = {
            month: set(group["login_code"])
            for month, group in referrals.groupby("month")
        }

    if first_deposits.empty:
        first_by_month = {}
        referral_first_by_month = {}
    else:
        first_deposits["month"] = first_deposits["trading_date"].apply(
            lambda value: value.strftime("%Y-%m")
        )
        first_by_month = first_deposits.groupby("month").size().to_dict()
        referral_first_by_month = {
            month: int(
                group["login_code"].isin(
                    referral_codes_by_month.get(month, set())
                ).sum()
            )
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
