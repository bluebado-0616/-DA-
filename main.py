import json
from pathlib import Path
import uvicorn
import os
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
from datetime import datetime, date, timedelta

# 导入自定义模块
from config import DAILY_STAT_PAGE_SIZE
from models import AllStatResp, FirstDepositStatResp
from utils import (
    trading_distribution_semaphore,
    year_stat_semaphore,
    daily_stat_semaphore,
    user_persona_semaphore,
    cache_load_df,
    cache_save_df,
    _cache_lock
)
import services

app = FastAPI(title="统计接口服务", version="1.0.0")

# 初始化模板引擎
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))


# ========== 接口一：入金+出金+净入金汇总（JSON） ==========
@app.get("/pyapi/all/stat/data", response_model=AllStatResp, include_in_schema=False)
@app.get("/pyapi/all/stat", response_model=AllStatResp, summary="入金+出金+净入金汇总")
def all_stat_data(
    start: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$", examples=["2021-01-01"]),
    end: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$", examples=["2026-01-01"])
):
    # 接口一通常数据量小，但为了统一，可以加简单的内存缓存或保持现状
    try:
        return services.query_all_stat(start, end)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@app.get("/pyapi/all/stat/dashboard", response_class=HTMLResponse, summary="入金出金图表和趋势图")
def all_stat_dashboard(
    request: Request,
    start: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
):
    today = date.today()
    yesterday = today - timedelta(days=1)
    start_d = date.fromisoformat(start) if start else date(yesterday.year, yesterday.month, 1)
    end_d = date.fromisoformat(end) if end else yesterday
    start_d = min(start_d, yesterday)
    end_d = min(end_d, yesterday)
    if start_d > end_d:
        start_d, end_d = end_d, start_d

    # services 延续接口一的 end 不包含约定；看板日期选择对用户按首尾日期均包含。
    end_exclusive = end_d + timedelta(days=1)
    summary, trend_df = services.query_all_stat_dashboard(
        start_d.isoformat(), end_exclusive.isoformat()
    )
    return templates.TemplateResponse("all_stat.html", {
        "request": request,
        "start": start_d.isoformat(),
        "end": end_d.isoformat(),
        "date_label": f"{start_d} 至 {end_d}",
        "summary": summary,
        "chart_data": trend_df.to_json(orient="records", force_ascii=False),
        "uses_historical_cache": start_d < date(2026, 1, 1),
    })


# ========== 接口二：日交易人数（HTML + 缓存 + 并发保护 + 分页） ==========
@app.get("/pyapi/day/stat", response_class=HTMLResponse, summary="日交易人数")
def daily_stat_html(
    request: Request,
    start: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    page: int = Query(1, ge=1),
):
    today = date.today()
    yesterday = today - timedelta(days=1)

    if start is None and end is None:
        start_d = date(today.year, today.month, 1)
        end_d = yesterday
    else:
        start_d = date.fromisoformat(start) if start else date(today.year, today.month, 1)
        end_d = date.fromisoformat(end) if end else yesterday
        if end_d > yesterday: end_d = yesterday
        if start_d > end_d: start_d, end_d = end_d, start_d

    date_label = f"{start_d} 至 {end_d}"
    cache_payload = {"start": start_d.isoformat(), "end": end_d.isoformat(), "logic": "daily_stat_v1"}

    if not daily_stat_semaphore.acquire(blocking=False):
        raise HTTPException(status_code=429, detail="日统计计算中，请稍后重试")

    try:
        with _cache_lock:
            df = cache_load_df("daily_stat", cache_payload, cache_subdir="daily_stat")

        if df is None or df.empty:
            df = services.query_daily_trading_stat_df(start_d, end_d)
            with _cache_lock:
                cache_save_df("daily_stat", cache_payload, df, ttl_seconds=3600 * 6, cache_subdir="daily_stat")
    finally:
        daily_stat_semaphore.release()

    if df.empty:
        return templates.TemplateResponse("day_stat.html", {
            "request": request, "date_label": date_label, "table_html": "<p>暂无数据</p>",
            "page": 1, "total_pages": 1, "page_info": "共 0 行", "DAILY_STAT_PAGE_SIZE": DAILY_STAT_PAGE_SIZE,
            "start": start_d.isoformat(), "end": end_d.isoformat(), "chart_data": "[]",
            "pagination_url": lambda p: f"/pyapi/day/stat?page={p}&start={start_d}&end={end_d}"
        })

    total_rows = len(df)
    total_pages = (total_rows + DAILY_STAT_PAGE_SIZE - 1) // DAILY_STAT_PAGE_SIZE
    page = min(page, max(1, total_pages))
    df_page = df.iloc[(page - 1) * DAILY_STAT_PAGE_SIZE : page * DAILY_STAT_PAGE_SIZE]
    table_html = df_page.to_html(index=False, classes='stat-table', float_format='%.0f')

    return templates.TemplateResponse("day_stat.html", {
        "request": request, "date_label": date_label, "table_html": table_html,
        "page": page, "total_pages": total_pages, "DAILY_STAT_PAGE_SIZE": DAILY_STAT_PAGE_SIZE,
        "start": start_d.isoformat(), "end": end_d.isoformat(),
        "chart_data": df.sort_values("日期").to_json(orient="records", force_ascii=False),
        "page_info": f"第 {page}/{total_pages} 页",
        "pagination_url": lambda p: f"/pyapi/day/stat?page={p}&start={start_d}&end={end_d}"
    })


# ========== 接口三：月交易人数（HTML + 缓存 + 并发保护） ==========
@app.get("/pyapi/year/stat", response_class=HTMLResponse, summary="月交易人数")
def trading_stat_html(
    request: Request,
    year: Optional[int] = Query(None, ge=2000, le=2035, description="统计年份")
):
    if year is None:
        year = datetime.today().year

    cache_payload = {"year": int(year), "logic": "year_stat_v1"}

    if not year_stat_semaphore.acquire(blocking=False):
        raise HTTPException(status_code=429, detail="年度统计计算中，请稍后重试")

    try:
        with _cache_lock:
            df = cache_load_df("year_stat", cache_payload, cache_subdir="year_stat")

        if df is None or df.empty:
            df = services.query_trading_stat_df(year)
            with _cache_lock:
                cache_save_df("year_stat", cache_payload, df, ttl_seconds=3600 * 12, cache_subdir="year_stat")
    finally:
        year_stat_semaphore.release()

    if df.empty:
        table_html = "<h2>暂无数据</h2>"
    else:
        table_html = df.to_html(index=False, classes='stat-table', float_format='%.0f')
        table_html = table_html.replace('<tr>\n    <td>全年</td>', '<tr style="font-weight:bold; background-color:#e6f3ff;">\n    <td>全年</td>')

    return templates.TemplateResponse("year_stat.html", {
        "request": request,
        "year": year,
        "table_html": table_html,
        "chart_data": df.to_json(orient="records", force_ascii=False) if not df.empty else "[]",
    })



# ========== 接口四：交易人数分布（HTML + 缓存 + 并发保护） ==========
@app.get("/pyapi/user/distribution", response_class=HTMLResponse, summary="交易人数分布")
def trading_distribution_html(
    request: Request,
    start_year: int = Query(2015, ge=2000, le=2035),
    end_year: int = Query(2026, ge=2000, le=2035),
    user_type: str = Query("开仓人数"),
):
    if start_year > end_year:
        start_year, end_year = end_year, start_year

    if not trading_distribution_semaphore.acquire(blocking=False):
        raise HTTPException(status_code=429, detail="计算中，请稍后重试")

    # 缓存 Key 包含所有参数，确保唯一性
    cache_payload = {"start": start_year, "end": end_year, "type": user_type, "logic": "user_dist_v4"}

    try:
        with _cache_lock:
            df = cache_load_df("user_dist", cache_payload, cache_subdir="user_dist")

        if df is None or df.empty:
            # services 层现在已经优化：2015-2025 读 JSON，2026 读 DB
            df = services.query_trading_distribution_df(start_year, end_year, user_type=user_type)
            with _cache_lock:
                cache_save_df("user_dist", cache_payload, df, ttl_seconds=3600 * 24, cache_subdir="user_dist")
    finally:
        trading_distribution_semaphore.release()

    if df.empty:
        table_html = "<p>暂无数据</p>"
    else:
        table_html = df.to_html(index=False, classes="stat-table", float_format="%.0f")

    return templates.TemplateResponse("distribution.html", {
        "request": request, "start_year": start_year, "end_year": end_year,
        "user_type": user_type, "table_html": table_html,
        "chart_data": df.to_json(orient="records", force_ascii=False) if not df.empty else "[]",
    })


# ========== 接口五：交易手数分布（HTML + 缓存 + 并发保护） ==========
@app.get("/pyapi/volume/distribution", response_class=HTMLResponse, summary="交易手数分布")
def trading_volume_distribution_html(
    request: Request,
    start_year: int = Query(2015, ge=2000, le=2035),
    end_year: int = Query(2026, ge=2000, le=2035),
    volume_type: str = Query("开仓手数"),
):
    if start_year > end_year:
        start_year, end_year = end_year, start_year

    if not trading_distribution_semaphore.acquire(blocking=False):
        raise HTTPException(status_code=429, detail="计算中，请稍后重试")

    cache_payload = {"start": start_year, "end": end_year, "type": volume_type, "logic": "vol_dist_v3"}

    try:
        with _cache_lock:
            df = cache_load_df("vol_dist", cache_payload, cache_subdir="vol_dist")

        if df is None or df.empty:
            # services 层优化：2015-2025 读 JSON，2026 读 DB
            df = services.query_trading_volume_distribution_df(start_year, end_year, volume_type=volume_type)
            with _cache_lock:
                cache_save_df("vol_dist", cache_payload, df, ttl_seconds=3600 * 24, cache_subdir="vol_dist")
    finally:
        trading_distribution_semaphore.release()

    if df.empty:
        table_html = "<p>暂无数据</p>"
    else:
        if "交易手数类型" in df.columns: 
            df = df.rename(columns={"交易手数类型": "交易手数"})
        table_html = df.to_html(index=False, classes="stat-table", float_format="%.2f")

    return templates.TemplateResponse("volume_distribution.html", {
        "request": request, "start_year": start_year, "end_year": end_year,
        "volume_type": volume_type, "table_html": table_html,
        "chart_data": df.to_json(orient="records", force_ascii=False) if not df.empty else "[]",
    })


# ========== 接口六：用户画像统计（HTML + 缓存 + 并发保护） ==========
@app.get("/pyapi/user/persona", response_class=HTMLResponse, summary="用户画像统计")
def user_persona_html(
    request: Request,
    start_year: int = Query(2015, ge=2015, le=2035),
    end_year: int = Query(2026, ge=2015, le=2035),
    user_type: str = Query("开仓+平仓 人数", description="交易人数或交易手数类型"),
    age_type: str = Query("current", pattern="^(current|opening)$", description="当前年龄或开户年龄")
):
    if not user_persona_semaphore.acquire(blocking=False):
        raise HTTPException(status_code=429, detail="计算中，请稍后重试")

    try:
        results = services.query_user_persona(start_year, end_year, age_type=age_type)
    finally:
        user_persona_semaphore.release()

    # 映射前端选择的类型到后端数据结构中的 key
    type_map = {
        "开仓人数": "open_users",
        "平仓人数": "close_users",
        "开仓+平仓 人数": "all_users",
        "开仓人数(A)": "activated_open_users",
        "平仓人数(A)": "activated_close_users",
        "开仓+平仓 人数(A)": "activated_all_users",
        "开仓手数": "open_users",
        "平仓手数": "close_users",
        "开仓+平仓 手数": "all_users",
        "开仓手数(A)": "activated_open_users",
        "平仓手数(A)": "activated_close_users",
        "开仓+平仓 手数(A)": "activated_all_users"
    }
    target_key = type_map.get(user_type, "all_users")
    is_volume = "手数" in user_type

    return templates.TemplateResponse("user_persona.html", {
        "request": request, 
        "start_year": start_year, 
        "end_year": end_year, 
        "user_type": user_type,
        "age_type": age_type,
        "target_key": target_key,
        "is_volume": is_volume,
        "results_json": json.dumps(results, ensure_ascii=False)
    })


# ========== 接口七：首入金统计（HTML 看板 + JSON 数据） ==========
@app.get(
    "/pyapi/first/deposit/stat/data",
    response_model=FirstDepositStatResp,
    include_in_schema=False,
)
def first_deposit_stat_data(
    year: Optional[int] = Query(None, ge=2015, le=2035),
):
    try:
        return services.query_first_deposit_stat(year or datetime.today().year)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@app.get(
    "/pyapi/first/deposit/stat",
    response_class=HTMLResponse,
    summary="首入金统计",
)
def first_deposit_stat_html(
    request: Request,
    year: Optional[int] = Query(
        None,
        ge=2015,
        le=2035,
        description="统计年份，默认当前年份",
    ),
):
    try:
        selected_year = year or datetime.today().year
        result = services.query_first_deposit_stat(selected_year)
        return templates.TemplateResponse("first_deposit_stat.html", {
            "request": request,
            "year": selected_year,
            "result": result,
            "chart_data": json.dumps(
                [row.model_dump() for row in result.list],
                ensure_ascii=False,
            ),
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
