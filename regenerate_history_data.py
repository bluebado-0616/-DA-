import sys
import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

# 将当前目录加入路径以便导入
sys.path.append(str(Path(__file__).parent))

from services import (
    _query_trading_distribution_from_db,
    _query_trading_volume_distribution_from_db,
    _query_trading_stat_from_db,
    _query_all_stat_data,
    _build_all_stat_trend_df,
    build_deposit_speed_history_payload,
    build_first_deposit_history_payload,
    build_first_answer_history_payload,
    clear_all_stat_history_cache,
    clear_year_stat_history_cache,
    clear_deposit_speed_history_cache,
    clear_first_deposit_history_cache,
    clear_first_answer_history_cache,
    clear_volume_distribution_source_cache,
    get_activated_login_codes,
    get_all_persona_data,
    get_online_year_persona,
    get_valid_login_codes
)

DATA_DIR = Path(__file__).parent / "data"

def regenerate_all_stat(start_year=2015, end_year=2025):
    print(f"正在生成出入金逐日统计 JSON ({start_year}-{end_year})...")
    output_path = DATA_DIR / "all_stat_daily_2015_2025.json"
    checkpoint_path = output_path.with_suffix(".checkpoint.json")
    all_data = {}
    completed_years = set()

    if checkpoint_path.exists():
        with open(checkpoint_path, "r", encoding="utf-8") as f:
            checkpoint = json.load(f)
        if checkpoint.get("start_year") == start_year and checkpoint.get("end_year") == end_year:
            all_data = checkpoint.get("data", {})
            completed_years = {int(year) for year in checkpoint.get("completed_years", [])}
            print(f"  发现检查点，已完成年份: {sorted(completed_years)}")

    for year in range(start_year, end_year + 1):
        if year in completed_years:
            continue
        print(f"  查询年份: {year}")
        start = f"{year}-01-01"
        end = f"{year + 1}-01-01"
        dep_df, wit_df = _query_all_stat_data(start, end)
        trend_df = _build_all_stat_trend_df(dep_df, wit_df, start, end)
        expected_days = (datetime(year + 1, 1, 1) - datetime(year, 1, 1)).days
        if len(trend_df) != expected_days:
            raise RuntimeError(f"{year} 出入金缓存不完整：应有 {expected_days} 天，实际 {len(trend_df)} 天")
        all_data[str(year)] = trend_df.to_dict(orient="records")
        completed_years.add(year)

        checkpoint_temp = checkpoint_path.with_suffix(".json.tmp")
        with open(checkpoint_temp, "w", encoding="utf-8", newline="\n") as f:
            json.dump({
                "start_year": start_year,
                "end_year": end_year,
                "completed_years": sorted(completed_years),
                "data": all_data,
            }, f, ensure_ascii=False, indent=2)
        os.replace(checkpoint_temp, checkpoint_path)
        print(f"  {year} 年完成，检查点已保存")

    temp_path = output_path.with_suffix(".json.tmp")
    with open(temp_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump({
            "meta": {
                "logic": "all_stat_daily_v1",
                "start_year": start_year,
                "end_year": end_year,
                "generated_at": datetime.now().isoformat(timespec="seconds"),
            },
            "data": all_data,
        }, f, ensure_ascii=False, indent=2)
    os.replace(temp_path, output_path)
    if checkpoint_path.exists():
        checkpoint_path.unlink()
    clear_all_stat_history_cache()
    print(f"完成！保存至: {output_path}")


def regenerate_year_stat(start_year=2015, end_year=2025):
    print(f"正在生成月交易人数 JSON ({start_year}-{end_year})...")
    output_path = DATA_DIR / "year_stat_2015_2025.json"
    checkpoint_path = output_path.with_suffix(".checkpoint.json")
    all_data = {}
    completed_years = set()

    if checkpoint_path.exists():
        with open(checkpoint_path, "r", encoding="utf-8") as f:
            checkpoint = json.load(f)
        if checkpoint.get("start_year") == start_year and checkpoint.get("end_year") == end_year:
            all_data = checkpoint.get("data", {})
            completed_years = {int(year) for year in checkpoint.get("completed_years", [])}
            print(f"  发现检查点，已完成年份: {sorted(completed_years)}")

    for year in range(start_year, end_year + 1):
        if year in completed_years:
            continue
        print(f"  查询年份: {year}")
        df = _query_trading_stat_from_db(year)
        if df.empty:
            raise RuntimeError(f"{year} 月交易人数缓存为空")
        if len(df[df["年月"] != "全年"]) != 12:
            raise RuntimeError(f"{year} 月交易人数缓存不完整：应有 12 个月")
        all_data[str(year)] = df.to_dict(orient="records")
        completed_years.add(year)

        checkpoint_temp = checkpoint_path.with_suffix(".json.tmp")
        with open(checkpoint_temp, "w", encoding="utf-8", newline="\n") as f:
            json.dump({
                "start_year": start_year,
                "end_year": end_year,
                "completed_years": sorted(completed_years),
                "data": all_data,
            }, f, ensure_ascii=False, indent=2)
        os.replace(checkpoint_temp, checkpoint_path)
        print(f"  {year} 年完成，检查点已保存")

    temp_path = output_path.with_suffix(".json.tmp")
    with open(temp_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump({
            "meta": {
                "logic": "year_stat_v1",
                "start_year": start_year,
                "end_year": end_year,
                "generated_at": datetime.now().isoformat(timespec="seconds"),
            },
            "data": all_data,
        }, f, ensure_ascii=False, indent=2)
    os.replace(temp_path, output_path)
    if checkpoint_path.exists():
        checkpoint_path.unlink()
    clear_year_stat_history_cache()
    print(f"完成！保存至: {output_path}")


def regenerate_deposit_speed(start_year=2015, end_year=2025):
    print(f"正在生成入金速度分布 JSON ({start_year}-{end_year})...")
    output_path = DATA_DIR / "deposit_speed_2015_2025.json"
    payload = build_deposit_speed_history_payload(start_year, end_year)
    expected_months = (end_year - start_year + 1) * 12
    if len(payload.get("data", {})) != expected_months:
        raise RuntimeError(
            f"入金速度缓存月份数不正确：期望 {expected_months}，实际 {len(payload.get('data', {}))}"
        )
    temp_path = output_path.with_suffix(".json.tmp")
    with open(temp_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(temp_path, output_path)
    clear_deposit_speed_history_cache()
    print(f"完成！保存至: {output_path}")


def regenerate_first_deposit_stat(start_year=2015, end_year=2025):
    print(f"正在生成首入金统计 JSON ({start_year}-{end_year})...")
    output_path = DATA_DIR / "first_deposit_stat_2015_2025.json"
    payload = build_first_deposit_history_payload(start_year, end_year)
    expected_years = end_year - start_year + 1
    if len(payload.get("data", {})) != expected_years:
        raise RuntimeError(
            f"首入金缓存年份数不正确：期望 {expected_years}，实际 {len(payload.get('data', {}))}"
        )
    temp_path = output_path.with_suffix(".json.tmp")
    with open(temp_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(temp_path, output_path)
    clear_first_deposit_history_cache()
    print(f"完成！保存至: {output_path}")


def regenerate_first_answer_stat(start_year=2015, end_year=2025):
    print(f"正在生成首次接听类型分析 JSON ({start_year}-{end_year})...")
    output_path = DATA_DIR / "first_answer_stat_2015_2025.json"
    payload = build_first_answer_history_payload(start_year, end_year)
    expected_years = end_year - start_year + 1
    if len(payload.get("data", {})) != expected_years:
        raise RuntimeError(
            f"首次接听缓存年份数不正确：期望 {expected_years}，实际 {len(payload.get('data', {}))}"
        )
    temp_path = output_path.with_suffix(".json.tmp")
    with open(temp_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(temp_path, output_path)
    clear_first_answer_history_cache()
    print(f"完成！保存至: {output_path}")


def regenerate_trading_distribution():
    print("正在重新生成交易人数分布 JSON (2015-2025)...")
    user_types = ["开仓人数", "平仓人数", "开仓+平仓 人数", "开仓人数(A)", "平仓人数(A)", "开仓+平仓 人数(A)"]
    all_data = {}
    
    for ut in user_types:
        print(f"  查询类型: {ut}")
        # _query_trading_distribution_from_db 内部已经应用了 get_trading_day
        df = _query_trading_distribution_from_db(2015, 2025, user_type=ut)
        all_data[ut] = df.to_dict(orient="records")
    
    output_path = DATA_DIR / "trading_distribution_2015_2025.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"data": all_data}, f, ensure_ascii=False, indent=2)
    print(f"完成！保存至: {output_path}")

def regenerate_volume_distribution(start_year=2015, end_year=2025):
    print(f"正在重新生成交易手数分布 JSON ({start_year}-{end_year})...")
    volume_types = ["开仓手数", "平仓手数", "开仓+平仓 手数", "开仓手数(A)", "平仓手数(A)", "开仓+平仓 手数(A)"]
    output_path = DATA_DIR / "volume_distribution_history_2015_2025.json"
    checkpoint_path = output_path.with_suffix(".checkpoint.json")
    all_data = {}
    completed_years = set()

    if checkpoint_path.exists():
        with open(checkpoint_path, "r", encoding="utf-8") as f:
            checkpoint = json.load(f)
        if checkpoint.get("start_year") == start_year and checkpoint.get("end_year") == end_year:
            all_data = checkpoint.get("data", {})
            completed_years = {int(year) for year in checkpoint.get("completed_years", [])}
            print(f"  发现检查点，已完成年份: {sorted(completed_years)}")
    elif output_path.exists():
        with open(output_path, "r", encoding="utf-8") as f:
            all_data = json.load(f).get("data", {})

    for year in range(start_year, end_year + 1):
        if year in completed_years:
            continue
        print(f"  查询年份: {year}")
        try:
            for volume_type in volume_types:
                # 同一年六种类型复用同一批数据库明细。
                df = _query_trading_volume_distribution_from_db(
                    year, year, volume_type=volume_type, reuse_source=True
                )
                if df.empty:
                    raise RuntimeError(f"{year} {volume_type} 查询结果为空")
                refreshed_rows = df.to_dict(orient="records")
                retained_rows = [
                    row for row in all_data.get(volume_type, [])
                    if int(row.get("年度", 0)) != year
                ]
                all_data[volume_type] = sorted(
                    retained_rows + refreshed_rows, key=lambda row: int(row["年度"])
                )
        finally:
            clear_volume_distribution_source_cache()

        completed_years.add(year)
        checkpoint_temp = checkpoint_path.with_suffix(".json.tmp")
        with open(checkpoint_temp, "w", encoding="utf-8", newline="\n") as f:
            json.dump({
                "start_year": start_year,
                "end_year": end_year,
                "completed_years": sorted(completed_years),
                "data": all_data,
            }, f, ensure_ascii=False, indent=2)
        os.replace(checkpoint_temp, checkpoint_path)
        print(f"  {year} 年完成，检查点已保存")

    validate_volume_persona_totals(volume_data=all_data)
    
    temp_path = output_path.with_suffix(".json.tmp")
    with open(temp_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump({"data": all_data}, f, ensure_ascii=False, indent=2)
    os.replace(temp_path, output_path)
    if checkpoint_path.exists():
        checkpoint_path.unlink()
    print(f"完成！保存至: {output_path}")

def regenerate_user_persona():
    print("正在重新生成用户画像统计 JSON (2015-2025)...")
    valid_codes = get_valid_login_codes()
    activated_codes = get_activated_login_codes()
    all_persona_df = get_all_persona_data()
    output_path = DATA_DIR / "user_persona_stats.json"
    checkpoint_path = output_path.with_suffix(".checkpoint.json")
    results_by_year = {}
    if checkpoint_path.exists():
        with open(checkpoint_path, "r", encoding="utf-8") as f:
            checkpoint_data = json.load(f)
        results_by_year = {int(item["year"]): item for item in checkpoint_data}
        print(f"  发现检查点，已完成年份: {sorted(results_by_year)}")

    for year in range(2015, 2026):
        if year in results_by_year:
            continue
        print(f"  查询年份: {year}")
        year_data = get_online_year_persona(year, valid_codes, activated_codes, all_persona_df)
        if year_data:
            results_by_year[year] = year_data
            checkpoint_temp = checkpoint_path.with_suffix(".json.tmp")
            with open(checkpoint_temp, "w", encoding="utf-8", newline="\n") as f:
                json.dump(
                    [results_by_year[key] for key in sorted(results_by_year)],
                    f, ensure_ascii=False, indent=2
                )
            os.replace(checkpoint_temp, checkpoint_path)
            print(f"  {year} 年完成，检查点已保存")

    if len(results_by_year) != 11:
        raise RuntimeError(f"用户画像历史数据不完整：应有 11 年，实际生成 {len(results_by_year)} 年")

    results = [results_by_year[year] for year in sorted(results_by_year)]
    temp_path = output_path.with_suffix(".json.tmp")
    with open(temp_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    os.replace(temp_path, output_path)
    if checkpoint_path.exists():
        checkpoint_path.unlink()
    print(f"完成！保存至: {output_path}")

def validate_volume_persona_totals(volume_data=None):
    """校验两个看板的年度总手数及组合类型注册年份明细。"""
    persona_path = DATA_DIR / "user_persona_stats.json"
    if volume_data is None:
        volume_path = DATA_DIR / "volume_distribution_history_2015_2025.json"
        with open(volume_path, "r", encoding="utf-8") as f:
            volume_data = json.load(f)["data"]
    with open(persona_path, "r", encoding="utf-8") as f:
        persona_data = {int(item["year"]): item["user_groups"] for item in json.load(f)}

    type_group_map = {
        "开仓手数": "open_users",
        "平仓手数": "close_users",
        "开仓+平仓 手数": "all_users",
        "开仓手数(A)": "activated_open_users",
        "平仓手数(A)": "activated_close_users",
        "开仓+平仓 手数(A)": "activated_all_users",
    }
    volume_rows = {
        volume_type: {int(row["年度"]): row for row in rows}
        for volume_type, rows in volume_data.items()
    }

    mismatches = []
    for volume_type, group_key in type_group_map.items():
        for year in range(2015, 2026):
            volume_total = float(volume_rows[volume_type][year]["交易手数类型"])
            persona_total = float(persona_data[year][group_key]["total_volume"])
            if abs(volume_total - persona_total) > 0.01:
                mismatches.append(
                    f"{year} {volume_type}: 手数看板={volume_total}, 用户画像={persona_total}"
                )

    reg_years = [str(year) for year in range(2015, 2027)]
    for suffix in ("", "(A)"):
        open_type = f"开仓手数{suffix}"
        close_type = f"平仓手数{suffix}"
        combined_type = f"开仓+平仓 手数{suffix}"
        for year in range(2015, 2026):
            for reg_year in reg_years:
                expected = (
                    float(volume_rows[open_type][year].get(reg_year, 0))
                    + float(volume_rows[close_type][year].get(reg_year, 0))
                )
                actual = float(volume_rows[combined_type][year].get(reg_year, 0))
                if abs(actual - expected) > 0.02:
                    mismatches.append(
                        f"{year} {combined_type} 注册年{reg_year}: 明细={actual}, 开仓+平仓={expected}"
                    )

    if mismatches:
        preview = "\n".join(mismatches[:20])
        raise RuntimeError(f"手数缓存一致性校验失败，共 {len(mismatches)} 项：\n{preview}")
    print("校验通过：两个看板六类年度总手数一致，组合类型明细正确。")

if __name__ == "__main__":
    if not DATA_DIR.exists():
        os.makedirs(DATA_DIR)
    
    try:
        regenerate_all_stat()
        regenerate_year_stat()
        regenerate_deposit_speed()
        regenerate_first_deposit_stat()
        regenerate_first_answer_stat()
        regenerate_trading_distribution()
        regenerate_user_persona()
        regenerate_volume_distribution()
        validate_volume_persona_totals()
        print("\n所有 2015-2025 历史数据已根据新交易日规则重新生成完毕！")
    except Exception as e:
        print(f"\n生成过程中出错: {e}")
        import traceback
        traceback.print_exc()
