# -*- coding: utf-8 -*-
"""Update & export Taiwan institutional (三大法人) holdings data.

功能重點：
- 自動抓 TWSE/TPEX 三大法人日交易 + 外資持股；
- 以 inst_baseline.csv 為基準點，校正投信 / 自營商持股；
- 計算三大法人持股比重；
- 計算多視窗變化：5 / 20 / 60 / 120 日；
- 輸出 ranking JSON + 每檔股票時序 JSON。
"""
import json
import os
from io import StringIO
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Optional
import math
import requests
import pandas as pd

from utils_columns import find_col_any, normalize_columns

DATA_DIR = "data"
DOCS_DIR = os.path.join("docs", "data")
TIMESERIES_DIR = os.path.join(DOCS_DIR, "timeseries")
INST_BASELINE_PATH = os.path.join(DATA_DIR, "inst_baseline.csv")

WINDOWS = [5, 20, 60, 120]


# ---------- generic helpers ----------

def ensure_dirs():
    for p in (DATA_DIR, DOCS_DIR, TIMESERIES_DIR):
        os.makedirs(p, exist_ok=True)


def get_taipei_today() -> date:
    tz = ZoneInfo("Asia/Taipei")
    return datetime.now(tz).date()


def is_weekend(d: date) -> bool:
    return d.weekday() >= 5  # 5=Sat, 6=Sun


def get_target_trade_date() -> date:
    """用台北時間的「昨天」，週末往前推到最近一個平日。"""
    today = get_taipei_today()
    target = today - timedelta(days=1)
    while is_weekend(target):
        target -= timedelta(days=1)
    return target


def get_last_date_from_csv(path: str):
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path, usecols=["date"])
    if df.empty:
        return None
    return pd.to_datetime(df["date"]).dt.date.max()


def iter_trading_days(start: date, end: date):
    cur = start
    while cur <= end:
        if not is_weekend(cur):
            yield cur
        cur += timedelta(days=1)


def numeric_series(series: pd.Series, to_float: bool = False) -> pd.Series:
    s = series.astype(str)

    # 1. 去掉千分位
    s = s.str.replace(",", "", regex=False)

    # 2. 統一各種 minus / plus 符號
    s = (
        s.str.replace("\u2212", "-", regex=False)  # ‘−’
         .str.replace("\uFF0D", "-", regex=False)  # 全形『－』
         .str.replace("\uFF0B", "+", regex=False)  # 全形『＋』
         .str.strip()
    )

    # 3. 括號負數: (1234) -> -1234
    mask_paren = s.str.match(r"^\([\d\.]+\)$")
    s.loc[mask_paren] = "-" + s.loc[mask_paren].str.strip("()")

    # 4. 純缺值 token -> 0
    missing_tokens = {"", "nan", "NaN", "None", "--"}
    s = s.where(~s.isin(missing_tokens), "0")

    if to_float:
        return pd.to_numeric(s, errors="coerce").fillna(0.0)

    return pd.to_numeric(s, errors="coerce").fillna(0).astype("Int64")



# ---------- TWSE: T86 (daily flows) ----------

def fetch_twse_t86(trade_date: date) -> pd.DataFrame:
    """三大法人買賣超統計資訊 (T86) for TWSE.

    注意：/fund/T86 是 Big5 編碼，必須用 cp950 解碼，否則欄位會是亂碼。
    """
    datestr = trade_date.strftime("%Y%m%d")
    url = "https://www.twse.com.tw/fund/T86"
    params = {
        "response": "csv",
        "date": datestr,
        "selectType": "ALLBUT0999",
    }
    resp = requests.get(url, params=params, timeout=20)

    csv_text = resp.content.decode("cp950", errors="ignore")
    df = pd.read_csv(StringIO(csv_text), header=1)

    df = df.dropna(how="all", axis=0)
    df = df.dropna(how="all", axis=1)
    df = normalize_columns(df)

    if df.empty or len(df.columns) == 0:
        return pd.DataFrame(
            columns=["date", "code", "name", "foreign_net", "trust_net", "dealer_net", "market"]
        )

    code_col = find_col_any(df, ["證券代號"])
    name_col = find_col_any(df, ["證券名稱"])

    col_foreign_ex_net = find_col_any(
        df,
        [
            "外陸資買賣超股數(不含外資自營商)",
            "外資及陸資(不含外資自營商)買賣超股數",
            "外資及陸資買賣超股數(不含外資自營商)",
        ],
    )
    col_foreign_self_net = find_col_any(df, ["外資自營商買賣超股數"])
    col_trust_net = find_col_any(df, ["投信買賣超股數"])
    col_dealer_net = find_col_any(
        df,
        [
            "自營商買賣超股數合計",
            "自營商買賣超股數",
        ],
    )

    df["code"] = df[code_col].astype(str).str.replace("=", "").str.replace('"', "")
    df["code"] = df["code"].str.strip().str.zfill(4)
    df["name"] = df[name_col].astype(str).str.strip()

    foreign_ex = numeric_series(df[col_foreign_ex_net])
    foreign_self = numeric_series(df[col_foreign_self_net])
    trust_net = numeric_series(df[col_trust_net])
    dealer_net = numeric_series(df[col_dealer_net])

    out = pd.DataFrame(
        {
            "date": trade_date,
            "code": df["code"],
            "name": df["name"],
            "foreign_net": (foreign_ex + foreign_self),
            "trust_net": trust_net,
            "dealer_net": dealer_net,
            "market": "TWSE",
        }
    )

    mask = out["code"].str.match(r"^\d{4,5}[A-Z]*$")
    out = out[mask]
    return out


# ---------- TWSE: MI_QFIIS (foreign holdings) ----------

def fetch_twse_mi_qfiis(trade_date: date) -> pd.DataFrame:
    """外資及陸資投資持股統計 (MI_QFIIS) for TWSE.

    若當日查無資料或格式異常，直接回傳空 DataFrame，避免後續 find_col_any 崩潰。
    """
    datestr = trade_date.strftime("%Y%m%d")
    url = "https://www.twse.com.tw/rwd/zh/fund/MI_QFIIS"
    params = {
        "response": "csv",
        "date": datestr,
        "selectType": "ALLBUT0999",
    }
    resp = requests.get(url, params=params, timeout=20)

    # TWSE MI_QFIIS is Big5/CP950 encoded, not UTF-8
    csv_text = resp.content.decode("cp950", errors="ignore")

    try:
        df = pd.read_csv(StringIO(csv_text), header=1)
    except Exception:
        return pd.DataFrame(
            columns=[
                "date",
                "code",
                "name",
                "market",
                "total_shares",
                "foreign_shares",
                "foreign_ratio",
            ]
        )

    df = df.dropna(how="all", axis=0)
    df = df.dropna(how="all", axis=1)
    df = normalize_columns(df)

    if df.empty or len(df.columns) == 0:
        return pd.DataFrame(
            columns=[
                "date",
                "code",
                "name",
                "market",
                "total_shares",
                "foreign_shares",
                "foreign_ratio",
            ]
        )

    code_col = find_col_any(df, ["證券代號"])
    name_col = find_col_any(df, ["證券名稱"])
    issued_col = find_col_any(df, ["發行股數"])
    foreign_shares_col = find_col_any(df, ["全體外資及陸資持有股數"])
    foreign_ratio_col = find_col_any(df, ["全體外資及陸資持股比率"])

    out = pd.DataFrame()
    out["code"] = df[code_col].astype(str).str.replace("=", "").str.replace('"', "").str.strip().str.zfill(4)
    out["name"] = df[name_col].astype(str).str.strip()

    mask = out["code"].str.match(r"^\d{4,5}[A-Z]*$")
    out = out[mask]

    if out.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "code",
                "name",
                "market",
                "total_shares",
                "foreign_shares",
                "foreign_ratio",
            ]
        )

    out["total_shares"] = numeric_series(df.loc[mask, issued_col])
    out["foreign_shares"] = numeric_series(df.loc[mask, foreign_shares_col])
    out["foreign_ratio"] = numeric_series(df.loc[mask, foreign_ratio_col], to_float=True)
    out["date"] = trade_date
    out["market"] = "TWSE"

    cols = [
        "date",
        "code",
        "name",
        "market",
        "total_shares",
        "foreign_shares",
        "foreign_ratio",
    ]
    return out[cols]


# ---------- TPEX helpers ----------

def roc_date(d: date) -> str:
    y = d.year - 1911
    return f"{y:03d}/{d.month:02d}/{d.day:02d}"


# ---------- TPEX: 三大法人 daily flows ----------

def fetch_tpex_flows(trade_date: date) -> pd.DataFrame:
    """上櫃股票三大法人買賣明細."""
    roc = roc_date(trade_date)
    url = "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php"
    params = {
        "d": roc,
        "l": "zh-tw",
        "o": "htm",
        "s": "0",
        "se": "EW",
        "t": "D",
    }
    resp = requests.get(url, params=params, timeout=20)
    resp.encoding = "utf-8"

    from io import StringIO as _SIO
    tables = pd.read_html(_SIO(resp.text))
    if not tables:
        return pd.DataFrame(
            columns=[
                "date",
                "code",
                "name",
                "foreign_net",
                "trust_net",
                "dealer_net",
                "market",
            ]
        )

    df = tables[0]
    df = normalize_columns(df)

    if df.empty or len(df.columns) == 0:
        return pd.DataFrame(
            columns=[
                "date",
                "code",
                "name",
                "foreign_net",
                "trust_net",
                "dealer_net",
                "market",
            ]
        )

    code_col = find_col_any(df, ["代號"])
    name_col = find_col_any(df, ["名稱"])

    col_foreign_ex_net = find_col_any(
        df,
        [
            "外資及陸資(不含外資自營商)買賣超股數",
            "外資及陸資買賣超股數(不含外資自營商)",
            "外資及陸資買賣超股數",
        ],
    )
    col_foreign_self_net = find_col_any(df, ["外資自營商買賣超股數"])
    col_trust_net = find_col_any(df, ["投信買賣超股數"])
    col_dealer_net = find_col_any(
        df,
        [
            "自營商買賣超股數合計",
            "自營商買賣超股數",
        ],
    )

    df["code"] = df[code_col].astype(str).str.strip().str.zfill(4)
    df["name"] = df[name_col].astype(str).str.strip()

    foreign_ex = numeric_series(df[col_foreign_ex_net])
    foreign_self = numeric_series(df[col_foreign_self_net])
    trust_net = numeric_series(df[col_trust_net])
    dealer_net = numeric_series(df[col_dealer_net])

    out = pd.DataFrame(
        {
            "date": trade_date,
            "code": df["code"],
            "name": df["name"],
            "foreign_net": (foreign_ex + foreign_self),
            "trust_net": trust_net,
            "dealer_net": dealer_net,
            "market": "TPEX",
        }
    )

    mask = out["code"].str.match(r"^\d{4,5}[A-Z]*$")
    out = out[mask]
    return out


# ---------- TPEX: 外資持股比例 (QFII) ----------

def fetch_tpex_qfii(trade_date: date) -> pd.DataFrame:
    """僑外資及陸資持股統計 (上櫃)."""
    url = "https://www.tpex.org.tw/web/stock/3insti/qfii/qfii_result.php"
    params = {
        "l": "zh-tw",
        "o": "data",
    }
    resp = requests.get(url, params=params, timeout=20)
    resp.encoding = "utf-8"

    df = pd.read_csv(StringIO(resp.text))
    df = df.dropna(how="all", axis=0)
    df = df.dropna(how="all", axis=1)
    df = normalize_columns(df)

    if df.empty or len(df.columns) == 0:
        return pd.DataFrame(
            columns=[
                "date",
                "code",
                "name",
                "market",
                "total_shares",
                "foreign_shares",
                "foreign_ratio",
            ]
        )

    code_col = find_col_any(df, ["證券代號", "代號"])
    name_col = find_col_any(df, ["證券名稱", "名稱"])
    shares_col = find_col_any(df, ["發行股數"])
    foreign_shares_col = find_col_any(df, ["僑外資及陸資持有股數"])
    foreign_ratio_col = find_col_any(df, ["僑外資及陸資持股比率"])

    out = pd.DataFrame()
    out["code"] = df[code_col].astype(str).str.strip().str.zfill(4)
    out["name"] = df[name_col].astype(str).str.strip()

    mask = out["code"].str.match(r"^\d{4,5}[A-Z]*$")
    out = out[mask]

    if out.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "code",
                "name",
                "market",
                "total_shares",
                "foreign_shares",
                "foreign_ratio",
            ]
        )

    out["total_shares"] = numeric_series(df.loc[mask, shares_col])
    out["foreign_shares"] = numeric_series(df.loc[mask, foreign_shares_col])
    out["foreign_ratio"] = numeric_series(df.loc[mask, foreign_ratio_col], to_float=True)
    out["date"] = trade_date
    out["market"] = "TPEX"

    cols = [
        "date",
        "code",
        "name",
        "market",
        "total_shares",
        "foreign_shares",
        "foreign_ratio",
    ]
    return out[cols]


# ---------- history append helpers ----------

def append_history(df_new: pd.DataFrame, path: str, key_cols: list[str]) -> pd.DataFrame:
    if os.path.exists(path):
        df_old = pd.read_csv(path, parse_dates=["date"])
        df_old["date"] = pd.to_datetime(df_old["date"]).dt.date
        df_new = df_new.copy()
        df_new["date"] = pd.to_datetime(df_new["date"]).dt.date
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new.copy()
        df_all["date"] = pd.to_datetime(df_all["date"]).dt.date

    df_all = df_all.drop_duplicates(subset=key_cols).sort_values(["date", "code"])
    df_all.to_csv(path, index=False, date_format="%Y-%m-%d")
    return df_all


# ---------- model: holdings estimation ----------

def build_foreign_master(twse: pd.DataFrame, tpex: pd.DataFrame) -> pd.DataFrame:
    all_df = pd.concat([twse, tpex], ignore_index=True)
    if all_df.empty:
        return all_df
    all_df = all_df.sort_values(["code", "date"])
    all_df["date"] = pd.to_datetime(all_df["date"]).dt.date
    all_df = (
        all_df.set_index(["code", "date"])
        .sort_index()
        .groupby(level=0)
        .ffill()
        .reset_index()
    )
    return all_df


def build_estimated_holdings(
    flows: pd.DataFrame,
    foreign_master: pd.DataFrame,
    baseline: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """建立三大法人持股估計，支援 baseline 校正。"""
    flows = flows.copy()
    flows["date"] = pd.to_datetime(flows["date"]).dt.date
    foreign_master = foreign_master.copy()
    foreign_master["date"] = pd.to_datetime(foreign_master["date"]).dt.date

    merged = flows.merge(
        foreign_master[
            [
                "date",
                "code",
                "market",
                "total_shares",
                "foreign_ratio",
            ]
        ],
        on=["date", "code", "market"],
        how="left",
    )

    if baseline is not None and not baseline.empty and "date" in baseline.columns:
        base = baseline.copy()
        base["date"] = pd.to_datetime(
            base["date"], format="%Y-%m-%d", errors="coerce"
        )
        base = base.dropna(subset=["date"])
        if not base.empty:
            base["date"] = base["date"].dt.date
            merged = merged.merge(
                base[["date", "code", "trust_shares_base", "dealer_shares_base"]],
                on=["date", "code"],
                how="left",
            )
        else:
            merged["trust_shares_base"] = pd.NA
            merged["dealer_shares_base"] = pd.NA
    else:
        merged["trust_shares_base"] = pd.NA
        merged["dealer_shares_base"] = pd.NA

    merged = merged.sort_values(["code", "date"])

    # total_shares 先轉 float，避免後面 replace/where 中 extension array 爆炸
    merged["total_shares"] = pd.to_numeric(
        merged["total_shares"], errors="coerce"
    ).fillna(0.0)

    def accumulate(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()
        g["trust_net"] = g["trust_net"].astype(float)
        g["dealer_net"] = g["dealer_net"].astype(float)

        g["trust_cum"] = g["trust_net"].cumsum()
        g["dealer_cum"] = g["dealer_net"].cumsum()

        # baseline 轉數值，避免 NAType
        base_trust = pd.to_numeric(g["trust_shares_base"], errors="coerce").fillna(0.0)
        base_dealer = pd.to_numeric(g["dealer_shares_base"], errors="coerce").fillna(0.0)

        base_trust_ff = base_trust.ffill().fillna(0.0)
        base_dealer_ff = base_dealer.ffill().fillna(0.0)

        trust_cum_at_base = g["trust_cum"].where(g["trust_shares_base"].notna()).ffill().fillna(0.0)
        dealer_cum_at_base = g["dealer_cum"].where(g["dealer_shares_base"].notna()).ffill().fillna(0.0)

        g["trust_shares_est"] = base_trust_ff + (g["trust_cum"] - trust_cum_at_base)
        g["dealer_shares_est"] = base_dealer_ff + (g["dealer_cum"] - dealer_cum_at_base)

        # 若沒有任何 baseline，退化為純 cumsum 模型
        mask_no_base = (base_trust_ff == 0.0) & (base_dealer_ff == 0.0)
        if mask_no_base.all():
            g["trust_shares_est"] = g["trust_cum"]
            g["dealer_shares_est"] = g["dealer_cum"]

        return g

    merged = merged.groupby("code", group_keys=False).apply(accumulate)

    # total_shares 已在前面轉成 float 並 fillna(0.0)
    denom = merged["total_shares"].astype("float64")
    valid = denom > 0.0

    # 先給預設 0，只有有總股數資訊時才算比重
    merged["trust_ratio_est"] = 0.0
    merged["dealer_ratio_est"] = 0.0

    merged.loc[valid, "trust_ratio_est"] = (
            merged.loc[valid, "trust_shares_est"].astype(float) / denom[valid] * 100.0
    )
    merged.loc[valid, "dealer_ratio_est"] = (
            merged.loc[valid, "dealer_shares_est"].astype(float) / denom[valid] * 100.0
    )

    merged["foreign_ratio"] = merged["foreign_ratio"].fillna(0.0)

    merged["three_inst_ratio_est"] = (
            merged["foreign_ratio"] + merged["trust_ratio_est"] + merged["dealer_ratio_est"]
    )
    return merged


def add_change_metrics(merged: pd.DataFrame, windows: list[int]) -> pd.DataFrame:
    merged = merged.sort_values(["code", "date"])

    def add_all(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()
        for w in windows:
            col = f"three_inst_ratio_change_{w}"
            g[col] = g["three_inst_ratio_est"].diff(periods=w)
        return g

    merged = merged.groupby("code", group_keys=False).apply(add_all)
    return merged


# ---------- export JSON ----------

def export_change_rankings(
    merged: pd.DataFrame, windows: list[int], out_dir: str = DOCS_DIR
):
    latest_date = pd.to_datetime(merged["date"]).dt.date.max()
    latest = merged[merged["date"] == latest_date].copy()

    import json
    os.makedirs(out_dir, exist_ok=True)

    for w in windows:
        col = f"three_inst_ratio_change_{w}"
        if col not in latest.columns:
            continue
        tmp = latest[latest[col].notna()].copy()
        if tmp.empty:
            continue

        up = tmp.sort_values(col, ascending=False).head(200)
        down = tmp.sort_values(col, ascending=True).head(200)

        def to_dict_list(df: pd.DataFrame):
            cols = ["code", "name", "market", "three_inst_ratio_est", col]
            records = []
            for _, row in df[cols].iterrows():
                records.append(
                    {
                        "code": row["code"],
                        "name": row["name"],
                        "market": row["market"],
                        "three_inst_ratio": float(row["three_inst_ratio_est"]),
                        "change": float(row[col]),
                    }
                )
            return records

        up_json = to_dict_list(up)
        down_json = to_dict_list(down)

        up_path = os.path.join(out_dir, f"top_three_inst_change_{w}_up.json")
        down_path = os.path.join(out_dir, f"top_three_inst_change_{w}_down.json")

        with open(up_path, "w", encoding="utf-8") as f:
            json.dump(up_json, f, ensure_ascii=False, indent=2)
        with open(down_path, "w", encoding="utf-8") as f:
            json.dump(down_json, f, ensure_ascii=False, indent=2)

def clean_float(val, default: float = 0.0) -> float:
    """把 NaN / inf / 非數字 清成 safe float，避免寫出非法 JSON。"""
    if val is None:
        return default
    try:
        f = float(val)
    except (TypeError, ValueError):
        return default
    if math.isnan(f) or math.isinf(f):
        return default
    return f


def export_timeseries_by_code(
    merged: pd.DataFrame,
    out_root: str = TIMESERIES_DIR,
    primary_window: int = 20,
):
    os.makedirs(out_root, exist_ok=True)

    merged = merged.sort_values(["code", "date"])
    col_change = f"three_inst_ratio_change_{primary_window}"

    for code, g in merged.groupby("code"):
        records = []
        for _, row in g.iterrows():
            date_str = (
                row["date"].strftime("%Y-%m-%d")
                if not isinstance(row["date"], str)
                else row["date"]
            )

            rec = {
                "date": date_str,
                "code": row.get("code", code),
                "name": row.get("name", ""),
                "market": row.get("market", ""),
                "foreign_ratio": clean_float(row.get("foreign_ratio", 0.0)),
                "trust_ratio": clean_float(row.get("trust_ratio_est", 0.0)),
                "dealer_ratio": clean_float(row.get("dealer_ratio_est", 0.0)),
                "three_inst_ratio": clean_float(row.get("three_inst_ratio_est", 0.0)),
            }

            if col_change in g.columns:
                rec[col_change] = clean_float(row.get(col_change, 0.0))

            records.append(rec)

        out_path = os.path.join(out_root, f"{code}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)


# ---------- main orchestration ----------

def main():
    ensure_dirs()

    twse_flows_path = os.path.join(DATA_DIR, "twse_flows.csv")
    tpex_flows_path = os.path.join(DATA_DIR, "tpex_flows.csv")
    twse_foreign_path = os.path.join(DATA_DIR, "twse_foreign.csv")
    tpex_foreign_path = os.path.join(DATA_DIR, "tpex_foreign.csv")

    target_date = get_target_trade_date()
    print(f"[INFO] target trade date (Taipei): {target_date}")

    last_twse_flow = get_last_date_from_csv(twse_flows_path)
    last_tpex_flow = get_last_date_from_csv(tpex_flows_path)
    last_twse_foreign = get_last_date_from_csv(twse_foreign_path)
    last_tpex_foreign = get_last_date_from_csv(tpex_foreign_path)

    def calc_start(last_date):
        if last_date is None:
            approx_start = target_date - timedelta(days=60)
            while is_weekend(approx_start):
                approx_start += timedelta(days=1)
            return approx_start
        else:
            return last_date + timedelta(days=1)

    start_flows_candidates = [
        calc_start(last_twse_flow),
        calc_start(last_tpex_flow),
    ]
    start_foreign_candidates = [
        calc_start(last_twse_foreign),
        calc_start(last_tpex_foreign),
    ]

    start_flows = min([d for d in start_flows_candidates if d is not None])
    start_foreign = min([d for d in start_foreign_candidates if d is not None])

    print(f"[INFO] flows update range:   {start_flows} -> {target_date}")
    print(f"[INFO] foreign update range: {start_foreign} -> {target_date}")

    # --- update flows ---
    flows_new_list = []
    for d in iter_trading_days(start_flows, target_date):
        print(f"[INFO] fetching flows for {d} ...")
        try:
            twse_df = fetch_twse_t86(d)
        except Exception as e:  # noqa: BLE001
            print(f"[WARN] TWSE T86 fetch failed at {d}: {e}")
            twse_df = pd.DataFrame()

        try:
            tpex_df = fetch_tpex_flows(d)
        except Exception as e:  # noqa: BLE001
            print(f"[WARN] TPEX flows fetch failed at {d}: {e}")
            tpex_df = pd.DataFrame()

        if not twse_df.empty:
            flows_new_list.append(twse_df)
        if not tpex_df.empty:
            flows_new_list.append(tpex_df)

    if flows_new_list:
        flows_new = pd.concat(flows_new_list, ignore_index=True)
        twse_new = flows_new[flows_new["market"] == "TWSE"].copy()
        tpex_new = flows_new[flows_new["market"] == "TPEX"].copy()

        if not twse_new.empty:
            twse_flows_all = append_history(
                twse_new, twse_flows_path, ["date", "code", "market"]
            )
        else:
            twse_flows_all = (
                pd.read_csv(twse_flows_path) if os.path.exists(twse_flows_path) else pd.DataFrame()
            )

        if not tpex_new.empty:
            tpex_flows_all = append_history(
                tpex_new, tpex_flows_path, ["date", "code", "market"]
            )
        else:
            tpex_flows_all = (
                pd.read_csv(tpex_flows_path) if os.path.exists(tpex_flows_path) else pd.DataFrame()
            )
    else:
        print("[INFO] no new flows fetched.")
        twse_flows_all = (
            pd.read_csv(twse_flows_path) if os.path.exists(twse_flows_path) else pd.DataFrame()
        )
        tpex_flows_all = (
            pd.read_csv(tpex_flows_path) if os.path.exists(tpex_flows_path) else pd.DataFrame()
        )

    # --- update foreign holdings ---
    foreign_new_list_twse = []
    foreign_new_list_tpex = []

    for d in iter_trading_days(start_foreign, target_date):
        print(f"[INFO] fetching foreign holdings for {d} ...")
        try:
            twse_f = fetch_twse_mi_qfiis(d)
        except Exception as e:  # noqa: BLE001
            print(f"[WARN] TWSE MI_QFIIS fetch failed at {d}: {e}")
            twse_f = pd.DataFrame()

        try:
            tpex_f = fetch_tpex_qfii(d)
        except Exception as e:  # noqa: BLE001
            print(f"[WARN] TPEX QFII fetch failed at {d}: {e}")
            tpex_f = pd.DataFrame()

        if not twse_f.empty:
            foreign_new_list_twse.append(twse_f)
        if not tpex_f.empty:
            foreign_new_list_tpex.append(tpex_f)

    if foreign_new_list_twse:
        twse_foreign_new = pd.concat(foreign_new_list_twse, ignore_index=True)
        twse_foreign_all = append_history(
            twse_foreign_new, twse_foreign_path, ["date", "code", "market"]
        )
    else:
        twse_foreign_all = (
            pd.read_csv(twse_foreign_path) if os.path.exists(twse_foreign_path) else pd.DataFrame()
        )

    if foreign_new_list_tpex:
        tpex_foreign_new = pd.concat(foreign_new_list_tpex, ignore_index=True)
        tpex_foreign_all = append_history(
            tpex_foreign_new, tpex_foreign_path, ["date", "code", "market"]
        )
    else:
        tpex_foreign_all = (
            pd.read_csv(tpex_foreign_path) if os.path.exists(tpex_foreign_path) else pd.DataFrame()
        )

    if twse_flows_all.empty and tpex_flows_all.empty:
        print("[WARN] no flows history available, aborting model/export.")
        return

    flows_all = pd.concat(
        [df for df in (twse_flows_all, tpex_flows_all) if not df.empty],
        ignore_index=True,
    )
    if twse_foreign_all.empty and tpex_foreign_all.empty:
        print("[WARN] no foreign holdings history available, aborting model/export.")
        return

    foreign_master = build_foreign_master(twse_foreign_all, tpex_foreign_all)
    if foreign_master.empty:
        print("[WARN] foreign_master is empty, aborting model/export.")
        return

    # baseline 校正
    if os.path.exists(INST_BASELINE_PATH):
        baseline_df = pd.read_csv(INST_BASELINE_PATH, comment="#")
        if baseline_df.empty:
            baseline_df = None
    else:
        baseline_df = None

    merged = build_estimated_holdings(flows_all, foreign_master, baseline=baseline_df)
    merged = add_change_metrics(merged, windows=WINDOWS)

    export_change_rankings(merged, windows=WINDOWS, out_dir=DOCS_DIR)
    export_timeseries_by_code(merged, out_root=TIMESERIES_DIR, primary_window=20)

    print("[INFO] update_all.py completed successfully.")


if __name__ == "__main__":
    main()
