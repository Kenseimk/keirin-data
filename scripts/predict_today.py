# -*- coding: utf-8 -*-
import sys, io as _io
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = _io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = _io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

"""
当日の競輪予想を生成してDiscordに投稿する
フィルター: top_score>=95 & score_gap>=2 & 7車限定
実行タイミング: JST 10:00 / 12:00 / 14:00 / 16:00
  → 各実行で「次の2時間に始まる」レース番号帯だけ投稿

race_no の時間帯推定（1日12レース想定）:
  10:00実行 → race_no 1-3
  12:00実行 → race_no 4-6
  14:00実行 → race_no 7-9
  16:00実行 → race_no 10以上
"""

import os, glob, re, warnings, requests, argparse
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
import lightgbm as lgb
from pathlib import Path

warnings.filterwarnings("ignore")

JST = timezone(timedelta(hours=9))

# ========== 引数 ==========
parser = argparse.ArgumentParser()
parser.add_argument("--date", default=None, help="対象日 (例: 2026-03-31)。省略時は今日")
parser.add_argument("--hour", type=int, default=None, help="実行時刻(JST時)。省略時は現在時刻")
parser.add_argument("--all",  action="store_true", help="全レース帯を投稿（テスト用）")
args = parser.parse_args()

now_jst     = datetime.now(JST)
TARGET_DATE = args.date or now_jst.strftime("%Y-%m-%d")
HOUR_JST    = args.hour if args.hour is not None else now_jst.hour

# 時間帯 → race_no 範囲
def race_no_range(hour):
    if hour < 12:   return (1,  3)
    elif hour < 14: return (4,  6)
    elif hour < 16: return (7,  9)
    else:           return (10, 99)

RACE_RANGE = (1, 99) if args.all else race_no_range(HOUR_JST)
print(f"対象日: {TARGET_DATE}  JST {HOUR_JST}時台  race_no {RACE_RANGE[0]}〜{RACE_RANGE[1]}")

DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_URL", "")

# ========== Discord投稿 ==========
def post_discord(content: str):
    if not DISCORD_WEBHOOK:
        print("[Discord] WEBHOOK未設定\n" + content)
        return
    r = requests.post(DISCORD_WEBHOOK, json={"content": content}, timeout=10)
    if r.status_code not in (200, 204):
        print(f"[Discord] 投稿失敗: {r.status_code} {r.text}")

# ========== データ読み込み ==========
DATA_DIR = Path("keirin_data")
files = [f for f in sorted(glob.glob(str(DATA_DIR / "202[3456]_*.csv"))) if "sample" not in f]
df_all = pd.concat([pd.read_csv(f, encoding="utf-8-sig") for f in files], ignore_index=True)
print(f"総行数: {len(df_all):,}")

def parse_payout(s):
    if pd.isna(s) or str(s).strip() == "": return None, None
    m = re.search(r"(\d+[-=]\d+(?:[-=]\d+)?)\s+([\d,]+)円", str(s).strip())
    if m: return m.group(1), int(m.group(2).replace(",", ""))
    return None, None

df_all[["ni_combo","ni_payout"]]   = df_all["ni_sha_tan"].apply(lambda x: pd.Series(parse_payout(x)))
df_all[["san_combo","san_payout"]] = df_all["san_ren_tan"].apply(lambda x: pd.Series(parse_payout(x)))

CLASS_MAP = {"S1":4,"S2":3,"A1":2,"A2":1,"B":0}
STYLE_MAP  = {"逃":5,"捲":4,"両":3,"差":2,"追":1,"マ":0}

df_all["player_key"]    = df_all["player_name"].astype(str)+"_"+df_all["term"].astype(str)
df_all["rank_num"]      = pd.to_numeric(df_all["rank"], errors="coerce")
df_all["class_num"]     = df_all["player_class"].map(CLASS_MAP).fillna(1)
df_all["style_num"]     = df_all["running_style"].map(STYLE_MAP).fillna(2)
df_all["race_score"]    = pd.to_numeric(df_all["race_score"], errors="coerce")
df_all["gear"]          = pd.to_numeric(df_all["gear"], errors="coerce")
df_all["banum"]         = pd.to_numeric(df_all["banum"], errors="coerce")
df_all["is_win"]        = (df_all["rank_num"]==1).astype(float)
df_all["is_winner"]     = (df_all["rank_num"]==1).astype(int)

df_all = df_all.sort_values(["player_key","date","race_no"]).reset_index(drop=True)
grp = df_all.groupby("player_key")["rank_num"]
df_all["prev1_rank"]     = grp.shift(1)
df_all["last3_avg_rank"] = grp.transform(lambda x: x.shift(1).rolling(3,min_periods=1).mean())
df_all["last5_avg_rank"] = grp.transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())
df_all["last5_win_rate"] = df_all.groupby("player_key")["is_win"].transform(
    lambda x: x.shift(1).rolling(5,min_periods=1).mean())
df_all["rank_trend"]     = df_all["prev1_rank"] - grp.shift(3)
df_all["days_since_last"]= (
    pd.to_datetime(df_all["date"]) -
    pd.to_datetime(df_all.groupby("player_key")["date"].shift(1))
).dt.days

def extract_honmei(x):
    if pd.isna(x): return None
    m = re.search(r"◎(\d+)", str(x))
    return int(m.group(1)) if m else None
df_all["honmei_banum"] = df_all["lineup"].apply(extract_honmei)
df_all["score_rank"]   = df_all.groupby("race_id")["race_score"].rank(ascending=False, method="min")
df_all["is_honmei"]    = (df_all["banum"]==df_all["honmei_banum"]).astype(int)
df_all["is_winner"]    = (df_all["rank_num"]==1).astype(int)
n_pl = df_all.groupby("race_id")["banum"].count().rename("n_players_in_race")
df_all = df_all.join(n_pl, on="race_id")

FEATURES = ["race_score","class_num","style_num","gear",
            "score_rank","is_honmei","n_players_in_race",
            "prev1_rank","last3_avg_rank","last5_avg_rank",
            "last5_win_rate","rank_trend","days_since_last"]

# score_gap・top_score・n_players をdf_allから計算（バグ修正済み）
def calc_gap(x):
    s = sorted(x.dropna(), reverse=True)
    return s[0]-s[1] if len(s)>=2 else np.nan

score_gap_raw = df_all.groupby("race_id")["race_score"].apply(calc_gap)
top_score_raw = df_all.groupby("race_id")["race_score"].max()
n_players_raw = df_all.groupby("race_id")["banum"].nunique()

df_model = df_all.dropna(subset=FEATURES+["rank_num"]).copy()

# ========== 訓練: 対象日より前 ==========
df_train = df_model[df_model["date"] < TARGET_DATE]
if len(df_train) < 1000:
    print("訓練データ不足")
    sys.exit(1)

print(f"訓練: {len(df_train):,}行 ({df_train['race_id'].nunique():,}レース)")
model = lgb.LGBMClassifier(n_estimators=300, learning_rate=0.05,
                            num_leaves=31, min_child_samples=50,
                            verbose=-1, random_state=42)
model.fit(df_train[FEATURES].values, df_train["is_winner"].values)

# ========== 当日データ ==========
df_today = df_model[df_model["date"] == TARGET_DATE].copy()
if df_today.empty:
    msg = f"**{TARGET_DATE} の予想データがありません**\n（当日データ未収集の可能性があります）"
    post_discord(msg)
    print(msg)
    sys.exit(0)

df_today["win_proba"] = model.predict_proba(df_today[FEATURES].values)[:, 1]
ds = df_today.sort_values(["race_id","win_proba"], ascending=[True,False])
rp = ds.groupby("race_id").agg(
    venue   =("venue_slug","first"),
    date    =("date","first"),
    race_no =("race_no","first"),
    pred_1st=("banum", lambda x: int(x.iloc[0])),
    pred_2nd=("banum", lambda x: int(x.iloc[1]) if len(x)>1 else None),
    pred_3rd=("banum", lambda x: int(x.iloc[2]) if len(x)>2 else None),
    top_proba=("win_proba", lambda x: x.iloc[0]),
).reset_index()

rp["score_gap"] = rp["race_id"].map(score_gap_raw)
rp["top_score"] = rp["race_id"].map(top_score_raw)
rp["n_players"] = rp["race_id"].map(n_players_raw)

# ========== フィルター & 時間帯絞り込み ==========
filtered = rp[
    (rp["top_score"] >= 95) &
    (rp["score_gap"] >= 2) &
    (rp["n_players"] == 7) &
    (rp["race_no"] >= RACE_RANGE[0]) &
    (rp["race_no"] <= RACE_RANGE[1])
].sort_values(["venue","race_no"])

print(f"フィルター通過: {len(filtered)}件")

# ========== Discord投稿 ==========
if filtered.empty:
    msg = (f"**{TARGET_DATE} 予想 ({HOUR_JST}時台)**\n"
           f"対象レースなし（race_no {RACE_RANGE[0]}〜{RACE_RANGE[1]}）")
    post_discord(msg)
    print("対象レースなし")
else:
    lines = [f"**:checkered_flag: {TARGET_DATE} 競輪予想 ({HOUR_JST}時台)**",
             f"フィルター: top_score≥95 & gap≥2 & 7車限定",
             f"対象: {len(filtered)}レース\n"]

    for _, row in filtered.iterrows():
        p2 = int(row["pred_2nd"]) if pd.notna(row["pred_2nd"]) else "?"
        p3 = int(row["pred_3rd"]) if pd.notna(row["pred_3rd"]) else "?"
        lines.append(
            f":round_pushpin: **{row['venue']} {int(row['race_no'])}R**\n"
            f"  予想: `{int(row['pred_1st'])}-{p2}-{p3}`\n"
            f"  top_score: {row['top_score']:.1f} / gap: {row['score_gap']:.1f}"
        )

    msg = "\n".join(lines)
    post_discord(msg)
    print(msg)
