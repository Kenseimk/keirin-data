# -*- coding: utf-8 -*-
import sys, io as _io
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = _io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = _io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

"""
ウォークフォワード済みデータを使ってフィルター条件をグリッドサーチ
目標: 件数 > 4703 かつ 回収率 > 120%
"""

import glob, re, warnings
import numpy as np
import pandas as pd
import lightgbm as lgb
from pathlib import Path
from itertools import product

warnings.filterwarnings("ignore")

DATA_DIR = Path("keirin_data")
files = sorted(glob.glob(str(DATA_DIR / "202[3456]_*.csv")))
files = [f for f in files if "sample" not in f]

print("データ読み込み中...")
df_all = pd.concat([pd.read_csv(f, encoding="utf-8-sig") for f in files], ignore_index=True)

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

df_model = df_all.dropna(subset=FEATURES+["rank_num","san_payout"]).copy()
df_model["month"] = pd.to_datetime(df_model["date"]).dt.to_period("M").astype(str)

# score_gap・top_score は dropna前の全選手データ（df_all）から計算（異常値対策）
df_all["race_score_num"] = pd.to_numeric(df_all["race_score"], errors="coerce")

def calc_gap(x):
    s = sorted(x.dropna(), reverse=True)
    return s[0] - s[1] if len(s) >= 2 else np.nan

score_gap_raw = df_all.groupby("race_id")["race_score_num"].apply(calc_gap)
top_score_raw = df_all.groupby("race_id")["race_score_num"].max()
df_model["score_gap"] = df_model["race_id"].map(score_gap_raw)
df_model["top_score"] = df_model["race_id"].map(top_score_raw)
# 本命の脚質
honmei_style_map = df_model[df_model["is_honmei"]==1].groupby("race_id")["style_num"].first()
df_model["honmei_style"] = df_model["race_id"].map(honmei_style_map)

# ===== ウォークフォワード =====
months = sorted(df_model["month"].unique())
MIN_TRAIN = 12
print(f"ウォークフォワード実行中（{len(months)-MIN_TRAIN}ヶ月）...")

wf_preds = []
for i, val_month in enumerate(months[MIN_TRAIN:], start=MIN_TRAIN):
    tr  = df_model[df_model["month"].isin(months[:i])]
    val = df_model[df_model["month"]==val_month]
    if len(tr)<100 or len(val)<10: continue
    mdl = lgb.LGBMClassifier(n_estimators=300,learning_rate=0.05,
                              num_leaves=31,min_child_samples=50,
                              verbose=-1,random_state=42)
    mdl.fit(tr[FEATURES].values, tr["is_winner"].values)
    val = val.copy()
    val["win_proba"] = mdl.predict_proba(val[FEATURES].values)[:,1]
    wf_preds.append(val)
    print(f"  {val_month} 完了", end="\r")

print()
df_wf = pd.concat(wf_preds, ignore_index=True)

# レース単位
ds = df_wf.sort_values(["race_id","win_proba"],ascending=[True,False])
rp = ds.groupby("race_id").agg(
    pred_1st  =("banum",  lambda x: x.iloc[0]),
    pred_2nd  =("banum",  lambda x: x.iloc[1] if len(x)>1 else None),
    pred_3rd  =("banum",  lambda x: x.iloc[2] if len(x)>2 else None),
    top_proba =("win_proba","first"),  # 既にソート済み
    san_payout=("san_payout","first"),
    top_score =("top_score","first"),
    score_gap =("score_gap","first"),
    n_players =("n_players_in_race","first"),
    honmei_style=("honmei_style","first"),
    month     =("month","first"),
).reset_index()

# top_probaは「最大値」にし直す（groupby後firstが正しい順序か確認）
top_proba_map = ds.groupby("race_id")["win_proba"].first()
rp["top_proba"] = rp["race_id"].map(top_proba_map)

w1 = df_wf[df_wf["rank_num"]==1].groupby("race_id")["banum"].first()
w2 = df_wf[df_wf["rank_num"]==2].groupby("race_id")["banum"].first()
w3 = df_wf[df_wf["rank_num"]==3].groupby("race_id")["banum"].first()
rp["actual_1st"] = rp["race_id"].map(w1)
rp["actual_2nd"] = rp["race_id"].map(w2)
rp["actual_3rd"] = rp["race_id"].map(w3)
rp = rp.dropna(subset=["actual_1st","san_payout"]).copy()
rp["hit"] = (
    (rp["pred_1st"]==rp["actual_1st"]) &
    (rp["pred_2nd"]==rp["actual_2nd"]) &
    (rp["pred_3rd"]==rp["actual_3rd"])
).astype(int)

print(f"対象レース数: {len(rp):,}")

# ===== ベースライン =====
def score_filter(sub, min_bets=1000):
    if len(sub) < min_bets: return None
    hits = sub["hit"].sum()
    rec  = sub[sub["hit"]==1]["san_payout"].sum() / (len(sub)*100)*100
    plus = (sub.groupby("month").apply(
        lambda g: g[g["hit"]==1]["san_payout"].sum()/(len(g)*100)*100 >= 100
    ).sum())
    return {"件数": len(sub), "的中率(%)": round(hits/len(sub)*100,1),
            "回収率(%)": round(rec,1), "プラス月": f"{int(plus)}/{sub['month'].nunique()}"}

print(f"\n--- ベースライン ---")
base = score_filter(rp)
print(f"全賭け: {base}")
old = score_filter(rp[(rp["top_proba"]>=0.30)&(rp["top_score"]>=100)&(rp["score_gap"]>=3)])
print(f"旧フィルター(score>=100,gap>=3): {old}")

# ===== グリッドサーチ =====
print(f"\n--- グリッドサーチ（件数>5000 かつ 回収率>120%） ---")

results = []

# 探索パラメータ
proba_thrs   = [0.0, 0.20, 0.30, 0.35, 0.40]
score_mins   = [0, 75, 80, 85, 90, 95]
gap_mins     = [0, 1, 2, 3, 5]
n_player_vals= [0, 7, 9]          # 0=全員, 7=7車限定, 9=9車限定
style_vals   = [0, 5]             # 0=全部, 5=逃げ本命のみ

for proba_thr, score_min, gap_min, n_pl_min, style_min in product(
        proba_thrs, score_mins, gap_mins, n_player_vals, style_vals):

    mask = (rp["top_proba"] >= proba_thr) & \
           (rp["top_score"] >= score_min) & \
           (rp["score_gap"] >= gap_min)

    if n_pl_min > 0:
        mask = mask & (rp["n_players"] == n_pl_min)
    if style_min > 0:
        mask = mask & (rp["honmei_style"] >= style_min)

    sub = rp[mask]
    if len(sub) < 5000: continue

    hits = sub["hit"].sum()
    rec  = sub[sub["hit"]==1]["san_payout"].sum() / (len(sub)*100)*100
    if rec < 120: continue

    plus = (sub.groupby("month").apply(
        lambda g: g[g["hit"]==1]["san_payout"].sum()/(len(g)*100)*100 >= 100
    ).sum())

    results.append({
        "top_proba>=": proba_thr,
        "top_score>=": score_min,
        "score_gap>=": gap_min,
        "n_players==": n_pl_min if n_pl_min>0 else "-",
        "逃げ本命": "yes" if style_min>0 else "-",
        "件数": len(sub),
        "的中率(%)": round(hits/len(sub)*100,1),
        "回収率(%)": round(rec,1),
        "プラス月": f"{int(plus)}/{sub['month'].nunique()}",
    })

if results:
    df_res = pd.DataFrame(results).sort_values("回収率(%)", ascending=False)
    print(df_res.head(20).to_string(index=False))
else:
    print("  条件を満たす組み合わせなし → 閾値を緩和して再探索...")

    # 緩和版: 件数>3000 & 回収率>115
    results2 = []
    for proba_thr, score_min, gap_min in product(proba_thrs, score_mins, gap_mins):
        mask = (rp["top_proba"]>=proba_thr)&(rp["top_score"]>=score_min)&(rp["score_gap"]>=gap_min)
        sub = rp[mask]
        if len(sub)<3000: continue
        hits = sub["hit"].sum()
        rec  = sub[sub["hit"]==1]["san_payout"].sum()/(len(sub)*100)*100
        if rec<115: continue
        plus = (sub.groupby("month").apply(
            lambda g: g[g["hit"]==1]["san_payout"].sum()/(len(g)*100)*100>=100
        ).sum())
        results2.append({
            "top_proba>=": proba_thr, "top_score>=": score_min, "score_gap>=": gap_min,
            "件数": len(sub), "的中率(%)": round(hits/len(sub)*100,1),
            "回収率(%)": round(rec,1), "プラス月": f"{int(plus)}/{sub['month'].nunique()}",
        })
    if results2:
        df2 = pd.DataFrame(results2).sort_values("回収率(%)",ascending=False)
        print(df2.head(20).to_string(index=False))
    else:
        print("  件数>3000 & 回収率>115% の条件も見つからず")
