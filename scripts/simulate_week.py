# -*- coding: utf-8 -*-
import sys, io as _io
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = _io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = _io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

"""
指定期間のシミュレーション（複数日）
フィルター: top_score>=95 & score_gap>=2 & 7車限定
"""

import argparse, glob, re, warnings
import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import roc_auc_score
from pathlib import Path

warnings.filterwarnings("ignore")

parser = argparse.ArgumentParser()
parser.add_argument("--dates", nargs="+", required=True, help="例: 2026-03-23 2026-03-24 ...")
args = parser.parse_args()
TARGET_DATES = sorted(args.dates)

# ========== データ読み込み ==========
DATA_DIR = Path("keirin_data")
files = [f for f in sorted(glob.glob(str(DATA_DIR / "202[3456]_*.csv"))) if "sample" not in f]
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

n_pl = df_all.groupby("race_id")["banum"].count().rename("n_players_in_race")
df_all = df_all.join(n_pl, on="race_id")

FEATURES = ["race_score","class_num","style_num","gear",
            "score_rank","is_honmei","n_players_in_race",
            "prev1_rank","last3_avg_rank","last5_avg_rank",
            "last5_win_rate","rank_trend","days_since_last"]

df_model = df_all.dropna(subset=FEATURES+["rank_num","san_payout"]).copy()

# score_gap・top_score をdf_allから計算（バグ修正済み）
df_all["race_score_num"] = df_all["race_score"]  # already numeric

def calc_gap(x):
    s = sorted(x.dropna(), reverse=True)
    return s[0]-s[1] if len(s)>=2 else np.nan

score_gap_raw = df_all.groupby("race_id")["race_score"].apply(calc_gap)
top_score_raw = df_all.groupby("race_id")["race_score"].max()
n_players_raw = df_all.groupby("race_id")["banum"].nunique()

# ========== 訓練は最初の対象日より前のデータ全部 ==========
first_date = TARGET_DATES[0]
df_train = df_model[df_model["date"] < first_date]
print(f"訓練データ: {len(df_train):,}行 ({df_train['race_id'].nunique():,}レース) ～{first_date}前")

model = lgb.LGBMClassifier(n_estimators=300, learning_rate=0.05,
                            num_leaves=31, min_child_samples=50,
                            verbose=-1, random_state=42)
model.fit(df_train[FEATURES].values, df_train["is_winner"].values)

# ========== 日別シミュレーション ==========
all_results = []
filter_results = []

for target_date in TARGET_DATES:
    df_val = df_model[df_model["date"] == target_date].copy()
    if df_val.empty:
        print(f"\n{target_date}: データなし")
        continue

    df_val["win_proba"] = model.predict_proba(df_val[FEATURES].values)[:, 1]
    ds = df_val.sort_values(["race_id","win_proba"], ascending=[True,False])
    rp = ds.groupby("race_id").agg(
        venue    =("venue_slug","first"),
        date     =("date","first"),
        race_no  =("race_no","first"),
        pred_1st =("banum", lambda x: int(x.iloc[0])),
        pred_2nd =("banum", lambda x: int(x.iloc[1]) if len(x)>1 else None),
        pred_3rd =("banum", lambda x: int(x.iloc[2]) if len(x)>2 else None),
        top_proba=("win_proba", lambda x: x.iloc[0]),
        ni_payout=("ni_payout","first"),
        san_payout=("san_payout","first"),
    ).reset_index()

    w1 = df_val[df_val["rank_num"]==1].groupby("race_id")["banum"].first()
    w2 = df_val[df_val["rank_num"]==2].groupby("race_id")["banum"].first()
    w3 = df_val[df_val["rank_num"]==3].groupby("race_id")["banum"].first()
    rp["actual_1st"] = rp["race_id"].map(w1)
    rp["actual_2nd"] = rp["race_id"].map(w2)
    rp["actual_3rd"] = rp["race_id"].map(w3)
    rp["score_gap"]  = rp["race_id"].map(score_gap_raw)
    rp["top_score"]  = rp["race_id"].map(top_score_raw)
    rp["n_players"]  = rp["race_id"].map(n_players_raw)

    rp["san_hit"] = (
        (rp["pred_1st"]==rp["actual_1st"]) &
        (rp["pred_2nd"]==rp["actual_2nd"]) &
        (rp["pred_3rd"]==rp["actual_3rd"])
    ).astype(int)
    rp["ni_hit"] = (
        (rp["pred_1st"]==rp["actual_1st"]) &
        (rp["pred_2nd"]==rp["actual_2nd"])
    ).astype(int)

    total = len(rp)
    san_hits = rp["san_hit"].sum()
    san_rec  = rp[rp["san_hit"]==1]["san_payout"].sum()/(total*100)*100 if total>0 else 0

    # フィルター適用
    f = rp[(rp["top_score"]>=95)&(rp["score_gap"]>=2)&(rp["n_players"]==7)]
    f_hits = f["san_hit"].sum()
    f_rec  = f[f["san_hit"]==1]["san_payout"].sum()/(len(f)*100)*100 if len(f)>0 else 0

    all_results.append({
        "日付": target_date,
        "全レース": total,
        "三連単的中": san_hits,
        "的中率(%)": round(san_hits/total*100,1) if total>0 else 0,
        "回収率(%)": round(san_rec,1),
    })
    filter_results.append({
        "日付": target_date,
        "対象": len(f),
        "的中": f_hits,
        "的中率(%)": round(f_hits/len(f)*100,1) if len(f)>0 else 0,
        "回収率(%)": round(f_rec,1),
        "的中レース": ", ".join([
            f"{row['venue']} {int(row['race_no'])}R "
            f"{int(row['pred_1st'])}-{int(row['pred_2nd']) if pd.notna(row['pred_2nd']) else '?'}"
            f"-{int(row['pred_3rd']) if pd.notna(row['pred_3rd']) else '?'} ({int(row['san_payout'])}円)"
            for _, row in f[f["san_hit"]==1].iterrows()
        ]) or "-",
    })

# ========== 結果表示 ==========
print(f"\n{'='*65}")
print("  【全レース賭け】三連勝単 日別結果")
print(f"{'='*65}")
df_all_res = pd.DataFrame(all_results)
print(df_all_res.to_string(index=False))
total_races = df_all_res["全レース"].sum()
total_hits  = df_all_res["三連単的中"].sum()
total_payout_all = sum(
    df_model[
        (df_model["date"]==r["日付"]) &
        (df_model["rank_num"]==1)
    ]["san_payout"].sum()
    for _, r in df_all_res.iterrows()
)
print(f"\n  週計: {total_races}レース / 的中{total_hits}件 / 的中率{total_hits/total_races*100:.1f}%")

print(f"\n{'='*65}")
print("  【フィルター: top_score>=95 & score_gap>=2 & 7車限定】")
print(f"{'='*65}")
df_f_res = pd.DataFrame(filter_results)
print(df_f_res.to_string(index=False))
f_total  = df_f_res["対象"].sum()
f_hits   = df_f_res["的中"].sum()
f_payout = 0
for target_date in TARGET_DATES:
    df_val = df_model[df_model["date"]==target_date].copy()
    if df_val.empty: continue
    df_val["win_proba"] = model.predict_proba(df_val[FEATURES].values)[:,1]
    ds = df_val.sort_values(["race_id","win_proba"],ascending=[True,False])
    rp2 = ds.groupby("race_id").agg(
        pred_1st=("banum",lambda x:int(x.iloc[0])),
        pred_2nd=("banum",lambda x:int(x.iloc[1]) if len(x)>1 else None),
        pred_3rd=("banum",lambda x:int(x.iloc[2]) if len(x)>2 else None),
        san_payout=("san_payout","first"),
        top_proba=("win_proba",lambda x:x.iloc[0]),
    ).reset_index()
    rp2["score_gap"] = rp2["race_id"].map(score_gap_raw)
    rp2["top_score"] = rp2["race_id"].map(top_score_raw)
    rp2["n_players"] = rp2["race_id"].map(n_players_raw)
    w1=df_val[df_val["rank_num"]==1].groupby("race_id")["banum"].first()
    w2=df_val[df_val["rank_num"]==2].groupby("race_id")["banum"].first()
    w3=df_val[df_val["rank_num"]==3].groupby("race_id")["banum"].first()
    rp2["actual_1st"]=rp2["race_id"].map(w1)
    rp2["actual_2nd"]=rp2["race_id"].map(w2)
    rp2["actual_3rd"]=rp2["race_id"].map(w3)
    rp2["san_hit"]=(
        (rp2["pred_1st"]==rp2["actual_1st"])&
        (rp2["pred_2nd"]==rp2["actual_2nd"])&
        (rp2["pred_3rd"]==rp2["actual_3rd"])
    ).astype(int)
    f2=rp2[(rp2["top_score"]>=95)&(rp2["score_gap"]>=2)&(rp2["n_players"]==7)]
    f_payout += f2[f2["san_hit"]==1]["san_payout"].sum()

f_rec_total = f_payout/(f_total*100)*100 if f_total>0 else 0
print(f"\n  週計: {f_total}件賭け / 的中{f_hits}件 / 的中率{f_hits/f_total*100:.1f}% / 回収率{f_rec_total:.1f}%" if f_total>0 else "\n  週計: 対象レースなし")
