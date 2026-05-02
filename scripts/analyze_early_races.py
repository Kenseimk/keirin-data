# -*- coding: utf-8 -*-
"""
早時間帯(race_no 1-4)のフィルター緩和によるROI影響分析
現行: top_score>=95 & score_gap>=2 & 7車
緩和: top_score>=85 / top_score>=90 / フィルターなし
期間: 訓練データより前に対して walk-forward的に検証
"""
import sys, io as _io
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = _io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import glob, re, warnings
import numpy as np
import pandas as pd
import lightgbm as lgb
from pathlib import Path

warnings.filterwarnings("ignore")

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
df_all["is_2nd"]        = (df_all["rank_num"]==2).astype(int)
df_all["is_3rd"]        = (df_all["rank_num"]==3).astype(int)

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

def calc_gap(x):
    s = sorted(x.dropna(), reverse=True)
    return s[0]-s[1] if len(s)>=2 else np.nan

score_gap_raw = df_all.groupby("race_id")["race_score"].apply(calc_gap)
top_score_raw = df_all.groupby("race_id")["race_score"].max()
n_players_raw = df_all.groupby("race_id")["banum"].nunique()

# 検証期間: 2025-01 以降（十分なデータ量を確保）
VAL_START = "2025-01-01"
VAL_END   = "2026-03-31"
train_mask = df_model["date"] < VAL_START
val_mask   = (df_model["date"] >= VAL_START) & (df_model["date"] <= VAL_END)

df_train = df_model[train_mask]
df_val   = df_model[val_mask]
print(f"訓練: {len(df_train):,}行 / 検証: {len(df_val):,}行  ({VAL_START}〜{VAL_END})")

# モデル訓練（1着のみで十分）
model = lgb.LGBMClassifier(n_estimators=300, learning_rate=0.05,
                            num_leaves=31, min_child_samples=50,
                            verbose=-1, random_state=42)
model.fit(df_train[FEATURES].values, df_train["is_winner"].values)
model2 = lgb.LGBMClassifier(n_estimators=300, learning_rate=0.05,
                             num_leaves=31, min_child_samples=50,
                             verbose=-1, random_state=42)
model2.fit(df_train[FEATURES].values, df_train["is_2nd"].values)
model3 = lgb.LGBMClassifier(n_estimators=300, learning_rate=0.05,
                             num_leaves=31, min_child_samples=50,
                             verbose=-1, random_state=42)
model3.fit(df_train[FEATURES].values, df_train["is_3rd"].values)

df_val = df_val.copy()
df_val["win_proba"] = model.predict_proba(df_val[FEATURES].values)[:, 1]
df_val["p2_proba"]  = model2.predict_proba(df_val[FEATURES].values)[:, 1]
df_val["p3_proba"]  = model3.predict_proba(df_val[FEATURES].values)[:, 1]

# レース単位に集約
rows = []
for race_id, g in df_val.groupby("race_id"):
    g1 = g.sort_values("win_proba", ascending=False)
    if len(g1) < 3: continue
    pred_1 = int(g1.iloc[0]["banum"])
    rest2  = g1[g1["banum"]!=pred_1].sort_values("p2_proba", ascending=False)
    pred_2 = int(rest2.iloc[0]["banum"]) if len(rest2)>=1 else None
    rest3  = g1[~g1["banum"].isin([pred_1, pred_2])].sort_values("p3_proba", ascending=False)
    pred_3 = int(rest3.iloc[0]["banum"]) if len(rest3)>=1 else None
    w1 = g1[g1["rank_num"]==1]["banum"].values
    w2 = g1[g1["rank_num"]==2]["banum"].values
    w3 = g1[g1["rank_num"]==3]["banum"].values
    rows.append({
        "race_id":   race_id,
        "date":      g1["date"].iloc[0],
        "race_no":   g1["race_no"].iloc[0],
        "venue":     g1["venue_slug"].iloc[0],
        "pred_1st":  pred_1,
        "pred_2nd":  pred_2,
        "pred_3rd":  pred_3,
        "actual_1st": int(w1[0]) if len(w1) else None,
        "actual_2nd": int(w2[0]) if len(w2) else None,
        "actual_3rd": int(w3[0]) if len(w3) else None,
        "san_payout": g1["san_payout"].iloc[0],
        "ni_payout":  g1["ni_payout"].iloc[0],
    })

rp = pd.DataFrame(rows)
rp["score_gap"] = rp["race_id"].map(score_gap_raw)
rp["top_score"] = rp["race_id"].map(top_score_raw)
rp["n_players"] = rp["race_id"].map(n_players_raw)

rp["san_hit"] = (
    (rp["pred_1st"]==rp["actual_1st"]) &
    (rp["pred_2nd"]==rp["actual_2nd"]) &
    (rp["pred_3rd"]==rp["actual_3rd"])
).astype(int)
rp["ni_hit"] = (
    (rp["pred_1st"]==rp["actual_1st"]) &
    (rp["pred_2nd"]==rp["actual_2nd"])
).astype(int)

# レース帯分類
rp["time_band"] = rp["race_no"].apply(lambda x: "早(1-4R)" if x <= 4 else ("中(5-9R)" if x <= 9 else "遅(10R+)"))

def roi_stats(df, label):
    n = len(df)
    if n == 0:
        return {"ラベル": label, "件数": 0, "的中": 0, "的中率(%)": 0, "ROI(%)": 0}
    hits = df["san_hit"].sum()
    payout = df[df["san_hit"]==1]["san_payout"].sum()
    roi = payout / (n * 100) * 100
    return {
        "ラベル": label,
        "件数": n,
        "的中": int(hits),
        "的中率(%)": round(hits/n*100, 1),
        "ROI(%)": round(roi, 1),
    }

print(f"\n{'='*70}")
print(f"  検証期間: {VAL_START} 〜 {VAL_END}")
print(f"  総レース数: {len(rp):,}")
print(f"{'='*70}")

# ---- 1. 時間帯別・現行フィルター ----
print("\n【現行フィルター: top_score>=95 & gap>=2 & 7車】")
results = []
for band in ["早(1-4R)", "中(5-9R)", "遅(10R+)", "全体"]:
    if band == "全体":
        base = rp
    else:
        base = rp[rp["time_band"] == band]
    f = base[(base["top_score"]>=95)&(base["score_gap"]>=2)&(base["n_players"]==7)]
    results.append(roi_stats(f, band))
print(pd.DataFrame(results).to_string(index=False))

# ---- 2. 早時間帯 (race_no 1-4) のフィルター比較 ----
print("\n【早時間帯(race_no 1-4)フィルター比較】")
early = rp[rp["race_no"] <= 4].copy()
results2 = []

# フィルターなし（7車のみ）
f0 = early[early["n_players"]==7]
results2.append(roi_stats(f0, "7車のみ"))

# top_score >= 85
f85 = early[(early["top_score"]>=85)&(early["score_gap"]>=2)&(early["n_players"]==7)]
results2.append(roi_stats(f85, "top_score>=85 & gap>=2 & 7車"))

# top_score >= 90
f90 = early[(early["top_score"]>=90)&(early["score_gap"]>=2)&(early["n_players"]==7)]
results2.append(roi_stats(f90, "top_score>=90 & gap>=2 & 7車"))

# 現行 (top_score >= 95)
f95 = early[(early["top_score"]>=95)&(early["score_gap"]>=2)&(early["n_players"]==7)]
results2.append(roi_stats(f95, "top_score>=95 & gap>=2 & 7車(現行)"))

# gap >= 1 に緩める
f85_g1 = early[(early["top_score"]>=85)&(early["score_gap"]>=1)&(early["n_players"]==7)]
results2.append(roi_stats(f85_g1, "top_score>=85 & gap>=1 & 7車"))

print(pd.DataFrame(results2).to_string(index=False))

# ---- 3. top_score分布（早時間帯）----
print("\n【早時間帯(race_no 1-4) top_score 分布 (7車限定)】")
e7 = early[early["n_players"]==7]
print(f"  レース数: {len(e7)}")
print(f"  top_score 平均: {e7['top_score'].mean():.1f}")
print(f"  top_score 中央値: {e7['top_score'].median():.1f}")
bins = [0, 80, 85, 88, 90, 92, 95, 100, 999]
labels_b = ["<80","80-85","85-88","88-90","90-92","92-95","95-100","100+"]
e7["score_bin"] = pd.cut(e7["top_score"], bins=bins, labels=labels_b, right=False)
print(e7.groupby("score_bin", observed=True)["race_id"].count().rename("件数").to_string())

# ---- 4. 中時間帯との比較参考 ----
print("\n【参考: 中時間帯(race_no 5-9) 現行フィルター】")
mid = rp[rp["race_no"].between(5,9)]
fm = mid[(mid["top_score"]>=95)&(mid["score_gap"]>=2)&(mid["n_players"]==7)]
print(roi_stats(fm, "中時間帯 現行フィルター"))
