# -*- coding: utf-8 -*-
import sys, io as _io
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = _io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = _io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

"""
top_proba単独フィルター vs 複合フィルター の月別安定性比較
ウォークフォワード検証（1ヶ月ずつ訓練窓を拡大）
"""

import glob, re, warnings
import numpy as np
import pandas as pd
import lightgbm as lgb
from pathlib import Path

warnings.filterwarnings("ignore")

DATA_DIR = Path("keirin_data")
files = sorted(glob.glob(str(DATA_DIR / "202[3456]_*.csv")))
files = [f for f in files if "sample" not in f]

print("データ読み込み中...")
dfs = [pd.read_csv(f, encoding="utf-8-sig") for f in files]
df_all = pd.concat(dfs, ignore_index=True)
print(f"  総行数: {len(df_all):,}")

# ===== 払戻パース =====
def parse_payout(s):
    if pd.isna(s) or str(s).strip() == "": return None, None
    m = re.search(r"(\d+[-=]\d+(?:[-=]\d+)?)\s+([\d,]+)円", str(s).strip())
    if m: return m.group(1), int(m.group(2).replace(",", ""))
    return None, None

df_all[["ni_combo", "ni_payout"]] = df_all["ni_sha_tan"].apply(lambda x: pd.Series(parse_payout(x)))
df_all[["san_combo", "san_payout"]] = df_all["san_ren_tan"].apply(lambda x: pd.Series(parse_payout(x)))

# ===== 特徴量 =====
CLASS_MAP = {"S1": 4, "S2": 3, "A1": 2, "A2": 1, "B": 0}
STYLE_MAP  = {"逃": 5, "捲": 4, "両": 3, "差": 2, "追": 1, "マ": 0}

df_all["player_key"] = df_all["player_name"].astype(str) + "_" + df_all["term"].astype(str)
df_all["rank_num"]   = pd.to_numeric(df_all["rank"], errors="coerce")
df_all["class_num"]  = df_all["player_class"].map(CLASS_MAP).fillna(1)
df_all["style_num"]  = df_all["running_style"].map(STYLE_MAP).fillna(2)
df_all["race_score"] = pd.to_numeric(df_all["race_score"], errors="coerce")
df_all["gear"]       = pd.to_numeric(df_all["gear"], errors="coerce")
df_all["banum"]      = pd.to_numeric(df_all["banum"], errors="coerce")
df_all["is_win"]     = (df_all["rank_num"] == 1).astype(float)

df_all = df_all.sort_values(["player_key", "date", "race_no"]).reset_index(drop=True)
grp = df_all.groupby("player_key")["rank_num"]
df_all["prev1_rank"]      = grp.shift(1)
df_all["last3_avg_rank"]  = grp.transform(lambda x: x.shift(1).rolling(3, min_periods=1).mean())
df_all["last5_avg_rank"]  = grp.transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
df_all["last5_win_rate"]  = df_all.groupby("player_key")["is_win"].transform(
    lambda x: x.shift(1).rolling(5, min_periods=1).mean())
df_all["rank_trend"]      = df_all["prev1_rank"] - grp.shift(3)
df_all["days_since_last"] = (
    pd.to_datetime(df_all["date"]) - pd.to_datetime(df_all.groupby("player_key")["date"].shift(1))
).dt.days

def extract_honmei(lineup):
    if pd.isna(lineup): return None
    m = re.search(r"◎(\d+)", str(lineup))
    return int(m.group(1)) if m else None
df_all["honmei_banum"] = df_all["lineup"].apply(extract_honmei)
df_all["score_rank"]   = df_all.groupby("race_id")["race_score"].rank(ascending=False, method="min")
df_all["is_honmei"]    = (df_all["banum"] == df_all["honmei_banum"]).astype(int)
df_all["is_winner"]    = (df_all["rank_num"] == 1).astype(int)
n_players = df_all.groupby("race_id")["banum"].count().rename("n_players_in_race")
df_all = df_all.join(n_players, on="race_id")

FEATURES = ["race_score", "class_num", "style_num", "gear",
            "score_rank", "is_honmei", "n_players_in_race",
            "prev1_rank", "last3_avg_rank", "last5_avg_rank",
            "last5_win_rate", "rank_trend", "days_since_last"]

df_model = df_all.dropna(subset=FEATURES + ["rank_num", "san_payout"]).copy()
df_model["month"] = pd.to_datetime(df_model["date"]).dt.to_period("M").astype(str)

# score_gap（レース単位）
score_gap_map = df_model.groupby("race_id")["race_score"].apply(
    lambda x: sorted(x.dropna(), reverse=True)
).apply(lambda x: x[0] - x[1] if len(x) > 1 else 0)
df_model["score_gap"] = df_model["race_id"].map(score_gap_map)
df_model["top_score"] = df_model.groupby("race_id")["race_score"].transform("max")

# ===== ウォークフォワード =====
months = sorted(df_model["month"].unique())
MIN_TRAIN = 12

print(f"ウォークフォワード実行中（{len(months)-MIN_TRAIN}ヶ月）...")
wf_preds = []
for i, val_month in enumerate(months[MIN_TRAIN:], start=MIN_TRAIN):
    tr = df_model[df_model["month"].isin(months[:i])]
    val = df_model[df_model["month"] == val_month]
    if len(tr) < 100 or len(val) < 10:
        continue
    m = lgb.LGBMClassifier(n_estimators=300, learning_rate=0.05,
                            num_leaves=31, min_child_samples=50,
                            verbose=-1, random_state=42)
    m.fit(tr[FEATURES].values, tr["is_winner"].values)
    val = val.copy()
    val["win_proba"] = m.predict_proba(val[FEATURES].values)[:, 1]
    wf_preds.append(val)
    print(f"  {val_month} 完了", end="\r")

print()
df_wf = pd.concat(wf_preds, ignore_index=True)

# レース単位集計
df_s = df_wf.sort_values(["race_id", "win_proba"], ascending=[True, False])
rp = df_s.groupby("race_id").agg(
    pred_1st =("banum", lambda x: x.iloc[0]),
    pred_2nd =("banum", lambda x: x.iloc[1] if len(x) > 1 else None),
    pred_3rd =("banum", lambda x: x.iloc[2] if len(x) > 2 else None),
    top_proba=("win_proba", lambda x: x.iloc[0]),
    san_payout=("san_payout", "first"),
    top_score=("top_score", "first"),
    score_gap=("score_gap", "first"),
    month    =("month", "first"),
).reset_index()

w1 = df_wf[df_wf["rank_num"]==1].groupby("race_id")["banum"].first()
w2 = df_wf[df_wf["rank_num"]==2].groupby("race_id")["banum"].first()
w3 = df_wf[df_wf["rank_num"]==3].groupby("race_id")["banum"].first()
rp["actual_1st"] = rp["race_id"].map(w1)
rp["actual_2nd"] = rp["race_id"].map(w2)
rp["actual_3rd"] = rp["race_id"].map(w3)

rp = rp.dropna(subset=["actual_1st", "san_payout"]).copy()
rp["hit"] = (
    (rp["pred_1st"] == rp["actual_1st"]) &
    (rp["pred_2nd"] == rp["actual_2nd"]) &
    (rp["pred_3rd"] == rp["actual_3rd"])
).astype(int)

# ===== 閾値別 全期間サマリー =====
print(f"\n{'='*65}")
print("  三連勝単 閾値別回収率（ウォークフォワード全期間）")
print(f"{'='*65}")
rows = []
for thr in [0.00, 0.20, 0.30, 0.35, 0.40, 0.45, 0.50]:
    sub = rp[rp["top_proba"] >= thr]
    if len(sub) == 0: continue
    hits = sub["hit"].sum()
    rec  = sub[sub["hit"]==1]["san_payout"].sum() / (len(sub) * 100) * 100
    plus = (sub.groupby("month").apply(
        lambda g: g[g["hit"]==1]["san_payout"].sum() / (len(g)*100)*100 >= 100
    ).sum())
    total_m = sub["month"].nunique()
    rows.append({"閾値": thr, "件数": len(sub), "的中率(%)": round(hits/len(sub)*100,1),
                 "回収率(%)": round(rec,1), "プラス月": f"{plus}/{total_m}"})
print(pd.DataFrame(rows).to_string(index=False))

# ===== 旧フィルターとの比較 =====
old = rp[(rp["top_proba"]>=0.30)&(rp["top_score"]>=100)&(rp["score_gap"]>=3)]
old_hits = old["hit"].sum()
old_rec  = old[old["hit"]==1]["san_payout"].sum() / (len(old)*100)*100 if len(old) > 0 else 0
old_plus = (old.groupby("month").apply(
    lambda g: g[g["hit"]==1]["san_payout"].sum()/(len(g)*100)*100 >= 100
).sum()) if len(old) > 0 else 0

print(f"\n  [旧] top_proba>=0.30 & score>=100 & gap>=3: {len(old)}件 "
      f"的中{round(old_hits/len(old)*100,1) if len(old)>0 else 0}% "
      f"回収{round(old_rec,1)}% プラス月{old_plus}/{old['month'].nunique() if len(old)>0 else 0}")

# ===== 最良閾値の月別詳細 =====
BEST_THR = 0.40
print(f"\n{'='*65}")
print(f"  top_proba >= {BEST_THR} 月別回収率")
print(f"{'='*65}")
best_sub = rp[rp["top_proba"] >= BEST_THR].copy()
monthly = best_sub.groupby("month").apply(lambda g: pd.Series({
    "件数": len(g),
    "的中": int(g["hit"].sum()),
    "的中率(%)": round(g["hit"].mean()*100, 1),
    "回収率(%)": round(g[g["hit"]==1]["san_payout"].sum()/(len(g)*100)*100, 1),
})).reset_index()
print(monthly.to_string(index=False))
plus = (monthly["回収率(%)"] >= 100).sum()
print(f"\n  プラス月: {plus}/{len(monthly)}  全期間回収率: "
      f"{round(best_sub[best_sub['hit']==1]['san_payout'].sum()/(len(best_sub)*100)*100,1)}%")

# ===== 複数閾値の月別比較（プラス月数） =====
print(f"\n{'='*65}")
print("  閾値別 月別プラス率まとめ")
print(f"{'='*65}")
for thr in [0.00, 0.30, 0.35, 0.40, 0.45]:
    sub = rp[rp["top_proba"] >= thr]
    if len(sub) == 0: continue
    m_rec = sub.groupby("month").apply(
        lambda g: g[g["hit"]==1]["san_payout"].sum()/(len(g)*100)*100
    )
    plus = (m_rec >= 100).sum()
    total = len(m_rec)
    avg_rec = round(sub[sub["hit"]==1]["san_payout"].sum()/(len(sub)*100)*100, 1)
    print(f"  top_proba>={thr:.2f}: {len(sub):,}件  プラス月{plus}/{total}  全期間{avg_rec}%")
