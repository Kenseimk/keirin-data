# -*- coding: utf-8 -*-
"""
フィルタ条件グリッドサーチ: 的中率最大化
訓練: ~2026-03  /  検証: 2026-04
"""
import sys, io as _io
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = _io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import glob, re, warnings
import numpy as np
import pandas as pd
import lightgbm as lgb
from pathlib import Path
import itertools

warnings.filterwarnings("ignore")

TEST_MONTH = "2026-04"

DATA_DIR = Path("keirin_data")
files = [f for f in sorted(glob.glob(str(DATA_DIR / "202[3456]_*.csv"))) if "sample" not in f and "bak" not in f]
df_all = pd.concat([pd.read_csv(f, encoding="utf-8-sig") for f in files], ignore_index=True)
print(f"総行数: {len(df_all):,}")

def parse_payout(s):
    if pd.isna(s) or str(s).strip() == "": return None, None
    m = re.search(r"(\d+[-=]\d+(?:[-=]\d+)?)\s+([\d,]+)円", str(s).strip())
    if m: return m.group(1), int(m.group(2).replace(",", ""))
    return None, None

df_all[["san_combo","san_payout"]] = df_all["san_ren_tan"].apply(lambda x: pd.Series(parse_payout(x)))

CLASS_MAP = {"S1":4,"S2":3,"A1":2,"A2":1,"B":0}
STYLE_MAP  = {"逃":5,"捲":4,"両":3,"差":2,"追":1,"マ":0}

df_all["player_key"]   = df_all["player_name"].astype(str)+"_"+df_all["term"].astype(str)
df_all["rank_num"]     = pd.to_numeric(df_all["rank"], errors="coerce")
df_all["class_num"]    = df_all["player_class"].map(CLASS_MAP).fillna(1)
df_all["style_num"]    = df_all["running_style"].map(STYLE_MAP).fillna(2)
df_all["race_score"]   = pd.to_numeric(df_all["race_score"], errors="coerce")
df_all["gear"]         = pd.to_numeric(df_all["gear"], errors="coerce")
df_all["banum"]        = pd.to_numeric(df_all["banum"], errors="coerce")
df_all["agari"]        = pd.to_numeric(df_all["agari"], errors="coerce")
df_all["is_win"]       = (df_all["rank_num"]==1).astype(float)
df_all["is_winner"]    = (df_all["rank_num"]==1).astype(int)
df_all["is_2nd"]       = (df_all["rank_num"]==2).astype(int)
df_all["is_3rd"]       = (df_all["rank_num"]==3).astype(int)
df_all["mark_num"]     = pd.to_numeric(df_all.get("mark_num"),     errors="coerce")
df_all["win_rate_4m"]  = pd.to_numeric(df_all.get("win_rate_4m"),  errors="coerce")
df_all["top2_rate_4m"] = pd.to_numeric(df_all.get("top2_rate_4m"), errors="coerce")
df_all["top3_rate_4m"] = pd.to_numeric(df_all.get("top3_rate_4m"), errors="coerce")
df_all["nige_4m"]      = pd.to_numeric(df_all.get("nige_4m"),      errors="coerce")
df_all["maku_4m"]      = pd.to_numeric(df_all.get("maku_4m"),      errors="coerce")

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

agari_grp = df_all.groupby("player_key")["agari"]
df_all["last3_avg_agari"] = agari_grp.transform(lambda x: x.shift(1).rolling(3,min_periods=1).mean())
df_all["last5_avg_agari"] = agari_grp.transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())
df_all["same_pref_count"] = (df_all.groupby(["race_id","pref"])["banum"].transform("count") - 1).clip(lower=0)
df_all["same_term_count"] = (df_all.groupby(["race_id","term"])["banum"].transform("count") - 1).clip(lower=0)
df_all["nige_in_race"]    = df_all.groupby("race_id")["running_style"].transform(lambda x: (x=="逃").sum())
df_all_s = df_all.sort_values(["player_key","date","race_no"])
df_all["venue_win_rate"]  = df_all_s.groupby(["player_key","venue_slug"])["is_win"].transform(
    lambda x: x.shift(1).rolling(10,min_periods=1).mean())

def extract_honmei(x):
    if pd.isna(x): return None
    m = re.search(r"◎(\d+)", str(x))
    return int(m.group(1)) if m else None
df_all["honmei_banum"]      = df_all["lineup"].apply(extract_honmei)
df_all["score_rank"]        = df_all.groupby("race_id")["race_score"].rank(ascending=False, method="min")
df_all["is_honmei"]         = (df_all["banum"]==df_all["honmei_banum"]).astype(int)
df_all["is_winner"]         = (df_all["rank_num"]==1).astype(int)
n_pl = df_all.groupby("race_id")["banum"].count().rename("n_players_in_race")
df_all = df_all.join(n_pl, on="race_id")

honmei_style = df_all[df_all["is_honmei"]==1].set_index("race_id")["style_num"]
honmei_score = df_all[df_all["is_honmei"]==1].set_index("race_id")["race_score"]
df_all["honmei_style_num"]  = df_all["race_id"].map(honmei_style)
df_all["honmei_race_score"] = df_all["race_id"].map(honmei_score)

def calc_gap(x):
    s = sorted(x.dropna(), reverse=True)
    return s[0]-s[1] if len(s)>=2 else np.nan

score_gap_raw = df_all.groupby("race_id")["race_score"].apply(calc_gap)
top_score_raw = df_all.groupby("race_id")["race_score"].max()
n_players_raw = df_all.groupby("race_id")["banum"].nunique()

F1  = ["race_score","class_num","style_num","gear","score_rank","is_honmei",
       "n_players_in_race","prev1_rank","last3_avg_rank","last5_avg_rank",
       "last5_win_rate","rank_trend","days_since_last",
       "last3_avg_agari","last5_avg_agari","same_pref_count","same_term_count",
       "nige_in_race","venue_win_rate",
       "mark_num","win_rate_4m","top2_rate_4m","top3_rate_4m","nige_4m","maku_4m"]
F23 = F1 + ["honmei_style_num","honmei_race_score"]

req = ["race_score","class_num","style_num","gear","score_rank","is_honmei",
       "n_players_in_race","prev1_rank","last3_avg_rank","last5_avg_rank",
       "last5_win_rate","rank_trend","days_since_last","rank_num"]
df_model = df_all.dropna(subset=req).copy()

df_train = df_model[df_model["date"].str[:7] < TEST_MONTH]
df_test  = df_model[df_model["date"].str[:7] == TEST_MONTH]
val_months = sorted(df_train["date"].str[:7].unique())[-3:]
val_mask   = df_train["date"].str[:7].isin(val_months)
df_tr, df_val = df_train[~val_mask], df_train[val_mask]

print(f"訓練: {len(df_tr):,}行  検証ES: {len(df_val):,}行  テスト: {len(df_test):,}行")

LGB_PARAMS = dict(n_estimators=1000, learning_rate=0.05,
                  num_leaves=31, min_child_samples=50, verbose=-1, random_state=42)

def fit_es(F_tr, F_val, y_tr, y_val):
    m = lgb.LGBMClassifier(**LGB_PARAMS)
    m.fit(F_tr, y_tr, eval_set=[(F_val, y_val)],
          callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(-1)])
    return m

m1 = fit_es(df_tr[F1].values,  df_val[F1].values,  df_tr["is_winner"].values, df_val["is_winner"].values)
m2 = fit_es(df_tr[F23].values, df_val[F23].values, df_tr["is_2nd"].values,    df_val["is_2nd"].values)
m3 = fit_es(df_tr[F23].values, df_val[F23].values, df_tr["is_3rd"].values,    df_val["is_3rd"].values)
print(f"best iter: 1着={m1.best_iteration_} / 2着={m2.best_iteration_} / 3着={m3.best_iteration_}")

df_test = df_test.copy()
df_test["win_proba"] = m1.predict_proba(df_test[F1].values)[:,1]
df_test["p2_proba"]  = m2.predict_proba(df_test[F23].values)[:,1]
df_test["p3_proba"]  = m3.predict_proba(df_test[F23].values)[:,1]

# 全レースの予測を一度だけ作成（p2_gap=0で全部2通り含む）
rows = []
for race_id, g in df_test.groupby("race_id"):
    g = g.sort_values("win_proba", ascending=False)
    if len(g) < 3: continue
    pred_1 = int(g.iloc[0]["banum"])
    top_proba = float(g.iloc[0]["win_proba"])

    rest2  = g[g["banum"] != pred_1].sort_values("p2_proba", ascending=False)
    pred_2a = int(rest2.iloc[0]["banum"]) if len(rest2) >= 1 else None
    pred_2b = int(rest2.iloc[1]["banum"]) if len(rest2) >= 2 else None
    p2_gap  = float(rest2.iloc[0]["p2_proba"] - rest2.iloc[1]["p2_proba"]) if len(rest2) >= 2 else 1.0

    def pick3(excl):
        r = g[~g["banum"].isin(excl)].sort_values("p3_proba", ascending=False)
        return int(r.iloc[0]["banum"]) if len(r) >= 1 else None

    meta = {"race_id": race_id, "top_proba": top_proba, "p2_gap": p2_gap,
            "san_combo": g["san_combo"].iloc[0], "san_payout": g["san_payout"].iloc[0],
            "score_gap": score_gap_raw.get(race_id, np.nan),
            "top_score": top_score_raw.get(race_id, np.nan),
            "n_players": n_players_raw.get(race_id, np.nan)}

    # 1通り（常に追加）
    rows.append({**meta, "is_2nd_form": False,
                 "pred_1st": pred_1, "pred_2nd": pred_2a, "pred_3rd": pick3([pred_1, pred_2a])})
    # 2通り目（p2_gapで後でフィルタ）
    if pred_2b is not None:
        rows.append({**meta, "is_2nd_form": True,
                     "pred_1st": pred_1, "pred_2nd": pred_2b, "pred_3rd": pick3([pred_1, pred_2b])})

rp_all = pd.DataFrame(rows)

def is_hit(row):
    combo = str(row["san_combo"]) if pd.notna(row["san_combo"]) else ""
    parts = re.split(r"[-=]", combo)
    if len(parts) < 3: return False
    try:
        return int(parts[0])==row["pred_1st"] and int(parts[1])==row["pred_2nd"] and int(parts[2])==row["pred_3rd"]
    except: return False

rp_all["hit"] = rp_all.apply(is_hit, axis=1)

# グリッドサーチ
print("\n" + "="*70)
print("  フィルタ条件グリッドサーチ（的中率 降順）")
print("="*70)
print(f"{'top_score':>10} {'score_gap':>10} {'p2_gap':>8} {'賭け数':>7} {'的中':>5} {'的中率':>7} {'回収率':>7}")
print("-"*70)

TOP_SCORES  = [95, 100, 105, 110]
SCORE_GAPS  = [2, 3, 4, 5, 6, 8]
P2_GAPS     = [0.0, 0.03, 0.05, 0.08, 999]  # 999=接戦スキップ

results = []
for ts, sg, pg in itertools.product(TOP_SCORES, SCORE_GAPS, P2_GAPS):
    # ベースフィルタ
    base = rp_all[(rp_all["top_score"] >= ts) &
                  (rp_all["score_gap"] >= sg) &
                  (rp_all["n_players"] == 7)].copy()
    if pg == 999:
        # 接戦スキップ: 2通り目を除外、かつ1通り目のみ
        filt = base[~base["is_2nd_form"]].copy()
    else:
        # 1通り目は常に含む + p2_gap < pg の2通り目を含む
        filt = base[(~base["is_2nd_form"]) | (base["p2_gap"] < pg)].copy()

    if len(filt) < 10:
        continue

    hits = filt["hit"].sum()
    bets = len(filt)
    hit_rate = hits / bets * 100
    payout = filt[filt["hit"]]["san_payout"].sum()
    roi = payout / (bets * 100) * 100

    pg_label = "skip" if pg == 999 else f"{pg:.2f}"
    results.append((ts, sg, pg_label, bets, hits, hit_rate, roi))

results.sort(key=lambda x: -x[5])

for ts, sg, pg, bets, hits, hr, roi in results[:30]:
    print(f"{ts:>10} {sg:>10} {pg:>8} {bets:>7} {hits:>5} {hr:>6.1f}% {roi:>6.1f}%")

print("\n--- 現在の設定 (top_score>=95, score_gap>=2, p2_gap<0.05) ---")
cur = [r for r in results if r[0]==95 and r[1]==2 and r[2]=="0.05"]
if cur:
    ts, sg, pg, bets, hits, hr, roi = cur[0]
    print(f"  的中率: {hr:.1f}% ({hits}/{bets})  回収率: {roi:.1f}%")
