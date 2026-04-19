# -*- coding: utf-8 -*-
"""win_proba 15-20% 月別詳細"""
import sys, io as _io
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = _io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import glob, re, warnings
import numpy as np, pandas as pd, lightgbm as lgb
from pathlib import Path
warnings.filterwarnings("ignore")

DATA_DIR = Path("keirin_data")
files = [f for f in sorted(glob.glob(str(DATA_DIR / "202[3456]_*.csv"))) if "sample" not in f]
df_all = pd.concat([pd.read_csv(f, encoding="utf-8-sig") for f in files], ignore_index=True)

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
df_all["is_win"]       = (df_all["rank_num"]==1).astype(float)
df_all["is_winner"]    = (df_all["rank_num"]==1).astype(int)
df_all["is_2nd"]       = (df_all["rank_num"]==2).astype(int)
df_all["is_3rd"]       = (df_all["rank_num"]==3).astype(int)
for col in ["mark_num","win_rate_4m","top2_rate_4m","top3_rate_4m","nige_4m","maku_4m"]:
    df_all[col] = pd.to_numeric(df_all.get(col), errors="coerce")
df_all = df_all.sort_values(["player_key","date","race_no"]).reset_index(drop=True)
grp = df_all.groupby("player_key")["rank_num"]
df_all["prev1_rank"]     = grp.shift(1)
df_all["last3_avg_rank"] = grp.transform(lambda x: x.shift(1).rolling(3,min_periods=1).mean())
df_all["last5_avg_rank"] = grp.transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())
df_all["last5_win_rate"] = df_all.groupby("player_key")["is_win"].transform(
    lambda x: x.shift(1).rolling(5,min_periods=1).mean())
df_all["rank_trend"]     = df_all["prev1_rank"] - grp.shift(3)
df_all["days_since_last"] = (
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

def calc_gap(x):
    s = sorted(x.dropna(), reverse=True)
    return s[0]-s[1] if len(s)>=2 else np.nan
score_gap_raw = df_all.groupby("race_id")["race_score"].apply(calc_gap)
top_score_raw = df_all.groupby("race_id")["race_score"].max()
n_players_raw = df_all.groupby("race_id")["banum"].nunique()

F = ["race_score","class_num","style_num","gear","score_rank","is_honmei",
     "n_players_in_race","prev1_rank","last3_avg_rank","last5_avg_rank",
     "last5_win_rate","rank_trend","days_since_last",
     "mark_num","win_rate_4m","top2_rate_4m","top3_rate_4m","nige_4m","maku_4m"]
req = ["race_score","class_num","style_num","gear","score_rank","is_honmei",
       "n_players_in_race","prev1_rank","last3_avg_rank","last5_avg_rank",
       "last5_win_rate","rank_trend","days_since_last","rank_num"]

LGB = dict(n_estimators=1000, learning_rate=0.05, num_leaves=31,
           min_child_samples=50, verbose=-1, random_state=42)

def fit_es(Xtr, ytr, Xv, yv):
    m = lgb.LGBMClassifier(**LGB)
    m.fit(Xtr, ytr, eval_set=[(Xv,yv)],
          callbacks=[lgb.early_stopping(50,verbose=False), lgb.log_evaluation(-1)])
    return m

def is_hit(pred_1, pred_2, pred_3, san_combo):
    combo = str(san_combo) if pd.notna(san_combo) else ""
    parts = re.split(r"[-=]", combo)
    if len(parts) < 3: return False
    try: return int(parts[0])==pred_1 and int(parts[1])==pred_2 and int(parts[2])==pred_3
    except: return False

df_model = df_all.dropna(subset=req).copy()
test_months = [m for m in sorted(df_model["date"].str[:7].unique()) if m >= "2024-01"]

all_rows = []  # 全予測行を保存

for test_month in test_months:
    train_data = df_model[df_model["date"].str[:7] < test_month]
    test_data  = df_model[df_model["date"].str[:7] == test_month]
    if len(train_data) < 5000 or len(test_data) == 0: continue

    val_months = sorted(train_data["date"].str[:7].unique())[-3:]
    val_mask = train_data["date"].str[:7].isin(val_months)
    df_tr, df_val = train_data[~val_mask], train_data[val_mask]

    m1 = fit_es(df_tr[F].values, df_tr["is_winner"].values, df_val[F].values, df_val["is_winner"].values)
    m2 = fit_es(df_tr[F].values, df_tr["is_2nd"].values,    df_val[F].values, df_val["is_2nd"].values)
    m3 = fit_es(df_tr[F].values, df_tr["is_3rd"].values,    df_val[F].values, df_val["is_3rd"].values)

    test_data = test_data.copy()
    test_data["win_proba"] = m1.predict_proba(test_data[F].values)[:,1]
    test_data["p2_proba"]  = m2.predict_proba(test_data[F].values)[:,1]
    test_data["p3_proba"]  = m3.predict_proba(test_data[F].values)[:,1]

    P2_GAP = 0.05
    for race_id, g in test_data.groupby("race_id"):
        g = g.sort_values("win_proba", ascending=False)
        if len(g) < 3: continue
        pred1     = int(g.iloc[0]["banum"])
        win_proba = float(g.iloc[0]["win_proba"])
        rest2     = g[g["banum"] != pred1].sort_values("p2_proba", ascending=False)
        pred2a    = int(rest2.iloc[0]["banum"]) if len(rest2) >= 1 else None
        pred2b    = int(rest2.iloc[1]["banum"]) if len(rest2) >= 2 else None
        p2_gap    = float(rest2.iloc[0]["p2_proba"] - rest2.iloc[1]["p2_proba"]) if len(rest2) >= 2 else 1.0
        use_form  = (p2_gap < P2_GAP) and (pred2b is not None)
        def pick3(excl):
            r = g[~g["banum"].isin(excl)].sort_values("p3_proba", ascending=False)
            return int(r.iloc[0]["banum"]) if len(r) >= 1 else None
        meta = {"month": test_month, "race_id": race_id, "win_proba": win_proba,
                "san_combo": g["san_combo"].iloc[0], "san_payout": g["san_payout"].iloc[0],
                "score_gap": score_gap_raw.get(race_id, np.nan),
                "top_score": top_score_raw.get(race_id, np.nan),
                "n_players": n_players_raw.get(race_id, np.nan)}
        all_rows.append({**meta, "pred_1st": pred1, "pred_2nd": pred2a,
                         "pred_3rd": pick3([pred1, pred2a])})
        if use_form:
            all_rows.append({**meta, "pred_1st": pred1, "pred_2nd": pred2b,
                             "pred_3rd": pick3([pred1, pred2b])})

rp = pd.DataFrame(all_rows)
rp["hit"] = rp.apply(
    lambda r: is_hit(r["pred_1st"], r["pred_2nd"], r["pred_3rd"], r["san_combo"]), axis=1)

# win_proba 15-20% に絞る
sub = rp[(rp["win_proba"] >= 0.15) & (rp["win_proba"] < 0.20)].copy()

print("=" * 60)
print("  win_proba 15〜20% 月別詳細")
print("=" * 60)
print(f"{'月':<10} {'賭件数':>6} {'的中':>5} {'的中率':>7} {'投資':>8} {'払戻':>10} {'回収率':>7}  的中払戻リスト")
print("-" * 60)

totals = {"bets":0, "hits":0, "invest":0, "payout":0}
for month, g in sub.groupby("month"):
    bets   = len(g)
    hits   = int(g["hit"].sum())
    invest = bets * 100
    payout = int(g[g["hit"]]["san_payout"].sum()) if hits > 0 else 0
    roi    = payout / invest * 100 if invest > 0 else 0
    hit_rate = hits / bets * 100 if bets > 0 else 0
    payouts_list = sorted(g[g["hit"]]["san_payout"].tolist(), reverse=True)
    pay_str = "  ".join([f"{int(p):,}円" for p in payouts_list]) if payouts_list else "-"
    print(f"{month:<10} {bets:>6} {hits:>5} {hit_rate:>6.1f}% {invest:>8,} {payout:>10,} {roi:>6.0f}%  {pay_str}")
    totals["bets"]   += bets
    totals["hits"]   += hits
    totals["invest"] += invest
    totals["payout"] += payout

print("-" * 60)
total_roi = totals["payout"] / totals["invest"] * 100 if totals["invest"] else 0
total_hr  = totals["hits"] / totals["bets"] * 100 if totals["bets"] else 0
print(f"{'合計':<10} {totals['bets']:>6} {totals['hits']:>5} {total_hr:>6.1f}% "
      f"{totals['invest']:>8,} {totals['payout']:>10,} {total_roi:>6.0f}%")

print(f"\n月平均回収率: {sub.groupby('month').apply(lambda g: g[g['hit']]['san_payout'].sum()/(len(g)*100)*100).mean():.0f}%")
print(f"的中時平均払戻: {sub[sub['hit']]['san_payout'].mean():.0f}円")
print(f"的中時中央値払戻: {sub[sub['hit']]['san_payout'].median():.0f}円")
print(f"最高払戻: {sub[sub['hit']]['san_payout'].max():,.0f}円")
