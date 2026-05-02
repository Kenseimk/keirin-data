# -*- coding: utf-8 -*-
"""高払戻し的中の特徴を分析"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
import pandas as pd, re, numpy as np, glob, warnings
import lightgbm as lgb
warnings.filterwarnings("ignore")

TEST_MONTH = "2026-04"
files = [f for f in sorted(glob.glob("keirin_data/202[3456]_*.csv")) if "sample" not in f]
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

df_model = df_all.dropna(subset=req).copy()
df_train  = df_model[df_model["date"].str[:7] < TEST_MONTH]
df_test   = df_model[df_model["date"].str[:7] == TEST_MONTH]
val_months = sorted(df_train["date"].str[:7].unique())[-3:]
df_tr  = df_train[~df_train["date"].str[:7].isin(val_months)]
df_val = df_train[ df_train["date"].str[:7].isin(val_months)]

LGB = dict(n_estimators=1000, learning_rate=0.05, num_leaves=31, min_child_samples=50, verbose=-1, random_state=42)
def fit_es(Xtr, ytr, Xv, yv):
    m = lgb.LGBMClassifier(**LGB)
    m.fit(Xtr, ytr, eval_set=[(Xv,yv)],
          callbacks=[lgb.early_stopping(50,verbose=False), lgb.log_evaluation(-1)])
    return m
m1 = fit_es(df_tr[F].values, df_tr["is_winner"].values, df_val[F].values, df_val["is_winner"].values)
m2 = fit_es(df_tr[F].values, df_tr["is_2nd"].values,    df_val[F].values, df_val["is_2nd"].values)
m3 = fit_es(df_tr[F].values, df_tr["is_3rd"].values,    df_val[F].values, df_val["is_3rd"].values)

df_test = df_test.copy()
df_test["win_proba"] = m1.predict_proba(df_test[F].values)[:,1]
df_test["p2_proba"]  = m2.predict_proba(df_test[F].values)[:,1]
df_test["p3_proba"]  = m3.predict_proba(df_test[F].values)[:,1]

P2_GAP = 0.05
rows = []
for race_id, g in df_test.groupby("race_id"):
    g = g.sort_values("win_proba", ascending=False)
    if len(g) < 3: continue
    pred1   = int(g.iloc[0]["banum"])
    rest2   = g[g["banum"] != pred1].sort_values("p2_proba", ascending=False)
    pred2a  = int(rest2.iloc[0]["banum"]) if len(rest2) >= 1 else None
    pred2b  = int(rest2.iloc[1]["banum"]) if len(rest2) >= 2 else None
    p2_gap  = float(rest2.iloc[0]["p2_proba"] - rest2.iloc[1]["p2_proba"]) if len(rest2) >= 2 else 1.0
    use_form = (p2_gap < P2_GAP) and (pred2b is not None)
    def pick3(excl):
        r = g[~g["banum"].isin(excl)].sort_values("p3_proba", ascending=False)
        return int(r.iloc[0]["banum"]) if len(r) >= 1 else None
    meta = {"race_id": race_id, "use_form": use_form, "p2_gap": p2_gap,
            "win_proba": float(g.iloc[0]["win_proba"]),
            "san_combo": g["san_combo"].iloc[0], "san_payout": g["san_payout"].iloc[0],
            "score_gap": score_gap_raw.get(race_id, np.nan),
            "top_score": top_score_raw.get(race_id, np.nan),
            "n_players": n_players_raw.get(race_id, np.nan)}
    rows.append({**meta, "pred_1st": pred1, "pred_2nd": pred2a, "pred_3rd": pick3([pred1,pred2a])})
    if use_form:
        rows.append({**meta, "pred_1st": pred1, "pred_2nd": pred2b, "pred_3rd": pick3([pred1,pred2b])})

rp = pd.DataFrame(rows)
rp["date"]    = rp["race_id"].map(df_test.groupby("race_id")["date"].first())
rp["venue"]   = rp["race_id"].map(df_test.groupby("race_id")["venue_slug"].first())
rp["race_no"] = rp["race_id"].map(df_test.groupby("race_id")["race_no"].first())

def is_hit(row):
    combo = str(row["san_combo"]) if pd.notna(row["san_combo"]) else ""
    parts = re.split(r"[-=]", combo)
    if len(parts) < 3: return False
    try:
        return (int(parts[0])==row["pred_1st"] and
                int(parts[1])==row["pred_2nd"] and
                int(parts[2])==row["pred_3rd"])
    except: return False
rp["hit"] = rp.apply(is_hit, axis=1)

hits = rp[rp["hit"]].copy()
hits["payout"] = hits["san_payout"]

print("=== 的中レースの払戻し分布（フィルターなし・4月全体）===")
print(f"的中件数: {len(hits)}件")
print(f"  1万円以上: {(hits['payout']>=10000).sum()}件")
print(f"  5千〜1万: {((hits['payout']>=5000)&(hits['payout']<10000)).sum()}件")
print(f"  1千〜5千: {((hits['payout']>=1000)&(hits['payout']<5000)).sum()}件")
print(f"  1千円未満: {(hits['payout']<1000).sum()}件")

print("\n=== 1万円以上の的中レース ===")
big = hits[hits["payout"] >= 10000].sort_values("payout", ascending=False)
for _, r in big.iterrows():
    print(f"  {r['date']} {r['venue']} {int(r['race_no'])}R  払戻:{int(r['payout']):,}円")
    print(f"    top_score={r['top_score']:.0f}  score_gap={r['score_gap']:.2f}  "
          f"win_proba={r['win_proba']:.3f}  p2_gap={r['p2_gap']:.3f}  n={r['n_players']:.0f}")

print("\n=== score_gap別 払戻し中央値・的中率（フィルターなし）===")
rp["gap_band"] = pd.cut(rp["score_gap"],
    bins=[-0.01, 0.5, 1.0, 2.0, 4.0, 100],
    labels=["0〜0.5", "0.5〜1", "1〜2", "2〜4", "4+"])
for band, g in rp.groupby("gap_band"):
    h = g[g["hit"]]
    hit_rate = len(h)/len(g)*100 if len(g) else 0
    med_pay = h["san_payout"].median() if len(h) else 0
    avg_pay = h["san_payout"].mean() if len(h) else 0
    roi = h["san_payout"].sum()/(len(g)*100)*100 if len(g) else 0
    print(f"  gap {band}: {len(g):4d}件  的中率{hit_rate:.1f}%  "
          f"中央値{med_pay:.0f}円  平均払戻{avg_pay:.0f}円  回収率{roi:.0f}%")

print("\n=== top_score別 払戻し中央値・的中率（フィルターなし）===")
rp["score_band"] = pd.cut(rp["top_score"],
    bins=[0, 80, 90, 95, 100, 105, 110, 200],
    labels=["〜80", "80-90", "90-95", "95-100", "100-105", "105-110", "110+"])
for band, g in rp.groupby("score_band"):
    h = g[g["hit"]]
    hit_rate = len(h)/len(g)*100 if len(g) else 0
    avg_pay = h["san_payout"].mean() if len(h) else 0
    roi = h["san_payout"].sum()/(len(g)*100)*100 if len(g) else 0
    print(f"  score {band}: {len(g):4d}件  的中率{hit_rate:.1f}%  平均払戻{avg_pay:.0f}円  回収率{roi:.0f}%")

print("\n=== win_proba別 払戻し・的中率（フィルターなし）===")
rp["proba_band"] = pd.cut(rp["win_proba"],
    bins=[0, 0.15, 0.20, 0.25, 0.30, 0.40, 1.0],
    labels=["〜15%", "15-20%", "20-25%", "25-30%", "30-40%", "40%+"])
for band, g in rp.groupby("proba_band"):
    h = g[g["hit"]]
    hit_rate = len(h)/len(g)*100 if len(g) else 0
    avg_pay = h["san_payout"].mean() if len(h) else 0
    roi = h["san_payout"].sum()/(len(g)*100)*100 if len(g) else 0
    print(f"  proba {band}: {len(g):4d}件  的中率{hit_rate:.1f}%  平均払戻{avg_pay:.0f}円  回収率{roi:.0f}%")
