# -*- coding: utf-8 -*-
"""旧モデル(追加特徴量) vs 現行モデル(revert後) walkforward比較"""
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
df_all["agari"]        = pd.to_numeric(df_all.get("agari"), errors="coerce")
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

# 追加特徴量（旧モデル用）
agari_grp = df_all.groupby("player_key")["agari"]
df_all["last3_avg_agari"] = agari_grp.transform(lambda x: x.shift(1).rolling(3,min_periods=1).mean())
df_all["last5_avg_agari"] = agari_grp.transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())
pref_count = df_all.groupby(["race_id","pref"])["banum"].transform("count")
term_count = df_all.groupby(["race_id","term"])["banum"].transform("count")
df_all["same_pref_count"] = (pref_count - 1).clip(lower=0)
df_all["same_term_count"] = (term_count - 1).clip(lower=0)
nige_in_race = df_all.groupby("race_id")["running_style"].transform(lambda x: (x=="逃").sum())
df_all["nige_in_race"] = nige_in_race
df_all_sorted = df_all.sort_values(["player_key","date","race_no"])
df_all["venue_win_rate"] = df_all_sorted.groupby(["player_key","venue_slug"])["is_win"].transform(
    lambda x: x.shift(1).rolling(10,min_periods=1).mean())

def extract_honmei(x):
    if pd.isna(x): return None
    m = re.search(r"◎(\d+)", str(x))
    return int(m.group(1)) if m else None
df_all["honmei_banum"] = df_all["lineup"].apply(extract_honmei)
df_all["score_rank"]   = df_all.groupby("race_id")["race_score"].rank(ascending=False, method="min")
df_all["is_honmei"]    = (df_all["banum"]==df_all["honmei_banum"]).astype(int)
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

# ========== 特徴量定義 ==========
F_NEW = ["race_score","class_num","style_num","gear","score_rank","is_honmei",
         "n_players_in_race","prev1_rank","last3_avg_rank","last5_avg_rank",
         "last5_win_rate","rank_trend","days_since_last",
         "mark_num","win_rate_4m","top2_rate_4m","top3_rate_4m","nige_4m","maku_4m"]

F_OLD      = F_NEW + ["last3_avg_agari","last5_avg_agari",
                      "same_pref_count","same_term_count","nige_in_race","venue_win_rate"]
F_OLD_COND = F_OLD + ["honmei_style_num","honmei_race_score"]  # model2/3用

REQ = ["race_score","class_num","style_num","gear","score_rank","is_honmei",
       "n_players_in_race","prev1_rank","last3_avg_rank","last5_avg_rank",
       "last5_win_rate","rank_trend","days_since_last","rank_num"]
REQ_PRED = ["race_score","class_num","style_num","gear","banum"]

LGB = dict(n_estimators=1000, learning_rate=0.05, num_leaves=31,
           min_child_samples=50, verbose=-1, random_state=42)

def fit_es(Xtr, ytr, Xv, yv):
    m = lgb.LGBMClassifier(**LGB)
    m.fit(Xtr, ytr, eval_set=[(Xv,yv)],
          callbacks=[lgb.early_stopping(50,verbose=False), lgb.log_evaluation(-1)])
    return m

def is_hit(p1, p2, p3, combo):
    combo = str(combo) if pd.notna(combo) else ""
    parts = re.split(r"[-=]", combo)
    if len(parts) < 3: return False
    try: return int(parts[0])==p1 and int(parts[1])==p2 and int(parts[2])==p3
    except: return False

df_train_pool = df_all.dropna(subset=REQ).copy()
df_pred_pool  = df_all.dropna(subset=REQ_PRED).copy()
test_months = [m for m in sorted(df_train_pool["date"].str[:7].unique()) if m >= "2024-01"]

results = {"current": [], "old": []}

for test_month in test_months:
    print(f"  {test_month}...", flush=True)
    train_data = df_train_pool[df_train_pool["date"].str[:7] < test_month]
    test_data  = df_pred_pool[df_pred_pool["date"].str[:7] == test_month]
    if len(train_data) < 5000 or len(test_data) == 0: continue

    val_months = sorted(train_data["date"].str[:7].unique())[-3:]
    val_mask = train_data["date"].str[:7].isin(val_months)
    df_tr, df_val = train_data[~val_mask], train_data[val_mask]

    # 現行モデル
    m1  = fit_es(df_tr[F_NEW].values, df_tr["is_winner"].values, df_val[F_NEW].values, df_val["is_winner"].values)
    m2  = fit_es(df_tr[F_NEW].values, df_tr["is_2nd"].values,    df_val[F_NEW].values, df_val["is_2nd"].values)
    m3  = fit_es(df_tr[F_NEW].values, df_tr["is_3rd"].values,    df_val[F_NEW].values, df_val["is_3rd"].values)

    # 旧モデル
    o1  = fit_es(df_tr[F_OLD].values,      df_tr["is_winner"].values, df_val[F_OLD].values,      df_val["is_winner"].values)
    o2  = fit_es(df_tr[F_OLD_COND].values, df_tr["is_2nd"].values,    df_val[F_OLD_COND].values, df_val["is_2nd"].values)
    o3  = fit_es(df_tr[F_OLD_COND].values, df_tr["is_3rd"].values,    df_val[F_OLD_COND].values, df_val["is_3rd"].values)

    td = test_data.copy()
    td["wp_cur"] = m1.predict_proba(td[F_NEW].values)[:,1]
    td["p2_cur"] = m2.predict_proba(td[F_NEW].values)[:,1]
    td["p3_cur"] = m3.predict_proba(td[F_NEW].values)[:,1]
    td["wp_old"] = o1.predict_proba(td[F_OLD].values)[:,1]
    td["p2_old"] = o2.predict_proba(td[F_OLD_COND].values)[:,1]
    td["p3_old"] = o3.predict_proba(td[F_OLD_COND].values)[:,1]

    P2_GAP = 0.05
    for race_id, g in td.groupby("race_id"):
        top_score = top_score_raw.get(race_id, np.nan)
        score_gap = score_gap_raw.get(race_id, np.nan)
        n_players = n_players_raw.get(race_id, np.nan)
        if not (top_score >= 95 and score_gap >= 2 and n_players == 7): continue
        if len(g) < 3: continue
        san_combo  = g["san_combo"].iloc[0]
        san_payout = g["san_payout"].iloc[0]

        for model_key, wp_col, p2_col, p3_col in [
            ("current", "wp_cur", "p2_cur", "p3_cur"),
            ("old",     "wp_old", "p2_old", "p3_old"),
        ]:
            gs = g.sort_values(wp_col, ascending=False)
            pred1  = int(gs.iloc[0]["banum"])
            rest2  = gs[gs["banum"]!=pred1].sort_values(p2_col, ascending=False)
            pred2a = int(rest2.iloc[0]["banum"]) if len(rest2)>=1 else None
            pred2b = int(rest2.iloc[1]["banum"]) if len(rest2)>=2 else None
            p2gap  = float(rest2.iloc[0][p2_col]-rest2.iloc[1][p2_col]) if len(rest2)>=2 else 1.0
            use_form = (p2gap < P2_GAP) and pred2b is not None
            def pick3(excl):
                r = gs[~gs["banum"].isin(excl)].sort_values(p3_col, ascending=False)
                return int(r.iloc[0]["banum"]) if len(r)>=1 else None
            meta = {"month": test_month, "san_combo": san_combo, "san_payout": san_payout}
            results[model_key].append({**meta, "pred_1st": pred1, "pred_2nd": pred2a,
                                       "pred_3rd": pick3([pred1, pred2a])})
            if use_form:
                results[model_key].append({**meta, "pred_1st": pred1, "pred_2nd": pred2b,
                                           "pred_3rd": pick3([pred1, pred2b])})

print("\n" + "="*65)
print(f"{'モデル':<10} {'賭件数':>6} {'的中':>5} {'的中率':>7} {'投資':>9} {'払戻':>10} {'回収率':>7}")
print("-"*65)
for key, label in [("current","現行(revert後)"), ("old","旧(追加特徴量)")]:
    rp = pd.DataFrame(results[key])
    rp["hit"] = rp.apply(lambda r: is_hit(r["pred_1st"],r["pred_2nd"],r["pred_3rd"],r["san_combo"]), axis=1)
    bets = len(rp); hits = int(rp["hit"].sum())
    invest = bets*100
    payout = int(rp[rp["hit"]]["san_payout"].sum()) if hits>0 else 0
    roi = payout/invest*100 if invest>0 else 0
    print(f"{label:<12} {bets:>6} {hits:>5} {hits/bets*100:>6.1f}% {invest:>9,} {payout:>10,} {roi:>6.0f}%")

    print(f"  月別回収率:")
    for month, g in rp.groupby("month"):
        b=len(g); h=int(g["hit"].sum()); p=int(g[g["hit"]]["san_payout"].sum()) if h>0 else 0
        r=p/(b*100)*100
        print(f"    {month}  {b:>4}件  {h}的中  {r:>6.0f}%")
    print()
