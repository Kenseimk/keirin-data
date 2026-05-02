# -*- coding: utf-8 -*-
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
    meta = {"race_id": race_id, "p2_gap": p2_gap, "use_form": use_form,
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

def stats(df, label):
    if len(df) == 0:
        print(f"{label}: データなし")
        return
    races = df["race_id"].nunique()
    bets  = len(df)
    hits  = int(df["hit"].sum())
    pay   = df[df["hit"]]["san_payout"].sum()
    roi   = pay / (bets * 100) * 100
    print(f"{label}: {races}レース {bets:4d}件  的中{hits:3d}件  的中率{hits/bets*100:.1f}%  回収率{roi:.0f}%")

print("=== 2026-04 フィルター比較 ===")
stats(rp, "フィルターなし         ")
stats(rp[rp["top_score"]>=95], "top_score>=95のみ      ")
stats(rp[rp["top_score"]>=95 & (rp["n_players"]==7)], "top>=95 & 7車          ")
stats(rp[(rp["top_score"]>=95) & (rp["score_gap"]>=2)], "top>=95 & gap>=2       ")
stats(rp[(rp["top_score"]>=95) & (rp["score_gap"]>=2) & (rp["n_players"]==7)], "現行(top+gap+7車)      ")

print()
print("=== 的中レース全件（フィルターなし） ===")
for _, r in rp[rp["hit"]].sort_values("date").iterrows():
    tag = "2通" if r["use_form"] else "1通"
    print(f"  {r['date']} {r['venue']} {int(r['race_no'])}R  "
          f"{int(r['pred_1st'])}-{int(r['pred_2nd'])}-{int(r['pred_3rd'])}  "
          f"[{tag}]  払戻:{int(r['san_payout']):,}円  "
          f"(score:{r['top_score']:.0f} gap:{r['score_gap']:.1f} n:{r['n_players']:.0f})")
