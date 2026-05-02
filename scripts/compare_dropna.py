# -*- coding: utf-8 -*-
"""strict dropna vs loose dropna の予測差分比較（2024-02）"""
import glob, re, warnings
import numpy as np, pandas as pd, lightgbm as lgb
warnings.filterwarnings("ignore")

files = sorted([f for f in glob.glob("keirin_data/202[34]_*.csv") if "sample" not in f])
df_all = pd.concat([pd.read_csv(f, encoding="utf-8-sig") for f in files], ignore_index=True)

def parse_payout(s):
    if pd.isna(s) or str(s).strip() == "": return None, None
    m = re.search(r"(\d+[-=]\d+(?:[-=]\d+)?)\s+([\d,]+)円", str(s).strip())
    if m: return m.group(1), int(m.group(2).replace(",", ""))
    return None, None

df_all[["san_combo","san_payout"]] = df_all["san_ren_tan"].apply(lambda x: pd.Series(parse_payout(x)))
CLASS_MAP = {"S1":4,"S2":3,"A1":2,"A2":1,"B":0}
STYLE_MAP  = {"逃":5,"捲":4,"両":3,"差":2,"追":1,"マ":0}
df_all["player_key"] = df_all["player_name"].astype(str)+"_"+df_all["term"].astype(str)
df_all["rank_num"]   = pd.to_numeric(df_all["rank"], errors="coerce")
df_all["class_num"]  = df_all["player_class"].map(CLASS_MAP).fillna(1)
df_all["style_num"]  = df_all["running_style"].map(STYLE_MAP).fillna(2)
df_all["race_score"] = pd.to_numeric(df_all["race_score"], errors="coerce")
df_all["gear"]       = pd.to_numeric(df_all["gear"], errors="coerce")
df_all["banum"]      = pd.to_numeric(df_all["banum"], errors="coerce")
df_all["is_win"]     = (df_all["rank_num"]==1).astype(float)
df_all["is_winner"]  = (df_all["rank_num"]==1).astype(int)
df_all["is_2nd"]     = (df_all["rank_num"]==2).astype(int)
df_all["is_3rd"]     = (df_all["rank_num"]==3).astype(int)
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
req_pred = ["race_score","class_num","style_num","gear","banum"]

LGB = dict(n_estimators=1000, learning_rate=0.05, num_leaves=31, min_child_samples=50, verbose=-1, random_state=42)
def fit_es(Xtr,ytr,Xv,yv):
    m = lgb.LGBMClassifier(**LGB)
    m.fit(Xtr,ytr,eval_set=[(Xv,yv)],callbacks=[lgb.early_stopping(50,verbose=False),lgb.log_evaluation(-1)])
    return m
def is_hit(p1,p2,p3,combo):
    combo=str(combo) if pd.notna(combo) else ""
    parts=re.split(r"[-=]",combo)
    if len(parts)<3: return False
    try: return int(parts[0])==p1 and int(parts[1])==p2 and int(parts[2])==p3
    except: return False

df_strict = df_all.dropna(subset=req).copy()
df_loose  = df_all.dropna(subset=req_pred).copy()

# 2024-02のみテスト（モデルは共通）
test_month = "2024-02"
train = df_strict[df_strict["date"].str[:7] < test_month]
val_months = sorted(train["date"].str[:7].unique())[-3:]
val_mask = train["date"].str[:7].isin(val_months)
df_tr, df_val = train[~val_mask], train[val_mask]
m1 = fit_es(df_tr[F].values, df_tr["is_winner"].values, df_val[F].values, df_val["is_winner"].values)
m2 = fit_es(df_tr[F].values, df_tr["is_2nd"].values,    df_val[F].values, df_val["is_2nd"].values)
m3 = fit_es(df_tr[F].values, df_tr["is_3rd"].values,    df_val[F].values, df_val["is_3rd"].values)

for label, td_src in [("旧(strict)", df_strict), ("新(loose)", df_loose)]:
    td = td_src[td_src["date"].str[:7]==test_month].copy()
    td["wp"] = m1.predict_proba(td[F].values)[:,1]
    td["p2"] = m2.predict_proba(td[F].values)[:,1]
    td["p3"] = m3.predict_proba(td[F].values)[:,1]
    rows = []
    for race_id, g in td.groupby("race_id"):
        ts=top_score_raw.get(race_id,np.nan); sg=score_gap_raw.get(race_id,np.nan); np_=n_players_raw.get(race_id,np.nan)
        if not (ts>=95 and sg>=2 and np_==7): continue
        if len(g)<3: continue
        g = g.sort_values("wp", ascending=False)
        pred1 = int(g.iloc[0]["banum"])
        rest2 = g[g["banum"]!=pred1].sort_values("p2", ascending=False)
        p2a = int(rest2.iloc[0]["banum"]) if len(rest2)>=1 else None
        p2b = int(rest2.iloc[1]["banum"]) if len(rest2)>=2 else None
        gap = float(rest2.iloc[0]["p2"]-rest2.iloc[1]["p2"]) if len(rest2)>=2 else 1.0
        def pick3(excl):
            r = g[~g["banum"].isin(excl)].sort_values("p3", ascending=False)
            return int(r.iloc[0]["banum"]) if len(r)>=1 else None
        sc = g["san_combo"].iloc[0]; sp = g["san_payout"].iloc[0]
        rows.append({"p1":pred1,"p2":p2a,"p3":pick3([pred1,p2a]),"sc":sc,"sp":sp})
        if gap < 0.05 and p2b:
            rows.append({"p1":pred1,"p2":p2b,"p3":pick3([pred1,p2b]),"sc":sc,"sp":sp})
    rp = pd.DataFrame(rows)
    rp["hit"] = rp.apply(lambda r: is_hit(r["p1"],r["p2"],r["p3"],r["sc"]), axis=1)
    bets = len(rp); hits = int(rp["hit"].sum())
    pay = int(rp[rp["hit"]]["sp"].sum()) if hits>0 else 0
    roi = pay/(bets*100)*100
    print(f"\n{label}: {bets}件 {hits}的中 ROI={roi:.0f}% (払戻{pay:,}円)")
    if hits>0:
        payouts = sorted(rp[rp["hit"]]["sp"].tolist(), reverse=True)
        print(f"  的中払戻リスト: {payouts}")
