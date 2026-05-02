# -*- coding: utf-8 -*-
"""
2026年4月 ブラインドテスト
訓練: ~2026-03  /  検証: 2026-04（答え合わせ）
動的フォーメーション: p2_gap < 0.05 → 2通り、>= 0.05 → 1通り
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

TEST_MONTH = "2026-04"

DATA_DIR = Path("keirin_data")
files = [f for f in sorted(glob.glob(str(DATA_DIR / "202[3456]_*.csv"))) if "sample" not in f]
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

# 訓練: TEST_MONTH より前、検証(ES用): 末尾3ヶ月
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

P2_GAP_THRESH = 0.05
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
    use_form = (p2_gap < P2_GAP_THRESH) and (pred_2b is not None)

    def pick3(excl):
        r = g[~g["banum"].isin(excl)].sort_values("p3_proba", ascending=False)
        return int(r.iloc[0]["banum"]) if len(r) >= 1 else None

    meta = {"race_id": race_id, "top_proba": top_proba, "p2_gap": p2_gap,
            "san_combo": g["san_combo"].iloc[0], "san_payout": g["san_payout"].iloc[0],
            "score_gap": score_gap_raw.get(race_id, np.nan),
            "top_score": top_score_raw.get(race_id, np.nan),
            "n_players": n_players_raw.get(race_id, np.nan),
            "use_form": use_form}

    rows.append({**meta, "pred_1st": pred_1, "pred_2nd": pred_2a, "pred_3rd": pick3([pred_1, pred_2a])})
    if use_form:
        rows.append({**meta, "pred_1st": pred_1, "pred_2nd": pred_2b, "pred_3rd": pick3([pred_1, pred_2b])})

rp = pd.DataFrame(rows)
filt = rp[(rp["top_score"] >= 95) & (rp["score_gap"] >= 2) & (rp["n_players"] == 7)].copy()

def is_hit(row):
    combo = str(row["san_combo"]) if pd.notna(row["san_combo"]) else ""
    parts = re.split(r"[-=]", combo)
    if len(parts) < 3: return False
    try:
        return int(parts[0])==row["pred_1st"] and int(parts[1])==row["pred_2nd"] and int(parts[2])==row["pred_3rd"]
    except: return False

filt["hit"] = filt.apply(is_hit, axis=1)

total_bets   = len(filt)
total_hits   = filt["hit"].sum()
total_payout = filt[filt["hit"]]["san_payout"].sum()
total_bet_yen = total_bets * 100
roi          = total_payout / total_bet_yen * 100 if total_bet_yen else 0
hit_rate     = total_hits / total_bets * 100 if total_bets else 0

# レース単位的中率（少なくとも1通りが当たった率）
race_hits = filt.groupby("race_id")["hit"].any().sum()
total_races = filt["race_id"].nunique()
race_hit_rate = race_hits / total_races * 100 if total_races else 0

# フォーメーション内訳
single = filt[~filt["use_form"]]
double = filt[filt["use_form"]]

print(f"\n{'='*50}")
print(f"  {TEST_MONTH} ブラインドテスト結果")
print(f"{'='*50}")
print(f"対象レース数: {total_races}  賭け件数: {total_bets}")
print(f"  うち確信→1通り: {single['race_id'].nunique()}レース ({len(single)}件)")
print(f"  うち接戦→2通り: {double['race_id'].nunique()}レース ({len(double)}件)")
print()
print(f"【全体】")
print(f"  的中数: {total_hits}  的中率(賭け単位): {hit_rate:.1f}%")
print(f"  レース的中率: {race_hit_rate:.1f}%  ({race_hits}/{total_races}レース)")
print(f"  総投資: {total_bet_yen:,}円  払戻: {total_payout:,}円  回収率: {roi:.1f}%")
print()

# 1通りvs2通りの的中率比較
if len(single) > 0:
    s_hits = single["hit"].sum()
    print(f"【確信→1通り (p2gap≥0.05)】")
    print(f"  的中率: {s_hits/len(single)*100:.1f}% ({s_hits}/{len(single)}件)  "
          f"回収率: {single[single['hit']]['san_payout'].sum()/len(single)/100*100:.1f}%")
if len(double) > 0:
    d_hits = double["hit"].sum()
    d_race_hits = double.groupby("race_id")["hit"].any().sum()
    d_races = double["race_id"].nunique()
    print(f"【接戦→2通り (p2gap<0.05)】")
    print(f"  賭け的中率: {d_hits/len(double)*100:.1f}% ({d_hits}/{len(double)}件)  "
          f"レース的中率: {d_race_hits/d_races*100:.1f}% ({d_race_hits}/{d_races}レース)  "
          f"回収率: {double[double['hit']]['san_payout'].sum()/len(double)/100*100:.1f}%")

# 日別集計
print(f"\n【日別】")
filt["date"] = filt["race_id"].map(df_test.groupby("race_id")["date"].first())
daily = filt.groupby("date").agg(
    bets=("hit","count"), hits=("hit","sum"),
    payout=("san_payout", lambda x: x[filt.loc[x.index,"hit"]].sum())
).reset_index()
daily["hit_rate"] = daily["hits"]/daily["bets"]*100
daily["roi"]      = daily["payout"]/daily["bets"]/100*100
for _, r in daily.iterrows():
    print(f"  {r['date']}  賭:{int(r['bets']):3d}件  的中:{int(r['hits']):2d}件  "
          f"({r['hit_rate']:.1f}%)  回収率:{r['roi']:.0f}%")

# 的中したレースの詳細
hits_df = filt[filt["hit"]].copy()
hits_df["date"] = hits_df["race_id"].map(df_test.groupby("race_id")["date"].first())
hits_df["venue"] = hits_df["race_id"].map(df_test.groupby("race_id")["venue_slug"].first())
hits_df["race_no"] = hits_df["race_id"].map(df_test.groupby("race_id")["race_no"].first())
print(f"\n【的中レース一覧 ({total_hits}件)】")
for _, r in hits_df.iterrows():
    p2 = int(r["pred_2nd"]) if pd.notna(r["pred_2nd"]) else "?"
    p3 = int(r["pred_3rd"]) if pd.notna(r["pred_3rd"]) else "?"
    form = "2通" if r["use_form"] else "1通"
    print(f"  {r['date']} {r['venue']} {int(r['race_no'])}R  "
          f"{int(r['pred_1st'])}-{p2}-{p3}  [{form}]  払戻:{int(r['san_payout']):,}円  "
          f"(score:{r['top_score']:.0f} gap:{r['score_gap']:.1f})")
