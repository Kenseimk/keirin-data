# -*- coding: utf-8 -*-
import sys, io as _io
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = _io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import pandas as pd, re, warnings
warnings.filterwarnings("ignore")
import numpy as np
import lightgbm as lgb
from pathlib import Path
import glob

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

def calc_gap(x):
    s = sorted(x.dropna(), reverse=True)
    return s[0]-s[1] if len(s)>=2 else np.nan

score_gap_raw = df_all.groupby("race_id")["race_score"].apply(calc_gap)
top_score_raw = df_all.groupby("race_id")["race_score"].max()
n_players_raw = df_all.groupby("race_id")["banum"].nunique()
df_model = df_all.dropna(subset=FEATURES+["rank_num"]).copy()

# 実際の結果
df_apr = pd.read_csv("keirin_data/2026_04_keirin.csv", encoding="utf-8-sig")
df_apr["rank_num"] = pd.to_numeric(df_apr["rank"], errors="coerce")
df1r = df_apr[df_apr["rank_num"]==1][["race_id","banum"]].rename(columns={"banum":"w1"})
df2r = df_apr[df_apr["rank_num"]==2][["race_id","banum"]].rename(columns={"banum":"w2"})
df3r = df_apr[df_apr["rank_num"]==3][["race_id","banum"]].rename(columns={"banum":"w3"})

def get_payout_val(s):
    m = re.search(r"([\d,]+)円", str(s))
    return int(m.group(1).replace(",","")) if m else 0

races_meta = df_apr.drop_duplicates("race_id")[["race_id","venue_slug","date","race_no","san_ren_tan"]]
races_meta = races_meta.merge(df1r,on="race_id",how="left").merge(df2r,on="race_id",how="left").merge(df3r,on="race_id",how="left")
races_meta["payout"] = races_meta["san_ren_tan"].apply(get_payout_val)

TARGET_DATES = ["2026-04-01","2026-04-02","2026-04-03","2026-04-04"]

all_filtered = []
for TARGET_DATE in TARGET_DATES:
    df_train = df_model[df_model["date"] < TARGET_DATE]
    if len(df_train) < 1000:
        continue
    model = lgb.LGBMClassifier(n_estimators=300, learning_rate=0.05,
                                num_leaves=31, min_child_samples=50,
                                verbose=-1, random_state=42)
    model.fit(df_train[FEATURES].values, df_train["is_winner"].values)

    df_today = df_all[df_all["date"] == TARGET_DATE].dropna(subset=FEATURES).copy()
    if df_today.empty:
        continue

    df_today["win_proba"] = model.predict_proba(df_today[FEATURES].values)[:, 1]
    ds = df_today.sort_values(["race_id","win_proba"], ascending=[True,False])
    rp = ds.groupby("race_id").agg(
        venue   =("venue_slug","first"),
        date    =("date","first"),
        race_no =("race_no","first"),
        pred_1st=("banum", lambda x: int(x.iloc[0])),
        pred_2nd=("banum", lambda x: int(x.iloc[1]) if len(x)>1 else None),
        pred_3rd=("banum", lambda x: int(x.iloc[2]) if len(x)>2 else None),
        top_proba=("win_proba", lambda x: x.iloc[0]),
    ).reset_index()

    rp["score_gap"] = rp["race_id"].map(score_gap_raw)
    rp["top_score"] = rp["race_id"].map(top_score_raw)
    rp["n_players"] = rp["race_id"].map(n_players_raw)

    filtered = rp[
        (rp["top_score"] >= 95) &
        (rp["score_gap"] >= 2) &
        (rp["n_players"] == 7)
    ].copy()
    all_filtered.append(filtered)

df_preds = pd.concat(all_filtered, ignore_index=True)
df_preds = df_preds.merge(races_meta[["race_id","w1","w2","w3","payout"]], on="race_id", how="left")

df_preds["tan_hit"] = (
    (df_preds["pred_1st"] == df_preds["w1"]) &
    (df_preds["pred_2nd"] == df_preds["w2"]) &
    (df_preds["pred_3rd"] == df_preds["w3"])
)
df_preds["fuku_hit"] = df_preds.apply(
    lambda r: {r["pred_1st"],r["pred_2nd"],r["pred_3rd"]} == {r["w1"],r["w2"],r["w3"]}
    if pd.notna(r["w1"]) else False, axis=1
)
df_preds["has_result"] = df_preds["w1"].notna()

print("=" * 72)
print("2026年4月 モデル予想 vs 実際結果（フィルター: top_score≥95 & gap≥2 & 7車）")
print("=" * 72)

for date in TARGET_DATES:
    day = df_preds[df_preds["date"] == date]
    if day.empty:
        print(f"\n{date}: 対象レースなし")
        continue
    print(f"\n【{date}】 対象{len(day)}件")
    print(f"  {'場':<12} {'R':>3}  {'予想':>8}  {'実際':>8}  {'三連単':>6}  {'三連複':>6}  {'配当':>8}")
    print(f"  {'-'*65}")
    for _, row in day.sort_values(["venue","race_no"]).iterrows():
        pred = f"{int(row['pred_1st'])}-{int(row['pred_2nd'])}-{int(row['pred_3rd'])}"
        if not row["has_result"]:
            print(f"  {row['venue']:<12} R{int(row['race_no']):>2}  {pred:>8}  {'(未)':>8}")
        else:
            actual = f"{int(row['w1'])}-{int(row['w2'])}-{int(row['w3'])}"
            st = "的中" if row["tan_hit"] else "-"
            sf = "的中" if row["fuku_hit"] else "-"
            pay = f"{int(row['payout']):,}円" if row["tan_hit"] else ""
            print(f"  {row['venue']:<12} R{int(row['race_no']):>2}  {pred:>8}  {actual:>8}  {st:>6}  {sf:>6}  {pay:>8}")

r = df_preds[df_preds["has_result"]]
total = len(r)
hit_tan = int(r["tan_hit"].sum())
hit_fuku = int(r["fuku_hit"].sum())
total_return = int(r[r["tan_hit"]]["payout"].sum())
total_bet = total * 100

print()
print("=" * 72)
print(f"【4月合計】フィルター通過 {len(df_preds)}件 / 結果あり {total}件")
print(f"  三連単的中: {hit_tan}/{total} ({hit_tan/total:.1%})")
print(f"  三連複的中: {hit_fuku}/{total} ({hit_fuku/total:.1%})")
print(f"  回収率: {total_return}円 / {total_bet}円 = {total_return/total_bet*100:.1f}%")
print()
print("【日別集計】")
for date in TARGET_DATES:
    day = r[r["date"] == date]
    if day.empty:
        continue
    ht = int(day["tan_hit"].sum())
    hf = int(day["fuku_hit"].sum())
    ret = int(day[day["tan_hit"]]["payout"].sum())
    bet = len(day) * 100
    print(f"  {date}: 三連単{ht}/{len(day)}的中  三連複{hf}/{len(day)}  回収率{ret/bet*100:.1f}%")
