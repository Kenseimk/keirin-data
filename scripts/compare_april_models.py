# -*- coding: utf-8 -*-
"""
旧モデル（個別予測+動的フォーメーション）vs 新モデル（コンボ）
2026-04 全レース比較
"""
import sys, io as _io
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = _io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import glob, re, warnings, itertools
import numpy as np
import pandas as pd
import lightgbm as lgb
from pathlib import Path

warnings.filterwarnings("ignore")

TEST_MONTH   = "2026-04"
TOP_K        = 5
TOP_SCORE_THRESH = 95
SCORE_GAP_THRESH = 2
P2_GAP_THRESH    = 0.05

DATA_DIR = Path("keirin_data")
files = [f for f in sorted(glob.glob(str(DATA_DIR / "202[3456]_*.csv"))) if "sample" not in f and "bak" not in f]
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
df_all["agari"]        = pd.to_numeric(df_all["agari"], errors="coerce")
df_all["age"]          = pd.to_numeric(df_all.get("age"), errors="coerce")
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
df_all["days_since_last"]= (pd.to_datetime(df_all["date"]) -
    pd.to_datetime(df_all.groupby("player_key")["date"].shift(1))).dt.days

agari_grp = df_all.groupby("player_key")["agari"]
df_all["last3_avg_agari"] = agari_grp.transform(lambda x: x.shift(1).rolling(3,min_periods=1).mean())
df_all["last5_avg_agari"] = agari_grp.transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())
df_all["same_pref_count"] = (df_all.groupby(["race_id","pref"])["banum"].transform("count") - 1).clip(lower=0)
df_all["same_term_count"] = (df_all.groupby(["race_id","term"])["banum"].transform("count") - 1).clip(lower=0)
df_all["nige_in_race"]    = df_all.groupby("race_id")["running_style"].transform(lambda x: (x=="逃").sum())
df_all["venue_win_rate"]  = df_all.sort_values(["player_key","date","race_no"]).groupby(
    ["player_key","venue_slug"])["is_win"].transform(lambda x: x.shift(1).rolling(10,min_periods=1).mean())

df_all["_is_nige_f"]  = (df_all["finish_type"] == "逃切").astype(float)
df_all["_is_sashi_f"] = df_all["finish_type"].isin(["差切","追込"]).astype(float)
df_all["is_nige_finish"]  = df_all.groupby("player_key")["_is_nige_f"].transform(
    lambda x: x.shift(1).rolling(10, min_periods=1).mean())
df_all["is_sashi_finish"] = df_all.groupby("player_key")["_is_sashi_f"].transform(
    lambda x: x.shift(1).rolling(10, min_periods=1).mean())

def extract_honmei(x):
    if pd.isna(x): return None
    m = re.search(r"◎(\d+)", str(x))
    return int(m.group(1)) if m else None
df_all["honmei_banum"]      = df_all["lineup"].apply(extract_honmei)
df_all["score_rank"]        = df_all.groupby("race_id")["race_score"].rank(ascending=False, method="min")
df_all["is_honmei"]         = (df_all["banum"]==df_all["honmei_banum"]).astype(int)
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

PLAYER_COLS = ["race_score","class_num","style_num","gear","last3_avg_rank","last5_avg_rank",
               "last5_win_rate","win_rate_4m","top2_rate_4m","top3_rate_4m","nige_4m","maku_4m",
               "venue_win_rate","same_pref_count","same_term_count","days_since_last",
               "age","is_nige_finish","is_sashi_finish","score_rank","is_honmei"]

COMBO_FEATS = [
    "p1_win_proba","p1_score","p1_class","p1_style","p1_win_rate",
    "p1_last3_rank","p1_last5_rank","p1_nige_rate","p1_maku_rate",
    "p1_venue_win","p1_days","p1_age","p1_score_rank","p1_is_honmei","p1_nige_finish",
    "p2_p2_proba","p2_score","p2_class","p2_style","p2_top2_rate",
    "p2_last3_rank","p2_last5_rank","p2_age","p2_score_rank","p2_is_honmei",
    "p3_p3_proba","p3_score","p3_class","p3_style","p3_top3_rate",
    "p3_last3_rank","p3_age","p3_score_rank","p3_sashi_finish",
    "same_pref_12","same_pref_13","same_pref_23","same_pref_total",
    "same_term_12","same_term_13","same_term_23","same_term_total",
    "style_1_nige","style_1_maku","style_3_sashi","nige_line_12","maku_line_12",
    "score_diff_12","score_diff_13","score_diff_23","class_diff_12","class_diff_13",
    "prob_product","prob_sum_log","win_proba_gap","banum_12","banum_13",
    "score_gap_race","top_score",
]

req = ["race_score","class_num","style_num","gear","score_rank","is_honmei",
       "n_players_in_race","prev1_rank","last3_avg_rank","last5_avg_rank",
       "last5_win_rate","rank_trend","days_since_last","rank_num"]
df_model = df_all.dropna(subset=req).copy()

df_train = df_model[df_model["date"].str[:7] < TEST_MONTH].copy()
df_test  = df_model[df_model["date"].str[:7] == TEST_MONTH].copy()
val_months = sorted(df_train["date"].str[:7].unique())[-3:]
val_mask   = df_train["date"].str[:7].isin(val_months)
df_tr  = df_train[~val_mask].copy()
df_val = df_train[val_mask].copy()
print(f"訓練: {len(df_tr):,}行  検証: {len(df_val):,}行  テスト: {len(df_test):,}行")

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

for df_part in [df_tr, df_val, df_test]:
    df_part["win_proba"] = m1.predict_proba(df_part[F1].values)[:,1]
    df_part["p2_proba"]  = m2.predict_proba(df_part[F23].values)[:,1]
    df_part["p3_proba"]  = m3.predict_proba(df_part[F23].values)[:,1]

# コンボモデル訓練
def make_combo_rows(df_part):
    rows = []
    for race_id, g in df_part.groupby("race_id"):
        if n_players_raw.get(race_id, 0) != 7: continue
        top_score = top_score_raw.get(race_id, 0)
        score_gap = score_gap_raw.get(race_id, 0)
        if top_score < TOP_SCORE_THRESH or score_gap < SCORE_GAP_THRESH: continue
        g = g.copy()
        san_combo = g["san_combo"].iloc[0]; san_payout = g["san_payout"].iloc[0]
        correct_1 = correct_2 = correct_3 = None
        if pd.notna(san_combo):
            parts = re.split(r"[-=]", str(san_combo))
            if len(parts) == 3:
                try: correct_1, correct_2, correct_3 = int(parts[0]), int(parts[1]), int(parts[2])
                except: pass
        top1 = g.nlargest(TOP_K,"win_proba")["banum"].astype(int).tolist()
        top2 = g.nlargest(TOP_K,"p2_proba")["banum"].astype(int).tolist()
        top3 = g.nlargest(TOP_K,"p3_proba")["banum"].astype(int).tolist()
        pi = g.set_index("banum")[PLAYER_COLS+["win_proba","p2_proba","p3_proba","pref","term"]]
        for b1,b2,b3 in itertools.product(top1,top2,top3):
            if len({b1,b2,b3})<3: continue
            try: p1=pi.loc[b1]; p2=pi.loc[b2]; p3=pi.loc[b3]
            except KeyError: continue
            sp12=int(p1["pref"]==p2["pref"]); sp13=int(p1["pref"]==p3["pref"]); sp23=int(p2["pref"]==p3["pref"])
            st12=int(p1["term"]==p2["term"]); st13=int(p1["term"]==p3["term"]); st23=int(p2["term"]==p3["term"])
            s1n=int(p1["style_num"]==5); s1m=int(p1["style_num"]==4)
            prob=float(p1["win_proba"])*float(p2["p2_proba"])*float(p3["p3_proba"])
            rows.append({
                "race_id":race_id,"b1":b1,"b2":b2,"b3":b3,
                "san_combo":san_combo,"san_payout":san_payout,"top_score":top_score,"score_gap":score_gap,
                "p1_win_proba":p1["win_proba"],"p1_score":p1["race_score"],"p1_class":p1["class_num"],
                "p1_style":p1["style_num"],"p1_win_rate":p1["win_rate_4m"],"p1_last3_rank":p1["last3_avg_rank"],
                "p1_last5_rank":p1["last5_avg_rank"],"p1_nige_rate":p1["nige_4m"],"p1_maku_rate":p1["maku_4m"],
                "p1_venue_win":p1["venue_win_rate"],"p1_days":p1["days_since_last"],"p1_age":p1["age"],
                "p1_score_rank":p1["score_rank"],"p1_is_honmei":p1["is_honmei"],"p1_nige_finish":p1["is_nige_finish"],
                "p2_p2_proba":p2["p2_proba"],"p2_score":p2["race_score"],"p2_class":p2["class_num"],
                "p2_style":p2["style_num"],"p2_top2_rate":p2["top2_rate_4m"],"p2_last3_rank":p2["last3_avg_rank"],
                "p2_last5_rank":p2["last5_avg_rank"],"p2_age":p2["age"],"p2_score_rank":p2["score_rank"],
                "p2_is_honmei":p2["is_honmei"],
                "p3_p3_proba":p3["p3_proba"],"p3_score":p3["race_score"],"p3_class":p3["class_num"],
                "p3_style":p3["style_num"],"p3_top3_rate":p3["top3_rate_4m"],"p3_last3_rank":p3["last3_avg_rank"],
                "p3_age":p3["age"],"p3_score_rank":p3["score_rank"],"p3_sashi_finish":p3["is_sashi_finish"],
                "same_pref_12":sp12,"same_pref_13":sp13,"same_pref_23":sp23,"same_pref_total":sp12+sp13+sp23,
                "same_term_12":st12,"same_term_13":st13,"same_term_23":st23,"same_term_total":st12+st13+st23,
                "style_1_nige":s1n,"style_1_maku":s1m,"style_3_sashi":int(float(p3["style_num"])<=2),
                "nige_line_12":s1n*sp12,"maku_line_12":s1m*sp12,
                "score_diff_12":float(p1["race_score"])-float(p2["race_score"]),
                "score_diff_13":float(p1["race_score"])-float(p3["race_score"]),
                "score_diff_23":float(p2["race_score"])-float(p3["race_score"]),
                "class_diff_12":float(p1["class_num"])-float(p2["class_num"]),
                "class_diff_13":float(p1["class_num"])-float(p3["class_num"]),
                "prob_product":prob,
                "prob_sum_log":np.log(float(p1["win_proba"])+1e-9)+np.log(float(p2["p2_proba"])+1e-9)+np.log(float(p3["p3_proba"])+1e-9),
                "win_proba_gap":float(p1["win_proba"])-float(p2["win_proba"]),
                "banum_12":b2-b1,"banum_13":b3-b1,"score_gap_race":score_gap,
                "is_hit":int(b1==correct_1 and b2==correct_2 and b3==correct_3),
            })
    return pd.DataFrame(rows)

print("コンボデータ生成中...")
combo_tr  = make_combo_rows(df_tr)
combo_val = make_combo_rows(df_val)
combo_te  = make_combo_rows(df_test)
pos=combo_tr["is_hit"].sum(); neg=len(combo_tr)-pos
m_combo = lgb.LGBMClassifier(n_estimators=2000,learning_rate=0.02,num_leaves=63,
    min_child_samples=10,scale_pos_weight=neg/pos,verbose=-1,random_state=42)
m_combo.fit(combo_tr[COMBO_FEATS].values,combo_tr["is_hit"].values,
    eval_set=[(combo_val[COMBO_FEATS].values,combo_val["is_hit"].values)],eval_metric="auc",
    callbacks=[lgb.early_stopping(100,verbose=False,first_metric_only=True),lgb.log_evaluation(-1)])
combo_te["combo_score"] = m_combo.predict_proba(combo_te[COMBO_FEATS].values)[:,1]

# 新モデル予測
new_preds = {}
for race_id, g in combo_te.groupby("race_id"):
    best = g.nlargest(1,"combo_score").iloc[0]
    new_preds[race_id] = (int(best["b1"]),int(best["b2"]),int(best["b3"]))

# 旧モデル予測（個別+p2_gap）
old_preds = {}  # race_id -> list of (p1,p2,p3)
for race_id, g in df_test.groupby("race_id"):
    top_score = top_score_raw.get(race_id,0)
    score_gap = score_gap_raw.get(race_id,0)
    if top_score < TOP_SCORE_THRESH or score_gap < SCORE_GAP_THRESH: continue
    if n_players_raw.get(race_id,0) != 7: continue
    g = g.sort_values("win_proba",ascending=False)
    p1 = int(g.iloc[0]["banum"])
    rest2 = g[g["banum"]!=p1].sort_values("p2_proba",ascending=False)
    p2a = int(rest2.iloc[0]["banum"]) if len(rest2)>=1 else None
    p2b = int(rest2.iloc[1]["banum"]) if len(rest2)>=2 else None
    p2_gap = float(rest2.iloc[0]["p2_proba"]-rest2.iloc[1]["p2_proba"]) if len(rest2)>=2 else 1.0
    def pick3(excl):
        r=g[~g["banum"].isin(excl)].sort_values("p3_proba",ascending=False)
        return int(r.iloc[0]["banum"]) if len(r)>=1 else None
    combos = [(p1,p2a,pick3([p1,p2a]))]
    if p2_gap < P2_GAP_THRESH and p2b:
        combos.append((p1,p2b,pick3([p1,p2b])))
    old_preds[race_id] = combos

# 結果照合
race_meta = df_test.groupby("race_id").agg(
    date=("date","first"), venue=("venue_slug","first"),
    race_no=("race_no","first"), san_combo=("san_combo","first"),
    san_payout=("san_payout","first")
).reset_index()

def check_hit(pred_tuple, actual_combo):
    if not actual_combo or pd.isna(actual_combo): return False
    parts = re.split(r"[-=]", str(actual_combo))
    if len(parts)<3: return False
    try: return (int(parts[0])==pred_tuple[0] and int(parts[1])==pred_tuple[1] and int(parts[2])==pred_tuple[2])
    except: return False

all_race_ids = sorted(set(new_preds.keys()) | set(old_preds.keys()))

rows_out = []
for race_id in all_race_ids:
    meta = race_meta[race_meta["race_id"]==race_id]
    if len(meta)==0: continue
    m = meta.iloc[0]
    actual = m["san_combo"]; payout = m["san_payout"]

    # 旧モデル
    old_list = old_preds.get(race_id, [])
    old_hit = any(check_hit(c, actual) for c in old_list)
    old_str = " / ".join(f"{c[0]}-{c[1]}-{c[2]}" for c in old_list) if old_list else "-"

    # 新モデル
    new_combo = new_preds.get(race_id)
    new_hit = check_hit(new_combo, actual) if new_combo else False
    new_str = f"{new_combo[0]}-{new_combo[1]}-{new_combo[2]}" if new_combo else "-"

    in_new = race_id in new_preds
    in_old = race_id in old_preds

    rows_out.append({
        "date": m["date"], "venue": m["venue"], "race_no": int(m["race_no"]),
        "actual": actual, "payout": payout,
        "旧予想": old_str, "旧的中": "○" if old_hit else "×",
        "新予想": new_str, "新的中": "○" if new_hit else "×",
        "old_hit": old_hit, "new_hit": new_hit,
        "old_payout": payout if old_hit else 0,
        "new_payout": payout if new_hit else 0,
        "old_bets": len(old_list) if in_old else 0,
        "in_old": in_old, "in_new": in_new,
    })

df_out = pd.DataFrame(rows_out).sort_values(["date","venue","race_no"])

# 出力
print("\n" + "="*100)
print(f"{'日付':10} {'会場':10} {'R':3} {'実結果':12} {'払戻':>7} | {'旧予想':20} {'旧':3} | {'新予想':14} {'新':3}")
print("="*100)

cur_date = None
for _, r in df_out.iterrows():
    if r["date"] != cur_date:
        if cur_date is not None: print()
        print(f"── {r['date']} ──")
        cur_date = r["date"]
    old_mark = r["旧的中"] if r["in_old"] else "-"
    new_mark = r["新的中"] if r["in_new"] else "-"
    pay_str = f"{int(r['payout']):,}円" if pd.notna(r["payout"]) else "---"
    print(f"  {r['venue']:10} {int(r['race_no']):2}R  {str(r['actual']):12} {pay_str:>8} | {r['旧予想']:22} {old_mark:3} | {r['新予想']:14} {new_mark}")

# サマリー
print("\n" + "="*60)
print("  サマリー比較")
print("="*60)

old_df = df_out[df_out["in_old"]]
new_df = df_out[df_out["in_new"]]

old_bets   = old_df["old_bets"].sum()
old_hits   = old_df["old_hit"].sum()
old_pay    = old_df["old_payout"].sum()
old_invest = old_bets * 100

new_bets   = len(new_df)
new_hits   = new_df["new_hit"].sum()
new_pay    = new_df["new_payout"].sum()
new_invest = new_bets * 100

print(f"{'':20} {'旧モデル':>15} {'新モデル':>15}")
print(f"{'対象レース':20} {old_df['race_no'].count():>15} {len(new_df):>15}")
print(f"{'賭け件数':20} {old_bets:>15} {new_bets:>15}")
print(f"{'的中数':20} {old_hits:>15} {new_hits:>15}")
print(f"{'的中率':20} {old_hits/old_bets*100:>14.1f}% {new_hits/new_bets*100:>14.1f}%")
print(f"{'総投資':20} {old_invest:>13,}円 {new_invest:>13,}円")
print(f"{'払戻合計':20} {int(old_pay):>13,}円 {int(new_pay):>13,}円")
print(f"{'回収率':20} {old_pay/old_invest*100:>14.1f}% {new_pay/new_invest*100:>14.1f}%")

# 両方的中・片方のみ的中の分析
both_hit    = df_out[df_out["in_old"] & df_out["in_new"] & df_out["old_hit"] & df_out["new_hit"]]
old_only    = df_out[df_out["in_old"] & df_out["in_new"] & df_out["old_hit"] & ~df_out["new_hit"]]
new_only    = df_out[df_out["in_old"] & df_out["in_new"] & ~df_out["old_hit"] & df_out["new_hit"]]
both_miss   = df_out[df_out["in_old"] & df_out["in_new"] & ~df_out["old_hit"] & ~df_out["new_hit"]]

print(f"\n【共通レースでの比較】")
print(f"  両方的中:  {len(both_hit)}件")
print(f"  旧のみ的中: {len(old_only)}件  (新が取りこぼし)")
print(f"  新のみ的中: {len(new_only)}件  (新が追加で取った)")
print(f"  両方外れ:  {len(both_miss)}件")

if len(new_only) > 0:
    print(f"\n【新モデルのみ的中レース】")
    for _, r in new_only.iterrows():
        print(f"  {r['date']} {r['venue']} {int(r['race_no'])}R  実:{r['actual']}  新予想:{r['新予想']}  払戻:{int(r['payout']):,}円")

if len(old_only) > 0:
    print(f"\n【旧モデルのみ的中レース】")
    for _, r in old_only.iterrows():
        print(f"  {r['date']} {r['venue']} {int(r['race_no'])}R  実:{r['actual']}  旧予想:{r['旧予想']}  払戻:{int(r['payout']):,}円")
