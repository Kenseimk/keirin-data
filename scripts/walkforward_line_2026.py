# -*- coding: utf-8 -*-
"""
ライン特徴量強化版 ウォークフォワード 2026年シミュレーション

過学習対策:
  - 各月のモデルはその月以前のデータのみで学習
  - 閾値はバリデーションセットのスコア分布から決定 (テストデータ不使用)
  - 月ごとに独立してモデルを再学習
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

TEST_MONTHS     = ["2026-01","2026-02","2026-03","2026-04","2026-05"]
TOP_K           = 5
TOP_SCORE_THRESH = 95
SCORE_GAP_THRESH = 2
# 閾値: バリデーションセットのスコア上位何%か (テストデータ不使用)
VAL_PERCENTILE  = 90  # 上位10% = percentile 90

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

# ライン特徴量
df_all["line_size"] = df_all.groupby(["race_id","pref"])["banum"].transform("count")
n_strong = df_all.groupby("race_id").apply(
    lambda g: int((g.groupby("pref")["banum"].count() >= 2).sum())
).rename("n_strong_lines")
df_all = df_all.join(n_strong, on="race_id")

def extract_markers(x):
    if pd.isna(x): return {}
    res = {}
    for sym, key in [("◎","honmei"),("○","niban"),("▲","attention"),("△","sanban")]:
        m = re.search(re.escape(sym) + r"(\d+)", str(x))
        if m: res[key] = int(m.group(1))
    return res

race_marker_df = df_all.drop_duplicates("race_id")[["race_id","lineup"]].copy()
race_marker_df["_mk"] = race_marker_df["lineup"].apply(extract_markers)
race_marker_df["lineup_niban"]  = race_marker_df["_mk"].apply(lambda d: d.get("niban"))
race_marker_df["lineup_sanban"] = race_marker_df["_mk"].apply(lambda d: d.get("sanban"))
race_marker_df["lineup_attn"]   = race_marker_df["_mk"].apply(lambda d: d.get("attention"))
race_marker_df = race_marker_df.set_index("race_id")
df_all["lineup_niban"]  = df_all["race_id"].map(race_marker_df["lineup_niban"])
df_all["lineup_sanban"] = df_all["race_id"].map(race_marker_df["lineup_sanban"])
df_all["lineup_attn"]   = df_all["race_id"].map(race_marker_df["lineup_attn"])
df_all["is_niban"]  = (df_all["banum"] == df_all["lineup_niban"]).astype(int)
df_all["is_sanban"] = (df_all["banum"] == df_all["lineup_sanban"]).astype(int)
df_all["is_attn"]   = (df_all["banum"] == df_all["lineup_attn"]).astype(int)

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
               "age","is_nige_finish","is_sashi_finish","score_rank","is_honmei",
               "line_size","is_niban","is_sanban","is_attn"]

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
    "p1_line_size","p2_line_size","p3_line_size","n_strong_lines",
    "b2_is_b1_bankey","b3_in_b1_line","b3_in_b2_line",
    "p2_is_niban","p3_is_sanban","p2_is_attn",
    "b1_honmei_b2_niban","b1_honmei_b2_niban_b3_sanban",
]

req = ["race_score","class_num","style_num","gear","score_rank","is_honmei",
       "n_players_in_race","prev1_rank","last3_avg_rank","last5_avg_rank",
       "last5_win_rate","rank_trend","days_since_last","rank_num"]
df_model = df_all.dropna(subset=req).copy()

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
        n_strong = int(g["n_strong_lines"].iloc[0])
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
                "san_combo":san_combo,"san_payout":san_payout,
                "top_score":top_score,"score_gap":score_gap,
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
                "p1_line_size":float(p1["line_size"]),"p2_line_size":float(p2["line_size"]),"p3_line_size":float(p3["line_size"]),
                "n_strong_lines":n_strong,
                "b2_is_b1_bankey":int((s1n or s1m) and sp12),
                "b3_in_b1_line":int(sp13 and not sp12),
                "b3_in_b2_line":int(sp23 and not sp13),
                "p2_is_niban":int(p2["is_niban"]),"p3_is_sanban":int(p3["is_sanban"]),"p2_is_attn":int(p2["is_attn"]),
                "b1_honmei_b2_niban":int(bool(p1["is_honmei"]) and bool(p2["is_niban"])),
                "b1_honmei_b2_niban_b3_sanban":int(bool(p1["is_honmei"]) and bool(p2["is_niban"]) and bool(p3["is_sanban"])),
                "is_hit":int(b1==correct_1 and b2==correct_2 and b3==correct_3),
            })
    return pd.DataFrame(rows)

def check_hit(b1,b2,b3,san_combo):
    if not san_combo or pd.isna(san_combo): return False
    parts = re.split(r"[-=]", str(san_combo))
    if len(parts)<3: return False
    try: return int(parts[0])==b1 and int(parts[1])==b2 and int(parts[2])==b3
    except: return False

LGB_PARAMS = dict(n_estimators=1000,learning_rate=0.05,num_leaves=31,min_child_samples=50,verbose=-1,random_state=42)

def fit_es(F_tr, F_val, y_tr, y_val):
    m = lgb.LGBMClassifier(**LGB_PARAMS)
    m.fit(F_tr, y_tr, eval_set=[(F_val, y_val)],
          callbacks=[lgb.early_stopping(50,verbose=False),lgb.log_evaluation(-1)])
    return m

# ── ウォークフォワード ──
print("="*70)
print(f"  ライン特徴量強化版 ウォークフォワード 2026年シミュレーション")
print(f"  閾値: バリデーションスコア上位{100-VAL_PERCENTILE}% (テストデータ不使用)")
print("="*70)

all_results = []

for test_month in TEST_MONTHS:
    df_train = df_model[df_model["date"].str[:7] < test_month].copy()
    df_test  = df_model[df_model["date"].str[:7] == test_month].copy()
    if len(df_test) == 0:
        print(f"  {test_month}: データなし")
        continue

    val_months = sorted(df_train["date"].str[:7].unique())[-3:]
    val_mask   = df_train["date"].str[:7].isin(val_months)
    df_tr  = df_train[~val_mask].copy()
    df_val = df_train[val_mask].copy()

    # 個別モデル学習
    m1 = fit_es(df_tr[F1].values, df_val[F1].values, df_tr["is_winner"].values, df_val["is_winner"].values)
    m2 = fit_es(df_tr[F23].values,df_val[F23].values,df_tr["is_2nd"].values,    df_val["is_2nd"].values)
    m3 = fit_es(df_tr[F23].values,df_val[F23].values,df_tr["is_3rd"].values,    df_val["is_3rd"].values)

    for df_part in [df_tr, df_val, df_test]:
        df_part["win_proba"] = m1.predict_proba(df_part[F1].values)[:,1]
        df_part["p2_proba"]  = m2.predict_proba(df_part[F23].values)[:,1]
        df_part["p3_proba"]  = m3.predict_proba(df_part[F23].values)[:,1]

    combo_tr  = make_combo_rows(df_tr)
    combo_val = make_combo_rows(df_val)
    combo_te  = make_combo_rows(df_test)
    if len(combo_tr)==0 or combo_tr["is_hit"].sum()==0: continue
    if len(combo_te)==0: continue

    # コンボモデル学習
    pos=combo_tr["is_hit"].sum(); neg=len(combo_tr)-pos
    m_combo = lgb.LGBMClassifier(n_estimators=2000,learning_rate=0.02,num_leaves=63,
        min_child_samples=10,scale_pos_weight=neg/pos,verbose=-1,random_state=42)
    m_combo.fit(combo_tr[COMBO_FEATS].values,combo_tr["is_hit"].values,
        eval_set=[(combo_val[COMBO_FEATS].values,combo_val["is_hit"].values)],eval_metric="auc",
        callbacks=[lgb.early_stopping(100,verbose=False,first_metric_only=True),lgb.log_evaluation(-1)])

    # バリデーションのスコアから閾値決定 (テストデータ不使用)
    combo_val["combo_score"] = m_combo.predict_proba(combo_val[COMBO_FEATS].values)[:,1]
    val_top1 = combo_val.groupby("race_id")["combo_score"].max().reset_index()
    threshold = np.percentile(val_top1["combo_score"], VAL_PERCENTILE)

    # テスト予測
    combo_te["combo_score"] = m_combo.predict_proba(combo_te[COMBO_FEATS].values)[:,1]

    pred_rows = []
    for race_id, g in combo_te.groupby("race_id"):
        best = g.nlargest(1,"combo_score").iloc[0]
        hit = check_hit(int(best["b1"]),int(best["b2"]),int(best["b3"]),best["san_combo"])
        pred_rows.append({
            "race_id":race_id,"combo_score":float(best["combo_score"]),
            "hit":hit,"payout":float(best["san_payout"]) if hit and pd.notna(best["san_payout"]) else 0,
        })
    pred = pd.DataFrame(pred_rows)

    # 全件結果
    n_all=len(pred); h_all=pred["hit"].sum(); pay_all=pred["payout"].sum()
    hr_all=h_all/n_all*100 if n_all else 0; roi_all=pay_all/(n_all*100)*100 if n_all else 0

    # 閾値フィルタ後
    pred_f = pred[pred["combo_score"]>=threshold]
    n_f=len(pred_f); h_f=pred_f["hit"].sum(); pay_f=pred_f["payout"].sum()
    hr_f=h_f/n_f*100 if n_f else 0; roi_f=pay_f/(n_f*100)*100 if n_f else 0

    print(f"\n── {test_month}  (val閾値:{threshold:.4f}) ──")
    print(f"  全件:      {n_all:3d}R  {h_all:2d}的中  {hr_all:.1f}%  回収率:{roi_all:.0f}%")
    print(f"  上位10%: {n_f:3d}R  {h_f:2d}的中  {hr_f:.1f}%  回収率:{roi_f:.0f}%")

    # 的中レース詳細
    hit_df = pred_f[pred_f["hit"]==True]
    if len(hit_df) > 0:
        race_meta = df_test.groupby("race_id").agg(
            date=("date","first"), venue=("venue_slug","first"), race_no=("race_no","first")).reset_index()
        for _, hr in hit_df.iterrows():
            meta = race_meta[race_meta["race_id"]==hr["race_id"]]
            if len(meta)==0: continue
            m = meta.iloc[0]
            c = combo_te[combo_te["race_id"]==hr["race_id"]].nlargest(1,"combo_score").iloc[0]
            print(f"    ○ {m['date']} {m['venue']} {int(m['race_no'])}R  "
                  f"{int(c['b1'])}-{int(c['b2'])}-{int(c['b3'])}  払戻:{int(hr['payout']):,}円  score:{hr['combo_score']:.4f}")

    all_results.append({
        "month":test_month,"threshold":threshold,
        "n_all":n_all,"h_all":h_all,"pay_all":pay_all,
        "n_f":n_f,"h_f":h_f,"pay_f":pay_f,
    })

# ── 総合サマリー ──
print("\n" + "="*70)
print("  総合サマリー")
print("="*70)
df_res = pd.DataFrame(all_results)
if len(df_res)==0:
    print("結果なし"); exit()

print(f"\n{'月':>9}  {'全件':>22}  {'上位10%(val閾値)':>25}")
print(f"{'':>9}  {'R':>5}{'的中':>5}{'的中率':>7}{'回収率':>7}  {'R':>5}{'的中':>5}{'的中率':>7}{'回収率':>7}")
print("-"*70)
for _, r in df_res.iterrows():
    hr_a=r.h_all/r.n_all*100; roi_a=r.pay_all/(r.n_all*100)*100
    hr_f=r.h_f/r.n_f*100 if r.n_f else 0; roi_f=r.pay_f/(r.n_f*100)*100 if r.n_f else 0
    print(f"  {r.month}  {int(r.n_all):>5}{int(r.h_all):>5}  {hr_a:>5.1f}%  {roi_a:>5.0f}%  "
          f"{int(r.n_f):>5}{int(r.h_f):>5}  {hr_f:>5.1f}%  {roi_f:>5.0f}%")

print("-"*70)
# 合計
sna=df_res.n_all.sum(); sha=df_res.h_all.sum(); spa=df_res.pay_all.sum()
snf=df_res.n_f.sum();   shf=df_res.h_f.sum();   spf=df_res.pay_f.sum()
print(f"  合計      {sna:>5}{sha:>5}  {sha/sna*100:>5.1f}%  {spa/(sna*100)*100:>5.0f}%  "
      f"{snf:>5}{shf:>5}  {shf/snf*100 if snf else 0:>5.1f}%  {spf/(snf*100)*100 if snf else 0:>5.0f}%")

print(f"\n【過学習チェック】月ごとの的中率のばらつき:")
hrs = [r.h_f/r.n_f*100 if r.n_f else 0 for _,r in df_res.iterrows()]
print(f"  最大:{max(hrs):.1f}%  最小:{min(hrs):.1f}%  平均:{np.mean(hrs):.1f}%  標準偏差:{np.std(hrs):.1f}pp")
print(f"  ※ 標準偏差が大きい場合は特定月への過学習の可能性あり")
