# -*- coding: utf-8 -*-
"""
3連単コンボモデル
個別モデル(1着/2着/3着)の確率 + 選手間インタラクション特徴量で
組み合わせを直接スコアリングする
訓練: ~2026-03  /  検証: 2026-04
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

TEST_MONTH = "2026-04"
TOP_K = 5  # 各ポジションの候補数
TOP_SCORE_THRESH = 95
SCORE_GAP_THRESH = 2

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

# コンボモデル用の追加特徴量（df_model作成前に計算）
df_all["age"]          = pd.to_numeric(df_all.get("age"), errors="coerce")
df_all["_is_nige_f"]   = (df_all["finish_type"] == "逃切").astype(float)
df_all["_is_sashi_f"]  = df_all["finish_type"].isin(["差切","追込"]).astype(float)
df_all["is_nige_finish"]  = df_all.groupby("player_key")["_is_nige_f"].transform(
    lambda x: x.shift(1).rolling(10, min_periods=1).mean())
df_all["is_sashi_finish"] = df_all.groupby("player_key")["_is_sashi_f"].transform(
    lambda x: x.shift(1).rolling(10, min_periods=1).mean())

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

df_train = df_model[df_model["date"].str[:7] < TEST_MONTH].copy()
df_test  = df_model[df_model["date"].str[:7] == TEST_MONTH].copy()
val_months = sorted(df_train["date"].str[:7].unique())[-3:]
val_mask   = df_train["date"].str[:7].isin(val_months)
df_tr  = df_train[~val_mask].copy()
df_val = df_train[val_mask].copy()
print(f"訓練: {len(df_tr):,}行  検証ES: {len(df_val):,}行  テスト: {len(df_test):,}行")

LGB_PARAMS = dict(n_estimators=1000, learning_rate=0.05,
                  num_leaves=31, min_child_samples=50, verbose=-1, random_state=42)

def fit_es(F_tr, F_val, y_tr, y_val):
    m = lgb.LGBMClassifier(**LGB_PARAMS)
    m.fit(F_tr, y_tr, eval_set=[(F_val, y_val)],
          callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(-1)])
    return m

# ステップ1: 個別モデル訓練
m1 = fit_es(df_tr[F1].values,  df_val[F1].values,  df_tr["is_winner"].values, df_val["is_winner"].values)
m2 = fit_es(df_tr[F23].values, df_val[F23].values, df_tr["is_2nd"].values,    df_val["is_2nd"].values)
m3 = fit_es(df_tr[F23].values, df_val[F23].values, df_tr["is_3rd"].values,    df_val["is_3rd"].values)
print(f"個別モデル best iter: 1着={m1.best_iteration_} / 2着={m2.best_iteration_} / 3着={m3.best_iteration_}")

# 個別確率を付与
for df_part in [df_tr, df_val, df_test]:
    df_part["win_proba"] = m1.predict_proba(df_part[F1].values)[:,1]
    df_part["p2_proba"]  = m2.predict_proba(df_part[F23].values)[:,1]
    df_part["p3_proba"]  = m3.predict_proba(df_part[F23].values)[:,1]

# ステップ2: コンボデータ生成
PLAYER_COLS = ["race_score","class_num","style_num","gear","last3_avg_rank","last5_avg_rank",
               "last5_win_rate","win_rate_4m","top2_rate_4m","top3_rate_4m","nige_4m","maku_4m",
               "venue_win_rate","same_pref_count","same_term_count","days_since_last",
               "age","is_nige_finish","is_sashi_finish","score_rank","is_honmei"]

def make_combo_rows(df_part, is_train=True):
    rows = []
    for race_id, g in df_part.groupby("race_id"):
        if n_players_raw.get(race_id, 0) != 7: continue
        top_score = top_score_raw.get(race_id, 0)
        score_gap = score_gap_raw.get(race_id, 0)
        if top_score < TOP_SCORE_THRESH or score_gap < SCORE_GAP_THRESH: continue

        g = g.copy()
        san_combo  = g["san_combo"].iloc[0]
        san_payout = g["san_payout"].iloc[0]

        # 正解の組み合わせ
        correct_1, correct_2, correct_3 = None, None, None
        if pd.notna(san_combo):
            parts = re.split(r"[-=]", str(san_combo))
            if len(parts) == 3:
                try:
                    correct_1, correct_2, correct_3 = int(parts[0]), int(parts[1]), int(parts[2])
                except: pass

        # top-K候補
        top1_cands = g.nlargest(TOP_K, "win_proba")["banum"].astype(int).tolist()
        top2_cands = g.nlargest(TOP_K, "p2_proba")["banum"].astype(int).tolist()
        top3_cands = g.nlargest(TOP_K, "p3_proba")["banum"].astype(int).tolist()

        player_info = g.set_index("banum")[PLAYER_COLS + ["win_proba","p2_proba","p3_proba","pref","term"]]

        for b1, b2, b3 in itertools.product(top1_cands, top2_cands, top3_cands):
            if len({b1, b2, b3}) < 3: continue
            try:
                p1 = player_info.loc[b1]
                p2 = player_info.loc[b2]
                p3 = player_info.loc[b3]
            except KeyError:
                continue

            is_correct = (b1==correct_1 and b2==correct_2 and b3==correct_3) if is_train else False

            # インタラクション特徴量
            same_pref_12 = int(p1["pref"] == p2["pref"])
            same_pref_13 = int(p1["pref"] == p3["pref"])
            same_pref_23 = int(p2["pref"] == p3["pref"])
            same_term_12 = int(p1["term"] == p2["term"])
            same_term_13 = int(p1["term"] == p3["term"])
            same_term_23 = int(p2["term"] == p3["term"])
            style_1_nige = int(p1["style_num"] == 5)
            style_1_maku = int(p1["style_num"] == 4)
            style_3_sashi = int(float(p3["style_num"]) <= 2)  # 差し・追い込み系
            score_diff_12 = float(p1["race_score"]) - float(p2["race_score"])
            score_diff_13 = float(p1["race_score"]) - float(p3["race_score"])
            score_diff_23 = float(p2["race_score"]) - float(p3["race_score"])
            class_diff_12 = float(p1["class_num"]) - float(p2["class_num"])
            class_diff_13 = float(p1["class_num"]) - float(p3["class_num"])
            # ライン関係: 逃げ選手+同県2着 = 番手の可能性
            nige_line_12 = style_1_nige * same_pref_12
            maku_line_12 = style_1_maku * same_pref_12
            # 確率の積（コンボ全体の確度）
            prob_product = float(p1["win_proba"]) * float(p2["p2_proba"]) * float(p3["p3_proba"])
            prob_sum_log = (np.log(float(p1["win_proba"])+1e-9) +
                            np.log(float(p2["p2_proba"])+1e-9) +
                            np.log(float(p3["p3_proba"])+1e-9))
            # win_probaの格差（1着候補への確信度）
            win_proba_gap = float(p1["win_proba"]) - float(p2["win_proba"])
            # バイク番号の位置関係（b1/b2/b3はすでにbanum値）
            banum_12 = b2 - b1
            banum_13 = b3 - b1
            # 同県ペア数・同班ペア数の合計
            same_pref_total = same_pref_12 + same_pref_13 + same_pref_23
            same_term_total = same_term_12 + same_term_13 + same_term_23

            row = {
                "race_id": race_id, "b1": b1, "b2": b2, "b3": b3,
                "san_combo": san_combo, "san_payout": san_payout,
                "top_score": top_score, "score_gap": score_gap,
                # 1着候補
                "p1_win_proba": p1["win_proba"],
                "p1_score": p1["race_score"], "p1_class": p1["class_num"],
                "p1_style": p1["style_num"], "p1_win_rate": p1["win_rate_4m"],
                "p1_last3_rank": p1["last3_avg_rank"], "p1_last5_rank": p1["last5_avg_rank"],
                "p1_nige_rate": p1["nige_4m"], "p1_maku_rate": p1["maku_4m"],
                "p1_venue_win": p1["venue_win_rate"], "p1_days": p1["days_since_last"],
                "p1_age": p1["age"], "p1_score_rank": p1["score_rank"],
                "p1_is_honmei": p1["is_honmei"], "p1_nige_finish": p1["is_nige_finish"],
                # 2着候補
                "p2_p2_proba": p2["p2_proba"],
                "p2_score": p2["race_score"], "p2_class": p2["class_num"],
                "p2_style": p2["style_num"], "p2_top2_rate": p2["top2_rate_4m"],
                "p2_last3_rank": p2["last3_avg_rank"], "p2_last5_rank": p2["last5_avg_rank"],
                "p2_age": p2["age"], "p2_score_rank": p2["score_rank"],
                "p2_is_honmei": p2["is_honmei"],
                # 3着候補
                "p3_p3_proba": p3["p3_proba"],
                "p3_score": p3["race_score"], "p3_class": p3["class_num"],
                "p3_style": p3["style_num"], "p3_top3_rate": p3["top3_rate_4m"],
                "p3_last3_rank": p3["last3_avg_rank"],
                "p3_age": p3["age"], "p3_score_rank": p3["score_rank"],
                "p3_sashi_finish": p3["is_sashi_finish"],
                # インタラクション
                "same_pref_12": same_pref_12, "same_pref_13": same_pref_13, "same_pref_23": same_pref_23,
                "same_term_12": same_term_12, "same_term_13": same_term_13, "same_term_23": same_term_23,
                "same_pref_total": same_pref_total, "same_term_total": same_term_total,
                "style_1_nige": style_1_nige, "style_1_maku": style_1_maku, "style_3_sashi": style_3_sashi,
                "nige_line_12": nige_line_12, "maku_line_12": maku_line_12,
                "score_diff_12": score_diff_12, "score_diff_13": score_diff_13, "score_diff_23": score_diff_23,
                "class_diff_12": class_diff_12, "class_diff_13": class_diff_13,
                "prob_product": prob_product, "prob_sum_log": prob_sum_log,
                "win_proba_gap": win_proba_gap,
                "banum_12": banum_12, "banum_13": banum_13,
                "score_gap_race": score_gap,
                "is_hit": int(is_correct),
            }
            rows.append(row)
    return pd.DataFrame(rows)

print("\nコンボデータ生成中...")
combo_tr  = make_combo_rows(df_tr,  is_train=True)
combo_val = make_combo_rows(df_val, is_train=True)
combo_te  = make_combo_rows(df_test, is_train=True)
print(f"訓練コンボ: {len(combo_tr):,}行  正解: {combo_tr['is_hit'].sum():,}件")
print(f"検証コンボ: {len(combo_val):,}行  正解: {combo_val['is_hit'].sum():,}件")
print(f"テストコンボ: {len(combo_te):,}行  正解: {combo_te['is_hit'].sum():,}件")

COMBO_FEATS = [
    # 1着候補
    "p1_win_proba","p1_score","p1_class","p1_style","p1_win_rate",
    "p1_last3_rank","p1_last5_rank","p1_nige_rate","p1_maku_rate",
    "p1_venue_win","p1_days","p1_age","p1_score_rank","p1_is_honmei","p1_nige_finish",
    # 2着候補
    "p2_p2_proba","p2_score","p2_class","p2_style","p2_top2_rate",
    "p2_last3_rank","p2_last5_rank","p2_age","p2_score_rank","p2_is_honmei",
    # 3着候補
    "p3_p3_proba","p3_score","p3_class","p3_style","p3_top3_rate",
    "p3_last3_rank","p3_age","p3_score_rank","p3_sashi_finish",
    # インタラクション
    "same_pref_12","same_pref_13","same_pref_23","same_pref_total",
    "same_term_12","same_term_13","same_term_23","same_term_total",
    "style_1_nige","style_1_maku","style_3_sashi",
    "nige_line_12","maku_line_12",
    "score_diff_12","score_diff_13","score_diff_23",
    "class_diff_12","class_diff_13",
    "prob_product","prob_sum_log","win_proba_gap",
    "banum_12","banum_13",
    "score_gap_race","top_score",
]

# ステップ3: コンボモデル訓練
print("\nコンボモデル訓練中...")
pos = combo_tr["is_hit"].sum()
neg = len(combo_tr) - pos
spw = neg / pos
print(f"  陽性: {pos:,}件  陰性: {neg:,}件  scale_pos_weight: {spw:.1f}")
COMBO_PARAMS = dict(n_estimators=2000, learning_rate=0.02,
                    num_leaves=63, min_child_samples=10,
                    scale_pos_weight=spw,
                    verbose=-1, random_state=42)
m_combo = lgb.LGBMClassifier(**COMBO_PARAMS)
m_combo.fit(
    combo_tr[COMBO_FEATS].values, combo_tr["is_hit"].values,
    eval_set=[(combo_val[COMBO_FEATS].values, combo_val["is_hit"].values)],
    eval_metric="auc",
    callbacks=[lgb.early_stopping(100, verbose=False, first_metric_only=True),
               lgb.log_evaluation(50)]
)
print(f"コンボモデル best iter: {m_combo.best_iteration_}")

combo_te["combo_score"] = m_combo.predict_proba(combo_te[COMBO_FEATS].values)[:,1]

# ステップ4: 各レースで最高スコアの組み合わせを選択
print("\n" + "="*50)
print(f"  {TEST_MONTH} コンボモデル結果")
print("="*50)

pred_rows = []
for race_id, g in combo_te.groupby("race_id"):
    best = g.nlargest(1, "combo_score").iloc[0]
    pred_rows.append({
        "race_id": race_id,
        "pred_1st": int(best["b1"]),
        "pred_2nd": int(best["b2"]),
        "pred_3rd": int(best["b3"]),
        "combo_score": best["combo_score"],
        "san_combo": best["san_combo"],
        "san_payout": best["san_payout"],
        "top_score": best["top_score"],
        "score_gap": best["score_gap"],
    })

pred_df = pd.DataFrame(pred_rows)

def is_hit(row):
    combo = str(row["san_combo"]) if pd.notna(row["san_combo"]) else ""
    parts = re.split(r"[-=]", combo)
    if len(parts) < 3: return False
    try:
        return int(parts[0])==row["pred_1st"] and int(parts[1])==row["pred_2nd"] and int(parts[2])==row["pred_3rd"]
    except: return False

pred_df["hit"] = pred_df.apply(is_hit, axis=1)

total_races = len(pred_df)
total_hits  = pred_df["hit"].sum()
total_payout = pred_df[pred_df["hit"]]["san_payout"].sum()
total_invest = total_races * 100
roi = total_payout / total_invest * 100 if total_invest else 0
hit_rate = total_hits / total_races * 100 if total_races else 0

print(f"対象レース数: {total_races}  (1レース1点買い)")
print(f"的中数: {total_hits}  的中率: {hit_rate:.1f}%")
print(f"総投資: {total_invest:,}円  払戻: {total_payout:,}円  回収率: {roi:.1f}%")

# 日別
pred_df["date"] = pred_df["race_id"].map(df_test.groupby("race_id")["date"].first())
print("\n【日別】")
daily = pred_df.groupby("date").agg(
    races=("hit","count"), hits=("hit","sum"),
    payout=("san_payout", lambda x: x[pred_df.loc[x.index,"hit"]].sum())
).reset_index()
for _, r in daily.iterrows():
    hr = r["hits"]/r["races"]*100
    roi_d = r["payout"]/r["races"]/100*100
    print(f"  {r['date']}  レース:{int(r['races']):3d}  的中:{int(r['hits']):2d}件  ({hr:.1f}%)  回収率:{roi_d:.0f}%")

# 的中詳細
hits_df = pred_df[pred_df["hit"]].copy()
hits_df["venue"] = hits_df["race_id"].map(df_test.groupby("race_id")["venue_slug"].first())
hits_df["race_no"] = hits_df["race_id"].map(df_test.groupby("race_id")["race_no"].first())
print(f"\n【的中レース ({total_hits}件)】")
for _, r in hits_df.iterrows():
    print(f"  {r['date']} {r['venue']} {int(r['race_no'])}R  "
          f"{int(r['pred_1st'])}-{int(r['pred_2nd'])}-{int(r['pred_3rd'])}  "
          f"払戻:{int(r['san_payout']):,}円  (score:{r['top_score']:.0f} gap:{r['score_gap']:.1f})")

# 旧モデルとの比較
print("\n" + "="*50)
print("  旧モデル(個別予測)との比較")
print("="*50)
print(f"旧: 3.5% (15/424件、賭け424件)  回収率29%")
print(f"新: {hit_rate:.1f}% ({total_hits}/{total_races}件、賭け{total_races}件)  回収率{roi:.1f}%")
