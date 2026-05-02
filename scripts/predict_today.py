# -*- coding: utf-8 -*-
import sys, io as _io
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = _io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = _io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

"""
当日の競輪予想を生成してDiscordに投稿する（コンボモデル版）
フィルター: top_score>=95 & score_gap>=2 & 7車限定
実行タイミング: JST 10:00 / 12:00 / 14:00 / 16:00
"""

import os, glob, re, warnings, requests, argparse, itertools
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
import lightgbm as lgb
from pathlib import Path

warnings.filterwarnings("ignore")

JST = timezone(timedelta(hours=9))

parser = argparse.ArgumentParser()
parser.add_argument("--date", default=None)
parser.add_argument("--hour", type=int, default=None)
parser.add_argument("--all",  action="store_true")
args = parser.parse_args()

now_jst     = datetime.now(JST)
TARGET_DATE = args.date or now_jst.strftime("%Y-%m-%d")
HOUR_JST    = args.hour if args.hour is not None else now_jst.hour

def race_no_range(hour):
    if hour < 12:   return (1,  4)
    elif hour < 19: return (5,  9)
    else:           return (10, 99)

RACE_RANGE = (1, 99) if args.all else race_no_range(HOUR_JST)
print(f"対象日: {TARGET_DATE}  JST {HOUR_JST}時台  race_no {RACE_RANGE[0]}〜{RACE_RANGE[1]}")

DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_URL", "")

def post_discord(content: str):
    if not DISCORD_WEBHOOK:
        print("[Discord] WEBHOOK未設定\n" + content)
        return
    r = requests.post(DISCORD_WEBHOOK, json={"content": content}, timeout=10)
    if r.status_code not in (200, 204):
        print(f"[Discord] 投稿失敗: {r.status_code} {r.text}")

# ========== データ読み込み ==========
TOP_K = 5
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

# ライン特徴量
df_all["line_size"] = df_all.groupby(["race_id","pref"])["banum"].transform("count")
n_strong_s = df_all.groupby("race_id").apply(
    lambda g: int((g.groupby("pref")["banum"].count() >= 2).sum())
).rename("n_strong_lines")
df_all = df_all.join(n_strong_s, on="race_id")

def extract_markers(x):
    if pd.isna(x): return {}
    res = {}
    for sym, key in [("○","niban"),("▲","attention"),("△","sanban")]:
        mk = re.search(re.escape(sym) + r"(\d+)", str(x))
        if mk: res[key] = int(mk.group(1))
    return res

_mdf = df_all.drop_duplicates("race_id")[["race_id","lineup"]].copy()
_mdf["_mk"] = _mdf["lineup"].apply(extract_markers)
_mdf["lineup_niban"]  = _mdf["_mk"].apply(lambda d: d.get("niban"))
_mdf["lineup_sanban"] = _mdf["_mk"].apply(lambda d: d.get("sanban"))
_mdf["lineup_attn"]   = _mdf["_mk"].apply(lambda d: d.get("attention"))
_mdf = _mdf.set_index("race_id")
df_all["lineup_niban"]  = df_all["race_id"].map(_mdf["lineup_niban"])
df_all["lineup_sanban"] = df_all["race_id"].map(_mdf["lineup_sanban"])
df_all["lineup_attn"]   = df_all["race_id"].map(_mdf["lineup_attn"])
df_all["is_niban"]  = (df_all["banum"] == df_all["lineup_niban"]).astype(int)
df_all["is_sanban"] = (df_all["banum"] == df_all["lineup_sanban"]).astype(int)
df_all["is_attn"]   = (df_all["banum"] == df_all["lineup_attn"]).astype(int)

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
    "style_1_nige","style_1_maku","style_3_sashi",
    "nige_line_12","maku_line_12",
    "score_diff_12","score_diff_13","score_diff_23",
    "class_diff_12","class_diff_13",
    "prob_product","prob_sum_log","win_proba_gap",
    "banum_12","banum_13",
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

# ========== 訓練: 対象日より前 ==========
df_train = df_model[df_model["date"] < TARGET_DATE].copy()
if len(df_train) < 1000:
    print("訓練データ不足")
    sys.exit(1)

print(f"訓練: {len(df_train):,}行")
val_months = sorted(df_train["date"].str[:7].unique())[-3:]
val_mask   = df_train["date"].str[:7].isin(val_months)
df_tr  = df_train[~val_mask].copy()
df_val = df_train[val_mask].copy()

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
print(f"個別モデル iter: 1着={m1.best_iteration_} / 2着={m2.best_iteration_} / 3着={m3.best_iteration_}")

for df_part in [df_tr, df_val]:
    df_part["win_proba"] = m1.predict_proba(df_part[F1].values)[:,1]
    df_part["p2_proba"]  = m2.predict_proba(df_part[F23].values)[:,1]
    df_part["p3_proba"]  = m3.predict_proba(df_part[F23].values)[:,1]

# コンボモデル訓練
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

        correct_1, correct_2, correct_3 = None, None, None
        if pd.notna(san_combo):
            parts = re.split(r"[-=]", str(san_combo))
            if len(parts) == 3:
                try: correct_1, correct_2, correct_3 = int(parts[0]), int(parts[1]), int(parts[2])
                except: pass

        n_strong = int(g["n_strong_lines"].iloc[0])
        top1_cands = g.nlargest(TOP_K, "win_proba")["banum"].astype(int).tolist()
        top2_cands = g.nlargest(TOP_K, "p2_proba")["banum"].astype(int).tolist()
        top3_cands = g.nlargest(TOP_K, "p3_proba")["banum"].astype(int).tolist()
        player_info = g.set_index("banum")[PLAYER_COLS + ["win_proba","p2_proba","p3_proba","pref","term"]]

        for b1, b2, b3 in itertools.product(top1_cands, top2_cands, top3_cands):
            if len({b1, b2, b3}) < 3: continue
            try:
                p1 = player_info.loc[b1]; p2 = player_info.loc[b2]; p3 = player_info.loc[b3]
            except KeyError: continue

            sp12 = int(p1["pref"]==p2["pref"]); sp13 = int(p1["pref"]==p3["pref"]); sp23 = int(p2["pref"]==p3["pref"])
            st12 = int(p1["term"]==p2["term"]); st13 = int(p1["term"]==p3["term"]); st23 = int(p2["term"]==p3["term"])
            s1n  = int(p1["style_num"]==5); s1m = int(p1["style_num"]==4)
            prob = float(p1["win_proba"])*float(p2["p2_proba"])*float(p3["p3_proba"])

            rows.append({
                "race_id": race_id, "b1": b1, "b2": b2, "b3": b3,
                "san_combo": san_combo, "san_payout": san_payout,
                "top_score": top_score, "score_gap": score_gap,
                "p1_win_proba": p1["win_proba"], "p1_score": p1["race_score"],
                "p1_class": p1["class_num"], "p1_style": p1["style_num"],
                "p1_win_rate": p1["win_rate_4m"], "p1_last3_rank": p1["last3_avg_rank"],
                "p1_last5_rank": p1["last5_avg_rank"], "p1_nige_rate": p1["nige_4m"],
                "p1_maku_rate": p1["maku_4m"], "p1_venue_win": p1["venue_win_rate"],
                "p1_days": p1["days_since_last"], "p1_age": p1["age"],
                "p1_score_rank": p1["score_rank"], "p1_is_honmei": p1["is_honmei"],
                "p1_nige_finish": p1["is_nige_finish"],
                "p2_p2_proba": p2["p2_proba"], "p2_score": p2["race_score"],
                "p2_class": p2["class_num"], "p2_style": p2["style_num"],
                "p2_top2_rate": p2["top2_rate_4m"], "p2_last3_rank": p2["last3_avg_rank"],
                "p2_last5_rank": p2["last5_avg_rank"], "p2_age": p2["age"],
                "p2_score_rank": p2["score_rank"], "p2_is_honmei": p2["is_honmei"],
                "p3_p3_proba": p3["p3_proba"], "p3_score": p3["race_score"],
                "p3_class": p3["class_num"], "p3_style": p3["style_num"],
                "p3_top3_rate": p3["top3_rate_4m"], "p3_last3_rank": p3["last3_avg_rank"],
                "p3_age": p3["age"], "p3_score_rank": p3["score_rank"],
                "p3_sashi_finish": p3["is_sashi_finish"],
                "same_pref_12": sp12, "same_pref_13": sp13, "same_pref_23": sp23, "same_pref_total": sp12+sp13+sp23,
                "same_term_12": st12, "same_term_13": st13, "same_term_23": st23, "same_term_total": st12+st13+st23,
                "style_1_nige": s1n, "style_1_maku": s1m,
                "style_3_sashi": int(float(p3["style_num"])<=2),
                "nige_line_12": s1n*sp12, "maku_line_12": s1m*sp12,
                "score_diff_12": float(p1["race_score"])-float(p2["race_score"]),
                "score_diff_13": float(p1["race_score"])-float(p3["race_score"]),
                "score_diff_23": float(p2["race_score"])-float(p3["race_score"]),
                "class_diff_12": float(p1["class_num"])-float(p2["class_num"]),
                "class_diff_13": float(p1["class_num"])-float(p3["class_num"]),
                "prob_product": prob,
                "prob_sum_log": np.log(float(p1["win_proba"])+1e-9)+np.log(float(p2["p2_proba"])+1e-9)+np.log(float(p3["p3_proba"])+1e-9),
                "win_proba_gap": float(p1["win_proba"])-float(p2["win_proba"]),
                "banum_12": b2-b1, "banum_13": b3-b1,
                "score_gap_race": score_gap,
                "p1_line_size": float(p1["line_size"]), "p2_line_size": float(p2["line_size"]), "p3_line_size": float(p3["line_size"]),
                "n_strong_lines": n_strong,
                "b2_is_b1_bankey": int((s1n or s1m) and sp12),
                "b3_in_b1_line": int(sp13 and not sp12),
                "b3_in_b2_line": int(sp23 and not sp13),
                "p2_is_niban": int(p2["is_niban"]), "p3_is_sanban": int(p3["is_sanban"]), "p2_is_attn": int(p2["is_attn"]),
                "b1_honmei_b2_niban": int(bool(p1["is_honmei"]) and bool(p2["is_niban"])),
                "b1_honmei_b2_niban_b3_sanban": int(bool(p1["is_honmei"]) and bool(p2["is_niban"]) and bool(p3["is_sanban"])),
                "is_hit": int(b1==correct_1 and b2==correct_2 and b3==correct_3),
            })
    return pd.DataFrame(rows)

print("コンボデータ生成中...")
combo_tr  = make_combo_rows(df_tr)
combo_val = make_combo_rows(df_val)
if len(combo_tr) == 0 or combo_tr["is_hit"].sum() == 0:
    print("コンボ訓練データ不足")
    sys.exit(1)

pos = combo_tr["is_hit"].sum(); neg = len(combo_tr) - pos
m_combo = lgb.LGBMClassifier(
    n_estimators=2000, learning_rate=0.02, num_leaves=63,
    min_child_samples=10, scale_pos_weight=neg/pos, verbose=-1, random_state=42)
m_combo.fit(
    combo_tr[COMBO_FEATS].values, combo_tr["is_hit"].values,
    eval_set=[(combo_val[COMBO_FEATS].values, combo_val["is_hit"].values)],
    eval_metric="auc",
    callbacks=[lgb.early_stopping(100, verbose=False, first_metric_only=True), lgb.log_evaluation(-1)]
)
print(f"コンボモデル best iter: {m_combo.best_iteration_}")

# バリデーションセットからコンボスコア閾値を決定 (上位10%, テストデータ不使用)
combo_val["combo_score"] = m_combo.predict_proba(combo_val[COMBO_FEATS].values)[:,1]
val_top1 = combo_val.groupby("race_id")["combo_score"].max().reset_index()
COMBO_THRESHOLD = np.percentile(val_top1["combo_score"], 90)
print(f"コンボ閾値 (val上位10%): {COMBO_THRESHOLD:.4f}")

# ========== 当日データで予測 ==========
df_today = df_all[df_all["date"] == TARGET_DATE].dropna(subset=req[:-1]).copy()
if df_today.empty:
    msg = f"**{TARGET_DATE} の予想データがありません**"
    post_discord(msg); print(msg); sys.exit(0)

df_today["win_proba"] = m1.predict_proba(df_today[F1].values)[:,1]
df_today["p2_proba"]  = m2.predict_proba(df_today[F23].values)[:,1]
df_today["p3_proba"]  = m3.predict_proba(df_today[F23].values)[:,1]

pred_rows = []
for race_id, g in df_today.groupby("race_id"):
    top_score = top_score_raw.get(race_id, 0)
    score_gap = score_gap_raw.get(race_id, 0)
    n_players = n_players_raw.get(race_id, 0)
    race_no   = int(g["race_no"].iloc[0])

    if (top_score < TOP_SCORE_THRESH or score_gap < SCORE_GAP_THRESH
            or n_players != 7 or not (RACE_RANGE[0] <= race_no <= RACE_RANGE[1])):
        continue

    g = g.copy()
    n_strong = int(g["n_strong_lines"].iloc[0])
    top1_cands = g.nlargest(TOP_K, "win_proba")["banum"].astype(int).tolist()
    top2_cands = g.nlargest(TOP_K, "p2_proba")["banum"].astype(int).tolist()
    top3_cands = g.nlargest(TOP_K, "p3_proba")["banum"].astype(int).tolist()
    player_info = g.set_index("banum")[PLAYER_COLS + ["win_proba","p2_proba","p3_proba","pref","term"]]

    combo_rows = []
    for b1, b2, b3 in itertools.product(top1_cands, top2_cands, top3_cands):
        if len({b1, b2, b3}) < 3: continue
        try:
            p1 = player_info.loc[b1]; p2 = player_info.loc[b2]; p3 = player_info.loc[b3]
        except KeyError: continue
        sp12 = int(p1["pref"]==p2["pref"]); sp13 = int(p1["pref"]==p3["pref"]); sp23 = int(p2["pref"]==p3["pref"])
        st12 = int(p1["term"]==p2["term"]); st13 = int(p1["term"]==p3["term"]); st23 = int(p2["term"]==p3["term"])
        s1n = int(p1["style_num"]==5); s1m = int(p1["style_num"]==4)
        prob = float(p1["win_proba"])*float(p2["p2_proba"])*float(p3["p3_proba"])
        combo_rows.append({
            "b1": b1, "b2": b2, "b3": b3,
            "p1_win_proba": p1["win_proba"], "p1_score": p1["race_score"],
            "p1_class": p1["class_num"], "p1_style": p1["style_num"],
            "p1_win_rate": p1["win_rate_4m"], "p1_last3_rank": p1["last3_avg_rank"],
            "p1_last5_rank": p1["last5_avg_rank"], "p1_nige_rate": p1["nige_4m"],
            "p1_maku_rate": p1["maku_4m"], "p1_venue_win": p1["venue_win_rate"],
            "p1_days": p1["days_since_last"], "p1_age": p1["age"],
            "p1_score_rank": p1["score_rank"], "p1_is_honmei": p1["is_honmei"],
            "p1_nige_finish": p1["is_nige_finish"],
            "p2_p2_proba": p2["p2_proba"], "p2_score": p2["race_score"],
            "p2_class": p2["class_num"], "p2_style": p2["style_num"],
            "p2_top2_rate": p2["top2_rate_4m"], "p2_last3_rank": p2["last3_avg_rank"],
            "p2_last5_rank": p2["last5_avg_rank"], "p2_age": p2["age"],
            "p2_score_rank": p2["score_rank"], "p2_is_honmei": p2["is_honmei"],
            "p3_p3_proba": p3["p3_proba"], "p3_score": p3["race_score"],
            "p3_class": p3["class_num"], "p3_style": p3["style_num"],
            "p3_top3_rate": p3["top3_rate_4m"], "p3_last3_rank": p3["last3_avg_rank"],
            "p3_age": p3["age"], "p3_score_rank": p3["score_rank"],
            "p3_sashi_finish": p3["is_sashi_finish"],
            "same_pref_12": sp12, "same_pref_13": sp13, "same_pref_23": sp23, "same_pref_total": sp12+sp13+sp23,
            "same_term_12": st12, "same_term_13": st13, "same_term_23": st23, "same_term_total": st12+st13+st23,
            "style_1_nige": s1n, "style_1_maku": s1m,
            "style_3_sashi": int(float(p3["style_num"])<=2),
            "nige_line_12": s1n*sp12, "maku_line_12": s1m*sp12,
            "score_diff_12": float(p1["race_score"])-float(p2["race_score"]),
            "score_diff_13": float(p1["race_score"])-float(p3["race_score"]),
            "score_diff_23": float(p2["race_score"])-float(p3["race_score"]),
            "class_diff_12": float(p1["class_num"])-float(p2["class_num"]),
            "class_diff_13": float(p1["class_num"])-float(p3["class_num"]),
            "prob_product": prob,
            "prob_sum_log": np.log(float(p1["win_proba"])+1e-9)+np.log(float(p2["p2_proba"])+1e-9)+np.log(float(p3["p3_proba"])+1e-9),
            "win_proba_gap": float(p1["win_proba"])-float(p2["win_proba"]),
            "banum_12": b2-b1, "banum_13": b3-b1,
            "score_gap_race": score_gap, "top_score": top_score,
            "p1_line_size": float(p1["line_size"]), "p2_line_size": float(p2["line_size"]), "p3_line_size": float(p3["line_size"]),
            "n_strong_lines": n_strong,
            "b2_is_b1_bankey": int((s1n or s1m) and sp12),
            "b3_in_b1_line": int(sp13 and not sp12),
            "b3_in_b2_line": int(sp23 and not sp13),
            "p2_is_niban": int(p2["is_niban"]), "p3_is_sanban": int(p3["is_sanban"]), "p2_is_attn": int(p2["is_attn"]),
            "b1_honmei_b2_niban": int(bool(p1["is_honmei"]) and bool(p2["is_niban"])),
            "b1_honmei_b2_niban_b3_sanban": int(bool(p1["is_honmei"]) and bool(p2["is_niban"]) and bool(p3["is_sanban"])),
        })

    if not combo_rows: continue
    combo_df = pd.DataFrame(combo_rows)
    combo_df["combo_score"] = m_combo.predict_proba(combo_df[COMBO_FEATS].values)[:,1]
    best = combo_df.nlargest(1, "combo_score").iloc[0]

    close_time = g["close_time"].iloc[0] if "close_time" in g.columns else ""
    pred_rows.append({
        "race_id": race_id, "venue": g["venue_slug"].iloc[0],
        "date": g["date"].iloc[0], "race_no": race_no,
        "pred_1st": int(best["b1"]), "pred_2nd": int(best["b2"]), "pred_3rd": int(best["b3"]),
        "combo_score": best["combo_score"],
        "passes_threshold": bool(best["combo_score"] >= COMBO_THRESHOLD),
        "top_score": top_score, "score_gap": score_gap,
        "close_time": close_time,
    })

n_high = sum(1 for r in pred_rows if r["passes_threshold"])
print(f"予測レース数: {len(pred_rows)} (高信頼度★: {n_high})")

# ========== Discord投稿 ==========
if not pred_rows:
    msg = f"**{TARGET_DATE} 予想 ({HOUR_JST}時台)**\n対象レースなし"
    post_discord(msg); print(msg)
else:
    lines = [
        f"**:checkered_flag: {TARGET_DATE} 競輪予想 ({HOUR_JST}時台) [ラインモデル]**",
        f"対象: {len(pred_rows)}レース  ★高信頼度(上位10%): {n_high}レース\n"
    ]
    for r in sorted(pred_rows, key=lambda x: (x["venue"], x["race_no"])):
        ct = r.get("close_time","")
        time_str = f"  締切:{ct}" if ct and str(ct) != "nan" else ""
        star = "★" if r["passes_threshold"] else "  "
        lines.append(
            f"{star}:round_pushpin: **{r['venue']} {r['race_no']}R**{time_str}\n"
            f"  予想: `{r['pred_1st']}-{r['pred_2nd']}-{r['pred_3rd']}`\n"
            f"  score:{r['top_score']:.0f} gap:{r['score_gap']:.1f}  (combo:{r['combo_score']:.4f})"
        )
    msg = "\n".join(lines)
    print(msg)
    post_discord(msg)
