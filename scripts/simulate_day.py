# -*- coding: utf-8 -*-
import sys, io as _io
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = _io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = _io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

"""
特定日のシミュレーション: 2023〜前日までで訓練 → 指定日で予測・検証
Usage: python simulate_day.py --date 2026-03-29
"""

import argparse
import glob
import re
import warnings
import numpy as np
import pandas as pd
from pathlib import Path

warnings.filterwarnings("ignore")

# ========== 引数 ==========
parser = argparse.ArgumentParser()
parser.add_argument("--date", default="2026-03-29")
args = parser.parse_args()
TARGET_DATE = args.date

print(f"\n{'='*60}")
print(f"  シミュレーション対象日: {TARGET_DATE}")
print(f"{'='*60}")

# ========== データ読み込み ==========
DATA_DIR = Path("keirin_data")
files = sorted(glob.glob(str(DATA_DIR / "202[3456]_*.csv")))
# sample csvは除外
files = [f for f in files if "sample" not in f]
dfs = []
for f in files:
    try:
        dfs.append(pd.read_csv(f, encoding="utf-8-sig"))
    except Exception as e:
        print(f"  スキップ {f}: {e}")

df_all = pd.concat(dfs, ignore_index=True)
print(f"  総行数: {len(df_all):,} / レース数: {df_all['race_id'].nunique():,}")

# 対象日が存在するか確認
target_rows = df_all[df_all["date"] == TARGET_DATE]
print(f"  {TARGET_DATE} レース数: {target_rows['race_id'].nunique()}")
if target_rows.empty:
    print("  対象日のデータがありません")
    sys.exit(1)

# ========== 払戻パース ==========
def parse_payout(s):
    if pd.isna(s) or str(s).strip() == "":
        return None, None
    s = str(s).strip()
    m = re.search(r"(\d+[-=]\d+(?:[-=]\d+)?)\s+([\d,]+)円", s)
    if m:
        return m.group(1), int(m.group(2).replace(",", ""))
    return None, None

df_all[["ni_combo", "ni_payout"]] = df_all["ni_sha_tan"].apply(
    lambda x: pd.Series(parse_payout(x))
)
df_all[["san_combo", "san_payout"]] = df_all["san_ren_tan"].apply(
    lambda x: pd.Series(parse_payout(x))
)

# ========== 選手トレンド特徴量（リークなし）==========
CLASS_MAP = {"S1": 4, "S2": 3, "A1": 2, "A2": 1, "B": 0}
STYLE_MAP  = {"逃": 5, "捲": 4, "両": 3, "差": 2, "追": 1, "マ": 0}

df_all["player_key"] = df_all["player_name"].astype(str) + "_" + df_all["term"].astype(str)
df_all["rank_num"]   = pd.to_numeric(df_all["rank"], errors="coerce")
df_all["class_num"]  = df_all["player_class"].map(CLASS_MAP).fillna(1)
df_all["style_num"]  = df_all["running_style"].map(STYLE_MAP).fillna(2)
df_all["race_score"] = pd.to_numeric(df_all["race_score"], errors="coerce")
df_all["gear"]       = pd.to_numeric(df_all["gear"], errors="coerce")
df_all["banum"]      = pd.to_numeric(df_all["banum"], errors="coerce")
df_all["is_win"]     = (df_all["rank_num"] == 1).astype(float)

df_all = df_all.sort_values(["player_key", "date", "race_no"]).reset_index(drop=True)

grp = df_all.groupby("player_key")["rank_num"]
df_all["prev1_rank"]     = grp.shift(1)
df_all["last3_avg_rank"] = grp.transform(lambda x: x.shift(1).rolling(3, min_periods=1).mean())
df_all["last5_avg_rank"] = grp.transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
df_all["last5_win_rate"] = df_all.groupby("player_key")["is_win"].transform(
    lambda x: x.shift(1).rolling(5, min_periods=1).mean()
)
df_all["rank_trend"]     = df_all["prev1_rank"] - grp.shift(3)
df_all["prev_date"]      = df_all.groupby("player_key")["date"].shift(1)
df_all["days_since_last"] = (
    pd.to_datetime(df_all["date"]) - pd.to_datetime(df_all["prev_date"])
).dt.days

# 本命車番
def extract_honmei(lineup):
    if pd.isna(lineup): return None
    m = re.search(r"◎(\d+)", str(lineup))
    return int(m.group(1)) if m else None
df_all["honmei_banum"] = df_all["lineup"].apply(extract_honmei)

# ========== 特徴量 ==========
FEATURES = ["race_score", "class_num", "style_num", "gear",
            "score_rank", "is_honmei", "n_players_in_race",
            "prev1_rank", "last3_avg_rank", "last5_avg_rank",
            "last5_win_rate", "rank_trend", "days_since_last"]

df_all["score_rank"] = df_all.groupby("race_id")["race_score"].rank(ascending=False, method="min")
df_all["is_honmei"]  = (df_all["banum"] == df_all["honmei_banum"]).astype(int)
n_players = df_all.groupby("race_id")["banum"].count().rename("n_players_in_race")
df_all = df_all.join(n_players, on="race_id")
df_all["is_winner"] = (df_all["rank_num"] == 1).astype(int)

df_model = df_all.dropna(subset=FEATURES + ["rank_num", "san_payout"]).copy()

# ========== 訓練: 対象日より前 / 検証: 対象日 ==========
train_mask = df_model["date"] < TARGET_DATE
val_mask   = df_model["date"] == TARGET_DATE

df_train = df_model[train_mask]
df_val   = df_model[val_mask]

print(f"\n  訓練データ: {len(df_train):,}行 ({df_train['race_id'].nunique():,}レース)")
print(f"  検証データ: {len(df_val):,}行 ({df_val['race_id'].nunique():,}レース)")

# ========== LightGBM 訓練 ==========
try:
    import lightgbm as lgb
    from sklearn.metrics import roc_auc_score

    X_tr = df_train[FEATURES].values
    y_tr = df_train["is_winner"].values
    X_val = df_val[FEATURES].values
    y_val = df_val["is_winner"].values

    model = lgb.LGBMClassifier(
        n_estimators=300, learning_rate=0.05,
        num_leaves=31, min_child_samples=50,
        verbose=-1, random_state=42
    )
    model.fit(X_tr, y_tr)

    val_proba = model.predict_proba(X_val)[:, 1]
    auc = roc_auc_score(y_val, val_proba)
    print(f"\n  検証AUC: {auc:.4f}")

    # ========== レース単位で予測 ==========
    df_val = df_val.copy()
    df_val["win_proba"] = val_proba

    df_sorted = df_val.sort_values(["race_id", "win_proba"], ascending=[True, False])
    race_preds = df_sorted.groupby("race_id").agg(
        venue=("venue_slug", "first"),
        date=("date", "first"),
        race_no=("race_no", "first"),
        pred_1st=("banum", lambda x: int(x.iloc[0])),
        pred_2nd=("banum", lambda x: int(x.iloc[1]) if len(x) > 1 else None),
        pred_3rd=("banum", lambda x: int(x.iloc[2]) if len(x) > 2 else None),
        top_proba=("win_proba", lambda x: x.iloc[0]),
        ni_payout=("ni_payout", "first"),
        san_payout=("san_payout", "first"),
        top_score=("race_score", "max"),
        honmei_banum=("honmei_banum", "first"),
    ).reset_index()

    winner_map = df_val[df_val["rank_num"] == 1].groupby("race_id")["banum"].first()
    second_map = df_val[df_val["rank_num"] == 2].groupby("race_id")["banum"].first()
    third_map  = df_val[df_val["rank_num"] == 3].groupby("race_id")["banum"].first()

    race_preds["actual_1st"] = race_preds["race_id"].map(winner_map)
    race_preds["actual_2nd"] = race_preds["race_id"].map(second_map)
    race_preds["actual_3rd"] = race_preds["race_id"].map(third_map)

    # score_gap・top_score は dropna前の全選手データ（df_all）から計算（異常値対策）
    target_races = race_preds["race_id"].unique()
    df_all_target = df_all[df_all["race_id"].isin(target_races)].copy()
    df_all_target["race_score_num"] = pd.to_numeric(df_all_target["race_score"], errors="coerce")

    def calc_gap(x):
        s = sorted(x.dropna(), reverse=True)
        if len(s) < 2: return np.nan
        return s[0] - s[1]

    score_gap_map  = df_all_target.groupby("race_id")["race_score_num"].apply(calc_gap)
    top_score_map  = df_all_target.groupby("race_id")["race_score_num"].max()
    race_preds["score_gap"] = race_preds["race_id"].map(score_gap_map)
    race_preds["top_score"] = race_preds["race_id"].map(top_score_map)

    # 的中判定
    race_preds["ni_hit"] = (
        (race_preds["pred_1st"] == race_preds["actual_1st"]) &
        (race_preds["pred_2nd"] == race_preds["actual_2nd"])
    ).astype(int)
    race_preds["san_hit"] = (
        (race_preds["pred_1st"] == race_preds["actual_1st"]) &
        (race_preds["pred_2nd"] == race_preds["actual_2nd"]) &
        (race_preds["pred_3rd"] == race_preds["actual_3rd"])
    ).astype(int)

    # ========== 全レース結果 ==========
    print(f"\n{'='*60}")
    print(f"  {TARGET_DATE} 全{len(race_preds)}レース 予測結果")
    print(f"{'='*60}")

    cols = ["venue", "race_no", "pred_1st", "pred_2nd", "pred_3rd",
            "actual_1st", "actual_2nd", "actual_3rd",
            "top_proba", "top_score", "score_gap",
            "ni_hit", "san_hit", "ni_payout", "san_payout"]
    print(race_preds[cols].to_string(index=False))

    # ========== 集計 ==========
    total = len(race_preds)
    ni_hits  = race_preds["ni_hit"].sum()
    san_hits = race_preds["san_hit"].sum()
    ni_recovery  = race_preds[race_preds["ni_hit"]==1]["ni_payout"].sum() / (total * 100) * 100
    san_recovery = race_preds[race_preds["san_hit"]==1]["san_payout"].sum() / (total * 100) * 100

    print(f"\n{'='*60}")
    print(f"  【全レース賭け】")
    print(f"  二車連単  的中: {ni_hits}/{total}  ({ni_hits/total*100:.1f}%)  回収率: {ni_recovery:.1f}%")
    print(f"  三連勝単  的中: {san_hits}/{total}  ({san_hits/total*100:.1f}%)  回収率: {san_recovery:.1f}%")

    # ========== フィルター戦略 ==========
    print(f"\n  【フィルター戦略: top_score>=95 & score_gap>=2 & 7車限定】")
    n_players_map = df_all[df_all["race_id"].isin(race_preds["race_id"])].groupby("race_id")["banum"].nunique()
    race_preds["n_players"] = race_preds["race_id"].map(n_players_map)
    filtered = race_preds[
        (race_preds["top_score"] >= 95) &
        (race_preds["score_gap"] >= 2) &
        (race_preds["n_players"] == 7)
    ]
    if len(filtered) > 0:
        f_san_hits = filtered["san_hit"].sum()
        f_san_rec  = filtered[filtered["san_hit"]==1]["san_payout"].sum() / (len(filtered) * 100) * 100
        print(f"  対象レース: {len(filtered)}件")
        print(f"  三連勝単  的中: {f_san_hits}/{len(filtered)}  ({f_san_hits/len(filtered)*100:.1f}%)  回収率: {f_san_rec:.1f}%")
        print(f"\n  --- 対象レース詳細 ---")
        print(filtered[cols].to_string(index=False))
    else:
        print("  条件に合うレースなし")

    # 閾値別サマリー
    print(f"\n  【三連勝単 閾値別回収率】")
    rows_thr = []
    for thr in [0.00, 0.20, 0.25, 0.30, 0.35, 0.40]:
        sub = race_preds[race_preds["top_proba"] >= thr]
        if len(sub) == 0: continue
        hits = sub["san_hit"].sum()
        rec  = sub[sub["san_hit"]==1]["san_payout"].sum() / (len(sub) * 100) * 100
        rows_thr.append({"top_proba閾値": thr, "件数": len(sub), "的中": hits,
                          "的中率(%)": round(hits/len(sub)*100, 1), "回収率(%)": round(rec, 1)})
    print(pd.DataFrame(rows_thr).to_string(index=False))

except ImportError:
    print("  lightgbm が見つかりません: pip install lightgbm")
