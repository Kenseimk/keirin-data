# -*- coding: utf-8 -*-
import sys, io as _io
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = _io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = _io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

"""
当日の競輪予想を生成してDiscordに投稿する
フィルター: top_score>=95 & score_gap>=2 & 7車限定
実行タイミング: JST 10:00 / 12:00 / 14:00 / 16:00
  → 各実行で「次の2時間に始まる」レース番号帯だけ投稿

race_no の時間帯推定（1日12レース想定）:
  10:00実行 → race_no 1-3
  12:00実行 → race_no 4-6
  14:00実行 → race_no 7-9
  16:00実行 → race_no 10以上
"""

import os, glob, re, warnings, requests, argparse
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
import lightgbm as lgb
from pathlib import Path

warnings.filterwarnings("ignore")

JST = timezone(timedelta(hours=9))

# ========== 引数 ==========
parser = argparse.ArgumentParser()
parser.add_argument("--date", default=None, help="対象日 (例: 2026-03-31)。省略時は今日")
parser.add_argument("--hour", type=int, default=None, help="実行時刻(JST時)。省略時は現在時刻")
parser.add_argument("--all",  action="store_true", help="全レース帯を投稿（テスト用）")
parser.add_argument("--min_ev", type=float, default=0.0,
                    help="期待値フィルター(円)。top_proba×過去平均払戻がこれ未満のレースを除外 (例: 500 → 期待収支+500円以上)")
args = parser.parse_args()

now_jst     = datetime.now(JST)
TARGET_DATE = args.date or now_jst.strftime("%Y-%m-%d")
HOUR_JST    = args.hour if args.hour is not None else now_jst.hour

# 時間帯 → race_no 範囲
def race_no_range(hour):
    if hour < 12:   return (1,  4)   # 8時台  → 1〜4R
    elif hour < 19: return (5,  9)   # 12時台 → 5〜9R
    else:           return (10, 99)  # 19時台 → 10R以降

RACE_RANGE = (1, 99) if args.all else race_no_range(HOUR_JST)
print(f"対象日: {TARGET_DATE}  JST {HOUR_JST}時台  race_no {RACE_RANGE[0]}〜{RACE_RANGE[1]}")

DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_URL", "")

# ========== Discord投稿 ==========
def post_discord(content: str):
    if not DISCORD_WEBHOOK:
        print("[Discord] WEBHOOK未設定\n" + content)
        return
    r = requests.post(DISCORD_WEBHOOK, json={"content": content}, timeout=10)
    if r.status_code not in (200, 204):
        print(f"[Discord] 投稿失敗: {r.status_code} {r.text}")

# ========== データ読み込み ==========
DATA_DIR = Path("keirin_data")
files = [f for f in sorted(glob.glob(str(DATA_DIR / "202[3456]_*.csv"))) if "sample" not in f]
df_all = pd.concat([pd.read_csv(f, encoding="utf-8-sig") for f in files], ignore_index=True)
print(f"総行数: {len(df_all):,}")

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
df_all["is_2nd"]        = (df_all["rank_num"]==2).astype(int)
df_all["is_3rd"]        = (df_all["rank_num"]==3).astype(int)
# 直近4ヶ月成績（新規スクレイピング分のみ存在、旧データはNaN→LGBMがNaN対応）
df_all["mark_num"]      = pd.to_numeric(df_all.get("mark_num"),      errors="coerce")
df_all["win_rate_4m"]   = pd.to_numeric(df_all.get("win_rate_4m"),   errors="coerce")
df_all["top2_rate_4m"]  = pd.to_numeric(df_all.get("top2_rate_4m"),  errors="coerce")
df_all["top3_rate_4m"]  = pd.to_numeric(df_all.get("top3_rate_4m"),  errors="coerce")
df_all["nige_4m"]       = pd.to_numeric(df_all.get("nige_4m"),       errors="coerce")
df_all["maku_4m"]       = pd.to_numeric(df_all.get("maku_4m"),       errors="coerce")

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
df_all["is_winner"]    = (df_all["rank_num"]==1).astype(int)
n_pl = df_all.groupby("race_id")["banum"].count().rename("n_players_in_race")
df_all = df_all.join(n_pl, on="race_id")

FEATURES_BASE = ["race_score","class_num","style_num","gear",
                 "score_rank","is_honmei","n_players_in_race",
                 "prev1_rank","last3_avg_rank","last5_avg_rank",
                 "last5_win_rate","rank_trend","days_since_last"]
# 直近4ヶ月成績（新規スクレイピング分のみ存在、旧データはNaN→LGBMがNaN対応）
FEATURES_NEW  = ["mark_num","win_rate_4m","top2_rate_4m","top3_rate_4m",
                 "nige_4m","maku_4m"]
FEATURES = FEATURES_BASE + FEATURES_NEW

# score_gap・top_score・n_players をdf_allから計算（バグ修正済み）
def calc_gap(x):
    s = sorted(x.dropna(), reverse=True)
    return s[0]-s[1] if len(s)>=2 else np.nan

score_gap_raw = df_all.groupby("race_id")["race_score"].apply(calc_gap)
top_score_raw = df_all.groupby("race_id")["race_score"].max()
n_players_raw = df_all.groupby("race_id")["banum"].nunique()

df_model = df_all.dropna(subset=FEATURES_BASE+["rank_num"]).copy()

# ========== 訓練: 対象日より前 ==========
df_train = df_model[df_model["date"] < TARGET_DATE]
if len(df_train) < 1000:
    print("訓練データ不足")
    sys.exit(1)

print(f"訓練: {len(df_train):,}行 ({df_train['race_id'].nunique():,}レース)")

# 訓練データの末尾3ヶ月を検証用（early stopping）
VAL_MONTHS = 3
ES_ROUNDS  = 50
all_months = sorted(df_train['date'].str[:7].unique())
val_months_list = all_months[-VAL_MONTHS:]
val_mask = df_train['date'].str[:7].isin(val_months_list)
df_tr  = df_train[~val_mask]
df_val = df_train[val_mask]
print(f"  学習: {len(df_tr):,}行  検証(early stopping): {len(df_val):,}行")

LGB_PARAMS = dict(n_estimators=1000, learning_rate=0.05,
                  num_leaves=31, min_child_samples=50,
                  verbose=-1, random_state=42)

def fit_es(params, X_tr, y_tr, X_val, y_val):
    m = lgb.LGBMClassifier(**params)
    m.fit(X_tr, y_tr,
          eval_set=[(X_val, y_val)],
          callbacks=[lgb.early_stopping(ES_ROUNDS, verbose=False),
                     lgb.log_evaluation(-1)])
    return m

X_tr  = df_tr[FEATURES].values
X_val = df_val[FEATURES].values
model  = fit_es(LGB_PARAMS, X_tr, df_tr["is_winner"].values, X_val, df_val["is_winner"].values)
model2 = fit_es(LGB_PARAMS, X_tr, df_tr["is_2nd"].values,    X_val, df_val["is_2nd"].values)
model3 = fit_es(LGB_PARAMS, X_tr, df_tr["is_3rd"].values,    X_val, df_val["is_3rd"].values)
print(f"  best iterations: 1着={model.best_iteration_} / 2着={model2.best_iteration_} / 3着={model3.best_iteration_}")

# ========== 当日データ（未完走レースもrank_numなしで予測対象にする） ==========
df_today = df_all[df_all["date"] == TARGET_DATE].dropna(subset=FEATURES_BASE).copy()
if df_today.empty:
    msg = f"**{TARGET_DATE} の予想データがありません**\n（当日データ未収集の可能性があります）"
    post_discord(msg)
    print(msg)
    sys.exit(0)

df_today["win_proba"] = model.predict_proba( df_today[FEATURES].values)[:, 1]
df_today["p2_proba"]  = model2.predict_proba(df_today[FEATURES].values)[:, 1]
df_today["p3_proba"]  = model3.predict_proba(df_today[FEATURES].values)[:, 1]

rows = []
for race_id, grp_r in df_today.groupby("race_id"):
    grp_r = grp_r.sort_values("win_proba", ascending=False)
    if len(grp_r) < 3:
        continue
    pred_1    = int(grp_r.iloc[0]["banum"])
    top_proba = float(grp_r.iloc[0]["win_proba"])
    close_time = grp_r["close_time"].iloc[0] if "close_time" in grp_r.columns else ""

    # 2着候補を上位2名取得
    rest2 = grp_r[grp_r["banum"] != pred_1].sort_values("p2_proba", ascending=False)
    pred_2a    = int(rest2.iloc[0]["banum"])   if len(rest2) >= 1 else None
    pred_2b    = int(rest2.iloc[1]["banum"])   if len(rest2) >= 2 else None
    p2_gap     = float(rest2.iloc[0]["p2_proba"] - rest2.iloc[1]["p2_proba"]) if len(rest2) >= 2 else 1.0
    # gap<0.05=接戦→2通り展開、gap>=0.05=確信度高→1通りに絞る
    use_formation = (p2_gap < 0.05) and (pred_2b is not None)

    def pick_3rd(exclude_banums):
        rest3 = grp_r[~grp_r["banum"].isin(exclude_banums)].sort_values("p3_proba", ascending=False)
        return int(rest3.iloc[0]["banum"]) if len(rest3) >= 1 else None

    pred_3a = pick_3rd([pred_1, pred_2a])
    pred_3b = pick_3rd([pred_1, pred_2b]) if pred_2b else None

    base = {"race_id": race_id, "venue": grp_r["venue_slug"].iloc[0],
            "date": grp_r["date"].iloc[0], "race_no": grp_r["race_no"].iloc[0],
            "pred_1st": pred_1, "top_proba": top_proba, "p2_gap": p2_gap,
            "close_time": close_time}
    rows.append({**base, "pred_2nd": pred_2a, "pred_3rd": pred_3a, "formation": "A"})
    if use_formation:
        rows.append({**base, "pred_2nd": pred_2b, "pred_3rd": pred_3b, "formation": "B"})

rp = pd.DataFrame(rows)

rp["score_gap"] = rp["race_id"].map(score_gap_raw)
rp["top_score"] = rp["race_id"].map(top_score_raw)
rp["n_players"] = rp["race_id"].map(n_players_raw)

# ========== バンド別・的中率&平均払戻テーブル（訓練データから算出） ==========
# 損益分岐払戻 = 100円 ÷ 的中率（例: 的中率6.6% → 1,515円必要）
df_train_pay = df_all[df_all["date"] < TARGET_DATE].copy()
df_train_pay[["_combo","_pay"]] = df_train_pay["san_ren_tan"].apply(
    lambda x: pd.Series(parse_payout(x)))
df_train_pay["_pay"] = pd.to_numeric(df_train_pay["_pay"], errors="coerce")
# レース単位で集約
race_stats = df_train_pay.groupby("race_id").agg(
    _pay=("_pay","first"), _combo=("_combo","first")
).reset_index()
race_stats["top_score"] = race_stats["race_id"].map(top_score_raw)
race_stats["score_gap"] = race_stats["race_id"].map(score_gap_raw)
race_stats["n_players"] = race_stats["race_id"].map(n_players_raw)
race_stats_f = race_stats[
    (race_stats["top_score"] >= 95) &
    (race_stats["score_gap"] >= 2) &
    (race_stats["n_players"] == 7)
].dropna(subset=["_pay"]).copy()

def score_band(s):
    if s < 100: return "95-100"
    elif s < 105: return "100-105"
    elif s < 110: return "105-110"
    else: return "110+"

def gap_band(g):
    if g < 4: return "2-4"
    elif g < 6: return "4-6"
    elif g < 8: return "6-8"
    else: return "8+"

race_stats_f["score_band"] = race_stats_f["top_score"].apply(score_band)
race_stats_f["gap_band"]   = race_stats_f["score_gap"].apply(gap_band)

# バンド別: 平均払戻 & 予測的中率（訓練データ内の的中レースを近似）
# 的中率は walkforward 結果に近い全体値を使い、バンド補正係数で調整
# （バンドごとのサンプル数が少ないため全体平均6.6%をベースに使用）
band_table = race_stats_f.groupby(
    ["score_band","gap_band"], observed=True
)["_pay"].agg(avg_payout="mean", race_count="count").reset_index()

# 全体的中率（walkforward実績）から損益分岐払戻を算出
OVERALL_HIT_RATE = 0.066  # walkforward実績値
band_table["breakeven_payout"] = 100 / OVERALL_HIT_RATE  # ≈1,515円
# 期待値 = avg_payout × hit_rate（損益分岐を超えているか）
band_table["ev_per_bet"] = band_table["avg_payout"] * OVERALL_HIT_RATE

rp["score_band"] = rp["top_score"].apply(score_band)
rp["gap_band"]   = rp["score_gap"].apply(gap_band)
rp = rp.merge(band_table[["score_band","gap_band","avg_payout","breakeven_payout","ev_per_bet"]],
              on=["score_band","gap_band"], how="left")
overall_avg = race_stats_f["_pay"].mean()
rp["avg_payout"]      = rp["avg_payout"].fillna(overall_avg)
rp["breakeven_payout"]= rp["breakeven_payout"].fillna(100 / OVERALL_HIT_RATE)
rp["ev_per_bet"]      = rp["ev_per_bet"].fillna(overall_avg * OVERALL_HIT_RATE)

# ========== フィルター & 時間帯絞り込み ==========
# proba>=0.35は的中率が上がるがオッズが低くなり回収率92%に落ちるため除外
MIN_EV = args.min_ev
filtered = rp[
    (rp["top_score"] >= 95) &
    (rp["score_gap"] >= 2) &
    (rp["n_players"] == 7) &
    (rp["ev_per_bet"] >= MIN_EV) &
    (rp["race_no"] >= RACE_RANGE[0]) &
    (rp["race_no"] <= RACE_RANGE[1])
].sort_values(["venue","race_no"])

print(f"フィルター通過: {len(filtered)}件")

# ========== Discord投稿 ==========
if filtered.empty:
    msg = (f"**{TARGET_DATE} 予想 ({HOUR_JST}時台)**\n"
           f"対象レースなし（race_no {RACE_RANGE[0]}〜{RACE_RANGE[1]}）")
    post_discord(msg)
    print("対象レースなし")
else:
    ev_note = f" / EV≥{MIN_EV:.0f}円" if MIN_EV > 0 else ""
    lines = [f"**:checkered_flag: {TARGET_DATE} 競輪予想 ({HOUR_JST}時台)**",
             f"フィルター: top_score≥95 & gap≥2 & 7車限定{ev_note}",
             f"対象: {len(filtered)}レース\n"]

    # レース単位でA/Bをまとめて表示
    for race_id, race_rows in filtered.groupby("race_id", sort=False):
        race_rows = race_rows.sort_values("formation")
        row = race_rows.iloc[0]
        ct = row.get("close_time", "")
        time_str   = f"  締切: {ct}" if ct and str(ct) != "nan" else ""
        avg_pay    = int(row["avg_payout"])       if pd.notna(row["avg_payout"])       else 0
        breakeven  = int(row["breakeven_payout"]) if pd.notna(row["breakeven_payout"]) else 0
        ev_per_bet = int(row["ev_per_bet"])        if pd.notna(row["ev_per_bet"])        else 0
        ev_sign = "+" if avg_pay >= breakeven else "-"

        combos = []
        for _, r in race_rows.iterrows():
            p2 = int(r["pred_2nd"]) if pd.notna(r["pred_2nd"]) else "?"
            p3 = int(r["pred_3rd"]) if pd.notna(r["pred_3rd"]) else "?"
            combos.append(f"`{int(r['pred_1st'])}-{p2}-{p3}`")
        combo_str = "  /  ".join(combos)
        n_combos  = len(race_rows)
        p2gap     = float(row["p2_gap"]) if "p2_gap" in row and pd.notna(row["p2_gap"]) else 0
        formation_label = f"{'接戦→2通り' if n_combos > 1 else '確信→1通り'} (p2gap:{p2gap:.3f})"

        lines.append(
            f":round_pushpin: **{row['venue']} {int(row['race_no'])}R**{time_str}\n"
            f"  予想: {combo_str}  [{formation_label}]\n"
            f"  score: {row['top_score']:.1f} / gap: {row['score_gap']:.1f}\n"
            f"  平均払戻: {avg_pay:,}円 / 損益分岐: {breakeven:,}円 / 期待値: {ev_sign}{ev_per_bet - 100:,}円"
        )

    msg = "\n".join(lines)
    print(msg)
    post_discord(msg)
