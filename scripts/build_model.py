"""
競輪 回収率100%超え戦略構築スクリプト
- 二車連単の期待値を予測し、プラス期待値の条件を特定する
"""

import pandas as pd
import numpy as np
import glob
import re
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

DATA_DIR = Path("keirin_data")
OUTPUT_FILE = DATA_DIR / "model_result.txt"

# ========== Step1: データ読み込み ==========

print("=== Step1: データ読み込み ===")
files = sorted(glob.glob(str(DATA_DIR / "202[345]_*.csv")))
print(f"  ファイル数: {len(files)}")

dfs = []
for f in files:
    try:
        df = pd.read_csv(f, encoding="utf-8-sig")
        dfs.append(df)
    except Exception as e:
        print(f"  スキップ {f}: {e}")

df_all = pd.concat(dfs, ignore_index=True)
print(f"  総行数: {len(df_all):,}")
print(f"  レース数: {df_all['race_id'].nunique():,}")

# ========== Step2: 払戻パース ==========

print("\n=== Step2: 払戻パース ===")

def parse_payout(s):
    """'1-5  1,220円(4)' → ('1-5', 1220)"""
    if pd.isna(s) or str(s).strip() == "":
        return None, None
    s = str(s).strip()
    m = re.search(r"(\d+[-=]\d+(?:[-=]\d+)?)\s+([\d,]+)円", s)
    if m:
        combo = m.group(1)
        payout = int(m.group(2).replace(",", ""))
        return combo, payout
    return None, None

df_all[["ni_combo", "ni_payout"]] = df_all["ni_sha_tan"].apply(
    lambda x: pd.Series(parse_payout(x))
)
df_all[["san_combo", "san_payout"]] = df_all["san_ren_tan"].apply(
    lambda x: pd.Series(parse_payout(x))
)

ni_ok = df_all["ni_payout"].notna().sum()
san_ok = df_all["san_payout"].notna().sum()
print(f"  二車連単パース成功: {ni_ok:,} / {len(df_all):,} ({ni_ok/len(df_all)*100:.1f}%)")
print(f"  三連勝単パース成功: {san_ok:,} / {len(df_all):,} ({san_ok/len(df_all)*100:.1f}%)")
print(f"  二車連単 払戻分布:")
print(df_all["ni_payout"].describe().apply(lambda x: f"{x:.0f}").to_string())

# ========== Step2.5: 選手別直近成績トレンド特徴量 ==========

print("\n=== Step2.5: 選手別直近成績トレンド特徴量 ===")

# 同姓同名対策: player_name + term（期）で選手を識別
df_all["player_key"] = df_all["player_name"].astype(str) + "_" + df_all["term"].astype(str)

# 同日複数レースの順序を保証するためrace_noでもソート
df_all["rank_num"] = pd.to_numeric(df_all["rank"], errors="coerce")
df_all = df_all.sort_values(["player_key", "date", "race_no"]).reset_index(drop=True)

# shift(1) でリーク防止（現在のレースより前のデータのみ使う）
grp = df_all.groupby("player_key")["rank_num"]

# 直近1〜3戦の着順
df_all["prev1_rank"] = grp.shift(1)
df_all["prev2_rank"] = grp.shift(2)
df_all["prev3_rank"] = grp.shift(3)

# 直近3戦平均着順（min_periods=1で出走数が少ない選手も対応）
df_all["last3_avg_rank"] = (
    grp.transform(lambda x: x.shift(1).rolling(3, min_periods=1).mean())
)

# 直近5戦平均着順
df_all["last5_avg_rank"] = (
    grp.transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
)

# 直近5戦勝率
df_all["is_win"] = (df_all["rank_num"] == 1).astype(float)
df_all["last5_win_rate"] = (
    df_all.groupby("player_key")["is_win"]
    .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
)

# 直近3戦トレンド（prev1 - prev3: マイナスほど改善中）
df_all["rank_trend"] = df_all["prev1_rank"] - df_all["prev3_rank"]

# 前走からの日数
df_all["prev_date"] = df_all.groupby("player_key")["date"].shift(1)
df_all["days_since_last"] = (
    pd.to_datetime(df_all["date"]) - pd.to_datetime(df_all["prev_date"])
).dt.days

trend_features = ["prev1_rank", "last3_avg_rank", "last5_avg_rank",
                  "last5_win_rate", "rank_trend", "days_since_last"]
valid = df_all[trend_features].notna().all(axis=1).sum()
print(f"  トレンド特徴量が揃っている行: {valid:,} / {len(df_all):,} ({valid/len(df_all)*100:.1f}%)")
print(f"  last3_avg_rank サンプル: {df_all['last3_avg_rank'].describe().round(2).to_dict()}")

# ========== Step3: レース単位データ作成 ==========

print("\n=== Step3: レース単位データ作成 ===")

# class → 数値
CLASS_MAP = {"S1": 4, "S2": 3, "A1": 2, "A2": 1, "B": 0}
STYLE_MAP = {"逃": 5, "捲": 4, "両": 3, "差": 2, "追": 1, "マ": 0}

df_all["class_num"] = df_all["player_class"].map(CLASS_MAP).fillna(1)
df_all["style_num"] = df_all["running_style"].map(STYLE_MAP).fillna(2)
df_all["race_score"] = pd.to_numeric(df_all["race_score"], errors="coerce")
df_all["gear"] = pd.to_numeric(df_all["gear"], errors="coerce")
df_all["rank"] = pd.to_numeric(df_all["rank"], errors="coerce")
df_all["agari"] = pd.to_numeric(df_all["agari"], errors="coerce")
df_all["banum"] = pd.to_numeric(df_all["banum"], errors="coerce")

# 本命車番をlineupから抽出
def extract_honmei(lineup):
    if pd.isna(lineup):
        return None
    m = re.search(r"◎(\d+)", str(lineup))
    return int(m.group(1)) if m else None

df_all["honmei_banum"] = df_all["lineup"].apply(extract_honmei)

# レース単位集計
def race_features(g):
    g = g.copy()
    g = g.sort_values("race_score", ascending=False)
    scores = g["race_score"].dropna().values
    n = len(g)
    top_score = scores[0] if len(scores) > 0 else np.nan
    score_gap = (scores[0] - scores[1]) if len(scores) > 1 else np.nan
    n_escape = (g["running_style"] == "逃").sum()
    n_s = (g["class_num"] >= 3).sum()
    top_class = g["class_num"].max()
    honmei_banum = g["honmei_banum"].iloc[0]
    honmei_row = g[g["banum"] == honmei_banum] if honmei_banum is not None else pd.DataFrame()
    honmei_score = honmei_row["race_score"].values[0] if len(honmei_row) > 0 else np.nan
    honmei_style = honmei_row["style_num"].values[0] if len(honmei_row) > 0 else np.nan
    winner_row = g[g["rank_num"] == 1]
    second_row = g[g["rank_num"] == 2]
    winner_banum = int(winner_row["banum"].values[0]) if len(winner_row) > 0 else None
    second_banum = int(second_row["banum"].values[0]) if len(second_row) > 0 else None
    winner_score = winner_row["race_score"].values[0] if len(winner_row) > 0 else np.nan
    winner_style = winner_row["style_num"].values[0] if len(winner_row) > 0 else np.nan
    third_row = g[g["rank_num"] == 3]
    third_banum = int(third_row["banum"].values[0]) if len(third_row) > 0 else None
    ni_combo = g["ni_combo"].iloc[0]
    ni_payout = g["ni_payout"].iloc[0]
    san_combo = g["san_combo"].iloc[0]
    san_payout = g["san_payout"].iloc[0]
    return pd.Series({
        "n_players": n,
        "top_score": top_score,
        "score_gap": score_gap,
        "n_escape": n_escape,
        "n_s_class": n_s,
        "top_class": top_class,
        "honmei_banum": honmei_banum,
        "honmei_score": honmei_score,
        "honmei_style_num": honmei_style,
        "winner_banum": winner_banum,
        "second_banum": second_banum,
        "third_banum": third_banum,
        "winner_score": winner_score,
        "winner_style_num": winner_style,
        "ni_combo": ni_combo,
        "ni_payout": ni_payout,
        "san_combo": san_combo,
        "san_payout": san_payout,
        "venue_slug": g["venue_slug"].iloc[0],
        "date": g["date"].iloc[0],
        "race_no": g["race_no"].iloc[0],
    })

print("  レース単位に集計中...")
race_df = df_all.groupby("race_id").apply(race_features).reset_index()
race_df = race_df.dropna(subset=["winner_banum", "ni_payout"])
print(f"  有効レース数: {len(race_df):,}")

# 本命が1着かどうか
race_df["honmei_win"] = (race_df["honmei_banum"] == race_df["winner_banum"]).astype(int)

# 払戻分布
print(f"\n  二車連単 払戻統計:")
print(f"    平均: {race_df['ni_payout'].mean():.0f}円")
print(f"    中央値: {race_df['ni_payout'].median():.0f}円")
print(f"    最小: {race_df['ni_payout'].min():.0f}円")
print(f"    最大: {race_df['ni_payout'].max():.0f}円")

# ========== Step4: 簡易分析（条件別回収率）==========

print("\n=== Step4: 条件別回収率分析 ===")

results = []

# 全レース賭けた場合のベースライン
baseline_return = race_df["ni_payout"].mean() / 100
print(f"  ベースライン（全レース購入）回収率: {baseline_return*100:.1f}%")

# ---- 分析1: 本命（◎）の強さ別 ----
print("\n  [分析1] 本命の競走得点別 本命1着率・平均配当")
race_df["score_bin"] = pd.cut(race_df["honmei_score"],
    bins=[0, 85, 90, 95, 100, 200], labels=["〜85", "85-90", "90-95", "95-100", "100+"])
g1 = race_df.groupby("score_bin", observed=True).agg(
    レース数=("honmei_win", "count"),
    本命1着率=("honmei_win", "mean"),
    平均配当=("ni_payout", "mean"),
).round(1)
g1["回収率(%)"] = (g1["本命1着率"] * g1["平均配当"] / 100 * 100).round(1)
print(g1.to_string())
results.append(("本命競走得点別", g1))

# ---- 分析2: score_gap（トップとの差）別 ----
print("\n  [分析2] 競走得点ギャップ別 本命1着率")
race_df["gap_bin"] = pd.cut(race_df["score_gap"],
    bins=[-1, 2, 5, 10, 20, 100], labels=["〜2", "2-5", "5-10", "10-20", "20+"])
g2 = race_df.groupby("gap_bin", observed=True).agg(
    レース数=("honmei_win", "count"),
    本命1着率=("honmei_win", "mean"),
    平均配当=("ni_payout", "mean"),
).round(1)
g2["回収率(%)"] = (g2["本命1着率"] * g2["平均配当"] / 100 * 100).round(1)
print(g2.to_string())
results.append(("得点ギャップ別", g2))

# ---- 分析3: 本命脚質別 ----
print("\n  [分析3] 本命脚質別")
style_inv = {v: k for k, v in STYLE_MAP.items()}
race_df["honmei_style"] = race_df["honmei_style_num"].map(style_inv)
g3 = race_df.groupby("honmei_style", observed=True).agg(
    レース数=("honmei_win", "count"),
    本命1着率=("honmei_win", "mean"),
    平均配当=("ni_payout", "mean"),
).round(1)
g3["回収率(%)"] = (g3["本命1着率"] * g3["平均配当"] / 100 * 100).round(1)
print(g3.to_string())
results.append(("本命脚質別", g3))

# ---- 分析4: 出走人数別 ----
print("\n  [分析4] 出走人数別")
g4 = race_df.groupby("n_players", observed=True).agg(
    レース数=("honmei_win", "count"),
    本命1着率=("honmei_win", "mean"),
    平均配当=("ni_payout", "mean"),
).round(1)
g4["回収率(%)"] = (g4["本命1着率"] * g4["平均配当"] / 100 * 100).round(1)
print(g4.to_string())
results.append(("出走人数別", g4))

# ========== Step5: LightGBMモデル（訓練:2024 / 検証:2025） ==========

print("\n=== Step5: LightGBMモデルで予測（訓練:2024年 / 検証:2025年） ===")

try:
    import lightgbm as lgb
    from sklearn.metrics import roc_auc_score

    # 選手単位データで特徴量作成
    df_model = df_all.copy()
    df_model = df_model.dropna(subset=["rank_num", "race_score", "ni_payout"])

    # レース内での順位特徴量
    df_model["score_rank"] = df_model.groupby("race_id")["race_score"].rank(
        ascending=False, method="min"
    )
    df_model["is_honmei"] = (df_model["banum"] == df_model["honmei_banum"]).astype(int)
    df_model["is_winner"] = (df_model["rank_num"] == 1).astype(int)

    features = ["race_score", "class_num", "style_num", "gear",
                "score_rank", "is_honmei", "n_players_in_race",
                "prev1_rank", "last3_avg_rank", "last5_avg_rank",
                "last5_win_rate", "rank_trend", "days_since_last"]

    # n_players_in_race を追加
    n_players = df_model.groupby("race_id")["banum"].count().rename("n_players_in_race")
    df_model = df_model.join(n_players, on="race_id")

    df_model = df_model.dropna(subset=features)
    df_model["year"] = pd.to_datetime(df_model["date"]).dt.year

    # 訓練: 2023〜2024年 / 検証: 2025年 に分割
    train_mask = df_model["year"].isin([2023, 2024])
    val_mask   = df_model["year"] == 2025

    df_train = df_model[train_mask]
    df_val   = df_model[val_mask]

    print(f"  訓練データ: {len(df_train):,}行 ({df_train['race_id'].nunique():,}レース)")
    print(f"  検証データ: {len(df_val):,}行 ({df_val['race_id'].nunique():,}レース)")

    X_tr, y_tr = df_train[features].values, df_train["is_winner"].values
    X_val, y_val = df_val[features].values, df_val["is_winner"].values

    model = lgb.LGBMClassifier(
        n_estimators=300, learning_rate=0.05,
        num_leaves=31, min_child_samples=50,
        verbose=-1, random_state=42
    )
    model.fit(X_tr, y_tr)

    val_proba = model.predict_proba(X_val)[:, 1]
    auc = roc_auc_score(y_val, val_proba)
    print(f"  検証AUC（2025年）: {auc:.4f}")

    # 特徴量重要度
    fi = pd.Series(model.feature_importances_, index=features).sort_values(ascending=False)
    print("\n  特徴量重要度:")
    print(fi.to_string())

    # 訓練データでの閾値探索（過学習防止のため訓練データのみで閾値を決める）
    print("\n  [閾値探索] 訓練データ(2024年)で最適閾値を探索...")
    tr_proba = model.predict_proba(X_tr)[:, 1]
    df_train = df_train.copy()
    df_train["win_proba"] = tr_proba

    # 訓練データでrace_df特徴量をマージして閾値候補を探索
    race_df_sub = race_df[["race_id", "top_score", "score_gap"]].copy()
    df_train_sorted = df_train.sort_values(["race_id", "win_proba"], ascending=[True, False])
    tr_race_preds = df_train_sorted.groupby("race_id").agg(
        pred_1st=("banum", lambda x: x.iloc[0]),
        pred_2nd=("banum", lambda x: x.iloc[1] if len(x) > 1 else None),
        pred_3rd=("banum", lambda x: x.iloc[2] if len(x) > 2 else None),
        top_proba=("win_proba", lambda x: x.iloc[0]),
        san_payout=("san_payout", "first"),
    ).reset_index()

    tr_winner_map = df_train[df_train["rank_num"] == 1].groupby("race_id")["banum"].first()
    tr_second_map = df_train[df_train["rank_num"] == 2].groupby("race_id")["banum"].first()
    tr_third_map  = df_train[df_train["rank_num"] == 3].groupby("race_id")["banum"].first()
    tr_race_preds["actual_winner"] = tr_race_preds["race_id"].map(tr_winner_map)
    tr_race_preds["actual_second"] = tr_race_preds["race_id"].map(tr_second_map)
    tr_race_preds["actual_third"]  = tr_race_preds["race_id"].map(tr_third_map)
    tr_race_preds = tr_race_preds.dropna(subset=["actual_winner", "san_payout"])
    tr_race_preds["hit"] = (
        (tr_race_preds["pred_1st"] == tr_race_preds["actual_winner"]) &
        (tr_race_preds["pred_2nd"] == tr_race_preds["actual_second"]) &
        (tr_race_preds["pred_3rd"] == tr_race_preds["actual_third"])
    ).astype(int)
    tr_race_preds = tr_race_preds.merge(race_df_sub, on="race_id", how="left")

    best_recovery = 0
    best_params = {}
    for thr in [0.20, 0.25, 0.30, 0.35, 0.40]:
        for score_min in [85, 90, 95, 100]:
            for gap_min in [0, 3, 5]:
                mask = (
                    (tr_race_preds["top_proba"] >= thr) &
                    (tr_race_preds["top_score"] >= score_min) &
                    (tr_race_preds["score_gap"] >= gap_min)
                )
                sub = tr_race_preds[mask]
                if len(sub) < 50:
                    continue
                recovery = sub[sub["hit"] == 1]["san_payout"].sum() / (len(sub) * 100) * 100
                if recovery > best_recovery:
                    best_recovery = recovery
                    best_params = {"top_proba": thr, "top_score": score_min, "gap": gap_min, "件数": len(sub)}

    print(f"  最良パラメータ(訓練データ): {best_params} → 回収率 {best_recovery:.1f}%")

    # 検証データに best_params を適用して評価
    df_val = df_val.copy()
    df_val["win_proba"] = val_proba
    df_val_sorted = df_val.sort_values(["race_id", "win_proba"], ascending=[True, False])
    val_race_preds = df_val_sorted.groupby("race_id").agg(
        pred_1st=("banum", lambda x: x.iloc[0]),
        pred_2nd=("banum", lambda x: x.iloc[1] if len(x) > 1 else None),
        pred_3rd=("banum", lambda x: x.iloc[2] if len(x) > 2 else None),
        top_proba=("win_proba", lambda x: x.iloc[0]),
        ni_payout=("ni_payout", "first"),
        san_payout=("san_payout", "first"),
    ).reset_index()

    val_winner_map = df_val[df_val["rank_num"] == 1].groupby("race_id")["banum"].first()
    val_second_map = df_val[df_val["rank_num"] == 2].groupby("race_id")["banum"].first()
    val_third_map  = df_val[df_val["rank_num"] == 3].groupby("race_id")["banum"].first()
    val_race_preds["actual_winner"] = val_race_preds["race_id"].map(val_winner_map)
    val_race_preds["actual_second"] = val_race_preds["race_id"].map(val_second_map)
    val_race_preds["actual_third"]  = val_race_preds["race_id"].map(val_third_map)
    val_race_preds = val_race_preds.merge(race_df_sub, on="race_id", how="left")

    val_race_preds_san = val_race_preds.dropna(subset=["actual_winner", "san_payout"])
    val_race_preds_san = val_race_preds_san.copy()
    val_race_preds_san["hit"] = (
        (val_race_preds_san["pred_1st"] == val_race_preds_san["actual_winner"]) &
        (val_race_preds_san["pred_2nd"] == val_race_preds_san["actual_second"]) &
        (val_race_preds_san["pred_3rd"] == val_race_preds_san["actual_third"])
    ).astype(int)

    df_model["win_proba"] = 0.0  # 後続コードのために初期化
    df_model.loc[val_mask, "win_proba"] = val_proba

    # ========== Step6: 検証データ(2025年)での評価 ==========
    print("\n=== Step6: 検証データ(2025年)での評価 ===")
    print("  ※ 閾値は訓練データ(2024年)で決定済み → 検証データに一度だけ適用")

    thresholds = [0.0, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4]

    # --- 二車連単（検証データ） ---
    val_race_preds_ni = val_race_preds.dropna(subset=["actual_winner", "ni_payout"]).copy()
    val_race_preds_ni["hit"] = (
        (val_race_preds_ni["pred_1st"] == val_race_preds_ni["actual_winner"]) &
        (val_race_preds_ni["pred_2nd"] == val_race_preds_ni["actual_second"])
    ).astype(int)

    ev_results_ni = []
    for thr in thresholds:
        sub = val_race_preds_ni[val_race_preds_ni["top_proba"] >= thr]
        if len(sub) == 0:
            continue
        bets = len(sub)
        hits = sub["hit"].sum()
        recovery = sub[sub["hit"] == 1]["ni_payout"].sum() / (bets * 100) * 100
        ev_results_ni.append({
            "top_proba閾値": thr, "賭け件数": bets, "的中数": int(hits),
            "的中率(%)": round(hits / bets * 100, 1), "回収率(%)": round(recovery, 1),
        })

    ev_ni_df = pd.DataFrame(ev_results_ni)
    print("\n  [検証] 閾値別 二車連単 回収率（2025年）:")
    print(ev_ni_df.to_string(index=False))
    results.append(("検証データ 二車連単 閾値別回収率", ev_ni_df))

    # --- 三連勝単（検証データ） ---
    ev_results_san = []
    for thr in thresholds:
        sub = val_race_preds_san[val_race_preds_san["top_proba"] >= thr]
        if len(sub) == 0:
            continue
        bets = len(sub)
        hits = sub["hit"].sum()
        recovery = sub[sub["hit"] == 1]["san_payout"].sum() / (bets * 100) * 100
        ev_results_san.append({
            "top_proba閾値": thr, "賭け件数": bets, "的中数": int(hits),
            "的中率(%)": round(hits / bets * 100, 1), "回収率(%)": round(recovery, 1),
        })

    ev_san_df = pd.DataFrame(ev_results_san)
    print("\n  [検証] 閾値別 三連勝単 回収率（2025年）:")
    print(ev_san_df.to_string(index=False))
    results.append(("検証データ 三連勝単 閾値別回収率", ev_san_df))

    best_san = ev_san_df[ev_san_df["回収率(%)"] == ev_san_df["回収率(%)"].max()].iloc[0]
    print(f"\n  最良戦略(三連勝単/検証): top_proba >= {best_san['top_proba閾値']} → 賭{best_san['賭け件数']:,.0f}件 的中{best_san['的中率(%)']:.1f}% 回収{best_san['回収率(%)']:.1f}%")

    # --- 訓練データで選んだ最良パラメータを検証データに適用 ---
    print(f"\n  [最終評価] 訓練データで選んだパラメータを検証データに適用:")
    print(f"  パラメータ: {best_params}")
    bp = best_params
    mask_val = (
        (val_race_preds_san["top_proba"] >= bp.get("top_proba", 0)) &
        (val_race_preds_san["top_score"] >= bp.get("top_score", 0)) &
        (val_race_preds_san["score_gap"] >= bp.get("gap", 0))
    )
    sub_final = val_race_preds_san[mask_val]
    if len(sub_final) > 0:
        bets = len(sub_final)
        hits = sub_final["hit"].sum()
        recovery = sub_final[sub_final["hit"] == 1]["san_payout"].sum() / (bets * 100) * 100
        print(f"  賭け件数: {bets:,}  的中率: {hits/bets*100:.1f}%  回収率: {recovery:.1f}%")
        results.append(("最終戦略 検証データ結果", pd.DataFrame([{
            "top_proba": bp.get("top_proba"), "top_score>=": bp.get("top_score"),
            "gap>=": bp.get("gap"), "賭け件数": bets,
            "的中率(%)": round(hits/bets*100, 1), "回収率(%)": round(recovery, 1),
        }])))
    else:
        print("  条件に合うレースなし（検証データが少ない可能性）")

    # ========== Step7: ウォークフォワード検証 ==========
    print("\n=== Step7: ウォークフォワード検証 ===")
    print("  訓練窓を1ヶ月ずつ拡大しながら翌月を予測")

    df_wf = df_model.copy()
    df_wf["month"] = pd.to_datetime(df_wf["date"]).dt.to_period("M").astype(str)
    months = sorted(df_wf["month"].unique())
    MIN_TRAIN_MONTHS = 12  # 最低12ヶ月の訓練データが必要

    wf_preds_list = []

    for i, val_month in enumerate(months[MIN_TRAIN_MONTHS:], start=MIN_TRAIN_MONTHS):
        train_months = months[:i]
        tr = df_wf[df_wf["month"].isin(train_months)]
        val = df_wf[df_wf["month"] == val_month]

        if len(tr) < 100 or len(val) < 10:
            continue

        X_tr = tr[features].values
        y_tr = tr["is_winner"].values
        X_val = val[features].values

        m = lgb.LGBMClassifier(
            n_estimators=300, learning_rate=0.05,
            num_leaves=31, min_child_samples=50,
            verbose=-1, random_state=42
        )
        m.fit(X_tr, y_tr)
        proba = m.predict_proba(X_val)[:, 1]

        val = val.copy()
        val["win_proba"] = proba
        wf_preds_list.append(val)

    df_wf_all = pd.concat(wf_preds_list, ignore_index=True)
    print(f"  ウォークフォワード対象: {len(months)-MIN_TRAIN_MONTHS}ヶ月 / {df_wf_all['race_id'].nunique():,}レース")

    # レース単位に集計
    df_wf_sorted = df_wf_all.sort_values(["race_id", "win_proba"], ascending=[True, False])
    wf_race_preds = df_wf_sorted.groupby("race_id").agg(
        pred_1st=("banum", lambda x: x.iloc[0]),
        pred_2nd=("banum", lambda x: x.iloc[1] if len(x) > 1 else None),
        pred_3rd=("banum", lambda x: x.iloc[2] if len(x) > 2 else None),
        top_proba=("win_proba", lambda x: x.iloc[0]),
        san_payout=("san_payout", "first"),
        ni_payout=("ni_payout", "first"),
        month=("month", "first"),
    ).reset_index()

    wf_winner = df_wf_all[df_wf_all["rank_num"] == 1].groupby("race_id")["banum"].first()
    wf_second = df_wf_all[df_wf_all["rank_num"] == 2].groupby("race_id")["banum"].first()
    wf_third  = df_wf_all[df_wf_all["rank_num"] == 3].groupby("race_id")["banum"].first()
    wf_race_preds["actual_winner"] = wf_race_preds["race_id"].map(wf_winner)
    wf_race_preds["actual_second"] = wf_race_preds["race_id"].map(wf_second)
    wf_race_preds["actual_third"]  = wf_race_preds["race_id"].map(wf_third)
    wf_race_preds = wf_race_preds.merge(
        race_df[["race_id", "top_score", "score_gap"]], on="race_id", how="left"
    )

    wf_san = wf_race_preds.dropna(subset=["actual_winner", "san_payout"]).copy()
    wf_san["hit"] = (
        (wf_san["pred_1st"] == wf_san["actual_winner"]) &
        (wf_san["pred_2nd"] == wf_san["actual_second"]) &
        (wf_san["pred_3rd"] == wf_san["actual_third"])
    ).astype(int)

    # 閾値別回収率（全ウォークフォワード期間）
    print("\n  [ウォークフォワード] 閾値別 三連勝単 回収率（全期間）:")
    wf_results = []
    for thr in [0.0, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40]:
        sub = wf_san[wf_san["top_proba"] >= thr]
        if len(sub) == 0:
            continue
        bets = len(sub)
        hits = sub["hit"].sum()
        recovery = sub[sub["hit"] == 1]["san_payout"].sum() / (bets * 100) * 100
        wf_results.append({
            "top_proba閾値": thr, "賭け件数": bets, "的中数": int(hits),
            "的中率(%)": round(hits / bets * 100, 1), "回収率(%)": round(recovery, 1),
        })
    wf_df = pd.DataFrame(wf_results)
    print(wf_df.to_string(index=False))
    results.append(("ウォークフォワード 三連勝単 閾値別回収率", wf_df))

    # 最良パラメータを全期間で適用した場合の月別回収率
    BEST_THR = 0.30
    BEST_SCORE = 95
    BEST_GAP = 3
    wf_filtered = wf_san[
        (wf_san["top_proba"] >= BEST_THR) &
        (wf_san["top_score"] >= BEST_SCORE) &
        (wf_san["score_gap"] >= BEST_GAP)
    ].copy()

    monthly = wf_filtered.groupby("month").apply(lambda g: pd.Series({
        "賭け件数": len(g),
        "的中数": int(g["hit"].sum()),
        "的中率(%)": round(g["hit"].mean() * 100, 1),
        "回収率(%)": round(
            g[g["hit"] == 1]["san_payout"].sum() / (len(g) * 100) * 100, 1
        ) if len(g) > 0 else 0,
    })).reset_index()

    print(f"\n  [ウォークフォワード] 月別回収率（top_proba>={BEST_THR}, top_score>={BEST_SCORE}, gap>={BEST_GAP}）:")
    print(monthly.to_string(index=False))

    profitable = (monthly["回収率(%)"] >= 100).sum()
    total_months = len(monthly[monthly["賭け件数"] > 0])
    overall_bets = wf_filtered["hit"].count()
    overall_recovery = (
        wf_filtered[wf_filtered["hit"] == 1]["san_payout"].sum()
        / (len(wf_filtered) * 100) * 100
    ) if len(wf_filtered) > 0 else 0
    print(f"\n  プラス月: {profitable}/{total_months}ヶ月")
    print(f"  全期間回収率: {overall_recovery:.1f}%  賭け件数: {len(wf_filtered):,}")
    results.append(("ウォークフォワード 月別回収率（条件フィルタあり）", monthly))

    # --- 全賭け（フィルタなし）の月別回収率 ---
    monthly_all = wf_san.groupby("month").apply(lambda g: pd.Series({
        "賭け件数": len(g),
        "的中数": int(g["hit"].sum()),
        "的中率(%)": round(g["hit"].mean() * 100, 1),
        "回収率(%)": round(
            g[g["hit"] == 1]["san_payout"].sum() / (len(g) * 100) * 100, 1
        ) if len(g) > 0 else 0,
    })).reset_index()

    print(f"\n  [ウォークフォワード] 月別回収率（全賭け・フィルタなし）:")
    print(monthly_all.to_string(index=False))

    profitable_all = (monthly_all["回収率(%)"] >= 100).sum()
    total_months_all = len(monthly_all[monthly_all["賭け件数"] > 0])
    overall_recovery_all = (
        wf_san[wf_san["hit"] == 1]["san_payout"].sum()
        / (len(wf_san) * 100) * 100
    )
    print(f"\n  プラス月: {profitable_all}/{total_months_all}ヶ月")
    print(f"  全期間回収率: {overall_recovery_all:.1f}%  賭け件数: {len(wf_san):,}")
    results.append(("ウォークフォワード 月別回収率（全賭け）", monthly_all))

    # ========== Step8: 赤字月パターン分析 ==========
    print("\n=== Step8: 赤字月パターン分析 ===")

    # 月ごとの特性を集計
    wf_all_races = wf_race_preds.copy()
    wf_all_races["month"] = wf_all_races["race_id"].map(
        df_wf_all.groupby("race_id")["month"].first()
    )
    monthly_chars = wf_all_races.groupby("month").agg(
        avg_top_proba=("top_proba", "mean"),
        avg_top_score=("top_score", "mean"),
        avg_score_gap=("score_gap", "mean"),
        avg_san_payout=("san_payout", "mean"),
        race_count=("race_id", "count"),
    ).round(2)

    # 三連勝単 全賭け回収率を結合
    monthly_chars = monthly_chars.join(
        monthly_all.set_index("month")[["回収率(%)", "的中率(%)"]]
    )
    monthly_chars["黒赤"] = monthly_chars["回収率(%)"].apply(
        lambda x: "plus" if x >= 100 else "minus"
    )
    monthly_chars = monthly_chars.sort_values("回収率(%)", ascending=False)

    print("\n  月別特性（回収率順）:")
    print(monthly_chars[["avg_top_proba","avg_top_score","avg_score_gap",
                          "avg_san_payout","的中率(%)","回収率(%)","黒赤"]].to_string())

    # プラス月 vs 赤字月の平均比較
    plus_m  = monthly_chars[monthly_chars["黒赤"] == "plus"]
    minus_m = monthly_chars[monthly_chars["黒赤"] == "minus"]
    print("\n  [プラス月 vs 赤字月 平均比較]")
    compare = pd.DataFrame({
        "プラス月": plus_m[["avg_top_proba","avg_top_score","avg_score_gap","avg_san_payout"]].mean(),
        "赤字月":   minus_m[["avg_top_proba","avg_top_score","avg_score_gap","avg_san_payout"]].mean(),
    }).round(3)
    print(compare.to_string())
    results.append(("赤字月パターン分析", monthly_chars.reset_index()))

    # ========== Step9: 三連複モデル ==========
    print("\n=== Step9: 三連複（順不問）モデル ===")
    print("  三連複: 1〜3着を順番不問で当てる券種（的中率は三連勝単の約6倍）")

    # san_comboから三連複コンボを生成（例: "1-4-7" → frozenset({1,4,7})）
    def parse_san_combo_set(s):
        if pd.isna(s): return None
        parts = re.split(r"[-=]", str(s).strip())
        if len(parts) == 3:
            try: return tuple(sorted(int(p) for p in parts))
            except: return None
        return None

    wf_san["actual_combo_set"] = wf_san.apply(
        lambda r: tuple(sorted([
            int(r["actual_winner"]), int(r["actual_second"]), int(r["actual_third"])
        ])) if pd.notna(r["actual_winner"]) and pd.notna(r["actual_second"])
             and pd.notna(r["actual_third"]) else None,
        axis=1
    )
    wf_san["pred_combo_set"] = wf_san.apply(
        lambda r: tuple(sorted([
            int(r["pred_1st"]), int(r["pred_2nd"]), int(r["pred_3rd"])
        ])) if pd.notna(r["pred_1st"]) and pd.notna(r["pred_2nd"])
             and pd.notna(r["pred_3rd"]) else None,
        axis=1
    )
    wf_san["hit_fuku"] = (
        wf_san["actual_combo_set"] == wf_san["pred_combo_set"]
    ).astype(int)

    # 三連複の払戻はデータにないため、三連勝単払戻 ÷ 約5 で近似
    # （実際の三連複配当は三連単の1/5程度が目安）
    wf_san["san_fuku_payout_est"] = wf_san["san_payout"] / 5.0

    print("\n  [三連複 ウォークフォワード全賭け] 月別回収率（三連勝単払戻÷5で近似）:")
    monthly_fuku = wf_san.groupby("month").apply(lambda g: pd.Series({
        "賭け件数": len(g),
        "的中数": int(g["hit_fuku"].sum()),
        "的中率(%)": round(g["hit_fuku"].mean() * 100, 1),
        "回収率(%)": round(
            g[g["hit_fuku"] == 1]["san_fuku_payout_est"].sum() / (len(g) * 100) * 100, 1
        ),
    })).reset_index()
    print(monthly_fuku.to_string(index=False))

    profitable_fuku = (monthly_fuku["回収率(%)"] >= 100).sum()
    total_fuku = len(monthly_fuku[monthly_fuku["賭け件数"] > 0])
    overall_fuku = (
        wf_san[wf_san["hit_fuku"] == 1]["san_fuku_payout_est"].sum()
        / (len(wf_san) * 100) * 100
    )
    print(f"\n  プラス月: {profitable_fuku}/{total_fuku}ヶ月")
    print(f"  全期間回収率（近似）: {overall_fuku:.1f}%")

    # 三連勝単 + 三連複 組み合わせ戦略
    print("\n  [組み合わせ戦略] 三連勝単 + 三連複 両方購入（各100円・計200円）:")
    wf_san["combo_return"] = (
        wf_san["hit"].astype(float) * wf_san["san_payout"] +
        wf_san["hit_fuku"].astype(float) * wf_san["san_fuku_payout_est"]
    )
    monthly_combo = wf_san.groupby("month").apply(lambda g: pd.Series({
        "賭け件数×2": len(g) * 2,
        "回収率(%)": round(g["combo_return"].sum() / (len(g) * 200) * 100, 1),
    })).reset_index()
    print(monthly_combo.to_string(index=False))
    profitable_combo = (monthly_combo["回収率(%)"] >= 100).sum()
    overall_combo = wf_san["combo_return"].sum() / (len(wf_san) * 200) * 100
    print(f"\n  プラス月: {profitable_combo}/{total_fuku}ヶ月")
    print(f"  全期間回収率: {overall_combo:.1f}%")
    results.append(("三連複 月別回収率（近似）", monthly_fuku))
    results.append(("組み合わせ戦略 月別回収率", monthly_combo))

except ImportError:
    print("  LightGBM未インストール。pip install lightgbm で追加してください。")
    fi = None

# ========== Step8: 結果保存 ==========

print("\n=== 結果保存 ===")
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write("=" * 60 + "\n")
    f.write("競輪 回収率分析レポート\n")
    f.write("=" * 60 + "\n\n")
    f.write(f"対象データ: 2024〜2025年（実行時点で収集済み）{len(race_df):,}レース\n")
    f.write(f"ベースライン回収率（全賭け）: {baseline_return*100:.1f}%\n\n")
    for name, tbl in results:
        f.write(f"--- {name} ---\n")
        f.write(tbl.to_string())
        f.write("\n\n")

print(f"  保存: {OUTPUT_FILE}")
print("\n=== 完了 ===")
