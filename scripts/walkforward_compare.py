# -*- coding: utf-8 -*-
"""
ウォークフォワード比較:
  旧: 基本特徴量 / 1通り予想
  新A: +agari/ライン/venue特徴量 / 1通り予想
  新B: +条件付き特徴量(honmei脚質) / フォーメーション2通り
比較軸: 的中率・回収率（フォーメーションは賭け金2倍で比較）
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
pref_count = df_all.groupby(["race_id","pref"])["banum"].transform("count")
term_count = df_all.groupby(["race_id","term"])["banum"].transform("count")
df_all["same_pref_count"] = (pref_count - 1).clip(lower=0)
df_all["same_term_count"] = (term_count - 1).clip(lower=0)
df_all["nige_in_race"] = df_all.groupby("race_id")["running_style"].transform(
    lambda x: (x == "逃").sum())
df_all_s = df_all.sort_values(["player_key","date","race_no"])
df_all["venue_win_rate"] = df_all_s.groupby(["player_key","venue_slug"])["is_win"].transform(
    lambda x: x.shift(1).rolling(10,min_periods=1).mean())

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

# 条件付き特徴量: ◎選手の脚質・得点をレース全選手に付与
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

F_BASE = ["race_score","class_num","style_num","gear",
          "score_rank","is_honmei","n_players_in_race",
          "prev1_rank","last3_avg_rank","last5_avg_rank",
          "last5_win_rate","rank_trend","days_since_last",
          "mark_num","win_rate_4m","top2_rate_4m","top3_rate_4m",
          "nige_4m","maku_4m"]
F_NEW  = F_BASE + ["last3_avg_agari","last5_avg_agari",
                   "same_pref_count","same_term_count",
                   "nige_in_race","venue_win_rate"]
F_COND = F_NEW + ["honmei_style_num","honmei_race_score"]  # 2着・3着モデル用

LGB_PARAMS = dict(n_estimators=1000, learning_rate=0.05,
                  num_leaves=31, min_child_samples=50,
                  verbose=-1, random_state=42)

def fit_es(params, X_tr, y_tr, X_val, y_val):
    m = lgb.LGBMClassifier(**params)
    m.fit(X_tr, y_tr,
          eval_set=[(X_val, y_val)],
          callbacks=[lgb.early_stopping(50, verbose=False),
                     lgb.log_evaluation(-1)])
    return m

def is_hit(pred_1, pred_2, pred_3, san_combo):
    combo = str(san_combo) if pd.notna(san_combo) else ""
    parts = re.split(r"[-=]", combo)
    if len(parts) < 3: return False
    try:
        return int(parts[0])==pred_1 and int(parts[1])==pred_2 and int(parts[2])==pred_3
    except: return False

def run_walkforward(f1st, f2nd3rd, use_formation, label):
    """
    f1st: 1着モデルの特徴量
    f2nd3rd: 2着・3着モデルの特徴量（条件付き特徴量を含む場合あり）
    use_formation: True=2着2通り, False=1通り
    """
    req = ["race_score","class_num","style_num","gear","score_rank","is_honmei",
           "n_players_in_race","prev1_rank","last3_avg_rank","last5_avg_rank",
           "last5_win_rate","rank_trend","days_since_last","rank_num"]
    df_model = df_all.dropna(subset=req).copy()

    test_months = [m for m in sorted(df_model["date"].str[:7].unique()) if m >= "2024-01"]
    records = []

    for test_month in test_months:
        train_data = df_model[df_model["date"].str[:7] < test_month]
        test_data  = df_model[df_model["date"].str[:7] == test_month]
        if len(train_data) < 5000 or len(test_data) == 0: continue

        val_months = sorted(train_data["date"].str[:7].unique())[-3:]
        val_mask = train_data["date"].str[:7].isin(val_months)
        df_tr, df_val = train_data[~val_mask], train_data[val_mask]

        m1 = fit_es(LGB_PARAMS, df_tr[f1st].values,    df_tr["is_winner"].values,
                                 df_val[f1st].values,   df_val["is_winner"].values)
        m2 = fit_es(LGB_PARAMS, df_tr[f2nd3rd].values, df_tr["is_2nd"].values,
                                 df_val[f2nd3rd].values, df_val["is_2nd"].values)
        m3 = fit_es(LGB_PARAMS, df_tr[f2nd3rd].values, df_tr["is_3rd"].values,
                                 df_val[f2nd3rd].values, df_val["is_3rd"].values)

        test_data = test_data.copy()
        test_data["win_proba"] = m1.predict_proba(test_data[f1st].values)[:, 1]
        test_data["p2_proba"]  = m2.predict_proba(test_data[f2nd3rd].values)[:, 1]
        test_data["p3_proba"]  = m3.predict_proba(test_data[f2nd3rd].values)[:, 1]

        rows = []
        for race_id, grp_r in test_data.groupby("race_id"):
            grp_r = grp_r.sort_values("win_proba", ascending=False)
            if len(grp_r) < 3: continue
            pred_1    = int(grp_r.iloc[0]["banum"])
            top_proba = float(grp_r.iloc[0]["win_proba"])
            san_combo  = grp_r["san_combo"].iloc[0]
            san_payout = grp_r["san_payout"].iloc[0]
            meta = {"race_id": race_id, "top_proba": top_proba,
                    "san_combo": san_combo, "san_payout": san_payout,
                    "score_gap": score_gap_raw.get(race_id, np.nan),
                    "top_score": top_score_raw.get(race_id, np.nan),
                    "n_players": n_players_raw.get(race_id, np.nan)}

            rest2 = grp_r[grp_r["banum"] != pred_1].sort_values("p2_proba", ascending=False)
            pred_2a = int(rest2.iloc[0]["banum"]) if len(rest2) >= 1 else None
            rest3a  = grp_r[~grp_r["banum"].isin([pred_1, pred_2a])].sort_values("p3_proba", ascending=False)
            pred_3a = int(rest3a.iloc[0]["banum"]) if len(rest3a) >= 1 else None
            rows.append({**meta, "pred_1st": pred_1, "pred_2nd": pred_2a, "pred_3rd": pred_3a})

            if use_formation and len(rest2) >= 2:
                pred_2b = int(rest2.iloc[1]["banum"])
                rest3b  = grp_r[~grp_r["banum"].isin([pred_1, pred_2b])].sort_values("p3_proba", ascending=False)
                pred_3b = int(rest3b.iloc[0]["banum"]) if len(rest3b) >= 1 else None
                rows.append({**meta, "pred_1st": pred_1, "pred_2nd": pred_2b, "pred_3rd": pred_3b})

        if not rows: continue
        rp = pd.DataFrame(rows)
        mask = (rp["top_score"] >= 95) & (rp["score_gap"] >= 2) & (rp["n_players"] == 7)
        sub  = rp[mask].copy()
        if sub.empty: continue

        sub["hit"] = sub.apply(
            lambda r: is_hit(r["pred_1st"], r["pred_2nd"], r["pred_3rd"], r["san_combo"]), axis=1)
        hits   = sub["hit"].sum()
        payout = sub[sub["hit"]]["san_payout"].sum()
        bet    = len(sub) * 100
        records.append({
            "month": test_month,
            "bets": len(sub), "hits": hits,
            "hit_rate": hits/len(sub)*100 if len(sub) else 0,
            "roi": payout/bet*100 if bet else 0,
        })
        print(f"  [{label}] {test_month} 完了  賭:{len(sub)} 的中:{hits} ({hits/len(sub)*100:.1f}%)")

    return pd.DataFrame(records)

print("\n=== ウォークフォワード比較 ===")
print("--- 旧: 基本特徴量 / 1通り ---")
df_old  = run_walkforward(F_BASE, F_BASE,  use_formation=False, label="旧")
print("--- 新A: +agari/ライン / 1通り ---")
df_newA = run_walkforward(F_NEW,  F_NEW,   use_formation=False, label="新A")
print("--- 新B: +条件付き / フォーメーション2通り ---")
df_newB = run_walkforward(F_NEW,  F_COND,  use_formation=True,  label="新B")

print("\n\n=== 結果サマリー ===")
for df, tag in [(df_old,"旧(1通り)"), (df_newA,"新A(1通り)"), (df_newB,"新B(2通り)")]:
    if df.empty: continue
    print(f"[{tag}]  月平均 的中率:{df['hit_rate'].mean():.1f}%  回収率:{df['roi'].mean():.1f}%"
          f"  | 総賭:{df['bets'].sum()}  総的中:{df['hits'].sum()}")

print("\n--- 月別詳細 (新B: 条件付き+フォーメーション) ---")
print(df_newB[["month","bets","hits","hit_rate","roi"]].to_string(index=False))
