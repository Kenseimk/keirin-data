# -*- coding: utf-8 -*-
"""
特徴量選択 + ホールドアウト評価

手順:
  Step1: 2022-2023 のwalk-forwardで特徴量重要度を算出 → 特徴量選択
  Step2: 選択した特徴量で 2024-2025 を完全ホールドアウト評価
         (2024-2025のデータはStep1で一切使わない)
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
import pandas as pd, re, glob, numpy as np, warnings
warnings.filterwarnings('ignore')
import lightgbm as lgb
from pathlib import Path

DATA_DIR = Path('keirin_data')
files = [f for f in sorted(glob.glob(str(DATA_DIR / '202[23456]_*.csv'))) if 'sample' not in f]
df_all = pd.concat([pd.read_csv(f, encoding='utf-8-sig') for f in files], ignore_index=True)
print(f'総行数: {len(df_all):,}')

CLASS_MAP = {'S1':4,'S2':3,'A1':2,'A2':1,'B':0}
STYLE_MAP  = {'逃':5,'捲':4,'両':3,'差':2,'追':1,'マ':0}
df_all['player_key']  = df_all['player_name'].astype(str)+'_'+df_all['term'].astype(str)
df_all['rank_num']    = pd.to_numeric(df_all['rank'], errors='coerce')
df_all['class_num']   = df_all['player_class'].map(CLASS_MAP).fillna(1)
df_all['style_num']   = df_all['running_style'].map(STYLE_MAP).fillna(2)
df_all['race_score']  = pd.to_numeric(df_all['race_score'], errors='coerce')
df_all['gear']        = pd.to_numeric(df_all['gear'], errors='coerce')
df_all['banum']       = pd.to_numeric(df_all['banum'], errors='coerce')
df_all['is_win']      = (df_all['rank_num']==1).astype(float)
df_all['is_winner']   = (df_all['rank_num']==1).astype(int)
df_all['is_2nd']      = (df_all['rank_num']==2).astype(int)
df_all['is_3rd']      = (df_all['rank_num']==3).astype(int)
for col in ['mark_num','win_rate_4m','top2_rate_4m','top3_rate_4m','nige_4m','maku_4m']:
    df_all[col] = pd.to_numeric(df_all.get(col), errors='coerce')

df_all = df_all.sort_values(['player_key','date','race_no']).reset_index(drop=True)
grp = df_all.groupby('player_key')['rank_num']
df_all['prev1_rank']      = grp.shift(1)
df_all['last3_avg_rank']  = grp.transform(lambda x: x.shift(1).rolling(3,min_periods=1).mean())
df_all['last5_avg_rank']  = grp.transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())
df_all['last5_win_rate']  = df_all.groupby('player_key')['is_win'].transform(
    lambda x: x.shift(1).rolling(5,min_periods=1).mean())
df_all['rank_trend']      = df_all['prev1_rank'] - grp.shift(3)
df_all['days_since_last'] = (
    pd.to_datetime(df_all['date']) -
    pd.to_datetime(df_all.groupby('player_key')['date'].shift(1))
).dt.days
def extract_honmei(x):
    if pd.isna(x): return None
    m = re.search(r'◎(\d+)', str(x))
    return int(m.group(1)) if m else None
df_all['honmei_banum'] = df_all['lineup'].apply(extract_honmei)
df_all['score_rank']   = df_all.groupby('race_id')['race_score'].rank(ascending=False, method='min')
df_all['is_honmei']    = (df_all['banum']==df_all['honmei_banum']).astype(int)
n_pl = df_all.groupby('race_id')['banum'].count().rename('n_players_in_race')
df_all = df_all.join(n_pl, on='race_id')

# 全候補特徴量
ALL_FEATURES = [
    'race_score','class_num','style_num','gear','score_rank','is_honmei',
    'n_players_in_race','prev1_rank','last3_avg_rank','last5_avg_rank',
    'last5_win_rate','rank_trend','days_since_last',
    'mark_num','win_rate_4m','top2_rate_4m','top3_rate_4m','nige_4m','maku_4m',
]

def calc_gap(x):
    s = sorted(x.dropna(), reverse=True)
    return s[0]-s[1] if len(s)>=2 else np.nan

score_gap_all = df_all.groupby('race_id')['race_score'].apply(calc_gap)
top_score_all = df_all.groupby('race_id')['race_score'].max()
n_players_all = df_all.groupby('race_id')['banum'].nunique()

df_model = df_all.dropna(subset=['race_score','class_num','style_num','gear','score_rank',
                                  'is_honmei','n_players_in_race','prev1_rank',
                                  'last3_avg_rank','last5_avg_rank','last5_win_rate',
                                  'rank_trend','days_since_last','rank_num']).copy()

def get_pay(s):
    m = re.search(r'([\d,]+)円', str(s))
    return int(m.group(1).replace(',','')) if m else 0

races_all = df_all.drop_duplicates('race_id')[['race_id','date','san_ren_tan','san_ren_fuku']].copy()
df1r = df_all[df_all['rank_num']==1][['race_id','banum']].rename(columns={'banum':'w1'})
df2r = df_all[df_all['rank_num']==2][['race_id','banum']].rename(columns={'banum':'w2'})
df3r = df_all[df_all['rank_num']==3][['race_id','banum']].rename(columns={'banum':'w3'})
races_all = (races_all
    .merge(df1r,on='race_id',how='left')
    .merge(df2r,on='race_id',how='left')
    .merge(df3r,on='race_id',how='left'))
races_all['san_pay']  = races_all['san_ren_tan'].apply(get_pay)
races_all['fuku_pay'] = races_all['san_ren_fuku'].apply(get_pay)

LGB_PARAMS = dict(n_estimators=1000, learning_rate=0.05, num_leaves=31,
                  min_child_samples=50, verbose=-1, random_state=42)
VAL_MONTHS = 3
ES_ROUNDS  = 50

def fit_es(X_tr, y_tr, X_val, y_val):
    m = lgb.LGBMClassifier(**LGB_PARAMS)
    m.fit(X_tr, y_tr,
          eval_set=[(X_val, y_val)],
          callbacks=[lgb.early_stopping(ES_ROUNDS, verbose=False),
                     lgb.log_evaluation(-1)])
    return m

def run_walkforward(months_range, features, label=''):
    """指定月範囲でwalk-forwardを実行し結果と特徴量重要度を返す"""
    results = []
    importances = {f: [] for f in features}

    for month in months_range:
        start = str(month.start_time.date())
        end   = str(month.end_time.date())

        df_before = df_model[df_model['date'] < start]
        df_test   = df_model[(df_model['date'] >= start) & (df_model['date'] <= end)]
        if len(df_before) < 2000 or df_test.empty:
            continue

        all_months = sorted(df_before['date'].str[:7].unique())
        val_months_list = all_months[-VAL_MONTHS:]
        val_mask = df_before['date'].str[:7].isin(val_months_list)
        df_tr  = df_before[~val_mask]
        df_val = df_before[val_mask]
        if len(df_tr) < 1000 or df_val.empty:
            continue

        X_tr  = df_tr[features].values
        X_val = df_val[features].values
        m1 = fit_es(X_tr, df_tr['is_winner'].values, X_val, df_val['is_winner'].values)
        m2 = fit_es(X_tr, df_tr['is_2nd'].values,    X_val, df_val['is_2nd'].values)
        m3 = fit_es(X_tr, df_tr['is_3rd'].values,    X_val, df_val['is_3rd'].values)

        # 特徴量重要度を累積
        for fi, fn in zip(m1.feature_importances_, features):
            importances[fn].append(fi)

        df_test = df_test.copy()
        df_test['p1'] = m1.predict_proba(df_test[features].values)[:, 1]
        df_test['p2'] = m2.predict_proba(df_test[features].values)[:, 1]
        df_test['p3'] = m3.predict_proba(df_test[features].values)[:, 1]

        rows = []
        for race_id, g in df_test.groupby('race_id'):
            g = g.sort_values('p1', ascending=False)
            if len(g) < 3: continue
            p1 = int(g.iloc[0]['banum'])
            rest2 = g[g['banum']!=p1].sort_values('p2', ascending=False)
            p2 = int(rest2.iloc[0]['banum']) if len(rest2)>=1 else None
            rest3 = g[~g['banum'].isin([p1,p2])].sort_values('p3', ascending=False)
            p3 = int(rest3.iloc[0]['banum']) if len(rest3)>=1 else None
            rows.append({'race_id': race_id, 'p1':p1, 'p2':p2, 'p3':p3})

        df_pred = pd.DataFrame(rows)
        df_pred['score_gap'] = df_pred['race_id'].map(score_gap_all)
        df_pred['top_score'] = df_pred['race_id'].map(top_score_all)
        df_pred['n_players'] = df_pred['race_id'].map(n_players_all)
        df_pred = df_pred.merge(
            races_all[['race_id','w1','w2','w3','san_pay','fuku_pay']], on='race_id', how='left')
        df_pred = df_pred[df_pred['w1'].notna()]
        f = df_pred[(df_pred['top_score']>=95) & (df_pred['score_gap']>=2) & (df_pred['n_players']==7)]
        if f.empty: continue

        tan_hit  = ((f['p1']==f['w1']) & (f['p2']==f['w2']) & (f['p3']==f['w3'])).sum()
        fuku_hit = f.apply(lambda r: {r['p1'],r['p2'],r['p3']} == {r['w1'],r['w2'],r['w3']}, axis=1).sum()
        hit1     = (f['p1']==f['w1']).sum()
        tan_ret  = f[((f['p1']==f['w1']) & (f['p2']==f['w2']) & (f['p3']==f['w3']))]['san_pay'].sum()
        n = len(f)
        results.append({'month': str(month), '件数':n, '1着':hit1/n*100,
                        '三単':tan_hit/n*100, '三複':fuku_hit/n*100, '三単回収':tan_ret/n})
        print(f'  {label} {month} 完了 ({n}件 三単{tan_hit/n*100:.1f}%)')

    return results, importances

# ============================================================
# Step 1: 2022-2023 で特徴量選択（2024-2025は一切見ない）
# ============================================================
print('\n' + '='*60)
print('Step1: 2022-2023 walk-forward で特徴量重要度を算出')
print('  テスト月: 2023-01 〜 2023-12')
print('='*60)

select_months = pd.period_range('2023-01', '2023-12', freq='M')
_, importances = run_walkforward(select_months, ALL_FEATURES, label='[選択期]')

# 特徴量重要度の平均を集計
imp_mean = {f: np.mean(v) if v else 0 for f, v in importances.items()}
imp_df = pd.DataFrame({'feature': list(imp_mean.keys()), 'importance': list(imp_mean.values())})
imp_df = imp_df.sort_values('importance', ascending=False)

print('\n--- 特徴量重要度（2023年walk-forward平均）---')
for _, row in imp_df.iterrows():
    bar = '█' * int(row['importance'] / imp_df['importance'].max() * 20)
    print(f'  {row["feature"]:>20}: {row["importance"]:>6.0f}  {bar}')

# 重要度 > 全体平均 の特徴量を選択
threshold = imp_df['importance'].mean()
selected = imp_df[imp_df['importance'] > threshold]['feature'].tolist()
print(f'\n閾値（平均重要度）: {threshold:.0f}')
print(f'選択された特徴量 ({len(selected)}/{len(ALL_FEATURES)}個):')
for f in selected:
    print(f'  - {f}')
excluded = [f for f in ALL_FEATURES if f not in selected]
print(f'除外された特徴量:')
for f in excluded:
    print(f'  - {f}  (重要度 {imp_mean[f]:.0f})')

# ============================================================
# Step 2: 2024-2025 完全ホールドアウト評価
#         (選択した特徴量を使い、2024-2025は初めて見る)
# ============================================================
print('\n' + '='*60)
print('Step2: 2024-2025 完全ホールドアウト評価')
print('  (2024-2025のデータはStep1で一切使っていない)')
print('='*60)

holdout_months = pd.period_range('2024-01', '2025-12', freq='M')

# 全特徴量
print('\n[全特徴量]')
results_all, _ = run_walkforward(holdout_months, ALL_FEATURES, label='[全特徴]')

# 選択特徴量
print('\n[選択特徴量]')
results_sel, _ = run_walkforward(holdout_months, selected, label='[選択]')

# 集計
def summarize(results, label):
    df = pd.DataFrame(results)
    if df.empty:
        print(f'[{label}] データなし')
        return
    n = df['件数'].sum()
    tan_ret = sum(r['三単回収']*r['件数'] for r in results)
    print(f'[{label}] 件数:{n}  1着:{df["1着"].mean():.1f}%  '
          f'三連単:{df["三単"].mean():.1f}%  '
          f'三連単回収率:{tan_ret/n:.1f}%  '
          f'三連複:{df["三複"].mean():.1f}%')

print('\n' + '='*60)
print('=== 最終結果（2024-2025 ホールドアウト）===')
print('='*60)
summarize(results_all, f'全{len(ALL_FEATURES)}特徴量')
summarize(results_sel, f'選択{len(selected)}特徴量')
print('\n※ これらの数字は特徴量選択に使っていないデータでの評価です')
