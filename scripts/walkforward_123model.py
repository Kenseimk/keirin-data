# -*- coding: utf-8 -*-
"""
ウォークフォワード検証: 1着・2着・3着 専用モデル vs 従来モデル
- 従来: 1着確率上位3人を三連単として予測
- 改善: 1着モデル→2着モデル（1着予測除外）→3着モデル（1-2着予測除外）
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
import pandas as pd, re, glob, numpy as np, warnings
warnings.filterwarnings('ignore')
import lightgbm as lgb
from pathlib import Path

DATA_DIR = Path('keirin_data')
files = [f for f in sorted(glob.glob(str(DATA_DIR / '202[23456]_*.csv'))) if 'sample' not in f]
print(f'ファイル数: {len(files)}')
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

FEATURES = ['race_score','class_num','style_num','gear','score_rank','is_honmei','n_players_in_race',
            'prev1_rank','last3_avg_rank','last5_avg_rank','last5_win_rate','rank_trend','days_since_last']

def calc_gap(x):
    s = sorted(x.dropna(), reverse=True)
    return s[0]-s[1] if len(s)>=2 else np.nan

score_gap_all = df_all.groupby('race_id')['race_score'].apply(calc_gap)
top_score_all = df_all.groupby('race_id')['race_score'].max()
n_players_all = df_all.groupby('race_id')['banum'].nunique()
df_model = df_all.dropna(subset=FEATURES+['rank_num']).copy()

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
VAL_MONTHS = 3   # 訓練データの末尾N月を検証用に使う
ES_ROUNDS  = 50  # early stopping rounds

def fit_with_es(params, X_tr, y_tr, X_val, y_val):
    """early stopping付きで学習"""
    m = lgb.LGBMClassifier(**params)
    m.fit(
        X_tr, y_tr,
        eval_set=[(X_val, y_val)],
        callbacks=[lgb.early_stopping(ES_ROUNDS, verbose=False),
                   lgb.log_evaluation(-1)],
    )
    return m

months = pd.period_range('2024-01', '2025-12', freq='M')
results = []

print(f'ウォークフォワード開始（各月3モデル学習、val={VAL_MONTHS}ヶ月 early_stopping={ES_ROUNDS}）...')
for month in months:
    start = str(month.start_time.date())
    end   = str(month.end_time.date())

    df_before = df_model[df_model['date'] < start]
    df_test   = df_model[(df_model['date'] >= start) & (df_model['date'] <= end)]
    if len(df_before) < 2000 or df_test.empty:
        continue

    # 訓練データ末尾VAL_MONTHS月を検証セットに
    all_months = sorted(df_before['date'].str[:7].unique())
    val_months_list = all_months[-VAL_MONTHS:]
    val_mask  = df_before['date'].str[:7].isin(val_months_list)
    df_tr  = df_before[~val_mask]
    df_val = df_before[val_mask]
    if len(df_tr) < 1000 or df_val.empty:
        continue

    X_tr, X_val = df_tr[FEATURES].values, df_val[FEATURES].values

    # --- 従来モデル（1着確率のみ）---
    m1 = fit_with_es(LGB_PARAMS, X_tr, df_tr['is_winner'].values, X_val, df_val['is_winner'].values)

    # --- 2着専用モデル ---
    m2 = fit_with_es(LGB_PARAMS, X_tr, df_tr['is_2nd'].values,    X_val, df_val['is_2nd'].values)

    # --- 3着専用モデル ---
    m3 = fit_with_es(LGB_PARAMS, X_tr, df_tr['is_3rd'].values,    X_val, df_val['is_3rd'].values)

    # テストデータ予測
    df_test = df_test.copy()
    df_test['p1'] = m1.predict_proba(df_test[FEATURES].values)[:, 1]
    df_test['p2'] = m2.predict_proba(df_test[FEATURES].values)[:, 1]
    df_test['p3'] = m3.predict_proba(df_test[FEATURES].values)[:, 1]

    old_rows = []
    new_rows = []

    for race_id, grp_r in df_test.groupby('race_id'):
        grp_r = grp_r.sort_values('p1', ascending=False)
        if len(grp_r) < 3:
            continue

        # --- 従来予測 ---
        old_1 = int(grp_r.iloc[0]['banum'])
        old_2 = int(grp_r.iloc[1]['banum'])
        old_3 = int(grp_r.iloc[2]['banum'])
        top_proba = float(grp_r.iloc[0]['p1'])

        # --- 改善予測 ---
        new_1 = int(grp_r.iloc[0]['banum'])  # 1着は同じ
        rest2 = grp_r[grp_r['banum'] != new_1].sort_values('p2', ascending=False)
        new_2 = int(rest2.iloc[0]['banum']) if len(rest2) >= 1 else old_2
        rest3 = grp_r[~grp_r['banum'].isin([new_1, new_2])].sort_values('p3', ascending=False)
        new_3 = int(rest3.iloc[0]['banum']) if len(rest3) >= 1 else old_3

        old_rows.append({'race_id': race_id, 'p1': old_1, 'p2': old_2, 'p3': old_3, 'top_proba': top_proba})
        new_rows.append({'race_id': race_id, 'p1': new_1, 'p2': new_2, 'p3': new_3, 'top_proba': top_proba})

    df_old = pd.DataFrame(old_rows)
    df_new = pd.DataFrame(new_rows)

    for df_pred, label in [(df_old,'従来'), (df_new,'改善')]:
        df_pred = df_pred.copy()
        df_pred['score_gap'] = df_pred['race_id'].map(score_gap_all)
        df_pred['top_score'] = df_pred['race_id'].map(top_score_all)
        df_pred['n_players'] = df_pred['race_id'].map(n_players_all)
        df_pred = df_pred.merge(
            races_all[['race_id','w1','w2','w3','san_pay','fuku_pay']], on='race_id', how='left')
        df_pred = df_pred[df_pred['w1'].notna()]
        f = df_pred[(df_pred['top_score']>=95) & (df_pred['score_gap']>=2) & (df_pred['n_players']==7)]
        if f.empty:
            continue

        tan_hit  = ((f['p1']==f['w1']) & (f['p2']==f['w2']) & (f['p3']==f['w3'])).sum()
        fuku_hit = f.apply(lambda r: {r['p1'],r['p2'],r['p3']} == {r['w1'],r['w2'],r['w3']}, axis=1).sum()
        hit1     = (f['p1']==f['w1']).sum()
        tan_ret  = f[((f['p1']==f['w1']) & (f['p2']==f['w2']) & (f['p3']==f['w3']))]['san_pay'].sum()
        fuku_ret = f[f.apply(lambda r: {r['p1'],r['p2'],r['p3']} == {r['w1'],r['w2'],r['w3']}, axis=1)]['fuku_pay'].sum()
        n = len(f)

        results.append({
            'month': str(month),
            'model': label,
            '件数': n,
            '1着的中率': hit1/n*100,
            '三連単的中率': tan_hit/n*100,
            '三連複的中率': fuku_hit/n*100,
            '三連単回収率': tan_ret/n,
            '三連複回収率': fuku_ret/n,
        })

    print(f'  {month} 完了')

df_res = pd.DataFrame(results)
old_r = df_res[df_res['model']=='従来']
new_r = df_res[df_res['model']=='改善']

print()
print('=' * 80)
print('ウォークフォワード比較（フィルター: top_score>=95, gap>=2, 7車）')
print('=' * 80)

print()
print(f'{"月":<10} {"従来_1着":>8} {"従来_三単":>9} {"従来_三複":>9} {"改善_1着":>8} {"改善_三単":>9} {"改善_三複":>9}')
print('-'*70)
for month in sorted(df_res['month'].unique()):
    o = old_r[old_r['month']==month].iloc[0] if not old_r[old_r['month']==month].empty else None
    n = new_r[new_r['month']==month].iloc[0] if not new_r[new_r['month']==month].empty else None
    if o is None or n is None:
        continue
    mark = '*' if n['三連単的中率'] > o['三連単的中率'] else ' '
    print(f'{month:<10} {o["1着的中率"]:>7.1f}% {o["三連単的中率"]:>8.1f}% {o["三連複的中率"]:>8.1f}%'
          f'  {n["1着的中率"]:>7.1f}% {n["三連単的中率"]:>8.1f}% {n["三連複的中率"]:>8.1f}% {mark}')

print()
print('=== 全期間合計 ===')
for label, r in [('従来', old_r), ('改善', new_r)]:
    total = r['件数'].sum()
    tan_ret = sum(row['三連単回収率']*row['件数'] for _, row in r.iterrows())
    fu_ret  = sum(row['三連複回収率']*row['件数'] for _, row in r.iterrows())
    print(f'[{label}] 件数:{total}  '
          f'1着的中率:{r["1着的中率"].mean():.1f}%  '
          f'三連単的中率:{r["三連単的中率"].mean():.1f}%  '
          f'三連単回収率:{tan_ret/total:.1f}%  '
          f'三連複的中率:{r["三連複的中率"].mean():.1f}%  '
          f'三連複回収率:{fu_ret/total:.1f}%')

print()
print('=== 三連単的中率が改善した月 ===')
improved = 0
for month in sorted(df_res['month'].unique()):
    o = old_r[old_r['month']==month]
    n = new_r[new_r['month']==month]
    if o.empty or n.empty:
        continue
    diff = n.iloc[0]['三連単的中率'] - o.iloc[0]['三連単的中率']
    if diff > 0:
        improved += 1
        print(f'  {month}: {o.iloc[0]["三連単的中率"]:.1f}% → {n.iloc[0]["三連単的中率"]:.1f}% (+{diff:.1f}pt)')
total_months = len(sorted(df_res['month'].unique()))
print(f'  改善月: {improved}/{total_months}ヶ月')
