# -*- coding: utf-8 -*-
"""
1着・2着・3着それぞれのモデル単体的中率を検証
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

# 2024-2025 ウォークフォワード（月次）
months = pd.period_range('2024-01', '2025-12', freq='M')
LGB_PARAMS = dict(n_estimators=300, learning_rate=0.05, num_leaves=31,
                  min_child_samples=50, verbose=-1, random_state=42)

acc_rows = []
print('検証中...')
for month in months:
    start = str(month.start_time.date())
    end   = str(month.end_time.date())
    df_train = df_model[df_model['date'] < start]
    df_test  = df_model[(df_model['date'] >= start) & (df_model['date'] <= end)]
    if len(df_train) < 1000 or df_test.empty:
        continue

    m1 = lgb.LGBMClassifier(**LGB_PARAMS)
    m2 = lgb.LGBMClassifier(**LGB_PARAMS)
    m3 = lgb.LGBMClassifier(**LGB_PARAMS)
    m1.fit(df_train[FEATURES].values, df_train['is_winner'].values)
    m2.fit(df_train[FEATURES].values, df_train['is_2nd'].values)
    m3.fit(df_train[FEATURES].values, df_train['is_3rd'].values)

    df_test = df_test.copy()
    df_test['p1'] = m1.predict_proba(df_test[FEATURES].values)[:, 1]
    df_test['p2'] = m2.predict_proba(df_test[FEATURES].values)[:, 1]
    df_test['p3'] = m3.predict_proba(df_test[FEATURES].values)[:, 1]

    # レースごとに各モデルの予測トップ選手と実際の着順を比較
    res = []
    for race_id, g in df_test.groupby('race_id'):
        g = g.copy()
        score_gap = score_gap_all.get(race_id, np.nan)
        top_score = top_score_all.get(race_id, np.nan)
        n_players = n_players_all.get(race_id, np.nan)
        # フィルター条件
        if top_score < 95 or score_gap < 2 or n_players != 7:
            continue

        actual_1 = g[g['rank_num']==1]['banum'].values
        actual_2 = g[g['rank_num']==2]['banum'].values
        actual_3 = g[g['rank_num']==3]['banum'].values
        if len(actual_1)==0 or len(actual_2)==0 or len(actual_3)==0:
            continue

        pred_1 = int(g.sort_values('p1', ascending=False).iloc[0]['banum'])
        # 2着: 1着予測除いた中のp2最大
        rest2  = g[g['banum'] != pred_1]
        pred_2 = int(rest2.sort_values('p2', ascending=False).iloc[0]['banum']) if len(rest2)>=1 else None
        # 3着: 1-2着予測除いた中のp3最大
        rest3  = g[~g['banum'].isin([pred_1, pred_2])]
        pred_3 = int(rest3.sort_values('p3', ascending=False).iloc[0]['banum']) if len(rest3)>=1 else None

        res.append({
            'hit1': pred_1 == int(actual_1[0]),
            'hit2': pred_2 == int(actual_2[0]),
            'hit3': pred_3 == int(actual_3[0]),
            'hit_tan': (pred_1==int(actual_1[0])) and (pred_2==int(actual_2[0])) and (pred_3==int(actual_3[0])),
        })

    if not res:
        continue
    df_res = pd.DataFrame(res)
    acc_rows.append({
        'month': str(month),
        '件数': len(df_res),
        '1着的中率': df_res['hit1'].mean()*100,
        '2着的中率': df_res['hit2'].mean()*100,
        '3着的中率': df_res['hit3'].mean()*100,
        '三連単的中率': df_res['hit_tan'].mean()*100,
    })
    print(f"  {month}: {len(df_res)}件 | 1着{df_res['hit1'].mean():.1%} 2着{df_res['hit2'].mean():.1%} 3着{df_res['hit3'].mean():.1%} 三連単{df_res['hit_tan'].mean():.1%}")

df_acc = pd.DataFrame(acc_rows)
print()
print('=' * 65)
print('全期間平均（フィルター: top_score>=95, gap>=2, 7車）')
print('=' * 65)
total = df_acc['件数'].sum()
w1 = (df_acc['1着的中率'] * df_acc['件数']).sum() / total
w2 = (df_acc['2着的中率'] * df_acc['件数']).sum() / total
w3 = (df_acc['3着的中率'] * df_acc['件数']).sum() / total
wt = (df_acc['三連単的中率'] * df_acc['件数']).sum() / total
print(f'総件数: {total}')
print(f'1着的中率: {w1:.1f}%  （モデルが予測した1着が実際に1着）')
print(f'2着的中率: {w2:.1f}%  （1着除いた中で予測した2着が実際に2着）')
print(f'3着的中率: {w3:.1f}%  （1-2着除いた中で予測した3着が実際に3着）')
print(f'三連単的中率: {wt:.1f}%  （3つ全部一致）')
print()
print(f'理論値（独立と仮定）: {w1/100 * w2/100 * w3/100 * 100:.1f}%')
print()
print('注: 2着的中率は「1着予測選手を除いた残り6人中で2着を当てる確率」')
print('    ランダムなら 1/6 = 16.7%  3着は 1/5 = 20.0%')
