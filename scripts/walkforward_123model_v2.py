# -*- coding: utf-8 -*-
"""
3着予測改善: 展開系特徴量を追加して3着モデルの精度を検証
追加特徴量:
  - last5_top3_rate   : 直近5戦の3着以内率
  - race_nige_count   : レース内の逃げ選手数
  - race_sashi_count  : レース内の差し・追い込み選手数
  - honmei_style_num  : 本命選手の脚質
  - score_vs_2nd_diff : 自分のスコアと2位スコアの差（負=格下）
  - is_chaser         : 差し・追い込み脚質か（逃げ展開で恩恵）
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
df_all['is_top3']     = (df_all['rank_num']<=3).astype(float)

df_all = df_all.sort_values(['player_key','date','race_no']).reset_index(drop=True)
grp = df_all.groupby('player_key')['rank_num']
df_all['prev1_rank']      = grp.shift(1)
df_all['last3_avg_rank']  = grp.transform(lambda x: x.shift(1).rolling(3,min_periods=1).mean())
df_all['last5_avg_rank']  = grp.transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())
df_all['last5_win_rate']  = df_all.groupby('player_key')['is_win'].transform(
    lambda x: x.shift(1).rolling(5,min_periods=1).mean())
# 追加: 直近5戦の3着以内率
df_all['last5_top3_rate'] = df_all.groupby('player_key')['is_top3'].transform(
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

# 追加: レース内の脚質分布
race_style = df_all.groupby('race_id')['style_num'].agg(
    race_nige_count  =lambda x: (x==5).sum(),   # 逃げ
    race_maku_count  =lambda x: (x==4).sum(),   # 捲り
    race_sashi_count =lambda x: (x<=2).sum(),   # 差し・追い込み
)
df_all = df_all.join(race_style, on='race_id')

# 追加: 本命選手の脚質
honmei_style = (df_all[df_all['is_honmei']==1]
    .groupby('race_id')['style_num'].first().rename('honmei_style_num'))
df_all = df_all.join(honmei_style, on='race_id')
df_all['honmei_style_num'] = df_all['honmei_style_num'].fillna(3)

# 追加: 自スコアと2位スコアの差（負=格下）
score2nd = df_all.groupby('race_id')['race_score'].apply(
    lambda x: sorted(x.dropna(), reverse=True)[1] if len(x.dropna())>=2 else np.nan
).rename('score_2nd_in_race')
df_all = df_all.join(score2nd, on='race_id')
df_all['score_vs_2nd'] = df_all['race_score'] - df_all['score_2nd_in_race']

# 追加: 差し・追い込み脚質フラグ
df_all['is_chaser'] = (df_all['style_num'] <= 2).astype(int)

FEATURES_BASE = ['race_score','class_num','style_num','gear','score_rank','is_honmei',
                 'n_players_in_race','prev1_rank','last3_avg_rank','last5_avg_rank',
                 'last5_win_rate','rank_trend','days_since_last']

FEATURES_NEW = FEATURES_BASE + [
    'last5_top3_rate',
    'race_nige_count', 'race_maku_count', 'race_sashi_count',
    'honmei_style_num', 'score_vs_2nd', 'is_chaser',
]

def calc_gap(x):
    s = sorted(x.dropna(), reverse=True)
    return s[0]-s[1] if len(s)>=2 else np.nan

score_gap_all = df_all.groupby('race_id')['race_score'].apply(calc_gap)
top_score_all = df_all.groupby('race_id')['race_score'].max()
n_players_all = df_all.groupby('race_id')['banum'].nunique()
df_model = df_all.dropna(subset=FEATURES_NEW+['rank_num']).copy()

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

months = pd.period_range('2024-01', '2025-12', freq='M')
LGB_PARAMS = dict(n_estimators=300, learning_rate=0.05, num_leaves=31,
                  min_child_samples=50, verbose=-1, random_state=42)

results = []
print('ウォークフォワード開始...')
for month in months:
    start = str(month.start_time.date())
    end   = str(month.end_time.date())
    df_train = df_model[df_model['date'] < start]
    df_test  = df_model[(df_model['date'] >= start) & (df_model['date'] <= end)]
    if len(df_train) < 1000 or df_test.empty:
        continue

    # 旧モデル（ベースライン特徴量）
    m1_old = lgb.LGBMClassifier(**LGB_PARAMS)
    m2_old = lgb.LGBMClassifier(**LGB_PARAMS)
    m3_old = lgb.LGBMClassifier(**LGB_PARAMS)
    m1_old.fit(df_train[FEATURES_BASE].values, df_train['is_winner'].values)
    m2_old.fit(df_train[FEATURES_BASE].values, df_train['is_2nd'].values)
    m3_old.fit(df_train[FEATURES_BASE].values, df_train['is_3rd'].values)

    # 新モデル（展開系特徴量追加）
    m1_new = lgb.LGBMClassifier(**LGB_PARAMS)
    m2_new = lgb.LGBMClassifier(**LGB_PARAMS)
    m3_new = lgb.LGBMClassifier(**LGB_PARAMS)
    m1_new.fit(df_train[FEATURES_NEW].values, df_train['is_winner'].values)
    m2_new.fit(df_train[FEATURES_NEW].values, df_train['is_2nd'].values)
    m3_new.fit(df_train[FEATURES_NEW].values, df_train['is_3rd'].values)

    df_test = df_test.copy()
    for label, m1, m2, m3, FEATS in [
        ('旧', m1_old, m2_old, m3_old, FEATURES_BASE),
        ('新', m1_new, m2_new, m3_new, FEATURES_NEW),
    ]:
        df_test[f'p1_{label}'] = m1.predict_proba(df_test[FEATS].values)[:, 1]
        df_test[f'p2_{label}'] = m2.predict_proba(df_test[FEATS].values)[:, 1]
        df_test[f'p3_{label}'] = m3.predict_proba(df_test[FEATS].values)[:, 1]

    hit1_old=hit2_old=hit3_old=hit_tan_old=0
    hit1_new=hit2_new=hit3_new=hit_tan_new=0
    n_races = 0

    for race_id, g in df_test.groupby('race_id'):
        sg  = score_gap_all.get(race_id, np.nan)
        ts  = top_score_all.get(race_id, np.nan)
        np_ = n_players_all.get(race_id, np.nan)
        if ts < 95 or sg < 2 or np_ != 7:
            continue
        a1 = g[g['rank_num']==1]['banum'].values
        a2 = g[g['rank_num']==2]['banum'].values
        a3 = g[g['rank_num']==3]['banum'].values
        if not (len(a1) and len(a2) and len(a3)):
            continue
        n_races += 1
        a1,a2,a3 = int(a1[0]),int(a2[0]),int(a3[0])

        for label in ['旧','新']:
            p1 = int(g.sort_values(f'p1_{label}', ascending=False).iloc[0]['banum'])
            r2 = g[g['banum']!=p1]
            p2 = int(r2.sort_values(f'p2_{label}', ascending=False).iloc[0]['banum']) if len(r2)>=1 else None
            r3 = g[~g['banum'].isin([p1,p2])]
            p3 = int(r3.sort_values(f'p3_{label}', ascending=False).iloc[0]['banum']) if len(r3)>=1 else None
            h1 = (p1==a1); h2 = (p2==a2); h3 = (p3==a3)
            ht = h1 and h2 and h3
            if label == '旧':
                hit1_old+=h1; hit2_old+=h2; hit3_old+=h3; hit_tan_old+=ht
            else:
                hit1_new+=h1; hit2_new+=h2; hit3_new+=h3; hit_tan_new+=ht

    if n_races == 0:
        continue

    results.append({
        'month': str(month), '件数': n_races,
        '旧_1着': hit1_old/n_races*100, '旧_2着': hit2_old/n_races*100,
        '旧_3着': hit3_old/n_races*100, '旧_三連単': hit_tan_old/n_races*100,
        '新_1着': hit1_new/n_races*100, '新_2着': hit2_new/n_races*100,
        '新_3着': hit3_new/n_races*100, '新_三連単': hit_tan_new/n_races*100,
    })
    print(f"  {month}: 旧[1着{hit1_old/n_races:.1%} 2着{hit2_old/n_races:.1%} 3着{hit3_old/n_races:.1%} 三単{hit_tan_old/n_races:.1%}]"
          f"  新[1着{hit1_new/n_races:.1%} 2着{hit2_new/n_races:.1%} 3着{hit3_new/n_races:.1%} 三単{hit_tan_new/n_races:.1%}]")

df_res = pd.DataFrame(results)
total = df_res['件数'].sum()

print()
print('=' * 75)
print('全期間合計（フィルター: top_score>=95, gap>=2, 7車）')
print('=' * 75)
for label in ['旧','新']:
    w1 = (df_res[f'{label}_1着']*df_res['件数']).sum()/total
    w2 = (df_res[f'{label}_2着']*df_res['件数']).sum()/total
    w3 = (df_res[f'{label}_3着']*df_res['件数']).sum()/total
    wt = (df_res[f'{label}_三連単']*df_res['件数']).sum()/total
    tag = '(ベースライン)' if label=='旧' else '(展開特徴量追加)'
    print(f'[{label} {tag}]  1着:{w1:.1f}%  2着:{w2:.1f}%  3着:{w3:.1f}%  三連単:{wt:.1f}%')

print()
print(f'{"月":<10} {"旧_3着":>7} {"新_3着":>7} {"差":>6}  {"旧_三単":>8} {"新_三単":>8} {"差":>6}')
print('-'*58)
for _, row in df_res.iterrows():
    d3 = row['新_3着'] - row['旧_3着']
    dt = row['新_三連単'] - row['旧_三連単']
    mark = '*' if d3 > 0 else ' '
    print(f'{row["month"]:<10} {row["旧_3着"]:>6.1f}% {row["新_3着"]:>6.1f}% {d3:>+5.1f}pt'
          f'  {row["旧_三連単"]:>7.1f}% {row["新_三連単"]:>7.1f}% {dt:>+5.1f}pt {mark}')

improved_3 = (df_res['新_3着'] > df_res['旧_3着']).sum()
improved_t = (df_res['新_三連単'] > df_res['旧_三連単']).sum()
print(f'\n3着的中率が改善した月: {improved_3}/{len(df_res)}ヶ月')
print(f'三連単的中率が改善した月: {improved_t}/{len(df_res)}ヶ月')
