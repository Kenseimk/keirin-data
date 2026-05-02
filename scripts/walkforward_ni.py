# -*- coding: utf-8 -*-
"""
ウォークフォワード検証: 二連単 vs 三連単 vs 三連複
フィルター条件(top_score>=95, gap>=2, 7車)での月別回収率比較
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

# 払戻パース
def parse_ni(s):
    if pd.isna(s) or str(s).strip() == '': return None, None, 0
    m = re.search(r'(\d+)-(\d+)\s+([\d,]+)円', str(s).strip())
    if m: return int(m.group(1)), int(m.group(2)), int(m.group(3).replace(',',''))
    return None, None, 0

def get_pay(s):
    m = re.search(r'([\d,]+)円', str(s))
    return int(m.group(1).replace(',','')) if m else 0

races_all = df_all.drop_duplicates('race_id')[
    ['race_id','date','venue_slug','race_no','ni_sha_tan','san_ren_tan','san_ren_fuku']].copy()
races_all[['ni_w1','ni_w2','ni_pay']] = races_all['ni_sha_tan'].apply(
    lambda x: pd.Series(parse_ni(x)))
df1r = df_all[df_all['rank_num']==1][['race_id','banum']].rename(columns={'banum':'w1'})
df2r = df_all[df_all['rank_num']==2][['race_id','banum']].rename(columns={'banum':'w2'})
df3r = df_all[df_all['rank_num']==3][['race_id','banum']].rename(columns={'banum':'w3'})
races_all = (races_all
    .merge(df1r,on='race_id',how='left')
    .merge(df2r,on='race_id',how='left')
    .merge(df3r,on='race_id',how='left'))
races_all['san_pay']  = races_all['san_ren_tan'].apply(get_pay)
races_all['fuku_pay'] = races_all['san_ren_fuku'].apply(get_pay)

# ウォークフォワード: 2024-01〜2025-12 月ごと
months = pd.period_range('2024-01', '2025-12', freq='M')
results = []

print('ウォークフォワード開始...')
for month in months:
    start = str(month.start_time.date())
    end   = str(month.end_time.date())
    df_train = df_model[df_model['date'] < start]
    df_test  = df_model[(df_model['date'] >= start) & (df_model['date'] <= end)]
    if len(df_train) < 1000 or df_test.empty:
        continue

    model = lgb.LGBMClassifier(n_estimators=300, learning_rate=0.05, num_leaves=31,
                                min_child_samples=50, verbose=-1, random_state=42)
    model.fit(df_train[FEATURES].values, df_train['is_winner'].values)

    df_test = df_test.copy()
    df_test['win_proba'] = model.predict_proba(df_test[FEATURES].values)[:, 1]
    ds = df_test.sort_values(['race_id','win_proba'], ascending=[True,False])
    rp = ds.groupby('race_id').agg(
        date     =('date','first'),
        pred_1st =('banum', lambda x: int(x.iloc[0])),
        pred_2nd =('banum', lambda x: int(x.iloc[1]) if len(x)>1 else None),
        pred_3rd =('banum', lambda x: int(x.iloc[2]) if len(x)>2 else None),
        top_proba=('win_proba', lambda x: x.iloc[0]),
    ).reset_index()
    rp['score_gap'] = rp['race_id'].map(score_gap_all)
    rp['top_score'] = rp['race_id'].map(top_score_all)
    rp['n_players'] = rp['race_id'].map(n_players_all)
    rp = rp.merge(races_all[['race_id','ni_w1','ni_w2','ni_pay','w1','w2','w3','san_pay','fuku_pay']],
                  on='race_id', how='left')
    rp = rp[rp['w1'].notna()]

    # フィルター
    f = rp[(rp['top_score']>=95) & (rp['score_gap']>=2) & (rp['n_players']==7)]
    if f.empty:
        continue

    ni_hit   = ((f['pred_1st']==f['ni_w1']) & (f['pred_2nd']==f['ni_w2'])).sum()
    tan_hit  = ((f['pred_1st']==f['w1']) & (f['pred_2nd']==f['w2']) & (f['pred_3rd']==f['w3'])).sum()
    fuku_hit = f.apply(lambda r: {r['pred_1st'],r['pred_2nd'],r['pred_3rd']} == {r['w1'],r['w2'],r['w3']}, axis=1).sum()
    ni_ret   = f[((f['pred_1st']==f['ni_w1']) & (f['pred_2nd']==f['ni_w2']))]['ni_pay'].sum()
    tan_ret  = f[((f['pred_1st']==f['w1']) & (f['pred_2nd']==f['w2']) & (f['pred_3rd']==f['w3']))]['san_pay'].sum()
    fuku_ret = f[f.apply(lambda r: {r['pred_1st'],r['pred_2nd'],r['pred_3rd']} == {r['w1'],r['w2'],r['w3']}, axis=1)]['fuku_pay'].sum()
    n = len(f)

    results.append({
        'month': str(month),
        '件数': n,
        '二連単_的中': ni_hit,
        '二連単_的中率': ni_hit/n*100,
        '二連単_回収率': ni_ret/n,
        '三連単_的中': tan_hit,
        '三連単_的中率': tan_hit/n*100,
        '三連単_回収率': tan_ret/n,
        '三連複_的中': fuku_hit,
        '三連複_的中率': fuku_hit/n*100,
        '三連複_回収率': fuku_ret/n,
    })
    print(f'  {month}: {n}件 | 二連単{ni_hit/n*100:.1f}%的中 回収{ni_ret/n:.1f}% | '
          f'三連単{tan_hit/n*100:.1f}%的中 回収{tan_ret/n:.1f}% | '
          f'三連複{fuku_hit/n*100:.1f}%的中 回収{fuku_ret/n:.1f}%')

df_res = pd.DataFrame(results)
print()
print('=' * 80)
print('ウォークフォワード集計（フィルター: top_score>=95, gap>=2, 7車）')
print('=' * 80)
print(df_res[['month','件数','二連単_的中率','二連単_回収率','三連単_的中率','三連単_回収率','三連複_的中率','三連複_回収率']].to_string(index=False, float_format='%.1f'))

print()
print('=== 全期間合計 ===')
total = df_res['件数'].sum()
ni_total_ret  = sum(r['二連単_回収率']*r['件数'] for _, r in df_res.iterrows())
tan_total_ret = sum(r['三連単_回収率']*r['件数'] for _, r in df_res.iterrows())
fu_total_ret  = sum(r['三連複_回収率']*r['件数'] for _, r in df_res.iterrows())
print(f'総件数: {total}')
print(f'二連単 平均的中率: {df_res["二連単_的中率"].mean():.1f}%  平均回収率: {ni_total_ret/total:.1f}%')
print(f'三連単 平均的中率: {df_res["三連単_的中率"].mean():.1f}%  平均回収率: {tan_total_ret/total:.1f}%')
print(f'三連複 平均的中率: {df_res["三連複_的中率"].mean():.1f}%  平均回収率: {fu_total_ret/total:.1f}%')

print()
print('=== 月別黒字率 ===')
print(f'二連単 黒字月: {(df_res["二連単_回収率"]>100).sum()}/{len(df_res)}ヶ月')
print(f'三連単 黒字月: {(df_res["三連単_回収率"]>100).sum()}/{len(df_res)}ヶ月')
print(f'三連複 黒字月: {(df_res["三連複_回収率"]>100).sum()}/{len(df_res)}ヶ月')

print()
print('=== 二連単 月別回収率（見やすく整理）===')
print(f'{"月":<10} {"件数":>5} {"的中率":>7} {"回収率":>8} {"黒赤":>5}')
for _, row in df_res.iterrows():
    mark = 'plus' if row['二連単_回収率'] > 100 else 'minus'
    print(f'{row["month"]:<10} {int(row["件数"]):>5} {row["二連単_的中率"]:>6.1f}% {row["二連単_回収率"]:>7.1f}% {mark:>5}')
