# -*- coding: utf-8 -*-
"""
三連単ミス分析: 1着/2着/3着のどこで外れているか
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

races_all = df_all.drop_duplicates('race_id')[
    ['race_id','date','venue_slug','san_ren_tan']].copy()
df1r = df_all[df_all['rank_num']==1][['race_id','banum']].rename(columns={'banum':'w1'})
df2r = df_all[df_all['rank_num']==2][['race_id','banum']].rename(columns={'banum':'w2'})
df3r = df_all[df_all['rank_num']==3][['race_id','banum']].rename(columns={'banum':'w3'})
races_all = (races_all
    .merge(df1r,on='race_id',how='left')
    .merge(df2r,on='race_id',how='left')
    .merge(df3r,on='race_id',how='left'))
races_all['san_pay'] = races_all['san_ren_tan'].apply(get_pay)

# ウォークフォワード 2024-2025
months = pd.period_range('2024-01', '2025-12', freq='M')
all_preds = []

print('分析中...')
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
        p2_proba =('win_proba', lambda x: x.iloc[1] if len(x)>1 else None),
        p3_proba =('win_proba', lambda x: x.iloc[2] if len(x)>2 else None),
    ).reset_index()
    rp['score_gap'] = rp['race_id'].map(score_gap_all)
    rp['top_score'] = rp['race_id'].map(top_score_all)
    rp['n_players'] = rp['race_id'].map(n_players_all)
    all_preds.append(rp)

df_preds = pd.concat(all_preds, ignore_index=True)
df_preds = df_preds.merge(
    races_all[['race_id','w1','w2','w3','san_pay']], on='race_id', how='left')
df_preds = df_preds[df_preds['w1'].notna()]
f = df_preds[(df_preds['top_score']>=95) & (df_preds['score_gap']>=2) & (df_preds['n_players']==7)]

f = f.copy()
f['hit_1st']  = (f['pred_1st'] == f['w1'])
f['hit_1_2']  = (f['pred_1st'] == f['w1']) & (f['pred_2nd'] == f['w2'])
f['hit_tan']  = (f['pred_1st'] == f['w1']) & (f['pred_2nd'] == f['w2']) & (f['pred_3rd'] == f['w3'])
f['hit_fuku'] = f.apply(lambda r: {r['pred_1st'],r['pred_2nd'],r['pred_3rd']} == {r['w1'],r['w2'],r['w3']}, axis=1)
# 1着は合ってるのに2着外れ
f['hit1_miss2'] = f['hit_1st'] & ~(f['pred_2nd'] == f['w2'])
# 1-2着合ってるのに3着外れ
f['hit12_miss3'] = f['hit_1_2'] & ~f['hit_tan']

n = len(f)
print()
print('=' * 60)
print(f'フィルター条件 (top_score>=95, gap>=2, 7車): {n}件')
print('=' * 60)
print(f'1着的中率:        {f["hit_1st"].mean():.1%} ({f["hit_1st"].sum()}/{n})')
print(f'1-2着的中率:      {f["hit_1_2"].mean():.1%} ({f["hit_1_2"].sum()}/{n})')
print(f'三連単的中率:     {f["hit_tan"].mean():.1%} ({f["hit_tan"].sum()}/{n})')
print(f'三連複的中率:     {f["hit_fuku"].mean():.1%} ({f["hit_fuku"].sum()}/{n})')
print()
print('--- 外れパターン内訳 ---')
miss = f[~f['hit_tan']]
n_miss = len(miss)
miss1 = (~miss['hit_1st']).sum()
hit1_miss2 = (miss['hit_1st'] & ~(miss['pred_2nd']==miss['w2'])).sum()
hit12_miss3 = miss['hit12_miss3'].sum()
print(f'1着から外れ:         {miss1}/{n_miss} ({miss1/n_miss:.1%})')
print(f'1着○ 2着×:          {hit1_miss2}/{n_miss} ({hit1_miss2/n_miss:.1%})')
print(f'1-2着○ 3着×:        {hit12_miss3}/{n_miss} ({hit12_miss3/n_miss:.1%})')
print()
print('--- 1着的中レースの詳細 ---')
hit1 = f[f['hit_1st']]
n_hit1 = len(hit1)
print(f'1着的中: {n_hit1}件')
print(f'  うち2着も的中: {hit1["hit_1_2"].sum()} ({hit1["hit_1_2"].mean():.1%})')
print(f'  うち三連単的中: {hit1["hit_tan"].sum()} ({hit1["hit_tan"].mean():.1%})')
print(f'  うち三連複的中: {hit1["hit_fuku"].sum()} ({hit1["hit_fuku"].mean():.1%})')
print()
print('--- top_proba別の1着的中率 ---')
bins = [0, 0.3, 0.35, 0.4, 0.45, 0.5, 0.6, 1.0]
f['proba_bin'] = pd.cut(f['top_proba'], bins=bins)
g = f.groupby('proba_bin', observed=True).agg(
    件数=('hit_1st','count'),
    一着的中率=('hit_1st','mean'),
    三連単的中率=('hit_tan','mean'),
    三連複的中率=('hit_fuku','mean'),
).reset_index()
g['一着的中率'] = g['一着的中率'].map('{:.1%}'.format)
g['三連単的中率'] = g['三連単的中率'].map('{:.1%}'.format)
g['三連複的中率'] = g['三連複的中率'].map('{:.1%}'.format)
print(g.to_string(index=False))

print()
print('--- 脚質別 三連単的中率 ---')
# 1着選手の脚質
style_map_inv = {5:'逃',4:'捲',3:'両',2:'差',1:'追',0:'マ'}
w1_style = df_all[df_all['rank_num']==1][['race_id','running_style']].rename(columns={'running_style':'w1_style'})
f2 = f.merge(w1_style, on='race_id', how='left')
g2 = f2.groupby('w1_style').agg(
    件数=('hit_tan','count'),
    三連単的中率=('hit_tan','mean'),
    一着的中率=('hit_1st','mean'),
).sort_values('件数', ascending=False)
g2['三連単的中率'] = g2['三連単的中率'].map('{:.1%}'.format)
g2['一着的中率'] = g2['一着的中率'].map('{:.1%}'.format)
print(g2.to_string())

print()
print('--- 場所別 三連単的中率（上位10場）---')
venue_map = df_all.drop_duplicates('race_id')[['race_id','venue_slug']]
f3 = f.merge(venue_map, on='race_id', how='left')
g3 = f3.groupby('venue_slug').agg(
    件数=('hit_tan','count'),
    三連単的中率=('hit_tan','mean'),
    三連複的中率=('hit_fuku','mean'),
).sort_values('三連単的中率', ascending=False).head(10)
g3['三連単的中率'] = g3['三連単的中率'].map('{:.1%}'.format)
g3['三連複的中率'] = g3['三連複的中率'].map('{:.1%}'.format)
print(g3.to_string())
