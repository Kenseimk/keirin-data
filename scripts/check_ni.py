# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
import pandas as pd, re, glob, numpy as np, warnings
warnings.filterwarnings('ignore')
import lightgbm as lgb

files = [f for f in sorted(glob.glob('keirin_data/202[23456]_*.csv')) if 'sample' not in f]
df_all = pd.concat([pd.read_csv(f, encoding='utf-8-sig') for f in files], ignore_index=True)

CLASS_MAP = {'S1':4,'S2':3,'A1':2,'A2':1,'B':0}
STYLE_MAP  = {'逃':5,'捲':4,'両':3,'差':2,'追':1,'マ':0}
df_all['player_key'] = df_all['player_name'].astype(str)+'_'+df_all['term'].astype(str)
df_all['rank_num']   = pd.to_numeric(df_all['rank'], errors='coerce')
df_all['class_num']  = df_all['player_class'].map(CLASS_MAP).fillna(1)
df_all['style_num']  = df_all['running_style'].map(STYLE_MAP).fillna(2)
df_all['race_score'] = pd.to_numeric(df_all['race_score'], errors='coerce')
df_all['gear']       = pd.to_numeric(df_all['gear'], errors='coerce')
df_all['banum']      = pd.to_numeric(df_all['banum'], errors='coerce')
df_all['is_win']     = (df_all['rank_num']==1).astype(float)
df_all['is_winner']  = (df_all['rank_num']==1).astype(int)
df_all = df_all.sort_values(['player_key','date','race_no']).reset_index(drop=True)
grp = df_all.groupby('player_key')['rank_num']
df_all['prev1_rank']     = grp.shift(1)
df_all['last3_avg_rank'] = grp.transform(lambda x: x.shift(1).rolling(3,min_periods=1).mean())
df_all['last5_avg_rank'] = grp.transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())
df_all['last5_win_rate'] = df_all.groupby('player_key')['is_win'].transform(
    lambda x: x.shift(1).rolling(5,min_periods=1).mean())
df_all['rank_trend']     = df_all['prev1_rank'] - grp.shift(3)
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

score_gap_raw = df_all.groupby('race_id')['race_score'].apply(calc_gap)
top_score_raw = df_all.groupby('race_id')['race_score'].max()
n_players_raw = df_all.groupby('race_id')['banum'].nunique()
df_model = df_all.dropna(subset=FEATURES+['rank_num']).copy()

# 4月データ
df_apr = pd.read_csv('keirin_data/2026_04_keirin.csv', encoding='utf-8-sig')
df_apr['rank_num'] = pd.to_numeric(df_apr['rank'], errors='coerce')

def parse_ni(s):
    if pd.isna(s) or str(s).strip() == '': return None, None, 0
    m = re.search(r'(\d+)-(\d+)\s+([\d,]+)円', str(s).strip())
    if m: return int(m.group(1)), int(m.group(2)), int(m.group(3).replace(',',''))
    return None, None, 0

def get_pay(s):
    m = re.search(r'([\d,]+)円', str(s))
    return int(m.group(1).replace(',','')) if m else 0

races_meta = df_apr.drop_duplicates('race_id')[
    ['race_id','venue_slug','date','race_no','ni_sha_tan','san_ren_tan','san_ren_fuku']].copy()
races_meta[['ni_w1','ni_w2','ni_pay']] = races_meta['ni_sha_tan'].apply(
    lambda x: pd.Series(parse_ni(x)))
df1r = df_apr[df_apr['rank_num']==1][['race_id','banum']].rename(columns={'banum':'w1'})
df2r = df_apr[df_apr['rank_num']==2][['race_id','banum']].rename(columns={'banum':'w2'})
df3r = df_apr[df_apr['rank_num']==3][['race_id','banum']].rename(columns={'banum':'w3'})
races_meta = (races_meta
    .merge(df1r,on='race_id',how='left')
    .merge(df2r,on='race_id',how='left')
    .merge(df3r,on='race_id',how='left'))
races_meta['san_pay']  = races_meta['san_ren_tan'].apply(get_pay)
races_meta['fuku_pay'] = races_meta['san_ren_fuku'].apply(get_pay)

TARGET_DATES = ['2026-04-03','2026-04-04']
all_results = []
for TARGET_DATE in TARGET_DATES:
    df_train = df_model[df_model['date'] < TARGET_DATE]
    model = lgb.LGBMClassifier(n_estimators=300, learning_rate=0.05, num_leaves=31,
                                min_child_samples=50, verbose=-1, random_state=42)
    model.fit(df_train[FEATURES].values, df_train['is_winner'].values)
    df_today = df_all[df_all['date'] == TARGET_DATE].dropna(subset=FEATURES).copy()
    if df_today.empty:
        continue
    df_today['win_proba'] = model.predict_proba(df_today[FEATURES].values)[:, 1]
    ds = df_today.sort_values(['race_id','win_proba'], ascending=[True,False])
    rp = ds.groupby('race_id').agg(
        venue    =('venue_slug','first'),
        date     =('date','first'),
        race_no  =('race_no','first'),
        pred_1st =('banum', lambda x: int(x.iloc[0])),
        pred_2nd =('banum', lambda x: int(x.iloc[1]) if len(x)>1 else None),
        pred_3rd =('banum', lambda x: int(x.iloc[2]) if len(x)>2 else None),
        top_proba=('win_proba', lambda x: x.iloc[0]),
    ).reset_index()
    rp['score_gap'] = rp['race_id'].map(score_gap_raw)
    rp['top_score'] = rp['race_id'].map(top_score_raw)
    rp['n_players'] = rp['race_id'].map(n_players_raw)
    all_results.append(rp)

df_preds = pd.concat(all_results, ignore_index=True)
df_preds = df_preds.merge(
    races_meta[['race_id','ni_w1','ni_w2','ni_pay','w1','w2','w3','san_pay','fuku_pay']],
    on='race_id', how='left')

df_preds['ni_hit']   = (df_preds['pred_1st']==df_preds['ni_w1']) & (df_preds['pred_2nd']==df_preds['ni_w2'])
df_preds['tan_hit']  = ((df_preds['pred_1st']==df_preds['w1']) &
                        (df_preds['pred_2nd']==df_preds['w2']) &
                        (df_preds['pred_3rd']==df_preds['w3']))
df_preds['fuku_hit'] = df_preds.apply(
    lambda r: ({r['pred_1st'],r['pred_2nd'],r['pred_3rd']} == {r['w1'],r['w2'],r['w3']}
               if pd.notna(r['w1']) else False), axis=1)
df_preds['has_result'] = df_preds['w1'].notna()

r = df_preds[df_preds['has_result']]
f = r[(r['top_score']>=95) & (r['score_gap']>=2) & (r['n_players']==7)]

print('=' * 60)
print('4/3〜4/4 賭け種類別比較（フィルター: top_score>=95, gap>=2, 7車）')
print('=' * 60)
print(f'対象レース数: {len(f)}件')
print()

ni_hit   = int(f['ni_hit'].sum())
tan_hit  = int(f['tan_hit'].sum())
fuku_hit = int(f['fuku_hit'].sum())
ni_ret   = int(f[f['ni_hit']]['ni_pay'].sum())
tan_ret  = int(f[f['tan_hit']]['san_pay'].sum())
fuku_ret = int(f[f['fuku_hit']]['fuku_pay'].sum())
n = len(f)

print(f'二連単  : {ni_hit}/{n}的中 ({ni_hit/n:.1%})  回収 {ni_ret}円/{n*100}円 = {ni_ret/n:.1f}%')
print(f'三連単  : {tan_hit}/{n}的中 ({tan_hit/n:.1%})  回収 {tan_ret}円/{n*100}円 = {tan_ret/n:.1f}%')
print(f'三連複  : {fuku_hit}/{n}的中 ({fuku_hit/n:.1%})  回収 {fuku_ret}円/{n*100}円 = {fuku_ret/n:.1f}%')

print()
print('=== 日別内訳 ===')
for d in TARGET_DATES:
    fd = f[f['date']==d]
    if fd.empty:
        print(f'{d}: 対象なし')
        continue
    ni_h  = int(fd['ni_hit'].sum())
    tan_h = int(fd['tan_hit'].sum())
    fu_h  = int(fd['fuku_hit'].sum())
    ni_r  = int(fd[fd['ni_hit']]['ni_pay'].sum())
    tan_r = int(fd[fd['tan_hit']]['san_pay'].sum())
    fu_r  = int(fd[fd['fuku_hit']]['fuku_pay'].sum())
    nd    = len(fd)
    print(f'{d} ({nd}件): 二連単{ni_h}的中({ni_r/nd:.1f}%)  三連単{tan_h}的中({tan_r/nd:.1f}%)  三連複{fu_h}的中({fu_r/nd:.1f}%)')

print()
print('=== 全レース（フィルターなし）===')
ni_hit2   = int(r['ni_hit'].sum())
tan_hit2  = int(r['tan_hit'].sum())
fuku_hit2 = int(r['fuku_hit'].sum())
ni_ret2   = int(r[r['ni_hit']]['ni_pay'].sum())
tan_ret2  = int(r[r['tan_hit']]['san_pay'].sum())
fuku_ret2 = int(r[r['fuku_hit']]['fuku_pay'].sum())
n2 = len(r)
print(f'対象: {n2}件')
print(f'二連単  : {ni_hit2}/{n2}的中 ({ni_hit2/n2:.1%})  回収 {ni_ret2/n2:.1f}%')
print(f'三連単  : {tan_hit2}/{n2}的中 ({tan_hit2/n2:.1%})  回収 {tan_ret2/n2:.1f}%')
print(f'三連複  : {fuku_hit2}/{n2}的中 ({fuku_hit2/n2:.1%})  回収 {fuku_ret2/n2:.1f}%')

print()
print('=== 二連単 平均配当 ===')
print(f'全体平均: {r["ni_pay"].mean():.0f}円')
print(f'的中時:   {r[r["ni_hit"]]["ni_pay"].mean():.0f}円')
print(f'三連単平均配当: {r["san_pay"].mean():.0f}円')
print(f'三連複平均配当: {r["fuku_pay"].mean():.0f}円')
