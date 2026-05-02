# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
import pandas as pd, glob, re, numpy as np, warnings
warnings.filterwarnings('ignore')
import lightgbm as lgb

files = [f for f in sorted(glob.glob('keirin_data/202[345]_*.csv')) if 'sample' not in f]
print(f'学習ファイル: {len(files)}件')
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
df_all['last5_win_rate'] = df_all.groupby('player_key')['is_win'].transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())
df_all['rank_trend']     = df_all['prev1_rank'] - grp.shift(3)
df_all['days_since_last']= (pd.to_datetime(df_all['date']) - pd.to_datetime(df_all.groupby('player_key')['date'].shift(1))).dt.days

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

df_model = df_all.dropna(subset=FEATURES+['rank_num']).copy()
df_train = df_model[df_model['date'] < '2026-04-01']
print(f'学習件数: {len(df_train)}')

model = lgb.LGBMClassifier(n_estimators=300, learning_rate=0.05, num_leaves=31,
                            min_child_samples=50, verbose=-1, random_state=42)
model.fit(df_train[FEATURES].values, df_train['is_winner'].values)
print('学習完了')

# 4月データ読み込み
df_apr = pd.read_csv('keirin_data/2026_04_keirin.csv', encoding='utf-8-sig')
df_apr['rank_num'] = pd.to_numeric(df_apr['rank'], errors='coerce')

def get_payout_val(s):
    m = re.search(r'([\d,]+)円', str(s))
    return int(m.group(1).replace(',','')) if m else 0

races_meta = df_apr.drop_duplicates('race_id')[['race_id','venue_slug','date','race_no','san_ren_tan','san_ren_fuku']]
df1r = df_apr[df_apr['rank_num']==1][['race_id','banum']].rename(columns={'banum':'w1'})
df2r = df_apr[df_apr['rank_num']==2][['race_id','banum']].rename(columns={'banum':'w2'})
df3r = df_apr[df_apr['rank_num']==3][['race_id','banum']].rename(columns={'banum':'w3'})
races_meta = races_meta.merge(df1r,on='race_id',how='left').merge(df2r,on='race_id',how='left').merge(df3r,on='race_id',how='left')
races_meta['san_payout']  = races_meta['san_ren_tan'].apply(get_payout_val)
races_meta['fuku_payout'] = races_meta['san_ren_fuku'].apply(get_payout_val)

# 4月データにも特徴量計算（学習データと結合して直近履歴を引き継ぐ）
df_apr2 = df_apr.copy()
df_apr2['player_key']  = df_apr2['player_name'].astype(str)+'_'+df_apr2['term'].astype(str)
df_apr2['class_num']   = df_apr2['player_class'].map(CLASS_MAP).fillna(1)
df_apr2['style_num']   = df_apr2['running_style'].map(STYLE_MAP).fillna(2)
df_apr2['race_score']  = pd.to_numeric(df_apr2['race_score'], errors='coerce')
df_apr2['gear']        = pd.to_numeric(df_apr2['gear'], errors='coerce')
df_apr2['banum']       = pd.to_numeric(df_apr2['banum'], errors='coerce')
df_apr2['is_win']      = (pd.to_numeric(df_apr2['rank'], errors='coerce')==1).astype(float)
df_apr2['is_winner']   = 0

df_combined = pd.concat([df_all, df_apr2], ignore_index=True).sort_values(['player_key','date','race_no']).reset_index(drop=True)
grp2 = df_combined.groupby('player_key')['rank_num']
df_combined['prev1_rank']     = grp2.shift(1)
df_combined['last3_avg_rank'] = grp2.transform(lambda x: x.shift(1).rolling(3,min_periods=1).mean())
df_combined['last5_avg_rank'] = grp2.transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())
df_combined['last5_win_rate'] = df_combined.groupby('player_key')['is_win'].transform(lambda x: x.shift(1).rolling(5,min_periods=1).mean())
df_combined['rank_trend']     = df_combined['prev1_rank'] - grp2.shift(3)
df_combined['days_since_last']= (pd.to_datetime(df_combined['date']) - pd.to_datetime(df_combined.groupby('player_key')['date'].shift(1))).dt.days
df_combined['honmei_banum']   = df_combined['lineup'].apply(extract_honmei)
df_combined['score_rank']     = df_combined.groupby('race_id')['race_score'].rank(ascending=False, method='min')
df_combined['is_honmei']      = (df_combined['banum']==df_combined['honmei_banum']).astype(int)
n_pl2 = df_combined.groupby('race_id')['banum'].count().rename('n_players_in_race2')
df_combined = df_combined.join(n_pl2, on='race_id')
df_combined['n_players_in_race'] = df_combined['n_players_in_race2'].fillna(df_combined['n_players_in_race'])

score_gap_apr = df_combined.groupby('race_id')['race_score'].apply(calc_gap)
top_score_apr = df_combined.groupby('race_id')['race_score'].max()
n_players_apr = df_combined.groupby('race_id')['banum'].nunique()

TARGET_DATES = ['2026-04-01','2026-04-02','2026-04-03','2026-04-04']
all_results = []
for TARGET_DATE in TARGET_DATES:
    df_today = df_combined[df_combined['date'] == TARGET_DATE].dropna(subset=FEATURES).copy()
    if df_today.empty:
        continue
    df_today['win_proba'] = model.predict_proba(df_today[FEATURES].values)[:, 1]
    ds = df_today.sort_values(['race_id','win_proba'], ascending=[True,False])
    rp = ds.groupby('race_id').agg(
        venue   =('venue_slug','first'),
        date    =('date','first'),
        race_no =('race_no','first'),
        pred_1st=('banum', lambda x: int(x.iloc[0])),
        pred_2nd=('banum', lambda x: int(x.iloc[1]) if len(x)>1 else None),
        pred_3rd=('banum', lambda x: int(x.iloc[2]) if len(x)>2 else None),
        top_proba=('win_proba', lambda x: x.iloc[0]),
    ).reset_index()
    rp['score_gap'] = rp['race_id'].map(score_gap_apr)
    rp['top_score'] = rp['race_id'].map(top_score_apr)
    rp['n_players'] = rp['race_id'].map(n_players_apr)
    all_results.append(rp)

df_preds = pd.concat(all_results, ignore_index=True)
df_preds = df_preds.merge(races_meta[['race_id','w1','w2','w3','san_payout','fuku_payout']], on='race_id', how='left')
df_preds['tan_hit']  = (df_preds['pred_1st']==df_preds['w1']) & (df_preds['pred_2nd']==df_preds['w2']) & (df_preds['pred_3rd']==df_preds['w3'])
df_preds['fuku_hit'] = df_preds.apply(
    lambda r: {r['pred_1st'],r['pred_2nd'],r['pred_3rd']} == {r['w1'],r['w2'],r['w3']} if pd.notna(r['w1']) else False, axis=1)
df_preds['has_result'] = df_preds['w1'].notna()

r = df_preds[df_preds['has_result']]
print()
print('=' * 60)
print('2026年4月 全レース結果')
print('=' * 60)
print(f'総レース数: {len(r)}')
ht = r['tan_hit'].sum()
hf = r['fuku_hit'].sum()
tr_t = int(r[r['tan_hit']]['san_payout'].sum())
tr_f = int(r[r['fuku_hit']]['fuku_payout'].sum())
print(f'三連単: {ht}/{len(r)}的中 ({r["tan_hit"].mean():.1%})  回収率 {tr_t/len(r):.1f}%')
print(f'三連複: {hf}/{len(r)}的中 ({r["fuku_hit"].mean():.1%})  回収率 {tr_f/len(r):.1f}%')

print()
print('=== フィルター(top_score>=95, gap>=2, 7車) ===')
f = r[(r['top_score']>=95) & (r['score_gap']>=2) & (r['n_players']==7)]
print(f'対象: {len(f)}件')
if len(f) > 0:
    ht2 = f['tan_hit'].sum()
    hf2 = f['fuku_hit'].sum()
    tr_t2 = int(f[f['tan_hit']]['san_payout'].sum())
    tr_f2 = int(f[f['fuku_hit']]['fuku_payout'].sum())
    print(f'三連単: {ht2}/{len(f)}的中 ({f["tan_hit"].mean():.1%})  回収率 {tr_t2/len(f):.1f}%')
    print(f'三連複: {hf2}/{len(f)}的中 ({f["fuku_hit"].mean():.1%})  回収率 {tr_f2/len(f):.1f}%')

print()
print('=== 日別（全レース）===')
for d in TARGET_DATES:
    day = r[r['date']==d]
    if day.empty:
        continue
    ht_d  = day['tan_hit'].sum()
    hf_d  = day['fuku_hit'].sum()
    tr_td = int(day[day['tan_hit']]['san_payout'].sum())
    tr_fd = int(day[day['fuku_hit']]['fuku_payout'].sum())
    print(f'{d}: {len(day)}R  三連単{ht_d}的中({tr_td/len(day):.1f}%)  三連複{hf_d}的中({tr_fd/len(day):.1f}%)')

print()
print('=== top_proba分布 ===')
print(r['top_proba'].describe())
print()
print('=== 的中レースのtop_proba ===')
print(r[r['tan_hit']]['top_proba'].describe())
