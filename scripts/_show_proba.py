# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
import pandas as pd, re, glob, numpy as np, warnings
warnings.filterwarnings('ignore')
import lightgbm as lgb
from pathlib import Path

TARGET_DATE = sys.argv[1] if len(sys.argv) > 1 else '2026-04-09'

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
df_all['days_since_last'] = (pd.to_datetime(df_all['date']) -
    pd.to_datetime(df_all.groupby('player_key')['date'].shift(1))).dt.days
def extract_honmei(x):
    if pd.isna(x): return None
    m = re.search(r'◎(\d+)', str(x))
    return int(m.group(1)) if m else None
df_all['honmei_banum'] = df_all['lineup'].apply(extract_honmei)
df_all['score_rank']   = df_all.groupby('race_id')['race_score'].rank(ascending=False, method='min')
df_all['is_honmei']    = (df_all['banum']==df_all['honmei_banum']).astype(int)
n_pl = df_all.groupby('race_id')['banum'].count().rename('n_players_in_race')
df_all = df_all.join(n_pl, on='race_id')

FEATURES_BASE = ['race_score','class_num','style_num','gear','score_rank','is_honmei',
                 'n_players_in_race','prev1_rank','last3_avg_rank','last5_avg_rank',
                 'last5_win_rate','rank_trend','days_since_last']
FEATURES_NEW  = ['mark_num','win_rate_4m','top2_rate_4m','top3_rate_4m','nige_4m','maku_4m']
FEATURES = FEATURES_BASE + FEATURES_NEW

def calc_gap(x):
    s = sorted(x.dropna(), reverse=True)
    return s[0]-s[1] if len(s)>=2 else np.nan

score_gap_raw = df_all.groupby('race_id')['race_score'].apply(calc_gap)
top_score_raw = df_all.groupby('race_id')['race_score'].max()
n_players_raw = df_all.groupby('race_id')['banum'].nunique()
df_model = df_all.dropna(subset=FEATURES_BASE+['rank_num']).copy()
df_train  = df_model[df_model['date'] < TARGET_DATE]

VAL_MONTHS = 3
ES_ROUNDS  = 50
all_months = sorted(df_train['date'].str[:7].unique())
val_months_list = all_months[-VAL_MONTHS:]
val_mask = df_train['date'].str[:7].isin(val_months_list)
df_tr  = df_train[~val_mask]
df_val = df_train[val_mask]

LGB_PARAMS = dict(n_estimators=1000, learning_rate=0.05, num_leaves=31,
                  min_child_samples=50, verbose=-1, random_state=42)

def fit_es(params, X_tr, y_tr, X_val, y_val):
    m = lgb.LGBMClassifier(**params)
    m.fit(X_tr, y_tr,
          eval_set=[(X_val, y_val)],
          callbacks=[lgb.early_stopping(ES_ROUNDS, verbose=False),
                     lgb.log_evaluation(-1)])
    return m

X_tr  = df_tr[FEATURES].values
X_val = df_val[FEATURES].values
m1 = fit_es(LGB_PARAMS, X_tr, df_tr['is_winner'].values, X_val, df_val['is_winner'].values)
m2 = fit_es(LGB_PARAMS, X_tr, df_tr['is_2nd'].values,    X_val, df_val['is_2nd'].values)
m3 = fit_es(LGB_PARAMS, X_tr, df_tr['is_3rd'].values,    X_val, df_val['is_3rd'].values)

df_today = df_all[df_all['date']==TARGET_DATE].dropna(subset=FEATURES_BASE).copy()
df_today['p1'] = m1.predict_proba(df_today[FEATURES].values)[:, 1]
df_today['p2'] = m2.predict_proba(df_today[FEATURES].values)[:, 1]
df_today['p3'] = m3.predict_proba(df_today[FEATURES].values)[:, 1]

STYLE_INV = {5:'逃',4:'捲',3:'両',2:'差',1:'追',0:'マ'}

for race_id, g in df_today.groupby('race_id'):
    sg = score_gap_raw.get(race_id, np.nan)
    ts = top_score_raw.get(race_id, np.nan)
    np_ = n_players_raw.get(race_id, np.nan)
    if ts < 95 or sg < 2 or np_ != 7:
        continue
    venue = g['venue_slug'].iloc[0]
    rno   = int(g['race_no'].iloc[0])

    g = g.sort_values('p1', ascending=False).copy()
    pred1 = int(g.iloc[0]['banum'])
    rest2 = g[g['banum']!=pred1].sort_values('p2', ascending=False)
    pred2 = int(rest2.iloc[0]['banum']) if len(rest2)>=1 else '-'
    rest3 = g[~g['banum'].isin([pred1,pred2])].sort_values('p3', ascending=False)
    pred3 = int(rest3.iloc[0]['banum']) if len(rest3)>=1 else '-'

    print(f'\n【{venue} {rno}R】予想: {pred1}-{pred2}-{pred3}  (top_score:{ts:.1f} gap:{sg:.1f})')
    print(f'  {"車":>3} {"脚質":>4} {"得点":>7} {"1着%":>7} {"2着%":>7} {"3着%":>7}  {"予想印"}')
    print(f'  {"-"*52}')
    for _, row in g.iterrows():
        b    = int(row['banum'])
        sty  = STYLE_INV.get(int(row['style_num']), '?')
        sc   = row['race_score']
        mark = row.get('mark', '') if 'mark' in g.columns else ''
        star = ''
        if b == pred1: star = '← 1着予想'
        elif b == pred2: star = '← 2着予想'
        elif b == pred3: star = '← 3着予想'
        print(f'  {b:>3} {sty:>4} {sc:>7.2f} {row["p1"]*100:>6.1f}% {row["p2"]*100:>6.1f}% {row["p3"]*100:>6.1f}%  {mark} {star}')
