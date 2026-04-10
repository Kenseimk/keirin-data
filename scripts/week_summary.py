# -*- coding: utf-8 -*-
"""
週次・月次成績サマリーをDiscordに投稿するスクリプト
使い方:
  python scripts/week_summary.py              # 直近7日
  python scripts/week_summary.py --month 2026-04  # 指定月
  python scripts/week_summary.py --from 2026-04-01 --to 2026-04-11  # 期間指定
"""
import sys, io, os, argparse, requests
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
import pandas as pd, numpy as np, re, glob, warnings, lightgbm as lgb
from pathlib import Path
from datetime import datetime, timedelta, timezone
warnings.filterwarnings('ignore')

parser = argparse.ArgumentParser()
parser.add_argument('--month', default=None, help='対象月 (例: 2026-04)')
parser.add_argument('--from', dest='date_from', default=None, help='開始日 (例: 2026-04-01)')
parser.add_argument('--to',   dest='date_to',   default=None, help='終了日 (例: 2026-04-11)')
args = parser.parse_args()

DISCORD_WEBHOOK = os.environ.get('DISCORD_WEBHOOK_URL', '')

def post_discord(content: str):
    if not DISCORD_WEBHOOK:
        print('[Discord] WEBHOOK未設定')
        return
    # Discordは2000文字制限があるので分割
    for i in range(0, len(content), 1900):
        chunk = content[i:i+1900]
        r = requests.post(DISCORD_WEBHOOK, json={'content': chunk}, timeout=10)
        if r.status_code not in (200, 204):
            print(f'[Discord] 投稿失敗: {r.status_code} {r.text}')

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
df_all['days_since_last'] = (
    pd.to_datetime(df_all['date']) -
    pd.to_datetime(df_all.groupby('player_key')['date'].shift(1))
).dt.days
def extract_honmei(x):
    if pd.isna(x): return None
    m = re.search(r'(\d+)', str(x)) if False else re.search(r'\u25ce(\d+)', str(x))
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

LGB_PARAMS = dict(n_estimators=1000, learning_rate=0.05, num_leaves=31,
                  min_child_samples=50, verbose=-1, random_state=42)

def fit_es(X_tr, y_tr, X_val, y_val):
    m = lgb.LGBMClassifier(**LGB_PARAMS)
    m.fit(X_tr, y_tr, eval_set=[(X_val, y_val)],
          callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(-1)])
    return m

def predict_date(target_date):
    df_train = df_model[df_model['date'] < target_date]
    if len(df_train) < 1000:
        return []
    all_months = sorted(df_train['date'].str[:7].unique())
    val_months_list = all_months[-3:]
    val_mask = df_train['date'].str[:7].isin(val_months_list)
    df_tr  = df_train[~val_mask]
    df_val = df_train[val_mask]
    X_tr, X_val = df_tr[FEATURES].values, df_val[FEATURES].values
    m1 = fit_es(X_tr, df_tr['is_winner'].values, X_val, df_val['is_winner'].values)
    m2 = fit_es(X_tr, df_tr['is_2nd'].values,    X_val, df_val['is_2nd'].values)
    m3 = fit_es(X_tr, df_tr['is_3rd'].values,    X_val, df_val['is_3rd'].values)

    df_today = df_all[df_all['date']==target_date].dropna(subset=FEATURES_BASE).copy()
    if df_today.empty:
        return []
    df_today['p1'] = m1.predict_proba(df_today[FEATURES].values)[:, 1]
    df_today['p2'] = m2.predict_proba(df_today[FEATURES].values)[:, 1]
    df_today['p3'] = m3.predict_proba(df_today[FEATURES].values)[:, 1]

    df_result = df_all[df_all['date']==target_date][['race_id','banum','rank_num','san_ren_tan']].copy()

    rows = []
    for race_id, g in df_today.groupby('race_id'):
        sg = score_gap_raw.get(race_id, np.nan)
        ts = top_score_raw.get(race_id, np.nan)
        np_ = n_players_raw.get(race_id, np.nan)
        if ts < 95 or sg < 2 or np_ != 7:
            continue
        venue = g['venue_slug'].iloc[0]
        rno   = int(g['race_no'].iloc[0])
        g = g.sort_values('p1', ascending=False).copy()
        p1 = int(g.iloc[0]['banum'])
        rest2 = g[g['banum']!=p1].sort_values('p2', ascending=False)
        p2 = int(rest2.iloc[0]['banum']) if len(rest2)>=1 else '-'
        rest3 = g[~g['banum'].isin([p1,p2])].sort_values('p3', ascending=False)
        p3 = int(rest3.iloc[0]['banum']) if len(rest3)>=1 else '-'

        res = df_result[df_result['race_id']==race_id]
        san = str(res['san_ren_tan'].iloc[0]) if not res.empty else ''
        a1 = res[res['rank_num']==1]['banum']
        a1 = int(a1.iloc[0]) if not a1.empty else None
        a2 = res[res['rank_num']==2]['banum']
        a2 = int(a2.iloc[0]) if not a2.empty else None
        a3 = res[res['rank_num']==3]['banum']
        a3 = int(a3.iloc[0]) if not a3.empty else None

        finished = a1 is not None
        hit  = finished and (p1==a1 and p2==a2 and p3==a3)
        hit1 = finished and (p1==a1)
        rows.append((venue, rno, p1, p2, p3, a1, a2, a3, san, hit, hit1, finished))
    return rows

# 対象日リストを決定
JST = timezone(timedelta(hours=9))
today = datetime.now(JST).strftime('%Y-%m-%d')

if args.month:
    y, m = map(int, args.month.split('-'))
    import calendar
    _, last_day = calendar.monthrange(y, m)
    d_from = f'{y}-{m:02d}-01'
    d_to   = f'{y}-{m:02d}-{last_day:02d}'
elif args.date_from and args.date_to:
    d_from, d_to = args.date_from, args.date_to
else:
    # デフォルト: 直近7日
    d_from = (datetime.now(JST) - timedelta(days=6)).strftime('%Y-%m-%d')
    d_to   = today

# CSVに存在する日付だけ対象にする
df_dates = df_all[
    (df_all['date'] >= d_from) & (df_all['date'] <= d_to)
]['date'].dropna().unique()
week_dates = sorted(df_dates)
label_period = f'{d_from} 〜 {d_to}'
print(f'対象期間: {label_period}  ({len(week_dates)}日)')

week_total = {'races':0,'hits':0,'hit1s':0,'done':0,'pay':0}
day_lines = []   # Discord用

for d in week_dates:
    print(f"\n{'='*58}")
    print(f"  {d}")
    print(f"{'='*58}")
    rows = predict_date(d)
    if not rows:
        print("  対象レースなし")
        continue
    day_hits = day_hit1s = day_done = 0
    disc_lines = [f'**{d}**']
    print(f"  {'会場':>10} {'R':>2} | 予想    | 結果    | 配当       | 判定")
    print(f"  {'-'*55}")
    for venue, rno, p1, p2, p3, a1, a2, a3, san, hit, hit1, finished in rows:
        week_total['races'] += 1
        if finished:
            day_done += 1
            week_total['done'] += 1
            result_str = f"{a1}-{a2}-{a3}"
            m = re.search(r'([\d,]+)\u5186', san)
            pay_str = m.group(1) + '円' if m else '-'
            pay_val = int(m.group(1).replace(',','')) if m else 0
            mark = "★三連単" if hit else ("◯1着" if hit1 else "✗")
            if hit:
                day_hits += 1
                week_total['hits'] += 1
                week_total['pay'] += pay_val
            if hit1:
                day_hit1s += 1
                week_total['hit1s'] += 1
        else:
            result_str = "未了"
            pay_str = '-'
            mark = "-"
        print(f"  {venue:>10} {rno:>2}R | {p1}-{p2}-{p3} | {result_str:<7} | {pay_str:>8} | {mark}")
        disc_lines.append(f'  {venue} {rno}R `{p1}-{p2}-{p3}` → {result_str} {pay_str} {mark}')
    if day_done > 0:
        summary = f'  → 三連単: {day_hits}/{day_done}  1着: {day_hit1s}/{day_done}'
        print(summary)
        disc_lines.append(summary)
    day_lines.extend(disc_lines)
    day_lines.append('')

# コンソール合計
n = max(week_total['done'], 1)
bet = week_total['done'] * 100
ret = week_total['pay']
roi = ret / bet * 100 if bet > 0 else 0
print(f"\n{'='*58}")
print(f"  期間: {label_period}")
print(f"  確定レース: {week_total['done']}R  投資: {bet:,}円  回収: {ret:,}円")
print(f"  純損益: {ret-bet:+,}円  回収率: {roi:.1f}%")
print(f"  三連単: {week_total['hits']}/{week_total['done']} ({week_total['hits']/n*100:.1f}%)  1着: {week_total['hit1s']}/{week_total['done']} ({week_total['hit1s']/n*100:.1f}%)")
print(f"{'='*58}")

# Discord投稿
disc_summary = '\n'.join([
    f'**:bar_chart: 競輪成績レポート**',
    f'期間: {label_period}',
    f'確定レース: {week_total["done"]}R',
    f'投資額: {bet:,}円  回収額: {ret:,}円',
    f'純損益: **{ret-bet:+,}円**  回収率: **{roi:.1f}%**',
    f'三連単: {week_total["hits"]}/{week_total["done"]} ({week_total["hits"]/n*100:.1f}%)',
    f'1着的中: {week_total["hit1s"]}/{week_total["done"]} ({week_total["hit1s"]/n*100:.1f}%)',
    '',
] + day_lines)

print(disc_summary)
post_discord(disc_summary)
