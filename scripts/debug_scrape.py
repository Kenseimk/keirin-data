# -*- coding: utf-8 -*-
import sys
sys.path.insert(0, '.')
from scripts.keirin_scraper import fetch, BASE_URL
import pandas as pd
from io import StringIO

out = open('debug_scrape_out.txt', 'w', encoding='utf-8')

df26 = pd.read_csv('keirin_data/2026_01_keirin.csv', encoding='utf-8-sig')
race_id = str(df26['race_id'].iloc[0])
venue   = df26['venue_slug'].iloc[0]

out.write(f'race: {venue} / {race_id}\n\n')

url = f'{BASE_URL}/{venue}/racedetail/{race_id}/?pageType=showResult'
resp = fetch(url)
tables = pd.read_html(StringIO(resp.text))

def flatten_col(c):
    if isinstance(c, tuple):
        return ' '.join(str(x) for x in c if 'Unnamed' not in str(x))
    return str(c)

def normalize_str(s):
    return str(s).replace(' ', '').replace('\u3000', '')

def find_col(df, keywords):
    for col in df.columns:
        if all(kw in col for kw in keywords):
            return col
    return None

# テーブル選択ロジックを再現
racecard_df = None
for i, t in enumerate(tables):
    cols_str = ' '.join(normalize_str(c) for c in t.columns)
    try:
        full_str = cols_str + ' ' + ' '.join(
            normalize_str(v) for v in t.values.flatten() if str(v) not in ('nan', 'NaN'))
    except Exception:
        full_str = cols_str
    has_score = '競走得点' in cols_str or '競走得点' in full_str
    has_style = '脚質' in cols_str or '脚質' in full_str
    has_name  = '選手名' in cols_str or '選手名' in full_str
    out.write(f'table[{i}]: score={has_score} style={has_style} name={has_name}\n')
    if has_score and has_style and has_name and racecard_df is None:
        racecard_df = t.copy()
        out.write(f'  → 出走表として選択！\n')

if racecard_df is None:
    out.write('出走表テーブル見つからず\n')
else:
    racecard_df.columns = [flatten_col(c) for c in racecard_df.columns]
    out.write(f'\n選択テーブル列名:\n{list(racecard_df.columns)}\n\n')

    # find_col テスト
    for label, keywords in [
        ('col_win_rate', ['勝 率']),
        ('col_win_rate2', ['勝率']),
        ('col_top2_rate', ['2連']),
        ('col_top3_rate', ['3連']),
        ('col_nige_4m', ['直近', '逃']),
        ('col_nige_4m2', ['4ヶ月', '逃']),
        ('col_maku_4m', ['直近', '捲']),
        ('col_mark', ['予 想']),
        ('col_mark2', ['予想']),
    ]:
        result = find_col(racecard_df, keywords)
        out.write(f'{label}({keywords}): {result}\n')

out.close()
