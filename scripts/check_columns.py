# -*- coding: utf-8 -*-
import sys, io as _io
sys.stdout = _io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, '.')
from scripts.keirin_scraper import fetch, BASE_URL
import pandas as pd, io

df26 = pd.read_csv('keirin_data/2026_01_keirin.csv', encoding='utf-8-sig')
race_id_26 = str(df26['race_id'].iloc[0])
venue_26   = df26['venue_slug'].iloc[0]

df25 = pd.read_csv('keirin_data/2025_12_keirin.csv', encoding='utf-8-sig')
race_id_25 = str(df25['race_id'].iloc[0])
venue_25   = df25['venue_slug'].iloc[0]

print(f'2025-12: {venue_25} / {race_id_25}')
print(f'2026-01: {venue_26} / {race_id_26}')

for venue, race_id, label in [(venue_25, race_id_25, '2025-12'), (venue_26, race_id_26, '2026-01')]:
    url = f'{BASE_URL}/{venue}/racedetail/{race_id}/?pageType=showResult'
    resp = fetch(url)
    if not resp:
        print(f'{label}: fetchに失敗')
        continue
    try:
        tables = pd.read_html(io.StringIO(resp.text))
    except Exception as e:
        print(f'{label}: テーブルなし ({e})')
        continue
    print(f'\n{label}: {len(tables)}テーブル')
    for i, t in enumerate(tables[:12]):
        def flatten(c):
            if isinstance(c, tuple):
                return ' '.join(str(x) for x in c if 'Unnamed' not in str(x))
            return str(c)
        cols = [flatten(c) for c in t.columns]
        keywords = ['得点', '勝率', '連対', '2連', '3連', '予想', '逃', '捲', '脚質']
        if any(k in str(cols) for k in keywords):
            print(f'  ★ [{i}] {len(t)}行 cols={cols}')
        else:
            print(f'    [{i}] {len(t)}行 cols={cols[:4]}')
