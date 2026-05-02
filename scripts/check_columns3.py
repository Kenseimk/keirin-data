# -*- coding: utf-8 -*-
import sys
sys.path.insert(0, '.')
from scripts.keirin_scraper import fetch, BASE_URL
import pandas as pd, io

out = open('check_col_out2.txt', 'w', encoding='utf-8')

df26 = pd.read_csv('keirin_data/2026_01_keirin.csv', encoding='utf-8-sig')
race_id_26 = str(df26['race_id'].iloc[0])
venue_26   = df26['venue_slug'].iloc[0]

df25 = pd.read_csv('keirin_data/2025_12_keirin.csv', encoding='utf-8-sig')
race_id_25 = str(df25['race_id'].iloc[0])
venue_25   = df25['venue_slug'].iloc[0]

for venue, race_id, label in [(venue_25, race_id_25, '2025-12'), (venue_26, race_id_26, '2026-01')]:
    url = f'{BASE_URL}/{venue}/racedetail/{race_id}/?pageType=showResult'
    resp = fetch(url)
    if not resp:
        out.write(f'{label}: fetchに失敗\n')
        continue
    tables = pd.read_html(io.StringIO(resp.text))
    out.write(f'\n{"="*60}\n{label}: {len(tables)}テーブル\n{"="*60}\n')
    for i, t in enumerate(tables[:12]):
        def flatten(c):
            if isinstance(c, tuple):
                return ' '.join(str(x) for x in c if 'Unnamed' not in str(x))
            return str(c)
        cols = [flatten(c) for c in t.columns]
        # 出走表らしいテーブル（競走得点 or 勝率 or 逃 を含む）
        if any(k in str(cols) for k in ['競走得点', '勝率', '勝 率', '2連', '3連', '逃', '捲', '直近']):
            out.write(f'\n  ★ table[{i}] {len(t)}行 x {len(t.columns)}列\n')
            out.write(f'  全カラム: {cols}\n')
            out.write(f'  内容先頭:\n{t.head(3).to_string()}\n')

out.close()
