# -*- coding: utf-8 -*-
"""2026-01〜03を全日付再スクレイプ"""
import subprocess, sys
from datetime import date, timedelta

start = date(2026, 1, 1)
end   = date(2026, 3, 31)

d = start
total = 0
while d <= end:
    ds = d.strftime('%Y-%m-%d')
    print(f"[{ds}] スクレイプ中...", flush=True)
    result = subprocess.run(
        [sys.executable, 'scripts/scrape_day.py', '--date', ds],
        capture_output=True, text=True, encoding='utf-8', errors='replace'
    )
    lines = result.stdout.strip().split('\n')
    # 追加行数を表示
    for line in lines:
        if '保存完了' in line or '追加分' in line or '⚠️' in line:
            print(f"  {line}", flush=True)
    total += 1
    d += timedelta(days=1)

print(f"\n完了: {total}日分スクレイプ")
