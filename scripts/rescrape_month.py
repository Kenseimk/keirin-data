# -*- coding: utf-8 -*-
"""指定月を全日付再スクレイプ"""
import subprocess, sys, argparse
from datetime import date, timedelta
import calendar

parser = argparse.ArgumentParser()
parser.add_argument('--year',  type=int, required=True)
parser.add_argument('--month', type=int, required=True)
parser.add_argument('--out',   type=str, required=True)
args = parser.parse_args()

_, last_day = calendar.monthrange(args.year, args.month)
start = date(args.year, args.month, 1)
end   = date(args.year, args.month, last_day)

with open(args.out, 'w', encoding='utf-8') as log:
    d = start
    while d <= end:
        ds = d.strftime('%Y-%m-%d')
        log.write(f'[{ds}] スクレイプ中...\n')
        log.flush()
        result = subprocess.run(
            [sys.executable, 'scripts/scrape_day.py', '--date', ds],
            capture_output=True, text=True, encoding='utf-8', errors='replace'
        )
        for line in result.stdout.strip().split('\n'):
            if '保存完了' in line or '追加分' in line or '⚠️' in line or 'エラー' in line:
                log.write(f'  {line}\n')
                log.flush()
        d += timedelta(days=1)
    log.write('完了\n')
