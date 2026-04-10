# -*- coding: utf-8 -*-
"""
既存CSVに新カラム（予想印・勝率・3連対率など）を追加するスクリプト
チェックポイント付き・中断再開対応

使い方:
  python scripts/add_stats_columns.py              # 全期間（新しい月から）
  python scripts/add_stats_columns.py --year 2025  # 2025年のみ
  python scripts/add_stats_columns.py --year 2024 --month 6  # 2024年6月のみ
  python scripts/add_stats_columns.py --resume     # 中断から再開
"""
import sys, io as _io
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = _io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = _io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import argparse, json, time, random
import pandas as pd
from pathlib import Path
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent))
from keirin_scraper import (
    fetch, human_wait, batch_rest,
    BATCH_SIZE, BATCH_REST_MIN, BATCH_REST_MAX
)
from bs4 import BeautifulSoup
from io import StringIO

DATA_DIR       = Path('keirin_data')
CHECKPOINT_DIR = DATA_DIR / 'checkpoints'
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

NEW_COLS = ['mark', 'mark_num', 'win_rate_4m', 'top2_rate_4m',
            'top3_rate_4m', 'nige_4m', 'maku_4m']
MARK_MAP = {'◎': 6, '○': 5, '△': 4, '▲': 3, '注': 2, '×': 1}

# ========== 1レースから新カラムのみ取得 ==========

def fetch_new_stats(venue_slug, race_id):
    """leaceページからmark/勝率/連対率のみ抽出。{banum: {cols...}} を返す"""
    url = f'https://keirin.kdreams.jp/{venue_slug}/racedetail/{race_id}/?pageType=showResult'
    resp = fetch(url)
    if resp is None:
        return {}
    try:
        tables = pd.read_html(StringIO(resp.text))
    except Exception:
        return {}

    def flatten_col(c):
        if isinstance(c, tuple):
            return ' '.join(str(x) for x in c if 'Unnamed' not in str(x))
        return str(c)

    def find_col(df, keywords):
        for col in df.columns:
            if all(kw in col for kw in keywords):
                return col
        return None

    # 競走得点・脚質・選手名を含むテーブルを特定
    for t in tables:
        cols_str = ' '.join(str(c) for c in t.columns)
        full_str = cols_str + ' ' + ' '.join(
            str(v) for v in t.values.flatten() if str(v) not in ('nan', 'NaN'))
        full_norm = full_str.replace(' ', '').replace('\u3000', '')
        if '競走得点' in full_norm and '勝率' in full_norm and '選手名' in full_norm:
            t.columns = [flatten_col(c) for c in t.columns]
            col_banum     = find_col(t, ['車 番']) or find_col(t, ['車番'])
            col_mark      = find_col(t, ['予 想']) or find_col(t, ['予想'])
            col_win_rate  = find_col(t, ['勝 率']) or find_col(t, ['勝率'])
            col_top2_rate = find_col(t, ['2連'])
            col_top3_rate = find_col(t, ['3連'])
            col_nige_4m   = find_col(t, ['直近', '逃']) or find_col(t, ['4ヶ月', '逃'])
            col_maku_4m   = find_col(t, ['直近', '捲']) or find_col(t, ['4ヶ月', '捲'])
            if not col_banum:
                continue
            result = {}
            for _, row in t.iterrows():
                try:
                    banum = str(int(float(str(row[col_banum]))))
                except Exception:
                    continue
                mark_raw = str(row[col_mark]).strip() if col_mark else ''
                result[banum] = {
                    'mark':         mark_raw,
                    'mark_num':     MARK_MAP.get(mark_raw, 0),
                    'win_rate_4m':  str(row[col_win_rate])  if col_win_rate  else '',
                    'top2_rate_4m': str(row[col_top2_rate]) if col_top2_rate else '',
                    'top3_rate_4m': str(row[col_top3_rate]) if col_top3_rate else '',
                    'nige_4m':      str(row[col_nige_4m])   if col_nige_4m   else '',
                    'maku_4m':      str(row[col_maku_4m])   if col_maku_4m   else '',
                }
            return result
    return {}

# ========== 月単位で処理 ==========

def process_month(year, month, resume=False):
    csv_path = DATA_DIR / f'{year}_{month:02d}_keirin.csv'
    if not csv_path.exists():
        print(f'  スキップ: {csv_path} が存在しない')
        return

    cp_path = CHECKPOINT_DIR / f'addstats_{year}_{month:02d}.json'

    df = pd.read_csv(csv_path, encoding='utf-8-sig')

    # 既に全カラム追加済みか確認
    if all(c in df.columns for c in NEW_COLS):
        already_filled = df['top3_rate_4m'].notna().sum()
        total_rows = len(df)
        if already_filled > total_rows * 0.9:
            print(f'  {year}-{month:02d}: 既に追加済み ({already_filled}/{total_rows}行) → スキップ')
            return

    # チェックポイント読み込み
    done_race_ids = set()
    if resume and cp_path.exists():
        with open(cp_path, encoding='utf-8') as f:
            cp = json.load(f)
        done_race_ids = set(cp.get('done', []))
        print(f'  復元: {len(done_race_ids)}レース完了済み')

    # 新カラムの初期化
    for col in NEW_COLS:
        if col not in df.columns:
            df[col] = None

    # 処理対象レース
    races = df.drop_duplicates('race_id')[['race_id', 'venue_slug']].values.tolist()
    remaining = [(rid, vs) for rid, vs in races if rid not in done_race_ids]
    print(f'  {year}-{month:02d}: {len(remaining)}/{len(races)}レース処理予定')

    batch_count = 0
    for i, (race_id, venue_slug) in enumerate(tqdm(remaining, desc=f'{year}-{month:02d}')):
        stats = fetch_new_stats(venue_slug, race_id)
        if stats:
            for banum, vals in stats.items():
                mask = (df['race_id'] == race_id) & (df['banum'].astype(str) == banum)
                for col, val in vals.items():
                    df.loc[mask, col] = val
        done_race_ids.add(race_id)
        human_wait()

        if (i + 1) % BATCH_SIZE == 0:
            batch_count += 1
            # 中間保存
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            with open(cp_path, 'w', encoding='utf-8') as f:
                json.dump({'done': list(done_race_ids)}, f)
            batch_rest(batch_count)

    # 最終保存
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    with open(cp_path, 'w', encoding='utf-8') as f:
        json.dump({'done': list(done_race_ids)}, f)

    filled = df['top3_rate_4m'].notna().sum()
    print(f'  → 保存完了: {filled}/{len(df)}行に新カラム追加')

# ========== メイン ==========

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--year',  type=int, default=None)
    parser.add_argument('--month', type=int, default=None)
    parser.add_argument('--resume', action='store_true')
    args = parser.parse_args()

    # 対象月リストを作成（新しい月から処理）
    import glob as _glob
    all_files = sorted(_glob.glob(str(DATA_DIR / '202[2345]_*.csv')), reverse=True)
    targets = []
    for f in all_files:
        stem = Path(f).stem  # e.g. 2025_12_keirin
        parts = stem.split('_')
        if len(parts) < 2:
            continue
        y, m = int(parts[0]), int(parts[1])
        if args.year and y != args.year:
            continue
        if args.month and m != args.month:
            continue
        targets.append((y, m))

    print(f'処理対象: {len(targets)}ヶ月分')
    total_races = 0
    for y, m in targets:
        df = pd.read_csv(DATA_DIR / f'{y}_{m:02d}_keirin.csv', encoding='utf-8-sig')
        total_races += df['race_id'].nunique()
    print(f'総レース数: {total_races:,}  推定時間: {total_races*1.5/3600:.1f}〜{total_races*2.5/3600:.1f}時間')
    print()

    for y, m in targets:
        print(f'\n=== {y}-{m:02d} ===')
        process_month(y, m, resume=args.resume)

    print('\n全処理完了！')

if __name__ == '__main__':
    main()
