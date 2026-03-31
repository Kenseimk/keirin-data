"""
特定日付のみスクレイピングして既存CSVに追記するスクリプト
Usage: python scrape_day.py --date 2026-03-30
"""

import argparse
import sys
from pathlib import Path

# keirin_scraper の関数を流用（io設定はkeirin_scraperに任せる）
sys.path.insert(0, str(Path(__file__).parent))
from keirin_scraper import (
    get_races_for_day, parse_race,
    human_wait, BATCH_SIZE, batch_rest
)

import pandas as pd
from tqdm import tqdm

OUTPUT_DIR = Path("./keirin_data")

def scrape_day(year, month, day):
    print(f"\n=== {year}-{month:02d}-{day:02d} のレースを取得 ===")
    races = get_races_for_day(year, month, day)
    print(f"  {len(races)}レース発見")
    if not races:
        print("  レースなし")
        return []

    rows = []
    for i, (venue_slug, race_id) in enumerate(tqdm(races, desc="取得中")):
        result = parse_race(venue_slug, race_id)
        rows.extend(result)
        human_wait()
        if (i + 1) % BATCH_SIZE == 0:
            batch_rest((i + 1) // BATCH_SIZE)

    print(f"  取得行数: {len(rows)}")
    return rows

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="例: 2026-03-30")
    args = parser.parse_args()

    year, month, day = map(int, args.date.split("-"))
    rows = scrape_day(year, month, day)
    if not rows:
        print("データなし、終了")
        return

    df_new = pd.DataFrame(rows)
    df_new["date"] = args.date  # race_idに埋まる開催初日ではなく実際のスクレイピング日付で上書き

    # 既存CSVに追記
    csv_path = OUTPUT_DIR / f"{year}_{month:02d}_keirin.csv"
    if csv_path.exists():
        df_existing = pd.read_csv(csv_path, encoding="utf-8-sig")
        # 重複除去（race_id単位）型不一致(int/str)対策で文字列に統一して比較
        if "race_id" in df_existing.columns and "race_id" in df_new.columns:
            new_ids = df_new["race_id"].astype(str).unique()
            df_existing = df_existing[~df_existing["race_id"].astype(str).isin(new_ids)]
        df_out = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_out = df_new

    df_out.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"\n保存完了: {csv_path} ({len(df_out)}行)")
    print(f"追加分: {len(df_new)}行")

if __name__ == "__main__":
    main()
