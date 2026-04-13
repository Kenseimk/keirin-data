# -*- coding: utf-8 -*-
import sys, io as _io
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = _io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = _io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

"""
ワイド払戻データ遡及補完スクリプト

特徴:
  - 既存CSVのrace_idごとにワイド払戻だけを取得してCSVを更新
  - チェックポイントで途中再開可能
  - --max_runtime_minutes で時間制限前に安全終了 (GitHub Actions用)
  - BAN対策: ランダム長休憩 / Refererヘッダー / セッションローテ / バックオフ

Usage:
  python scripts/patch_wide.py                            # 全CSV
  python scripts/patch_wide.py --year 2022                # 1年分
  python scripts/patch_wide.py --year 2022 --month 3      # 1ヶ月分
  python scripts/patch_wide.py --resume                   # チェックポイントから継続
  python scripts/patch_wide.py --year 2022 --resume --max_runtime_minutes 330
"""

import argparse
import glob
import json
import random
import re
import time
from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

# ========== 設定 ==========
BASE_URL    = "https://keirin.kdreams.jp"
DATA_DIR    = Path("keirin_data")
CKPT_FILE   = DATA_DIR / "checkpoints" / "patch_wide_done.txt"

# ---- BAN対策パラメータ（スクレイパー本体より保守的）----
REQ_MIN     = 2.0    # リクエスト間隔 最短(秒)
REQ_MAX     = 4.0    # リクエスト間隔 最長(秒)
BATCH_SIZE  = 15     # バッチサイズ
BATCH_REST_MIN = 20  # バッチ後休憩 最短(秒)
BATCH_REST_MAX = 45  # バッチ後休憩 最長(秒)
LONG_BREAK_EVERY = random.randint(80, 130)  # N回ごとに長休憩
LONG_BREAK_MIN   = 60   # 長休憩 最短(秒)
LONG_BREAK_MAX   = 150  # 長休憩 最長(秒)
SESSION_REFRESH  = random.randint(40, 70)   # Nリクエストごとにセッション刷新
BACKOFF_BASE     = 15.0

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
]

# ========== セッション管理 ==========
_session      = None
_session_count = 0

def make_headers(referer=None):
    h = {
        "User-Agent":              random.choice(UA_POOL),
        "Accept":                  "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language":         "ja,en-US;q=0.9,en;q=0.8",
        "Accept-Encoding":         "gzip, deflate, br",
        "Connection":              "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest":          "document",
        "Sec-Fetch-Mode":          "navigate",
        "Sec-Fetch-Site":          "same-origin" if referer else "none",
        "Cache-Control":           "max-age=0",
    }
    if referer:
        h["Referer"] = referer
    return h

def new_session():
    global _session_count
    s = requests.Session()
    ua = random.choice(UA_POOL)
    s.headers.update(make_headers())
    try:
        # トップページを経由してcookie取得（自然なナビゲーションに見せる）
        s.get(f"{BASE_URL}/", headers=make_headers(), timeout=12)
        time.sleep(random.uniform(1.5, 3.5))
    except Exception:
        pass
    _session_count = 0
    return s

def get_session():
    global _session, _session_count
    if _session is None or _session_count >= SESSION_REFRESH:
        if _session is not None:
            print(f"    [セッション刷新 {_session_count}req]")
        _session = new_session()
    _session_count += 1
    return _session

def fetch_with_referer(url, venue_slug, retries=4):
    """Refererを付けてフェッチ（レース結果ページは会場一覧ページから遷移する想定）"""
    referer = f"{BASE_URL}/{venue_slug}/"
    session = get_session()
    for attempt in range(retries):
        try:
            resp = session.get(url, headers=make_headers(referer=referer), timeout=18)
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 404:
                return None
            elif resp.status_code in (429, 503):
                wait = BACKOFF_BASE * (2 ** attempt) + random.uniform(5, 15)
                print(f"\n    [!] HTTP {resp.status_code} → {wait:.0f}秒後リトライ (試行{attempt+1}/{retries})")
                time.sleep(wait)
                global _session
                _session = None
            elif resp.status_code in (403, 406):
                wait = BACKOFF_BASE * (attempt + 2)
                print(f"\n    [!] HTTP {resp.status_code} → {wait:.0f}秒後リトライ")
                time.sleep(wait)
                _session = None
            else:
                time.sleep(BACKOFF_BASE)
        except requests.exceptions.ConnectionError:
            time.sleep(BACKOFF_BASE * (attempt + 1))
        except requests.exceptions.Timeout:
            print(f"\n    [!] タイムアウト (試行{attempt+1}/{retries})")
            time.sleep(BACKOFF_BASE)
        except Exception as e:
            print(f"\n    [!] エラー: {e}")
            time.sleep(BACKOFF_BASE)
    return None

def human_wait():
    time.sleep(random.uniform(REQ_MIN, REQ_MAX))

def batch_rest(n):
    global _session
    wait = random.uniform(BATCH_REST_MIN, BATCH_REST_MAX)
    print(f"\n  [バッチ{n}完了] {wait:.0f}秒休憩...")
    time.sleep(wait)
    # バッチ後はセッション刷新
    _session = None

def long_break(req_count):
    wait = random.uniform(LONG_BREAK_MIN, LONG_BREAK_MAX)
    print(f"\n  [長休憩 {req_count}req時点] {wait:.0f}秒...")
    time.sleep(wait)

# ========== ワイド払戻パーサー ==========

def fetch_wide(venue_slug, race_id):
    """レースページからワイド払戻のみ抽出。{(a,b): payout} dict を返す。失敗時は None"""
    url = f"{BASE_URL}/{venue_slug}/racedetail/{race_id}/?pageType=showResult"
    resp = fetch_with_referer(url, venue_slug)
    if resp is None:
        return None

    try:
        tables = pd.read_html(StringIO(resp.text))
    except Exception:
        return None

    wide_list = []
    try:
        for t in tables:
            t_str_norm = t.to_string().replace(" ", "").replace("\u3000", "")
            if "ワイド" not in t_str_norm:
                continue
            for row_vals_raw in t.values:
                vals_norm = [str(v).replace(" ", "").replace("\u3000", "") for v in row_vals_raw]
                vals_orig = [str(v) for v in row_vals_raw]
                for j, v in enumerate(vals_norm):
                    is_wide = (v == "ワイド") or (v == "ド" and j > 0 and "ワイ" in vals_norm[j - 1])
                    if is_wide and j + 1 < len(vals_orig):
                        nxt = vals_orig[j + 1].strip()
                        if nxt and nxt not in ("nan", "NaN"):
                            wide_list.append(nxt)
            break  # 払戻テーブルは1つだけ
    except Exception:
        pass

    return " / ".join(wide_list) if wide_list else ""

# ========== チェックポイント ==========

def load_done():
    CKPT_FILE.parent.mkdir(parents=True, exist_ok=True)
    if CKPT_FILE.exists():
        return set(CKPT_FILE.read_text(encoding='utf-8').splitlines())
    return set()

def save_done(done_set):
    CKPT_FILE.parent.mkdir(parents=True, exist_ok=True)
    CKPT_FILE.write_text("\n".join(sorted(done_set)), encoding='utf-8')

# ========== メイン ==========

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year",  type=int, default=None, help="処理する年")
    parser.add_argument("--month", type=int, default=None, help="処理する月")
    parser.add_argument("--resume", action="store_true", help="チェックポイントから継続")
    parser.add_argument("--max_runtime_minutes", type=int, default=0,
                        help="最大実行時間(分)。超える前に安全終了 (0=無制限)")
    args = parser.parse_args()

    start_time = time.time()
    def time_remaining():
        if args.max_runtime_minutes <= 0:
            return float("inf")
        elapsed = (time.time() - start_time) / 60
        return args.max_runtime_minutes - elapsed

    # 対象CSVを選定
    if args.year and args.month:
        pattern = str(DATA_DIR / f"{args.year}_{args.month:02d}_keirin.csv")
    elif args.year:
        pattern = str(DATA_DIR / f"{args.year}_*_keirin.csv")
    else:
        pattern = str(DATA_DIR / "202[2-9]_*_keirin.csv")

    csv_files = sorted(f for f in glob.glob(pattern) if "sample" not in f)
    if not csv_files:
        print(f"対象CSVなし: {pattern}")
        return

    done = load_done() if args.resume else set()
    print(f"対象CSV: {len(csv_files)}件")
    print(f"チェックポイント済み race_id: {len(done)}件")
    if args.max_runtime_minutes > 0:
        print(f"最大実行時間: {args.max_runtime_minutes}分")

    total_updated  = 0
    total_skipped  = 0
    req_count      = 0
    time_limit_hit = False
    next_long_break = LONG_BREAK_EVERY

    for csv_path in csv_files:
        csv_path = Path(csv_path)

        # 時間チェック（CSV単位）
        remaining = time_remaining()
        if remaining < 10:
            print(f"\n[時間制限] 残り{remaining:.1f}分 → 安全終了")
            time_limit_hit = True
            break

        print(f"\n{'='*60}")
        print(f"処理中: {csv_path.name}  (残り{remaining:.0f}分)")

        df = pd.read_csv(csv_path, encoding='utf-8-sig')

        # wide列がなければ空で初期化
        if 'wide' not in df.columns:
            df['wide'] = ''
        else:
            df['wide'] = df['wide'].fillna('')

        race_info = df.drop_duplicates('race_id')[['race_id','venue_slug','wide']].copy()
        needs_patch = race_info[race_info['wide'] == '']['race_id'].tolist()
        if args.resume:
            needs_patch = [r for r in needs_patch if str(r) not in done]

        total_in_csv = len(race_info)
        print(f"  全{total_in_csv}レース / wideなし{len(race_info[race_info['wide']==''])} / 今回対象{len(needs_patch)}")

        if not needs_patch:
            print("  スキップ (全済み)")
            continue

        venue_map = race_info.set_index('race_id')['venue_slug'].to_dict()
        wide_map  = {}

        for i, race_id in enumerate(tqdm(needs_patch, desc=f"  {csv_path.stem}")):
            # 時間チェック（レース単位）
            remaining = time_remaining()
            if remaining < 10:
                print(f"\n  [時間制限] 残り{remaining:.1f}分 → このCSVを中断")
                time_limit_hit = True
                break

            venue_slug = venue_map.get(race_id, "")
            result = fetch_wide(str(venue_slug), str(race_id))
            req_count += 1

            if result is None:
                total_skipped += 1
            else:
                wide_map[race_id] = result
                done.add(str(race_id))
                total_updated += 1

            # リクエスト間隔
            human_wait()

            # バッチ休憩
            if (i + 1) % BATCH_SIZE == 0:
                batch_rest((i + 1) // BATCH_SIZE)
                save_done(done)

            # 長休憩
            if req_count >= next_long_break:
                long_break(req_count)
                next_long_break = req_count + random.randint(LONG_BREAK_EVERY - 20, LONG_BREAK_EVERY + 30)

        # DataFrameに反映してCSV保存
        if wide_map:
            race_id_col = df['race_id'].astype(str)
            for rid, val in wide_map.items():
                df.loc[race_id_col == str(rid), 'wide'] = val
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print(f"  保存: {len(wide_map)}件更新 → {csv_path.name}")

        save_done(done)

        if time_limit_hit:
            break

    # 残り件数を計算して報告
    remaining_count = 0
    for csv_path in csv_files:
        try:
            df_check = pd.read_csv(csv_path, encoding='utf-8-sig')
            if 'wide' not in df_check.columns:
                remaining_count += df_check['race_id'].nunique()
            else:
                empty = (df_check['wide'].fillna('') == '')
                ids = df_check.loc[empty, 'race_id'].astype(str).unique()
                remaining_count += sum(1 for i in ids if i not in done)
        except Exception:
            pass

    print(f"\n{'='*60}")
    print(f"完了: 更新{total_updated}件 / フェッチ失敗{total_skipped}件")
    print(f"残り未処理: {remaining_count}件")
    elapsed_min = (time.time() - start_time) / 60
    print(f"経過時間: {elapsed_min:.1f}分")

    # GitHub Actions向け出力
    with open("patch_wide_status.txt", "w", encoding="utf-8") as f:
        f.write(f"updated={total_updated}\n")
        f.write(f"skipped={total_skipped}\n")
        f.write(f"remaining={remaining_count}\n")
        f.write(f"time_limit_hit={'true' if time_limit_hit else 'false'}\n")
        f.write(f"elapsed_minutes={elapsed_min:.1f}\n")

if __name__ == "__main__":
    main()
