"""
競輪データ収集スクリプト v3.0
ソース: 楽天Kドリームス (keirin.kdreams.jp)

【v3.0 修正点】
  - 正しいURL構造: /{venue_slug}/racedetail/{race_id}/?pageType=showResult
  - venue_slugを日付別一覧ページから自動取得
  - テーブル番号を実データで確認済み（[1][33][34][35]）
  - ban対策（ランダム待機・UAローテ・セッション管理・指数バックオフ）
  - チェックポイント保存/再開

使い方:
  pip install requests beautifulsoup4 pandas tqdm lxml html5lib
  python keirin_scraper.py --year 2024 --month 10
  python keirin_scraper.py --year 2024 --start_month 1 --end_month 12
  python keirin_scraper.py --year 2024 --month 10 --resume
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time, re, json, random, argparse
from datetime import datetime
from tqdm import tqdm
from pathlib import Path

# ========== 設定 ==========
BASE_URL = "https://keirin.kdreams.jp"
OUTPUT_DIR = Path("./keirin_data")
CHECKPOINT_DIR = OUTPUT_DIR / "checkpoints"

INTERVAL_MIN = 1.0      # 短縮（2.5→1.0秒）
INTERVAL_MAX = 2.0      # 短縮（5.5→2.0秒）
BATCH_SIZE = 20
BATCH_REST_MIN = 5      # 短縮（15→5秒）
BATCH_REST_MAX = 10     # 短縮（30→10秒）
BACKOFF_BASE = 10.0

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
]

def make_headers(ua=None):
    return {
        "User-Agent": ua or random.choice(UA_POOL),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Cache-Control": "max-age=0",
    }

# ========== セッション管理 ==========
_session = None
_session_count = 0
SESSION_REFRESH = 100

def new_session():
    s = requests.Session()
    s.headers.update(make_headers())
    try:
        s.get(f"{BASE_URL}/", timeout=10)
        time.sleep(random.uniform(1.5, 3.0))
    except Exception:
        pass
    return s

def get_session():
    global _session, _session_count
    if _session is None or _session_count >= SESSION_REFRESH:
        _session = new_session()
        _session_count = 0
    _session_count += 1
    return _session

def human_wait(extra=0.0):
    time.sleep(random.uniform(INTERVAL_MIN, INTERVAL_MAX) + extra)

def batch_rest(n):
    global _session
    wait = random.uniform(BATCH_REST_MIN, BATCH_REST_MAX)
    print(f"\n  ☕ バッチ{n}完了 - {wait:.0f}秒休憩...")
    time.sleep(wait)
    _session = None

def fetch(url, retries=4):
    session = get_session()
    for attempt in range(retries):
        try:
            resp = session.get(url, headers=make_headers(), timeout=15)
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 404:
                return None
            elif resp.status_code in (429, 503):
                wait = BACKOFF_BASE * (2 ** attempt) + random.uniform(0, 5)
                print(f"\n  ⚠️  HTTP {resp.status_code} → {wait:.0f}秒後リトライ")
                time.sleep(wait)
                global _session
                _session = None
            elif resp.status_code in (403, 406):
                time.sleep(BACKOFF_BASE * (attempt + 1))
            else:
                time.sleep(BACKOFF_BASE)
        except requests.exceptions.ConnectionError:
            time.sleep(BACKOFF_BASE * (attempt + 1))
        except requests.exceptions.Timeout:
            time.sleep(BACKOFF_BASE)
        except Exception as e:
            print(f"  エラー: {e}")
            time.sleep(BACKOFF_BASE)
    return None

# ========== チェックポイント ==========

def save_checkpoint(year, month, done_items, rows):
    """done_items = [(venue_slug, race_id), ...]"""
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    cp = {
        "year": year, "month": month,
        "done_items": done_items,
        "saved_at": datetime.now().isoformat(),
    }
    with open(CHECKPOINT_DIR / f"{year}_{month:02d}_checkpoint.json", "w", encoding="utf-8") as f:
        json.dump(cp, f, ensure_ascii=False, indent=2)
    if rows:
        pd.DataFrame(rows).to_csv(
            CHECKPOINT_DIR / f"{year}_{month:02d}_partial.csv",
            index=False, encoding="utf-8-sig"
        )

def load_checkpoint(year, month):
    cp_path = CHECKPOINT_DIR / f"{year}_{month:02d}_checkpoint.json"
    tmp_path = CHECKPOINT_DIR / f"{year}_{month:02d}_partial.csv"
    if not cp_path.exists():
        return [], []
    with open(cp_path, encoding="utf-8") as f:
        cp = json.load(f)
    done_items = [tuple(x) for x in cp.get("done_items", [])]
    rows = []
    if tmp_path.exists():
        rows = pd.read_csv(tmp_path, encoding="utf-8-sig").to_dict(orient="records")
    print(f"  📂 復元: {len(done_items)}件完了済み ({cp['saved_at']})")
    return done_items, rows

# ========== Step1: 日付別ページからレース一覧取得 ==========

def get_races_for_day(year, month, day):
    """
    返り値: [(venue_slug, race_id), ...]
    URL例: /raceresult/2024/10/01/
    リンク例: /hakodate/racedetail/1120241001010001/?pageType=showResult
    """
    url = f"{BASE_URL}/raceresult/{year}/{month:02d}/{day:02d}/"
    resp = fetch(url)
    if resp is None:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    races = []
    seen = set()
    for a in soup.find_all("a", href=True):
        m = re.search(r"/([a-z]+)/racedetail/(\d+)/", a["href"])
        if m:
            slug, rid = m.group(1), m.group(2)
            key = (slug, rid)
            if key not in seen:
                seen.add(key)
                races.append(key)
    return races

def get_races_for_month(year, month):
    if month == 12:
        days = (datetime(year + 1, 1, 1) - datetime(year, 12, 1)).days
    else:
        days = (datetime(year, month + 1, 1) - datetime(year, month, 1)).days

    all_races = []
    print(f"\n📅 {year}年{month}月 レース一覧取得中...")
    for day in tqdm(range(1, days + 1), desc="日付スキャン"):
        all_races.extend(get_races_for_day(year, month, day))
        human_wait()
    print(f"  → {len(all_races)}レース発見")
    return all_races

# ========== Step2: レース詳細ページをパース ==========

def parse_race(venue_slug, race_id):
    """
    1ページに出走表・結果・払戻が全て含まれる。
    テーブル構成（デバッグ確認済み）:
      [1]  出走表メイン（車番・選手名・級班・脚質・競走得点）
      [33] ライン情報（予想・周回・戦法）
      [34] レース結果（着順・車番・選手名・上り・決まり手）
      [35] 払戻金（二車単・三連単等）
    """
    url = f"{BASE_URL}/{venue_slug}/racedetail/{race_id}/?pageType=showResult"
    resp = fetch(url)
    if resp is None:
        return []

    # race_idから日付・競輪場・レース番号を解析
    # 構造: venue_id(2) + year(4) + month(2) + day(2) + 開催回(2) + レースNo(4)
    date_str = f"{race_id[2:6]}-{race_id[6:8]}-{race_id[8:10]}"
    try:
        race_no = int(race_id[12:16])
    except (ValueError, IndexError):
        race_no = None

    try:
        tables = pd.read_html(url)
    except Exception as e:
        print(f"  ⚠️  テーブル取得失敗 {race_id}: {e}")
        return []

    # --- 出走表テーブルを特定（列数に依存しないキーワード検索）---
    racecard_df = None
    for t in tables:
        cols_str = " ".join(str(c) for c in t.columns)
        if "競走得点" in cols_str and "脚質" in cols_str and "選手名" in cols_str:
            racecard_df = t.copy()
            break
    if racecard_df is None:
        # デバッグ: テーブル一覧を表示
        col_previews = [" ".join(str(c) for c in t.columns)[:50] for t in tables[:5]]
        print(f"  ⚠️  出走表テーブル見つからず {race_id}: テーブル数={len(tables)}, 列プレビュー={col_previews}")
        return []

    # 列名を文字列にフラット化（MultiIndexの場合も対応）
    def flatten_col(c):
        if isinstance(c, tuple):
            return " ".join(str(x) for x in c if "Unnamed" not in str(x))
        return str(c)
    racecard_df.columns = [flatten_col(c) for c in racecard_df.columns]

    # 必要な列をキーワードで特定
    def find_col(df, keywords):
        for col in df.columns:
            if all(kw in col for kw in keywords):
                return col
        return None

    col_banum   = find_col(racecard_df, ["車 番"]) or find_col(racecard_df, ["車番"])
    col_name    = find_col(racecard_df, ["選手名"])
    col_class   = find_col(racecard_df, ["級 班"]) or find_col(racecard_df, ["級班"])
    col_style   = find_col(racecard_df, ["脚 質"]) or find_col(racecard_df, ["脚質"])
    col_gear    = find_col(racecard_df, ["ギヤ"])
    col_score   = find_col(racecard_df, ["競走得点"])

    # --- ライン情報（テーブルを番号でなく内容で特定）---
    lineup_text = ""
    try:
        for t in tables:
            t_str = t.to_string()
            if "最 終 B S" in t_str or "周 回 ・ 戦 法" in t_str:
                if len(t) >= 2:
                    marks = [str(v) for v in t.iloc[0].values if str(v) not in ("nan", "NaN")]
                    banums = [str(v) for v in t.iloc[1].values if str(v) not in ("nan", "NaN")]
                    lineup_text = " ".join(f"{m}{b}" for m, b in zip(marks, banums))
                break
    except Exception:
        pass

    # --- レース結果テーブル（内容で特定）---
    result_lookup = {}
    try:
        for t in tables:
            cols_str = " ".join(str(c) for c in t.columns)
            if "着 順" in cols_str or "着順" in cols_str:
                t.columns = [flatten_col(c) for c in t.columns]
                rc_banum  = find_col(t, ["車 番"]) or find_col(t, ["車番"])
                rc_rank   = find_col(t, ["着 順"]) or find_col(t, ["着順"])
                rc_agari  = find_col(t, ["上り"]) or find_col(t, ["上 り"])
                rc_finish = find_col(t, ["決ま"]) or find_col(t, ["決まり手"])
                rc_margin = find_col(t, ["着差"])
                rc_winlose= find_col(t, ["勝敗因"])
                if not (rc_banum and rc_rank):
                    continue
                for _, row in t.iterrows():
                    try:
                        banum = str(int(float(str(row[rc_banum]))))
                        result_lookup[banum] = {
                            "rank":        str(row[rc_rank]),
                            "agari":       str(row[rc_agari])   if rc_agari   else "",
                            "finish_type": str(row[rc_finish])  if rc_finish  else "",
                            "margin":      str(row[rc_margin])  if rc_margin  else "",
                            "win_lose":    str(row[rc_winlose]) if rc_winlose else "",
                        }
                    except Exception:
                        continue
                break
    except Exception:
        pass

    # --- 払戻金テーブル（内容で特定）---
    payout_text = ""
    try:
        for t in tables:
            t_str = t.to_string()
            if "2 車 連 単" in t_str or "3 連 勝 単" in t_str or "二車連単" in t_str:
                payout_text = t_str[:300]
                break
    except Exception:
        pass

    # --- 選手単位のレコードに変換 ---
    rows = []
    for _, player in racecard_df.iterrows():
        try:
            banum_raw = player[col_banum] if col_banum else ""
            banum = str(int(float(str(banum_raw))))
        except Exception:
            continue

        # 選手名・府県・年齢・期別を分割（例: "外崎 一成  北海道/37/94"）
        raw_name = str(player[col_name]) if col_name else ""
        name, pref, age, term = "", "", "", ""
        m = re.match(r"(.+?)\s+(\S+)/(\d+)/(\d+)", raw_name)
        if m:
            name = m.group(1).strip()
            pref = m.group(2)
            age  = m.group(3)
            term = m.group(4)
        else:
            name = raw_name

        row = {
            "race_id":      race_id,
            "venue_slug":   venue_slug,
            "date":         date_str,
            "race_no":      race_no,
            "lineup":       lineup_text,
            "payout":       payout_text[:200],
            # 選手情報
            "banum":        banum,
            "player_name":  name,
            "pref":         pref,
            "age":          age,
            "term":         term,
            "player_class": str(player[col_class])  if col_class  else "",
            "running_style":str(player[col_style])  if col_style  else "",
            "gear":         str(player[col_gear])   if col_gear   else "",
            "race_score":   str(player[col_score])  if col_score  else "",
        }

        # 結果を結合
        if banum in result_lookup:
            row.update(result_lookup[banum])
        else:
            row.update({"rank": "", "agari": "", "finish_type": "", "margin": "", "win_lose": ""})

        rows.append(row)

    return rows

# ========== メイン処理 ==========

def scrape_month(year, month, resume=False):
    print(f"\n{'='*55}")
    print(f"🚴 競輪データ収集 v3.0: {year}年{month}月")
    print(f"{'='*55}")

    done_items, all_rows = [], []
    if resume:
        done_items, all_rows = load_checkpoint(year, month)

    all_races = get_races_for_month(year, month)
    if not all_races:
        print("⚠️  レースが見つかりませんでした")
        return []

    done_set = set(done_items)
    remaining = [r for r in all_races if r not in done_set]
    print(f"  未処理: {len(remaining)}件 / 全{len(all_races)}件")

    batch_count = 0
    parse_success = 0
    parse_empty = 0

    for i, (venue_slug, race_id) in enumerate(tqdm(remaining, desc="レース取得")):
        try:
            rows = parse_race(venue_slug, race_id)
            if rows:
                parse_success += 1
                all_rows.extend(rows)
            else:
                parse_empty += 1
                if parse_empty <= 5:  # 最初の5件だけ警告表示
                    print(f"  ⚠️  空データ: {venue_slug}/{race_id}")
            done_items.append((venue_slug, race_id))
            human_wait()

            if (i + 1) % BATCH_SIZE == 0:
                batch_count += 1
                save_checkpoint(year, month, done_items, all_rows)
                print(f"  📊 進捗: 成功={parse_success} 空={parse_empty} 累計行数={len(all_rows)}")
                batch_rest(batch_count)

        except KeyboardInterrupt:
            print("\n\n⚡ 中断 → チェックポイント保存中...")
            save_checkpoint(year, month, done_items, all_rows)
            print("  → 次回: --resume で再開")
            raise
        except Exception as e:
            print(f"  ⚠️  スキップ {venue_slug}/{race_id}: {e}")
            continue

    print(f"\n📊 最終集計: 成功={parse_success} 空={parse_empty} 累計行数={len(all_rows)}")
    save_checkpoint(year, month, done_items, all_rows)
    return all_rows

def save_month_csv(year, month, rows):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fp = OUTPUT_DIR / f"{year}_{month:02d}_keirin.csv"
    pd.DataFrame(rows).to_csv(fp, index=False, encoding="utf-8-sig")
    print(f"\n💾 保存: {fp} ({len(rows)}行)")
    return fp

def main():
    parser = argparse.ArgumentParser(description="競輪データ収集 v3.0")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int)
    parser.add_argument("--start_month", type=int)
    parser.add_argument("--end_month", type=int)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    if args.month:
        months = [args.month]
    elif args.start_month and args.end_month:
        months = list(range(args.start_month, args.end_month + 1))
    else:
        print("エラー: --month または --start_month/--end_month を指定してください")
        return

    all_total = []
    for month in months:
        rows = scrape_month(args.year, month, args.resume)
        print(f"\n📋 scrape_month 戻り値: {len(rows)}行")
        # rowsが空でもCSVを保存（デバッグ用・auto_runner.pyがCSV存在確認するため）
        save_month_csv(args.year, month, rows)
        if rows:
            all_total.extend(rows)
            print(f"  サンプル（先頭3行）:")
            print(pd.DataFrame(rows).head(3).to_string())
        else:
            print(f"  ⚠️  データが空です。parse_raceの戻り値を確認してください。")

    if len(months) > 1 and all_total:
        fp = OUTPUT_DIR / f"{args.year}_all_keirin.csv"
        pd.DataFrame(all_total).to_csv(fp, index=False, encoding="utf-8-sig")
        print(f"\n✅ 年間統合: {fp} ({len(all_total)}行)")

    print("\n🎉 完了！")

if __name__ == "__main__":
    main()
