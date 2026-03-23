"""
auto_runner.py
「どの年月を取得するか」を自動判断してスクレイパーを呼び出す。
Discord通知（開始・完了・エラー）も担当。

判断ロジック:
  1. keirin_data/ に存在するCSVを確認
  2. 2024年1月から現在月まで、未取得 or 未完了の月を探す
  3. 1ヶ月分だけ処理（GitHub Actions の6時間制限内に収める）
  4. 全月完了したら「収集完了」通知を送って終了
"""

import os
import sys
import json
import requests
import subprocess
from datetime import datetime
from pathlib import Path

# ========== 設定 ==========
DATA_DIR       = Path("keirin_data")
CHECKPOINT_DIR = DATA_DIR / "checkpoints"
START_YEAR     = 2023
START_MONTH    = 1

DISCORD_WEBHOOK  = os.environ.get("DISCORD_WEBHOOK_URL", "")
NOTION_TOKEN     = os.environ.get("NOTION_TOKEN", "")
NOTION_DB_ID     = "deb76e05-64ed-4a64-9fbd-881df8cd903a"  # 🚴 競輪スクレイピングログDB
FORCE_YEAR  = os.environ.get("FORCE_YEAR", "").strip()
FORCE_MONTH = os.environ.get("FORCE_MONTH", "").strip()

# ========== Notion通知 ==========

def notion_log(title, status, year, month, race_count=0, row_count=0, elapsed_min=0, error_msg=""):
    """NotionのスクレイピングログDBにレコードを追加"""
    if not NOTION_TOKEN:
        print(f"[Notion通知スキップ] {title}")
        return
    now = datetime.now()
    payload = {
        "parent": {"database_id": NOTION_DB_ID},
        "properties": {
            "タイトル":      {"title": [{"text": {"content": title}}]},
            "ステータス":    {"select": {"name": status}},
            "対象年月":      {"rich_text": [{"text": {"content": f"{year}年{month}月"}}]},
            "取得レース数":  {"number": race_count},
            "総行数":        {"number": row_count},
            "実行日時":      {"date": {"start": now.strftime("%Y-%m-%dT%H:%M:%S"), "time_zone": "Asia/Tokyo"}},
            "実行時間(分)":  {"number": round(elapsed_min, 1)},
            "エラー内容":    {"rich_text": [{"text": {"content": error_msg[:500]}}]},
            "GitHubリポジトリ": {"url": os.environ.get("GITHUB_SERVER_URL", "") + "/" + os.environ.get("GITHUB_REPOSITORY", "") or None},
        }
    }
    # url が空の場合はNoneにする（Notionのurl型はNone不可のため除外）
    if not payload["properties"]["GitHubリポジトリ"]["url"]:
        del payload["properties"]["GitHubリポジトリ"]
    try:
        r = requests.post(
            "https://api.notion.com/v1/pages",
            headers={
                "Authorization": f"Bearer {NOTION_TOKEN}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=15,
        )
        r.raise_for_status()
        print(f"  📝 Notionログ記録: {title}")
    except Exception as e:
        print(f"  Notion通知失敗: {e}")

# ========== Discord通知 ==========

def notify(title: str, description: str, color: int = 0x3498db):
    """Discordにembedメッセージを送信"""
    if not DISCORD_WEBHOOK:
        print(f"[Discord通知スキップ] {title}: {description}")
        return
    payload = {
        "embeds": [{
            "title": title,
            "description": description,
            "color": color,
            "footer": {"text": f"keirin-scraper • {datetime.now().strftime('%Y-%m-%d %H:%M JST')}"}
        }]
    }
    try:
        r = requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
        r.raise_for_status()
    except Exception as e:
        print(f"Discord通知失敗: {e}")

def notify_start(year, month, remaining_months):
    notify(
        title="🚴 競輪データ収集 開始",
        description=(
            f"**対象:** {year}年{month}月\n"
            f"**残り未取得月数:** {remaining_months}ヶ月\n"
            f"**実行環境:** GitHub Actions"
        ),
        color=0x3498db  # 青
    )

def notify_done(year, month, race_count, row_count):
    notify(
        title="✅ 収集完了",
        description=(
            f"**対象:** {year}年{month}月\n"
            f"**取得レース数:** {race_count:,}レース\n"
            f"**総行数:** {row_count:,}行\n"
            f"**CSV:** keirin_data/{year}_{month:02d}_keirin.csv"
        ),
        color=0x2ecc71  # 緑
    )

def notify_all_done(total_months, total_races):
    notify(
        title="🎉 全月収集完了！",
        description=(
            f"**収集完了月数:** {total_months}ヶ月\n"
            f"**推定総レース数:** {total_races:,}レース以上\n"
            f"スコアモデルの構築を開始できます。"
        ),
        color=0xf1c40f  # 金
    )

def notify_error(year, month, error_msg):
    notify(
        title="❌ エラー発生",
        description=(
            f"**対象:** {year}年{month}月\n"
            f"**エラー:** {error_msg[:300]}\n"
            f"次回の実行で `--resume` により自動再試行します。"
        ),
        color=0xe74c3c  # 赤
    )

# ========== 取得状況の確認 ==========

def get_target_months():
    """2024年1月〜現在月のリストを返す"""
    now = datetime.now()
    months = []
    year, month = START_YEAR, START_MONTH
    while (year, month) <= (now.year, now.month):
        months.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months

def is_month_complete(year, month):
    """
    月が完了しているか判定。
    完了条件: keirin_data/{year}_{month:02d}_keirin.csv が存在する
    """
    csv_path = DATA_DIR / f"{year}_{month:02d}_keirin.csv"
    return csv_path.exists()

def is_month_partial(year, month):
    """チェックポイントが存在する（途中まで取得済み）"""
    cp_path = CHECKPOINT_DIR / f"{year}_{month:02d}_checkpoint.json"
    return cp_path.exists()

def get_checkpoint_progress(year, month):
    """チェックポイントから進捗を取得"""
    cp_path = CHECKPOINT_DIR / f"{year}_{month:02d}_checkpoint.json"
    if not cp_path.exists():
        return 0
    with open(cp_path) as f:
        cp = json.load(f)
    return len(cp.get("done_items", []))

def find_next_target():
    """次に処理すべき（year, month）を返す。全完了ならNone"""
    for year, month in get_target_months():
        if not is_month_complete(year, month):
            return year, month
    return None

def count_remaining():
    """未完了月数を返す"""
    return sum(
        1 for y, m in get_target_months()
        if not is_month_complete(y, m)
    )

# ========== メイン ==========

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    # 強制指定がある場合（手動実行時）
    if FORCE_YEAR and FORCE_MONTH:
        year  = int(FORCE_YEAR)
        month = int(FORCE_MONTH)
        print(f"[手動指定] {year}年{month}月")
    else:
        # 自動判断
        target = find_next_target()
        if target is None:
            msg = "全月のデータ収集が完了しています。"
            print(msg)
            notify_all_done(
                total_months=len(get_target_months()),
                total_races=len(get_target_months()) * 650
            )
            return
        year, month = target

    remaining = count_remaining()
    is_resume = is_month_partial(year, month) and not is_month_complete(year, month)
    progress  = get_checkpoint_progress(year, month) if is_resume else 0

    print(f"{'='*55}")
    print(f"対象: {year}年{month}月")
    print(f"残り: {remaining}ヶ月")
    print(f"再開: {is_resume} (既存チェックポイント: {progress}件)")
    print(f"{'='*55}")

    # Discord通知: 開始
    notify_start(year, month, remaining)
    notion_log(f"🔄 {year}年{month}月 収集開始", "🔄 実行中", year, month)

    # スクレイパー実行
    start_time = datetime.now()
    cmd = [
        sys.executable, "scripts/keirin_scraper.py",
        "--year", str(year),
        "--month", str(month),
    ]
    if is_resume:
        cmd.append("--resume")

    print(f"実行コマンド: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            check=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        error_msg = str(e)
        elapsed = (datetime.now() - start_time).total_seconds() / 60
        print(f"スクレイパーエラー: {error_msg}")
        notify_error(year, month, error_msg)
        notion_log(f"❌ {year}年{month}月 エラー", "❌ エラー", year, month,
                   elapsed_min=elapsed, error_msg=error_msg)
        sys.exit(1)
    except Exception as e:
        elapsed = (datetime.now() - start_time).total_seconds() / 60
        notify_error(year, month, str(e))
        notion_log(f"❌ {year}年{month}月 エラー", "❌ エラー", year, month,
                   elapsed_min=elapsed, error_msg=str(e))
        raise

    elapsed = (datetime.now() - start_time).total_seconds() / 60

    # 完了確認
    csv_path = DATA_DIR / f"{year}_{month:02d}_keirin.csv"
    if csv_path.exists():
        import pandas as pd
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        race_count = df['race_id'].nunique() if 'race_id' in df.columns else 0
        row_count  = len(df)

        # Discord通知: 完了
        notify_done(year, month, race_count, row_count)
        notion_log(f"✅ {year}年{month}月 完了", "✅ 完了", year, month,
                   race_count=race_count, row_count=row_count, elapsed_min=elapsed)

        # 全月完了チェック
        if count_remaining() == 0:
            notify_all_done(
                total_months=len(get_target_months()),
                total_races=len(get_target_months()) * 650
            )
    else:
        notify_error(year, month, f"CSVファイルが生成されませんでした: {csv_path}")
        sys.exit(1)

if __name__ == "__main__":
    main()
