# 競輪データ自動収集

楽天Kドリームスから競輪レースデータを毎日自動収集し、CSVとしてGitHubに保存するシステム。

## セットアップ手順

### 1. リポジトリの準備

```bash
git clone https://github.com/あなたのユーザー名/keirin-data.git
cd keirin-data
```

既存のCSVがある場合は `keirin_data/` に入れておく。

### 2. GitHub Secrets の設定

GitHubリポジトリの `Settings → Secrets and variables → Actions` で以下を追加：

| Secret名 | 値 | 説明 |
|----------|-----|------|
| `DISCORD_WEBHOOK_URL` | `https://discord.com/api/webhooks/...` | Discord Webhook URL |

#### Discord Webhook URLの取得方法
1. Discordのチャンネル設定 → 連携サービス → ウェブフック
2. 「新しいウェブフック」を作成
3. 「ウェブフックURLをコピー」

### 3. GitHub Actionsの有効化

リポジトリの `Actions` タブ → ワークフローを有効化

### 4. 動作確認（手動実行）

`Actions` タブ → `競輪データ自動収集` → `Run workflow`

- `force_year`: 空白（自動判断）
- `force_month`: 空白（自動判断）

で実行して動作確認。

---

## 動作仕様

### 自動実行タイミング
- 毎日 **JST 9:00** に実行

### 1回の実行で何をするか
1. `keirin_data/` を確認し、**未取得の最も古い月**を1ヶ月分取得
2. CSVを `keirin_data/{year}_{month:02d}_keirin.csv` に保存
3. Gitにコミット＆プッシュ
4. Discord通知（開始・完了・エラー）

### 途中で止まった場合
チェックポイントが `keirin_data/checkpoints/` に保存されるため、
次回実行時に自動的に続きから再開します。

### 全月完了後
2024年1月〜現在月まで全て揃ったら「全月完了」通知が届きます。

---

## ファイル構成

```
keirin_data/
  2024_01_keirin.csv    ← 月別データ
  2024_02_keirin.csv
  ...
  checkpoints/          ← 途中再開用（Gitには含まれない）
    2024_03_checkpoint.json
    2024_03_partial.csv
```

## CSVのカラム

| カラム | 内容 |
|--------|------|
| race_id | レースID（16桁） |
| venue_slug | 競輪場（英語スラッグ） |
| date | 日付（YYYY-MM-DD） |
| race_no | レース番号 |
| banum | 車番 |
| player_name | 選手名 |
| pref | 登録府県 |
| age | 年齢 |
| term | 期別 |
| player_class | 級班（S1/S2/A1/A2/A3） |
| running_style | 脚質（逃/両/追） |
| gear | ギヤ倍数 |
| race_score | 競走得点 |
| rank | 着順 |
| agari | 上りタイム（秒） |
| finish_type | 決まり手 |
| margin | 着差 |
| win_lose | 勝敗因 |
| lineup | ライン情報 |
| payout | 払戻金 |
