# form_automation

Airtable から対象企業を取得し、Dify API で生成した Playwright コードでお問い合わせフォームを自動入力するツール。

## セットアップ

```bash
poetry install
```

`.env` に以下を設定:

```
DIFY_API_KEY=...
AIRTABLE_API_KEY=...
AIRTABLE_BASE_ID=...
AIRTABLE_TABLE_ID=tblvipFnhShnzbfW1
SLACK_WEBHOOK_URL=...
```

営業担当者データを `config_example.json` に設定。

## 使い方

### 基本（ドライラン）

フォームAの未送信分を入力確認まで実行（送信はしない）:

```bash
poetry run python run_batch.py --source airtable --form A --screenshot --save-code
```

### 本番（送信あり）

```bash
poetry run python run_batch.py --source airtable --form A --submit
```

### フォーム種別

`--form` でキャンペーンを指定:

| オプション | キャンペーン名 |
|---|---|
| `--form A` | フォームA |
| `--form B` | フォームB |
| `--form C` | フォームC |

### 件数制限

```bash
# 最初の5件だけ実行
poetry run python run_batch.py --source airtable --form A --limit 5 --screenshot
```

### 再処理

```bash
# エラーのレコードのみ再処理
poetry run python run_batch.py --source airtable --form A --retry --submit

# 送信済み含む全レコードを再処理
poetry run python run_batch.py --source airtable --form A --retry-all --submit
```

### その他のオプション

| オプション | 説明 | デフォルト |
|---|---|---|
| `--submit` | 送信ボタンをクリックする | off（ドライラン） |
| `--screenshot` | スクリーンショットを保存 | off |
| `--save-code` | 生成コードをファイルに保存 | off |
| `--headed` | ブラウザを表示する | off（ヘッドレス） |
| `--workers N` | 同時実行数 | 10 |
| `--timeout N` | タイムアウト（秒） | 30 |
| `--slow-mo N` | 操作間の遅延（ミリ秒） | 0 |
| `--config PATH` | 営業担当者データJSON | config_example.json |
| `--no-render` | HTML事前レンダリングをスキップ | off |

### CSVモード（従来方式）

```bash
poetry run python run_batch.py data/targets.csv --submit --screenshot
```

## 処理フロー

1. Airtable からキャンペーンに紐づく対象企業を取得
2. Playwright でフォームページの HTML を取得
3. Dify API に HTML + 企業情報を送信し、フォーム入力コードを生成
4. 生成コードを実行してフォームに入力
5. 確認ボタン → 送信ボタン（`--submit` 時）をクリック
6. 結果を Slack + Airtable に通知（フォーム送信状況・フォーム本文）

## 出力

- `screenshots/` — スクリーンショット
- `generated_code/` — 生成された Playwright コード
- `reports/` — バッチ実行結果の CSV レポート
