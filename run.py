"""
フォーム営業自動化 統合スクリプト

URLを渡すだけでDify API呼び出し → フォーム自動入力まで一気通貫で実行する。

使い方:
  poetry run python run.py http://www.example.com https://www.example.com/contact --headed
  poetry run python run.py http://www.example.com https://www.example.com/contact --headed --submit
  poetry run python run.py http://www.example.com https://www.example.com/contact --headed --data my_data.json
"""
import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

from form_filler import run as run_form_filler

load_dotenv()

DIFY_API_KEY = os.environ.get("DIFY_API_KEY", "")
DIFY_BASE_URL = os.environ.get("DIFY_BASE_URL", "https://api.dify.ai/v1")


def call_dify_workflow(company_url: str, contact_url: str) -> dict:
    """Dify ワークフローAPIを呼び出してフォーム解析結果を取得する（ストリーミング）"""
    endpoint = f"{DIFY_BASE_URL}/workflows/run"
    headers = {
        "Authorization": f"Bearer {DIFY_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "inputs": {"company_url": company_url, "contact_url": contact_url},
        "response_mode": "streaming",
        "user": "form-filler",
    }

    print(f"  Dify API 呼び出し中（ストリーミング）...")
    resp = requests.post(endpoint, headers=headers, json=payload, timeout=600, stream=True)
    if resp.status_code != 200:
        print(f"  [ERROR] ステータスコード: {resp.status_code}")
        print(f"  レスポンス: {resp.text}")
        sys.exit(1)

    # SSEストリームからworkflow_finishedイベントを取得
    outputs = None
    for line in resp.iter_lines(decode_unicode=True):
        if not line or not line.startswith("data: "):
            continue
        data = json.loads(line[6:])
        event = data.get("event", "")

        if event == "workflow_started":
            print(f"  ワークフロー開始...")
        elif event == "node_started":
            node_title = data.get("data", {}).get("title", "")
            if node_title:
                print(f"  処理中: {node_title}")
        elif event == "workflow_finished":
            outputs = data.get("data", {}).get("outputs", {})
            print(f"  ワークフロー完了")
        elif event == "error":
            msg = data.get("message", "不明なエラー")
            print(f"  [ERROR] {msg}")
            sys.exit(1)

    if not outputs:
        print(f"  [ERROR] ワークフロー出力を取得できませんでした")
        sys.exit(1)

    # fields は JSON文字列なのでパース
    fields_raw = outputs.get("fields", "")
    if isinstance(fields_raw, str):
        form_data = json.loads(fields_raw)
    else:
        form_data = fields_raw

    return form_data


def parse_args():
    parser = argparse.ArgumentParser(
        description="フォーム営業自動化（Dify + Playwright）"
    )
    parser.add_argument("company_url", help="会社のURL")
    parser.add_argument("contact_url", help="お問い合わせフォームのURL")
    parser.add_argument(
        "--data",
        default=str(Path(__file__).parent / "config_example.json"),
        help="入力データJSONファイルパス（デフォルト: config_example.json）",
    )
    parser.add_argument(
        "--submit", action="store_true", default=False,
        help="実際にフォームを送信する（デフォルト: ドライラン）",
    )
    parser.add_argument(
        "--headed", action="store_true", default=False,
        help="ブラウザを表示する（デフォルト: ヘッドレス）",
    )
    parser.add_argument(
        "--screenshot", action="store_true", default=False,
        help="入力後にスクリーンショットを保存する",
    )
    parser.add_argument(
        "--timeout", type=int, default=30,
        help="ページ読み込みタイムアウト（秒）（デフォルト: 30）",
    )
    parser.add_argument(
        "--slow-mo", type=int, default=100,
        help="操作間の遅延（ミリ秒）（デフォルト: 100）",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if not DIFY_API_KEY:
        print("[ERROR] .env に DIFY_API_KEY を設定してください")
        sys.exit(1)

    print("\n=== フォーム営業自動化 ===\n")
    print(f"  会社URL: {args.company_url}")
    print(f"  お問い合わせURL: {args.contact_url}")
    print(f"  モード: {'実送信' if args.submit else 'ドライラン'}")
    print(f"  ブラウザ: {'表示' if args.headed else 'ヘッドレス'}")
    print()

    # 1. Dify API でフォーム解析
    print("--- Step 1: Dify フォーム解析 ---\n")
    form_data = call_dify_workflow(args.company_url, args.contact_url)
    print(f"  フィールド数: {len(form_data.get('fields', []))}")
    print(f"  解析完了\n")

    # 2. form_filler に渡す引数オブジェクトを構築
    class FormFillerArgs:
        pass

    filler_args = FormFillerArgs()
    filler_args.form = None  # 直接データを渡すので不要
    filler_args.data = args.data
    filler_args.submit = args.submit
    filler_args.headed = args.headed
    filler_args.screenshot = args.screenshot
    filler_args.timeout = args.timeout
    filler_args.slow_mo = args.slow_mo

    # 3. form_filler の run() を直接呼び出し
    #    form_filler.run() はファイルからJSONを読むので、一時的にファイルに保存する
    print("--- Step 2: Playwright フォーム入力 ---\n")
    tmp_form_path = Path(__file__).parent / ".form_analysis_tmp.json"
    try:
        with open(tmp_form_path, "w", encoding="utf-8") as f:
            json.dump(form_data, f, ensure_ascii=False, indent=2)

        filler_args.form = str(tmp_form_path)
        asyncio.run(run_form_filler(filler_args))
    finally:
        if tmp_form_path.exists():
            tmp_form_path.unlink()


if __name__ == "__main__":
    main()
