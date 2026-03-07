"""
バッチ実行モード: CSVから複数のお問い合わせフォームを一括処理

入力CSVの各行に対してDify APIでPlaywrightコードを生成・実行し、
結果をレポートCSVに出力する。

使い方:
  poetry run python run_batch.py data/targets.csv
  poetry run python run_batch.py data/targets.csv --headed --screenshot --save-code
"""
import argparse
import asyncio
import csv
import sys
from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright

from fetch_html import fetch_rendered_html
from run_codegen import execute_code, fetch_code_from_dify, load_sales_data, log
from slack_notifier import notify as slack_notify


# CSV列名のマッピング（日本語列名 → 内部名）
COLUMN_MAP = {
    "企業名": "company_name",
    "会社サイトURL": "company_url",
    "お問い合わせURL": "contact_url",
    "企業概要": "company_overview",
    "事業内容一言説明": "business_summary",
    "Riskdog業界": "riskdog_industry",
    # 英語列名はそのまま
    "company_name": "company_name",
    "company_url": "company_url",
    "contact_url": "contact_url",
    "company_overview": "company_overview",
    "business_summary": "business_summary",
    "riskdog_industry": "riskdog_industry",
}


def read_csv(csv_path: str) -> list[dict]:
    """CSVファイルを読み込み、列名を正規化して行のリストを返す"""
    path = Path(csv_path)
    if not path.exists():
        log(f"CSVファイルが見つかりません: {csv_path}", "ERROR")
        sys.exit(1)

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames:
            log("CSVにヘッダー行がありません", "ERROR")
            sys.exit(1)

        # 列名マッピングを適用
        raw_rows = list(reader)

    # 必須列の存在チェック（日本語 or 英語どちらか）
    fields = set(raw_rows[0].keys()) if raw_rows else set()
    has_company_url = "company_url" in fields or "会社サイトURL" in fields
    has_contact_url = "contact_url" in fields or "お問い合わせURL" in fields

    if not has_company_url or not has_contact_url:
        log("CSVに必須列がありません（company_url/会社サイトURL, contact_url/お問い合わせURL）", "ERROR")
        sys.exit(1)

    # 列名を正規化して必要なフィールドだけ取り出す
    rows = []
    skipped = 0
    for raw in raw_rows:
        normalized = {}
        for raw_key, value in raw.items():
            mapped = COLUMN_MAP.get(raw_key)
            if mapped:
                normalized[mapped] = value

        # contact_url が空の行はスキップ
        if not normalized.get("contact_url"):
            skipped += 1
            continue

        rows.append(normalized)

    if skipped:
        log(f"お問い合わせURLが空の {skipped} 件をスキップしました", "WARN")

    if not rows:
        log("処理対象の行がありません", "ERROR")
        sys.exit(1)

    return rows


def write_report(results: list[dict], report_dir: str = "reports") -> str:
    """結果リストからレポートCSVを生成し、ファイルパスを返す"""
    report_path = Path(report_dir)
    report_path.mkdir(exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = report_path / f"batch_{ts}.csv"

    fieldnames = [
        "company_name",
        "company_url",
        "contact_url",
        "status",
        "message",
        "screenshot",
        "timestamp",
    ]

    with open(filename, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    return str(filename)


def parse_args():
    parser = argparse.ArgumentParser(description="CSVバッチ実行スクリプト")
    parser.add_argument("csv_file", help="入力CSVファイルのパス")
    parser.add_argument(
        "--headed", action="store_true", default=False,
        help="ブラウザを表示する",
    )
    parser.add_argument(
        "--screenshot", action="store_true", default=False,
        help="スクリーンショットを保存する",
    )
    parser.add_argument(
        "--timeout", type=int, default=30,
        help="タイムアウト（秒）（デフォルト: 30）",
    )
    parser.add_argument(
        "--slow-mo", type=int, default=100,
        help="操作間の遅延（ミリ秒）（デフォルト: 100）",
    )
    parser.add_argument(
        "--delay", type=int, default=5,
        help="行間の待機（秒）（デフォルト: 5）",
    )
    parser.add_argument(
        "--save-code", action="store_true", default=False,
        help="生成コードをファイルに保存する",
    )
    parser.add_argument(
        "--submit", action="store_true", default=False,
        help="フォーム入力後に送信ボタンをクリックする（デフォルト: ドライラン）",
    )
    parser.add_argument(
        "--config", default="",
        help="営業担当者データJSONファイルのパス（デフォルト: config_example.json）",
    )
    parser.add_argument(
        "--no-render", action="store_true", default=False,
        help="PlaywrightによるHTML事前レンダリングをスキップする（従来動作）",
    )
    return parser.parse_args()


async def process_batch(args, rows, sales_data):
    """バッチ処理のメインループ（ブラウザインスタンスを再利用）"""
    total = len(rows)
    results = []
    success_count = 0
    error_count = 0

    # HTML事前レンダリング用ブラウザ（--no-render でなければ起動）
    render_pw = None
    render_browser = None
    if not args.no_render:
        render_pw = await async_playwright().start()
        render_browser = await render_pw.chromium.launch(headless=True)
        log("HTML取得用ブラウザ起動", "OK")

    try:
        for i, row in enumerate(rows, 1):
            company_name = row.get("company_name", "")
            company_url = row["company_url"]
            contact_url = row["contact_url"]
            company_overview = row.get("company_overview", "")
            business_summary = row.get("business_summary", "")
            riskdog_industry = row.get("riskdog_industry", "")

            label = company_name or company_url
            print(f"\n--- [{i}/{total}] {label} ---\n")

            result_row = {
                "company_name": company_name,
                "company_url": company_url,
                "contact_url": contact_url,
                "status": "error",
                "message": "",
                "screenshot": "",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            try:
                # Step 0: フォームHTML取得（レンダリング有効時）
                contact_html = ""
                if render_browser:
                    log("PlaywrightでフォームHTML取得中...")
                    contact_html = await fetch_rendered_html(
                        url=contact_url,
                        extract_form=True,
                        browser=render_browser,
                    )
                    if contact_html:
                        log(f"フォームHTML取得完了: {len(contact_html)} 文字", "OK")
                    else:
                        log("フォームHTML取得失敗（Dify側HTTPリクエストにフォールバック）", "WARN")

                # Step 1: Dify APIからコード取得
                code = fetch_code_from_dify(
                    company_url=company_url,
                    contact_url=contact_url,
                    sales_data=sales_data,
                    contact_html=contact_html,
                    company_name=company_name,
                    company_overview=company_overview,
                    business_summary=business_summary,
                    riskdog_industry=riskdog_industry,
                )

                # フォームが見つからなかった場合はスキップ
                if code.startswith("ERROR:"):
                    result_row["status"] = "skip"
                    result_row["message"] = code
                    error_count += 1
                    log(f"スキップ: {code}", "WARN")
                    results.append(result_row)
                    slack_notify(
                        company_name=company_name,
                        contact_url=contact_url,
                        status=result_row["status"],
                        message=result_row["message"],
                    )
                    if i < total:
                        log(f"{args.delay}秒待機中...")
                        await asyncio.sleep(args.delay)
                    continue

                # コード保存
                if args.save_code:
                    save_dir = Path("generated_code")
                    save_dir.mkdir(exist_ok=True)
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    save_path = save_dir / f"form_code_{ts}.py"
                    with open(save_path, "w", encoding="utf-8") as f:
                        f.write(code)
                    log(f"生成コード保存: {save_path}", "OK")

                # Step 2: コード実行
                exec_result = await execute_code(
                    code=code,
                    contact_url=contact_url,
                    headed=args.headed,
                    screenshot=args.screenshot,
                    timeout=args.timeout,
                    slow_mo=args.slow_mo,
                    submit=args.submit,
                )

                result_row["status"] = exec_result["status"]
                result_row["message"] = exec_result["message"]
                if exec_result["screenshots"]:
                    result_row["screenshot"] = exec_result["screenshots"][0]

                if exec_result["status"] == "ok":
                    success_count += 1
                else:
                    error_count += 1

            except SystemExit:
                result_row["status"] = "error"
                result_row["message"] = "Dify API エラー"
                error_count += 1
                log("Dify APIエラーが発生しましたが、次の行に進みます", "WARN")

            except Exception as e:
                result_row["status"] = "error"
                result_row["message"] = str(e)
                error_count += 1
                log(f"予期しないエラー: {e}", "ERROR")

            results.append(result_row)

            # Slack通知（1件ごと）
            slack_notify(
                company_name=company_name,
                contact_url=contact_url,
                status=result_row["status"],
                message=result_row["message"],
            )

            # 行間の待機（最後の行以外）
            if i < total:
                log(f"{args.delay}秒待機中...")
                await asyncio.sleep(args.delay)

    finally:
        if render_browser:
            await render_browser.close()
        if render_pw:
            await render_pw.stop()
            log("HTML取得用ブラウザ終了", "OK")

    return results, success_count, error_count, total


def main():
    args = parse_args()

    print("\n=== バッチ実行モード ===\n")

    rows = read_csv(args.csv_file)
    total = len(rows)
    log(f"CSV読み込み完了: {total} 件")

    sales_data = load_sales_data(args.config)
    log(f"営業担当者データ読み込み完了: {args.config or 'config_example.json'}", "OK")

    results, success_count, error_count, total = asyncio.run(
        process_batch(args, rows, sales_data)
    )

    # レポート生成
    report_file = write_report(results)

    # サマリー表示
    summary = (
        f"バッチ実行完了: 成功 {success_count}件 / "
        f"失敗 {error_count}件 / 合計 {total}件"
    )
    print(f"\n{'=' * 40}")
    print(f"  {summary}")
    print(f"  レポート: {report_file}")
    print(f"{'=' * 40}\n")

    # Slack通知（バッチサマリー）
    slack_notify(
        company_name="【バッチサマリー】",
        contact_url=report_file,
        status="ok" if error_count == 0 else "error",
        message=summary,
    )

    sys.exit(0 if error_count == 0 else 1)


if __name__ == "__main__":
    main()
