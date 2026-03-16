"""
バッチ実行モード: CSVから複数のお問い合わせフォームを一括処理（並列対応）

入力CSVの各行に対してDify APIでPlaywrightコードを生成・実行し、
結果をレポートCSVに出力する。--workers で同時実行数を制御できる。

使い方:
  # ドライラン（入力だけ、送信しない）
  poetry run python run_batch.py data/targets.csv --screenshot --save-code

  # 本番（実際に送信する）
  poetry run python run_batch.py data/targets.csv --submit --screenshot --save-code

  # 並列数を変更する場合
  poetry run python run_batch.py data/targets.csv --submit --workers 5
"""
import argparse
import asyncio
import csv
import re
import sys
from datetime import datetime
from pathlib import Path

import aiohttp
from playwright.async_api import async_playwright

from fetch_html import fetch_rendered_html
from run_codegen import DifyApiError, execute_code, fetch_code_from_dify, load_sales_data, log
from slack_notifier import async_notify as slack_notify


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


def safe_filename(value: str) -> str:
    """ファイル名に使えない文字を除去する"""
    value = re.sub(r'[\\/:*?"<>|]+', "_", value)
    value = re.sub(r"\s+", "_", value).strip("_")
    return value[:50] or "no_name"


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
        "--slow-mo", type=int, default=0,
        help="操作間の遅延（ミリ秒）（デフォルト: 0、デバッグ時に100等を指定）",
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
    parser.add_argument(
        "--workers", type=int, default=10,
        help="同時実行数（デフォルト: 10）",
    )
    return parser.parse_args()


async def process_single(
    index: int,
    total: int,
    row: dict,
    args,
    sales_data: str,
    semaphore: asyncio.Semaphore,
    http_session: aiohttp.ClientSession,
    exec_browser,
    render_browser,
) -> dict:
    """1社分の処理を行うワーカー"""
    async with semaphore:
        company_name = row.get("company_name", "")
        company_url = row["company_url"]
        contact_url = row["contact_url"]
        company_overview = row.get("company_overview", "")
        business_summary = row.get("business_summary", "")
        riskdog_industry = row.get("riskdog_industry", "")

        label = company_name or company_url
        log(f"[{index}/{total}] {label} 開始")

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
                log(f"[{index}/{total}] PlaywrightでフォームHTML取得中...")
                contact_html = await fetch_rendered_html(
                    url=contact_url,
                    extract_form=True,
                    browser=render_browser,
                )
                if contact_html:
                    log(f"[{index}/{total}] フォームHTML取得完了: {len(contact_html)} 文字", "OK")
                else:
                    log(f"[{index}/{total}] フォームHTML取得失敗（Dify側HTTPリクエストにフォールバック）", "WARN")

            # Step 1: Dify APIからコード取得（リトライ付き）
            # ※ここはコード生成の依頼のみ。フォーム送信はStep 2なので二重送信の心配なし
            max_retries = 3
            dify_result = None
            for attempt in range(1, max_retries + 1):
                try:
                    dify_result = await fetch_code_from_dify(
                        company_url=company_url,
                        contact_url=contact_url,
                        sales_data=sales_data,
                        contact_html=contact_html,
                        company_name=company_name,
                        company_overview=company_overview,
                        business_summary=business_summary,
                        riskdog_industry=riskdog_industry,
                        label=f"{index}/{total}",
                        session=http_session,
                    )
                    break
                except DifyApiError as e:
                    if attempt < max_retries:
                        wait = 2 ** attempt
                        log(f"[{index}/{total}] リトライ {attempt}/{max_retries}: {e} → {wait}秒待機", "WARN")
                        await asyncio.sleep(wait)
                    else:
                        raise

            # 防御ガード: リトライロジック変更時の安全策
            if dify_result is None:
                raise DifyApiError("コード取得に失敗しました")
            code = dify_result["playwright_code"]
            no_fit_reason = dify_result.get("no_fit_reason", "")

            # フォームが見つからなかった場合 or 不適合の場合はスキップ
            if code.startswith("ERROR:"):
                result_row["status"] = "skip"
                result_row["message"] = code
                log(f"[{index}/{total}] スキップ: {code}", "WARN")
                if no_fit_reason:
                    log(f"[{index}/{total}] 理由: {no_fit_reason}", "WARN")
                await slack_notify(
                    company_name=company_name,
                    contact_url=contact_url,
                    status=result_row["status"],
                    message=result_row["message"],
                    no_fit_reason=no_fit_reason,
                    session=http_session,
                )
                return result_row

            # コード保存
            if args.save_code:
                save_dir = Path("generated_code")
                save_dir.mkdir(exist_ok=True)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                safe_label = safe_filename(label)
                save_path = save_dir / f"form_code_{safe_label}_{ts}.py"
                with open(save_path, "w", encoding="utf-8") as f:
                    f.write(code)
                log(f"[{index}/{total}] 生成コード保存: {save_path}", "OK")

            # Step 2: コード実行
            exec_result = await execute_code(
                code=code,
                contact_url=contact_url,
                headed=args.headed,
                screenshot=args.screenshot,
                timeout=args.timeout,
                slow_mo=args.slow_mo,
                submit=args.submit,
                browser=exec_browser,
                label=f"{index}/{total}",
            )

            result_row["status"] = exec_result["status"]
            result_row["message"] = exec_result["message"]
            if exec_result["screenshots"]:
                result_row["screenshot"] = exec_result["screenshots"][0]

        except DifyApiError as e:
            result_row["status"] = "error"
            result_row["message"] = str(e)
            log(f"[{index}/{total}] Dify APIエラー: {e}", "WARN")

        except Exception as e:
            result_row["status"] = "error"
            result_row["message"] = str(e)
            log(f"[{index}/{total}] 予期しないエラー: {e}", "ERROR")

        # Slack通知（1件ごと）
        await slack_notify(
            company_name=company_name,
            contact_url=contact_url,
            status=result_row["status"],
            message=result_row["message"],
            session=http_session,
        )

        status_sym = "OK" if result_row["status"] == "ok" else "WARN"
        log(f"[{index}/{total}] {label} 完了 → {result_row['status']}", status_sym)

        return result_row


async def process_batch(args, rows, sales_data):
    """バッチ処理のメインループ（並列ワーカー）"""
    total = len(rows)
    workers = args.workers

    log(f"並列実行モード: {workers} ワーカー × {total} 件")

    semaphore = asyncio.Semaphore(workers)

    # 共有リソースの初期化
    http_session = aiohttp.ClientSession()

    # コード実行用ブラウザ
    exec_pw = await async_playwright().start()
    exec_browser = await exec_pw.chromium.launch(
        headless=not args.headed,
        slow_mo=args.slow_mo,
    )
    log("コード実行用ブラウザ起動", "OK")

    # HTML事前レンダリング用ブラウザ（--no-render でなければ起動）
    render_pw = None
    render_browser = None
    if not args.no_render:
        render_pw = await async_playwright().start()
        render_browser = await render_pw.chromium.launch(headless=True)
        log("HTML取得用ブラウザ起動", "OK")

    try:
        # 全タスクを並列実行（Semaphore で同時実行数を制御）
        tasks = [
            process_single(
                index=i,
                total=total,
                row=row,
                args=args,
                sales_data=sales_data,
                semaphore=semaphore,
                http_session=http_session,
                exec_browser=exec_browser,
                render_browser=render_browser,
            )
            for i, row in enumerate(rows, 1)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 例外を結果に変換
        processed_results = []
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                row = rows[i]
                log(f"[{i+1}/{total}] 致命的エラー: {r}", "ERROR")
                processed_results.append({
                    "company_name": row.get("company_name", ""),
                    "company_url": row.get("company_url", ""),
                    "contact_url": row.get("contact_url", ""),
                    "status": "error",
                    "message": str(r),
                    "screenshot": "",
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })
            else:
                processed_results.append(r)

        success_count = sum(1 for r in processed_results if r["status"] == "ok")
        error_count = total - success_count

    finally:
        await http_session.close()
        await exec_browser.close()
        await exec_pw.stop()
        log("コード実行用ブラウザ終了", "OK")
        if render_browser:
            await render_browser.close()
        if render_pw:
            await render_pw.stop()
            log("HTML取得用ブラウザ終了", "OK")

    return processed_results, success_count, error_count, total


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
    asyncio.run(slack_notify(
        company_name="【バッチサマリー】",
        contact_url=report_file,
        status="ok" if error_count == 0 else "error",
        message=summary,
    ))

    sys.exit(0 if error_count == 0 else 1)


if __name__ == "__main__":
    main()
