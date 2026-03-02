"""
フォーム営業自動化 Playwright スクリプト

2つの使い方:

  ■ Difyモード（推奨）:
    Difyが生成したフォーム解析JSONと入力データJSONを渡す

    python form_filler.py --form form_analysis.json --data input_data.json
    python form_filler.py --form form_analysis.json --data input_data.json --headed
    python form_filler.py --form form_analysis.json --data input_data.json --submit

  ■ 手動モード:
    フォーム解析JSONと固定の営業テンプレートを渡す

    python form_filler.py --form form_analysis.json --data config.json

  --data の形式:
    Difyモード: {"prmName1": "山田", "prmName2": "太郎", ...}  (フィールドnameをキー)
    手動モード: {"name_sei": "山田", "email": "...", ...}       (汎用キー)
    どちらも自動判別されます。
"""
import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright

from field_handler import fill_field, match_field_to_data
from form_analyzer import (
    detect_captcha,
    detect_confirm_button,
    detect_errors,
    detect_submit_button,
    wait_for_page_ready,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="フォーム自動入力・送信スクリプト（Playwright）"
    )
    parser.add_argument(
        "--form",
        required=True,
        help="Dify出力のフォーム解析JSONファイルパス",
    )
    parser.add_argument(
        "--data",
        required=False,
        default=None,
        help="入力データJSONファイルパス（Dify出力 or 営業テンプレート）。省略時はフォーム解析JSONのvalueを使用",
    )
    parser.add_argument(
        "--submit",
        action="store_true",
        default=False,
        help="実際にフォームを送信する（デフォルト: ドライラン）",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        default=False,
        help="ブラウザを表示する（デフォルト: ヘッドレス）",
    )
    parser.add_argument(
        "--screenshot",
        action="store_true",
        default=False,
        help="入力後にスクリーンショットを保存する",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="ページ読み込みタイムアウト（秒）（デフォルト: 30）",
    )
    parser.add_argument(
        "--slow-mo",
        type=int,
        default=100,
        help="操作間の遅延（ミリ秒）（デフォルト: 100）",
    )
    return parser.parse_args()


def load_form_json(path: str) -> dict:
    """フォーム解析JSONを読み込む（Dify出力形式に対応）"""
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    # Dify出力形式: {"text": "{...JSON文字列...}", "usage": {...}}
    if "text" in raw and isinstance(raw["text"], str):
        try:
            return json.loads(raw["text"])
        except json.JSONDecodeError:
            pass

    # 直接JSONの場合
    return raw


def load_data_json(path: str) -> dict:
    """営業データJSONを読み込む"""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def log(msg: str, level: str = "INFO"):
    """ログ出力"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    symbols = {"INFO": "[*]", "OK": "[+]", "WARN": "[!]", "ERROR": "[-]", "SKIP": "[~]"}
    symbol = symbols.get(level, "[*]")
    print(f"  {timestamp} {symbol} {msg}")


async def run(args):
    # ─── ファイル読み込み ───
    print("\n=== フォーム自動入力スクリプト ===\n")

    log("フォーム解析JSON読み込み中...")
    form_data = load_form_json(args.form)

    config_data = {}
    if args.data:
        log("入力データJSON読み込み中...")
        config_data = load_data_json(args.data)
        # Dify出力形式の場合も自動パース
        if "text" in config_data and isinstance(config_data["text"], str):
            try:
                config_data = json.loads(config_data["text"])
            except json.JSONDecodeError:
                pass
        log(f"入力データキー数: {len(config_data)}")
    else:
        log("入力データなし（フォーム解析JSONのvalue値を使用）", "INFO")

    page_info = form_data.get("page", {})
    url = page_info.get("url", "")
    fields = form_data.get("fields", [])

    if not url:
        log("URLが指定されていません", "ERROR")
        sys.exit(1)

    log(f"対象URL: {url}")
    log(f"フィールド数: {len(fields)}")
    log(f"モード: {'実送信' if args.submit else 'ドライラン（入力のみ）'}")
    log(f"ブラウザ: {'表示' if args.headed else 'ヘッドレス'}")
    print()

    # ─── ブラウザ起動 ───
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=not args.headed,
            slow_mo=args.slow_mo,
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="ja-JP",
        )
        page = await context.new_page()

        try:
            # ─── ページ遷移 ───
            log(f"ページに遷移中... ({url})")
            await page.goto(url, timeout=args.timeout * 1000)
            await wait_for_page_ready(page, timeout=args.timeout * 1000)
            log("ページ読み込み完了", "OK")

            # ─── CAPTCHA検出 ───
            captcha = await detect_captcha(page)
            if captcha["detected"]:
                log(f"CAPTCHA検出: {captcha['type']}", "WARN")
                log(captcha["message"], "WARN")
                if not args.headed:
                    log("CAPTCHAが検出されました。--headed モードで手動対応してください。", "ERROR")
                    await browser.close()
                    sys.exit(1)
                else:
                    log("CAPTCHAが検出されましたが、headed モードのため続行します。手動で解いてください。", "WARN")
            else:
                log("CAPTCHA: なし", "OK")

            # ─── フィールド入力 ───
            print("\n--- フィールド入力 ---\n")
            results = []
            ok_count = 0
            skip_count = 0
            error_count = 0

            for field in fields:
                field_name = field.get("name", "?")
                field_label = field.get("label", "")
                field_type = field.get("type", "text")

                # 値の解決（優先順位）:
                # 1. 入力データJSONでフィールドnameに直接マッチ（Difyモード）
                # 2. 入力データJSONでラベルマッピングにマッチ（手動モード）
                # 3. フォーム解析JSONのfields[].valueに値がある場合
                value = None
                if config_data:
                    value = match_field_to_data(field, config_data, fields)
                if not value and field.get("value"):
                    value = field["value"]

                result = await fill_field(page, field, value or "")

                results.append(result)

                if result["status"] == "ok":
                    ok_count += 1
                    log(f"{field_label or field_name} ({field_type}): {result['message']}", "OK")
                elif result["status"] == "skip":
                    skip_count += 1
                    log(f"{field_label or field_name} ({field_type}): {result['message']}", "SKIP")
                else:
                    error_count += 1
                    log(f"{field_label or field_name} ({field_type}): {result['message']}", "ERROR")

            # ─── 入力結果サマリー ───
            print(f"\n--- 入力結果 ---")
            print(f"  成功: {ok_count}  スキップ: {skip_count}  エラー: {error_count}")
            print()

            # ─── スクリーンショット ───
            if args.screenshot:
                screenshot_dir = Path("screenshots")
                screenshot_dir.mkdir(exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                screenshot_path = screenshot_dir / f"form_{timestamp}.png"
                await page.screenshot(path=str(screenshot_path), full_page=True)
                log(f"スクリーンショット保存: {screenshot_path}", "OK")

            # ─── 確認ボタン対応 ───
            confirm_step = page_info.get("confirm_step_suspected", False)
            confirm = await detect_confirm_button(page, form_data)

            if confirm["found"] or confirm_step:
                print("\n--- 確認画面 ---\n")
                log(f"確認ボタン検出: '{confirm.get('text', '')}'")

                if confirm.get("selector"):
                    await page.click(confirm["selector"])
                elif confirm.get("_locator"):
                    await confirm["_locator"].click()
                else:
                    # submit情報から直接クリック
                    submit_info = form_data.get("submit", {})
                    submit_name = submit_info.get("name", "")
                    if submit_name:
                        await page.click(f'[name="{submit_name}"]')

                log("確認ボタンクリック完了", "OK")
                await wait_for_page_ready(page)

                # 確認画面でスクリーンショット
                if args.screenshot:
                    confirm_path = screenshot_dir / f"form_confirm_{timestamp}.png"
                    await page.screenshot(path=str(confirm_path), full_page=True)
                    log(f"確認画面スクリーンショット保存: {confirm_path}", "OK")

                # ─── ドライランなら確認画面で停止 ───
                if not args.submit:
                    log("ドライランモード: 確認画面まで完了。送信は行いません", "WARN")
                    if args.headed:
                        log("ブラウザを閉じるにはEnterキーを押してください...")
                        try:
                            input()
                        except EOFError:
                            pass
                    await browser.close()
                    return results

                # 確認画面後の最終送信ボタン
                print("\n--- 送信処理 ---\n")
                final_submit = await detect_submit_button(page)
                if final_submit["found"]:
                    log(f"最終送信ボタン検出: '{final_submit.get('text', '')}'")
                    if final_submit.get("_locator"):
                        await final_submit["_locator"].click()
                    log("最終送信完了", "OK")
                else:
                    log("最終送信ボタンが見つかりません", "WARN")
            else:
                # ─── 確認画面なし：ドライランなら停止 ───
                if not args.submit:
                    log("ドライランモード: 送信は行いません", "WARN")
                    if args.headed:
                        log("ブラウザを閉じるにはEnterキーを押してください...")
                        try:
                            input()
                        except EOFError:
                            pass
                    await browser.close()
                    return results

                # 直接送信
                print("\n--- 送信処理 ---\n")
                submit_info = form_data.get("submit", {})
                submit_name = submit_info.get("name", "")
                if submit_name:
                    await page.click(f'[name="{submit_name}"]')
                    log("送信ボタンクリック完了", "OK")
                else:
                    # submitボタンを探す
                    submit_btn = await detect_submit_button(page)
                    if submit_btn["found"] and submit_btn.get("_locator"):
                        await submit_btn["_locator"].click()
                        log("送信ボタンクリック完了", "OK")
                    else:
                        log("送信ボタンが見つかりません", "ERROR")

            await wait_for_page_ready(page)

            # ─── エラーメッセージ検出 ───
            errors = await detect_errors(page)
            if errors:
                log("送信後にエラーが検出されました:", "ERROR")
                for err in errors:
                    log(f"  {err['text']}", "ERROR")
            else:
                log("送信完了（エラーなし）", "OK")

            # 送信後スクリーンショット
            if args.screenshot:
                result_path = screenshot_dir / f"form_result_{timestamp}.png"
                await page.screenshot(path=str(result_path), full_page=True)
                log(f"送信結果スクリーンショット保存: {result_path}", "OK")

            if args.headed:
                log("ブラウザを閉じるにはEnterキーを押してください...")
                input()

            return results

        except Exception as e:
            log(f"エラーが発生しました: {e}", "ERROR")
            if args.screenshot:
                screenshot_dir = Path("screenshots")
                screenshot_dir.mkdir(exist_ok=True)
                error_path = screenshot_dir / f"form_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                await page.screenshot(path=str(error_path), full_page=True)
                log(f"エラー時スクリーンショット: {error_path}", "OK")
            raise
        finally:
            await browser.close()


def main():
    args = parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
