"""
コード生成モード: Dify生成Playwrightコードの実行スクリプト

DifyワークフローがLLM8の出力から生成したPlaywrightコードを取得し、
ローカルで実行する。run.py（JSON駆動モード）とは独立して動作する。

使い方:
  # Dify APIからコードを取得して実行
  poetry run python run_codegen.py http://www.example.com https://www.example.com/contact --headed

  # ローカルのコードファイルを直接実行（デバッグ用）
  poetry run python run_codegen.py --file generated_code.py --headed
"""
import argparse
import asyncio
import json
import os
import re
import sys
import traceback
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright

from slack_notifier import notify as slack_notify

load_dotenv()

DIFY_API_KEY = os.environ.get("DIFY_API_KEY", "")
DIFY_BASE_URL = os.environ.get("DIFY_BASE_URL", "https://api.dify.ai/v1")


def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    symbols = {"INFO": "[*]", "OK": "[+]", "WARN": "[!]", "ERROR": "[-]"}
    symbol = symbols.get(level, "[*]")
    print(f"  {timestamp} {symbol} {msg}")


def fetch_code_from_dify(company_url: str, contact_url: str) -> str:
    """Dify ワークフローAPIを呼び出してPlaywrightコードを取得する"""
    endpoint = f"{DIFY_BASE_URL}/workflows/run"
    headers = {
        "Authorization": f"Bearer {DIFY_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "inputs": {"company_url": company_url, "contact_url": contact_url},
        "response_mode": "streaming",
        "user": "form-filler-codegen",
    }

    log("Dify API 呼び出し中（ストリーミング）...")
    resp = requests.post(endpoint, headers=headers, json=payload, timeout=600, stream=True)
    if resp.status_code != 200:
        log(f"ステータスコード: {resp.status_code}", "ERROR")
        log(f"レスポンス: {resp.text}", "ERROR")
        sys.exit(1)

    outputs = None
    for line in resp.iter_lines(decode_unicode=True):
        if not line or not line.startswith("data: "):
            continue
        data = json.loads(line[6:])
        event = data.get("event", "")

        if event == "workflow_started":
            log("ワークフロー開始...")
        elif event == "node_started":
            node_title = data.get("data", {}).get("title", "")
            if node_title:
                log(f"処理中: {node_title}")
        elif event == "workflow_finished":
            outputs = data.get("data", {}).get("outputs", {})
            log("ワークフロー完了", "OK")
        elif event == "error":
            msg = data.get("message", "不明なエラー")
            log(f"{msg}", "ERROR")
            sys.exit(1)

    if not outputs:
        log("ワークフロー出力を取得できませんでした", "ERROR")
        sys.exit(1)

    playwright_code = outputs.get("playwright_code", "")
    if not playwright_code:
        log("playwright_code が空です", "ERROR")
        sys.exit(1)

    return playwright_code


def sanitize_code(code: str) -> str:
    """LLM出力からコードブロックマーカーを除去し、危険なパターンを警告する"""
    # ```python ... ``` を除去
    code = re.sub(r"^```(?:python)?\s*\n?", "", code.strip())
    code = re.sub(r"\n?```\s*$", "", code)

    dangerous_patterns = [
        (r"\bos\.(system|popen|exec)", "os.system/exec"),
        (r"\bsubprocess\b", "subprocess"),
        (r"\b__import__\b", "__import__"),
        (r"\beval\s*\(", "eval()"),
        (r"\bopen\s*\([^)]*['\"]w", "ファイル書き込み"),
    ]

    for pattern, desc in dangerous_patterns:
        if re.search(pattern, code):
            log(f"警告: 生成コードに危険なパターン検出: {desc}", "WARN")

    return code


def inject_url(code: str, contact_url: str) -> str:
    """goto("")のURLを置換、またはgoto自体がない場合は挿入する"""
    if not contact_url:
        return code

    # パターン1: goto("") → goto("contact_url")
    if 'page.goto("")' in code:
        code = re.sub(
            r'await page\.goto\(\s*""\s*\)',
            f'await page.goto("{contact_url}")',
            code,
        )
        return code

    # パターン2: goto がない場合
    if "page.goto(" not in code:
        goto_lines = (
            f'    await page.goto("{contact_url}")\n'
            f'    await page.wait_for_load_state("domcontentloaded")\n'
            f'    await page.wait_for_load_state("networkidle")\n'
        )

        # wait_for_load_state があればその前に挿入
        if "page.wait_for_load_state(" in code:
            code = re.sub(
                r'(    await page\.wait_for_load_state\()',
                f'    await page.goto("{contact_url}")\n' + r'\1',
                code,
                count=1,
            )
        else:
            # goto も wait_for_load_state もない場合、関数定義の直後に挿入
            code = re.sub(
                r'(async def fill_form\(page\):)\n',
                r'\1\n' + goto_lines,
                code,
            )

    return code


def prepare_function(code: str) -> str:
    """コードに fill_form(page) 関数が含まれていなければラップする"""
    if "async def fill_form" in code:
        return code

    lines = code.split("\n")
    indented = "\n".join(f"    {line}" for line in lines)
    return f"async def fill_form(page):\n{indented}"


async def find_and_click_submit(page) -> str:
    """送信ボタンを検出してクリックする。クリックしたボタンのテキストを返す。"""
    # 送信ボタンの候補セレクターとテキスト
    submit_selectors = [
        'input[type="submit"]',
        'button[type="submit"]',
    ]
    submit_texts = ["送信", "送る", "Submit", "送信する", "申し込む", "申込む", "登録"]

    # セレクターで検索
    for sel in submit_selectors:
        btn = page.locator(sel)
        if await btn.count() > 0:
            text = await btn.first.get_attribute("value") or await btn.first.text_content() or sel
            await btn.first.scroll_into_view_if_needed()
            await btn.first.click()
            await page.wait_for_load_state("networkidle")
            return text.strip()

    # テキストで検索
    for text in submit_texts:
        btn = page.get_by_role("button", name=text)
        if await btn.count() > 0:
            await btn.first.scroll_into_view_if_needed()
            await btn.first.click()
            await page.wait_for_load_state("networkidle")
            return text

    return ""


async def execute_code(
    code: str,
    contact_url: str = "",
    headed: bool = False,
    screenshot: bool = False,
    timeout: int = 30,
    slow_mo: int = 100,
    submit: bool = False,
) -> dict:
    """生成されたPlaywrightコードを実行する"""
    result = {
        "status": "unknown",
        "message": "",
        "screenshots": [],
        "errors": [],
    }

    code = sanitize_code(code)
    code = inject_url(code, contact_url)
    code = prepare_function(code)

    log(f"生成コード: {len(code)} 文字")

    # fill_form 関数をコンパイル
    namespace = {}
    try:
        exec(compile(code, "<generated>", "exec"), namespace)
    except SyntaxError as e:
        result["status"] = "error"
        result["message"] = f"構文エラー: {e}"
        log(f"構文エラー: {e}", "ERROR")
        return result

    fill_form = namespace.get("fill_form")
    if fill_form is None:
        result["status"] = "error"
        result["message"] = "fill_form 関数が見つかりません"
        log("fill_form 関数が見つかりません", "ERROR")
        return result

    log("fill_form 関数を検出", "OK")

    # ブラウザ起動・実行
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=not headed,
            slow_mo=slow_mo,
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="ja-JP",
        )
        page = await context.new_page()
        page.set_default_timeout(timeout * 1000)

        try:
            log("生成コードを実行中...")
            await fill_form(page)

            if submit:
                log("送信ボタンを検索中...")
                btn_text = await find_and_click_submit(page)
                if btn_text:
                    result["status"] = "ok"
                    result["message"] = f"送信完了（{btn_text}）"
                    log(f"送信完了: {btn_text}", "OK")
                else:
                    result["status"] = "error"
                    result["message"] = "送信ボタンが見つかりませんでした"
                    log("送信ボタンが見つかりませんでした", "WARN")
            else:
                result["status"] = "ok"
                result["message"] = "実行完了（ドライラン）"
                log("実行完了（ドライラン）", "OK")

            if screenshot:
                screenshot_dir = Path("screenshots")
                screenshot_dir.mkdir(exist_ok=True)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                path = screenshot_dir / f"codegen_{ts}.png"
                await page.screenshot(path=str(path), full_page=True)
                result["screenshots"].append(str(path))
                log(f"スクリーンショット保存: {path}", "OK")

            if headed:
                log("ブラウザを閉じるにはEnterキーを押してください...")
                try:
                    input()
                except EOFError:
                    pass

        except Exception as e:
            result["status"] = "error"
            result["message"] = str(e)
            result["errors"].append(traceback.format_exc())
            log(f"実行エラー: {e}", "ERROR")

            if screenshot:
                screenshot_dir = Path("screenshots")
                screenshot_dir.mkdir(exist_ok=True)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                path = screenshot_dir / f"codegen_error_{ts}.png"
                try:
                    await page.screenshot(path=str(path), full_page=True)
                    result["screenshots"].append(str(path))
                except Exception:
                    pass

        finally:
            await browser.close()

    return result


def parse_args():
    parser = argparse.ArgumentParser(
        description="Dify生成Playwrightコード実行スクリプト"
    )
    parser.add_argument(
        "company_url", nargs="?", default=None,
        help="会社のURL（Dify APIモード時）",
    )
    parser.add_argument(
        "contact_url", nargs="?", default=None,
        help="お問い合わせフォームのURL（Dify APIモード時）",
    )
    parser.add_argument(
        "--file",
        help="ローカルのPythonコードファイルを直接実行（デバッグ用）",
    )
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
        "--save-code", action="store_true", default=False,
        help="Difyから取得したコードをファイルに保存する",
    )
    parser.add_argument(
        "--submit", action="store_true", default=False,
        help="フォーム入力後に送信ボタンをクリックする（デフォルト: ドライラン）",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print("\n=== Playwright コード生成モード ===\n")

    # コード取得: ファイル or Dify API
    if args.file:
        log(f"ローカルファイルから読み込み: {args.file}")
        with open(args.file, "r", encoding="utf-8") as f:
            code = f.read()
    else:
        if not args.company_url or not args.contact_url:
            print("[ERROR] company_url と contact_url を指定するか、--file でコードファイルを指定してください")
            sys.exit(1)
        if not DIFY_API_KEY:
            print("[ERROR] .env に DIFY_API_KEY を設定してください")
            sys.exit(1)

        print(f"  会社URL: {args.company_url}")
        print(f"  お問い合わせURL: {args.contact_url}")
        print()

        print("--- Step 1: Dify コード生成 ---\n")
        code = fetch_code_from_dify(args.company_url, args.contact_url)

        # コード保存オプション
        if args.save_code:
            save_dir = Path("generated_code")
            save_dir.mkdir(exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_path = save_dir / f"form_code_{ts}.py"
            with open(save_path, "w", encoding="utf-8") as f:
                f.write(code)
            log(f"生成コード保存: {save_path}", "OK")

    print(f"\n--- {'Step 2: ' if not args.file else ''}コード実行 ---\n")

    result = asyncio.run(execute_code(
        code=code,
        contact_url=args.contact_url or "",
        headed=args.headed,
        screenshot=args.screenshot,
        timeout=args.timeout,
        slow_mo=args.slow_mo,
        submit=args.submit,
    ))

    # Slack通知（非同期・エラーでもフローを止めない）
    slack_notify(
        company_name=args.company_url or "",
        contact_url=args.contact_url or "",
        status=result["status"],
        message=result["message"],
    )

    # 結果レポート
    print(f"\n--- 実行結果 ---")
    print(f"  ステータス: {result['status']}")
    print(f"  メッセージ: {result['message']}")
    if result["screenshots"]:
        print(f"  スクリーンショット: {', '.join(result['screenshots'])}")
    if result["errors"]:
        print(f"\n  エラー詳細:")
        for err in result["errors"]:
            for line in err.strip().split("\n"):
                print(f"    {line}")

    sys.exit(0 if result["status"] == "ok" else 1)


if __name__ == "__main__":
    main()
