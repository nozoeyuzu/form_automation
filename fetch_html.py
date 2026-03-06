"""
HTML取得モジュール: PlaywrightでJavaScript描画されたページのHTMLを取得する

SPAフレームワーク(React, Vue, Angular)で構築されたページに対応するため、
PlaywrightでページをレンダリングしてからHTMLを取得する。

使い方:
  # フォーム部分のみ抽出（デフォルト）
  poetry run python fetch_html.py https://example.com/contact

  # ページ全体を取得
  poetry run python fetch_html.py https://example.com/contact --full-page

  # ファイルに出力
  poetry run python fetch_html.py https://example.com/contact --output form.html
"""
import argparse
import asyncio
import re
from datetime import datetime

from playwright.async_api import async_playwright

# Dify API の入力サイズ制限（1MB）にマージンを持たせる
MAX_HTML_SIZE = 900_000


def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    symbols = {"INFO": "[*]", "OK": "[+]", "WARN": "[!]", "ERROR": "[-]"}
    symbol = symbols.get(level, "[*]")
    print(f"  {timestamp} {symbol} {msg}")


def clean_html(html: str, max_size: int = MAX_HTML_SIZE) -> str:
    """フォーム解析に不要なHTML要素・属性を除去してサイズを削減する。

    削除対象:
      - script / style / svg / img / picture / video / audio / canvas / noscript / link / meta タグ
      - HTMLコメント
      - 不要な属性: style, class, data-*（data-vv-name を除く）, aria-*（aria-label 等を除く）,
        tabindex, onclick 等イベントハンドラ
      - 過剰な空白・改行
    """
    original_size = len(html)

    # --- タグごと除去 ---
    # HTMLコメント
    html = re.sub(r"<!--[\s\S]*?-->", "", html)
    # script / style / svg（中身ごと）
    html = re.sub(r"<script\b[\s\S]*?</script>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<style\b[\s\S]*?</style>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<svg\b[\s\S]*?</svg>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<noscript\b[\s\S]*?</noscript>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<picture\b[\s\S]*?</picture>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<video\b[\s\S]*?</video>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<audio\b[\s\S]*?</audio>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<canvas\b[\s\S]*?</canvas>", "", html, flags=re.IGNORECASE)
    # 自己終了タグ (img, link, meta)
    html = re.sub(r"<img\b[^>]*/?>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<link\b[^>]*/?>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<meta\b[^>]*/?>", "", html, flags=re.IGNORECASE)

    # --- 不要な属性を除去 ---
    # style 属性
    html = re.sub(r'\s+style\s*=\s*"[^"]*"', "", html)
    html = re.sub(r"\s+style\s*=\s*'[^']*'", "", html)
    # class 属性
    html = re.sub(r'\s+class\s*=\s*"[^"]*"', "", html)
    html = re.sub(r"\s+class\s*=\s*'[^']*'", "", html)
    # data-* 属性（data-vv-name は保持）
    html = re.sub(r'\s+data-(?!vv-name)[\w-]+\s*=\s*"[^"]*"', "", html)
    html = re.sub(r"\s+data-(?!vv-name)[\w-]+\s*=\s*'[^']*'", "", html)
    # boolean data-* 属性（値なし）
    html = re.sub(r"\s+data-(?!vv-name)[\w-]+(?=[\s/>])", "", html)
    # aria-* 属性（aria-label, aria-labelledby, aria-describedby は保持）
    html = re.sub(
        r'\s+aria-(?!label\b|labelledby\b|describedby\b)[\w-]+\s*=\s*"[^"]*"',
        "", html,
    )
    html = re.sub(
        r"\s+aria-(?!label\b|labelledby\b|describedby\b)[\w-]+\s*=\s*'[^']*'",
        "", html,
    )
    # イベントハンドラ (onclick, onchange, onfocus, etc.)
    html = re.sub(r'\s+on\w+\s*=\s*"[^"]*"', "", html)
    html = re.sub(r"\s+on\w+\s*=\s*'[^']*'", "", html)
    # tabindex
    html = re.sub(r'\s+tabindex\s*=\s*"[^"]*"', "", html)
    html = re.sub(r"\s+tabindex\s*=\s*'[^']*'", "", html)

    # --- 空白圧縮 ---
    # 連続する空白・タブを1つに
    html = re.sub(r"[ \t]+", " ", html)
    # 連続する改行を1つに
    html = re.sub(r"\n\s*\n+", "\n", html)
    # 各行の前後空白を除去し、空行を除去
    html = "\n".join(line.strip() for line in html.split("\n") if line.strip())

    # --- サイズ制限 ---
    if len(html) > max_size:
        html = html[:max_size]
        # タグ境界で切る
        last_close = html.rfind(">")
        if last_close > max_size * 0.8:
            html = html[: last_close + 1]
        log(f"HTMLサイズ超過のため切り詰め: {len(html)} 文字", "WARN")

    reduction = (1 - len(html) / original_size) * 100 if original_size else 0
    log(
        f"HTMLクリーニング: {original_size:,} → {len(html):,} 文字 "
        f"({reduction:.0f}% 削減)",
        "OK",
    )
    return html


async def _extract_form_html(page) -> str:
    """ページからform要素のHTMLのみ抽出する（トークン節約）

    メインページにformがない場合はiframe内も探索する。
    """
    form_count = await page.locator("form").count()

    if form_count > 0:
        forms_html = []
        for i in range(form_count):
            form = page.locator("form").nth(i)
            outer = await form.evaluate("el => el.outerHTML")
            forms_html.append(outer)
        result = "\n".join(forms_html)
        log(f"フォーム抽出: {form_count}個のform要素, {len(result)} 文字", "OK")
        return result

    # メインページにformがない → iframe内を探索
    frames = page.frames
    for frame in frames:
        if frame == page.main_frame:
            continue
        try:
            form_count = await frame.locator("form").count()
            if form_count > 0:
                log(f"iframe内にform要素を発見: {frame.url}", "OK")
                forms_html = []
                for i in range(form_count):
                    form = frame.locator("form").nth(i)
                    outer = await form.evaluate("el => el.outerHTML")
                    forms_html.append(outer)
                result = "\n".join(forms_html)
                log(f"フォーム抽出（iframe）: {form_count}個のform要素, {len(result)} 文字", "OK")
                return result
        except Exception:
            continue

    log("form要素が見つかりません（iframe含む）。ページ全体を返します", "WARN")
    return await page.content()


async def fetch_rendered_html(
    url: str,
    timeout: int = 30,
    extract_form: bool = True,
    browser=None,
) -> str:
    """
    Playwrightでページをレンダリングし、HTMLを返す。

    Args:
        url: 取得するページのURL
        timeout: ページ読み込みタイムアウト（秒）
        extract_form: Trueの場合、<form>要素のHTMLのみ抽出する
        browser: 既存のブラウザインスタンス（バッチ処理での再利用用）
                 Noneの場合は内部で起動・終了する
    Returns:
        レンダリング済みHTML文字列。エラー時は空文字列
    """
    own_browser = browser is None
    p = None

    if own_browser:
        p = await async_playwright().start()
        browser = await p.chromium.launch(headless=True)

    try:
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="ja-JP",
        )
        page = await context.new_page()
        page.set_default_timeout(timeout * 1000)

        log(f"ページ取得中: {url}")
        await page.goto(url, wait_until="domcontentloaded")
        try:
            await page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            log("networkidle待機タイムアウト（DOM読み込み済みで続行）", "WARN")

        if extract_form:
            html = await _extract_form_html(page)
        else:
            html = await page.content()

        html = clean_html(html)
        log(f"HTML取得完了: {len(html):,} 文字", "OK")
        await context.close()
        return html

    except Exception as e:
        log(f"HTML取得エラー: {e}", "ERROR")
        return ""

    finally:
        if own_browser:
            await browser.close()
            if p:
                await p.stop()


def parse_args():
    parser = argparse.ArgumentParser(description="PlaywrightでページのHTMLを取得する")
    parser.add_argument("url", help="取得するページのURL")
    parser.add_argument(
        "--full-page", action="store_true", default=False,
        help="ページ全体を取得する（デフォルト: フォームのみ抽出）",
    )
    parser.add_argument(
        "--timeout", type=int, default=30,
        help="ページ読み込みタイムアウト（秒）（デフォルト: 30）",
    )
    parser.add_argument(
        "--output", default="",
        help="HTMLをファイルに出力する（デフォルト: 標準出力）",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    print("\n=== HTML取得モード ===\n")

    html = asyncio.run(fetch_rendered_html(
        url=args.url,
        timeout=args.timeout,
        extract_form=not args.full_page,
    ))

    if not html:
        print("HTMLの取得に失敗しました")
        return

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(html)
        log(f"ファイル出力: {args.output}", "OK")
    else:
        print(f"\n--- HTML ({len(html)} 文字) ---\n")
        print(html)


if __name__ == "__main__":
    main()
