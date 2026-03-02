"""
フォーム解析・検出ロジック
- CAPTCHA検出
- 確認画面検出
- エラーメッセージ検出
- SPA検出
"""
import re


# ─── 確認ボタンの文言パターン ───
CONFIRM_BUTTON_PATTERNS = [
    r"確認",
    r"入力内容確認",
    r"次へ",
    r"次のステップ",
    r"confirm",
    r"proceed",
    r"next",
    r"review",
]

# ─── 送信ボタンの文言パターン ───
SUBMIT_BUTTON_PATTERNS = [
    r"送信",
    r"送る",
    r"submit",
    r"send",
    r"完了",
    r"申し込む",
    r"申込",
    r"登録",
]

# ─── エラーメッセージのパターン ───
ERROR_PATTERNS = [
    r"入力エラー",
    r"エラーがあります",
    r"正しく入力",
    r"必須項目",
    r"入力してください",
    r"無効な",
    r"不正な",
    r"error",
    r"invalid",
    r"required",
    r"please enter",
    r"please fill",
]


async def detect_captcha(page) -> dict:
    """
    CAPTCHA（reCAPTCHA / hCaptcha / Turnstile）を検出する。

    Returns:
        {"detected": bool, "type": str, "message": str}
    """
    checks = [
        ('iframe[src*="recaptcha"]', "reCAPTCHA"),
        ('iframe[src*="google.com/recaptcha"]', "reCAPTCHA"),
        (".g-recaptcha", "reCAPTCHA"),
        ("#g-recaptcha", "reCAPTCHA"),
        ('iframe[src*="hcaptcha"]', "hCaptcha"),
        (".h-captcha", "hCaptcha"),
        ('iframe[src*="turnstile"]', "Cloudflare Turnstile"),
        (".cf-turnstile", "Cloudflare Turnstile"),
    ]

    for selector, captcha_type in checks:
        try:
            count = await page.locator(selector).count()
            if count > 0:
                return {
                    "detected": True,
                    "type": captcha_type,
                    "message": f"{captcha_type} detected. Auto-submit may not work.",
                }
        except Exception:
            continue

    return {"detected": False, "type": None, "message": "No CAPTCHA detected"}


async def detect_confirm_button(page, form_data: dict = None) -> dict:
    """
    確認ボタンを検出する。

    Args:
        page: Playwright Page
        form_data: フォーム解析JSON（submit情報を参照）

    Returns:
        {"found": bool, "selector": str, "text": str}
    """
    # フォーム解析JSONにsubmit情報がある場合はそれを使う
    if form_data:
        submit_info = form_data.get("submit", {})
        submit_value = submit_info.get("value", "")
        submit_label = submit_info.get("label", "")
        submit_name = submit_info.get("name", "")

        # 確認系の文言かチェック
        check_text = f"{submit_value} {submit_label}"
        is_confirm = any(
            re.search(p, check_text, re.IGNORECASE) for p in CONFIRM_BUTTON_PATTERNS
        )

        if is_confirm and submit_name:
            selector = f'[name="{submit_name}"]'
            try:
                if await page.locator(selector).count() > 0:
                    return {
                        "found": True,
                        "selector": selector,
                        "text": submit_value or submit_label,
                    }
            except Exception:
                pass

    # ページ上のボタンをスキャン
    for pattern in CONFIRM_BUTTON_PATTERNS:
        # input[type="submit"] / input[type="button"]
        for tag in ['input[type="submit"]', 'input[type="button"]', "button"]:
            try:
                locator = page.locator(tag)
                count = await locator.count()
                for i in range(count):
                    el = locator.nth(i)
                    # ボタンのテキストまたはvalue属性を取得
                    text = await el.inner_text() if tag == "button" else await el.get_attribute("value") or ""
                    if re.search(pattern, text, re.IGNORECASE):
                        return {
                            "found": True,
                            "selector": None,
                            "text": text,
                            "_locator": el,
                        }
            except Exception:
                continue

    return {"found": False, "selector": None, "text": ""}


async def detect_submit_button(page) -> dict:
    """
    最終送信ボタンを検出する。

    Returns:
        {"found": bool, "selector": str, "text": str}
    """
    for pattern in SUBMIT_BUTTON_PATTERNS:
        for tag in ['input[type="submit"]', 'input[type="button"]', "button"]:
            try:
                locator = page.locator(tag)
                count = await locator.count()
                for i in range(count):
                    el = locator.nth(i)
                    text = await el.inner_text() if tag == "button" else await el.get_attribute("value") or ""
                    if re.search(pattern, text, re.IGNORECASE):
                        return {
                            "found": True,
                            "selector": None,
                            "text": text,
                            "_locator": el,
                        }
            except Exception:
                continue

    return {"found": False, "selector": None, "text": ""}


async def detect_errors(page) -> list[dict]:
    """
    ページ上のエラーメッセージを検出する。

    Returns:
        エラー情報のリスト [{"text": str, "selector": str}]
    """
    errors = []

    # エラー系のCSSクラスを持つ要素を探す
    error_selectors = [
        ".error",
        ".err",
        ".validation-error",
        ".form-error",
        ".field-error",
        ".is-error",
        ".has-error",
        '[class*="error"]',
        '[role="alert"]',
    ]

    for selector in error_selectors:
        try:
            locator = page.locator(selector)
            count = await locator.count()
            for i in range(count):
                el = locator.nth(i)
                if await el.is_visible():
                    text = (await el.inner_text()).strip()
                    if text and len(text) < 200:
                        errors.append({"text": text, "selector": selector})
        except Exception:
            continue

    # テキストパターンで検出
    for pattern in ERROR_PATTERNS:
        try:
            locator = page.locator(f'text=/{pattern}/i')
            count = await locator.count()
            for i in range(min(count, 5)):
                el = locator.nth(i)
                if await el.is_visible():
                    text = (await el.inner_text()).strip()
                    if text and len(text) < 200:
                        # 重複チェック
                        if not any(e["text"] == text for e in errors):
                            errors.append({"text": text, "selector": f"text=/{pattern}/i"})
        except Exception:
            continue

    return errors


async def wait_for_page_ready(page, timeout: int = 30000):
    """
    ページの読み込み完了を待つ（SPA対応含む）。
    """
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=timeout)
    except Exception:
        pass

    try:
        await page.wait_for_load_state("networkidle", timeout=timeout)
    except Exception:
        # networkidleはタイムアウトしやすいので無視
        pass

    # フォーム要素が存在するまで待つ
    try:
        await page.wait_for_selector("form", timeout=10000)
    except Exception:
        # formタグがない場合もある
        pass
