"""
フィールドタイプ別の入力ロジック + 営業データマッチング
"""
import re


# ─── 営業データキー → フォームのlabel/nameパターンのマッピング ───
# 左: config.jsonのキー, 右: フォームのlabel or nameに含まれうるパターン（正規表現）
LABEL_MAPPING = {
    "company": [r"会社", r"企業", r"法人", r"company", r"organization", r"corp"],
    "department": [r"部署", r"部門", r"所属", r"department", r"division"],
    "position": [r"役職", r"肩書", r"position", r"title", r"job.?title"],
    "name_sei": [r"^姓$", r"氏名.*姓", r"お名前.*姓", r"名前.*姓", r"last.?name", r"family.?name", r"sei"],
    "name_mei": [r"^名$", r"氏名.*名", r"お名前.*名", r"名前.*名", r"first.?name", r"given.?name", r"mei"],
    "fullname": [r"氏名", r"お名前", r"名前", r"full.?name", r"your.?name", r"^name$"],
    "kana_sei": [r"セイ", r"フリガナ.*姓", r"ふりがな.*姓", r"カナ.*姓", r"kana.*sei"],
    "kana_mei": [r"メイ", r"フリガナ.*名", r"ふりがな.*名", r"カナ.*名", r"kana.*mei"],
    "kana_full": [r"フリガナ", r"ふりがな", r"カナ", r"kana"],
    "email": [r"メール", r"e-?mail", r"mail", r"メールアドレス"],
    "email_confirm": [r"メール.*確認", r"メール.*再", r"確認.*メール", r"e-?mail.*confirm", r"mail.*conf"],
    "phone": [r"電話", r"tel", r"phone", r"連絡先"],
    "phone1": [r"電話.*1", r"tel.*1", r"市外"],
    "phone2": [r"電話.*2", r"tel.*2", r"市内"],
    "phone3": [r"電話.*3", r"tel.*3"],
    "fax": [r"fax", r"ファックス", r"ファクス"],
    "zipcode": [r"郵便番号", r"zip", r"postal", r"〒"],
    "zipcode1": [r"郵便番号.*前", r"zip.*1", r"postal.*1"],
    "zipcode2": [r"郵便番号.*後", r"zip.*2", r"postal.*2"],
    "prefecture": [r"都道府県", r"prefecture", r"pref"],
    "city": [r"市区?町村", r"市郡", r"city"],
    "address": [r"番地", r"丁目", r"住所", r"address", r"addr"],
    "building": [r"マンション", r"アパート", r"建物", r"ビル", r"building"],
    "message": [r"お問い合わせ", r"問い合わせ", r"内容", r"メッセージ", r"ご用件", r"ご相談", r"ご意見", r"ご質問", r"message", r"inquiry", r"comment", r"memo", r"備考"],
    "url": [r"url", r"ホームページ", r"サイト", r"website", r"web"],
    "gender": [r"性別", r"gender", r"sex"],
    "birth_year": [r"生年月日.*年", r"birth.*year", r"年$"],
    "birth_month": [r"生年月日.*月", r"birth.*month", r"月$"],
    "birth_day": [r"生年月日.*日", r"birth.*day", r"日$"],
}

# 電話番号・郵便番号の分割パターン検出用
PHONE_SPLIT_PATTERNS = [
    (r"tel.*1|電話.*1|市外|phone.*1", 0),
    (r"tel.*2|電話.*2|市内|phone.*2", 1),
    (r"tel.*3|電話.*3|phone.*3", 2),
]

ZIPCODE_SPLIT_PATTERNS = [
    (r"zip.*1|郵便.*前|postal.*1", 0),
    (r"zip.*2|郵便.*後|postal.*2", 1),
]



def match_field_to_data(field: dict, config_data: dict, all_fields: list) -> str | None:
    """
    フォームフィールドのlabel/nameから営業データの適切な値を見つける。

    Args:
        field: フォーム解析JSONの1フィールド
        config_data: 営業データ辞書
        all_fields: 全フィールドリスト（文脈判定用）

    Returns:
        マッチした値、またはNone
    """
    label = field.get("label", "")
    name = field.get("name", "")
    placeholder = field.get("placeholder", "")

    if not label and not name and not placeholder:
        return None

    # 直接的なキーマッチ（config_dataのキーがnameと一致する場合）
    if name in config_data:
        return str(config_data[name])

    # 電話番号・郵便番号の分割対応（label + name のみで判定、placeholderは含めない）
    label_name = f"{label} {name}".strip()

    # 電話番号の分割対応
    if "phone" in config_data:
        phone_parts = re.split(r"[-ー−]", config_data["phone"])
        for pattern, idx in PHONE_SPLIT_PATTERNS:
            if re.search(pattern, label_name, re.IGNORECASE) and idx < len(phone_parts):
                return phone_parts[idx]

    # 郵便番号の分割対応
    if "zipcode" in config_data:
        zip_parts = re.split(r"[-ー−]", config_data["zipcode"])
        for pattern, idx in ZIPCODE_SPLIT_PATTERNS:
            if re.search(pattern, label_name, re.IGNORECASE) and idx < len(zip_parts):
                return zip_parts[idx]

    # 電話番号のフィールドグループ検出（連続するtel系フィールドのインデックスで判定）
    if "phone" in config_data and field.get("type") in ("text", "tel", "number"):
        phone_parts = re.split(r"[-ー−]", config_data["phone"])
        phone_field_group = _find_phone_field_group(field, all_fields)
        if phone_field_group is not None and phone_field_group < len(phone_parts):
            return phone_parts[phone_field_group]

    # 郵便番号のフィールドグループ検出
    if "zipcode" in config_data and field.get("type") in ("text", "tel", "number"):
        zip_parts = re.split(r"[-ー−]", config_data["zipcode"])
        zip_field_group = _find_zip_field_group(field, all_fields)
        if zip_field_group is not None and zip_field_group < len(zip_parts):
            return zip_parts[zip_field_group]

    # ラベルマッピングで検索（labelを優先し、次にnameで検索）
    for config_key, patterns in LABEL_MAPPING.items():
        if config_key not in config_data:
            continue
        for pattern in patterns:
            if label and re.search(pattern, label, re.IGNORECASE):
                return str(config_data[config_key])

    for config_key, patterns in LABEL_MAPPING.items():
        if config_key not in config_data:
            continue
        for pattern in patterns:
            if name and re.search(pattern, name, re.IGNORECASE):
                return str(config_data[config_key])

    return None


def _find_phone_field_group(field: dict, all_fields: list) -> int | None:
    """連続する電話番号フィールドグループ内でのインデックスを返す"""
    name = field.get("name", "")
    label = field.get("label", "")
    search_text = f"{label} {name}"

    if not re.search(r"tel|電話|phone", search_text, re.IGNORECASE):
        return None

    phone_fields = []
    for f in all_fields:
        f_search = f"{f.get('label', '')} {f.get('name', '')}"
        if re.search(r"tel|電話|phone", f_search, re.IGNORECASE) and f.get("type") in ("text", "tel", "number"):
            phone_fields.append(f)

    # フィールドが1つだけなら分割不要
    if len(phone_fields) < 2:
        return None

    for i, pf in enumerate(phone_fields):
        if pf.get("name") == name:
            return i

    return None


def _find_zip_field_group(field: dict, all_fields: list) -> int | None:
    """連続する郵便番号フィールドグループ内でのインデックスを返す"""
    name = field.get("name", "")
    label = field.get("label", "")
    search_text = f"{label} {name}"

    if not re.search(r"zip|郵便|postal|〒", search_text, re.IGNORECASE):
        return None

    zip_fields = []
    for f in all_fields:
        f_search = f"{f.get('label', '')} {f.get('name', '')}"
        if re.search(r"zip|郵便|postal|〒", f_search, re.IGNORECASE) and f.get("type") in ("text", "tel", "number"):
            zip_fields.append(f)

    # フィールドが1つだけなら分割不要
    if len(zip_fields) < 2:
        return None

    for i, zf in enumerate(zip_fields):
        if zf.get("name") == name:
            return i

    return None


async def fill_field(page, field: dict, value: str) -> dict:
    """
    1つのフィールドに値を入力する。

    Args:
        page: Playwright Page オブジェクト
        field: フォーム解析JSONの1フィールド
        value: 入力する値

    Returns:
        結果辞書 {"field": name, "status": "ok"|"skip"|"error", "message": str}
    """
    field_name = field.get("name", "")
    field_type = field.get("type", "text")
    label = field.get("label", "")

    result = {"field": field_name, "label": label, "type": field_type, "value": value}

    # hidden はスキップ
    if field_type == "hidden":
        result["status"] = "skip"
        result["message"] = "hidden field"
        return result

    # submit ボタンはスキップ
    if field_type in ("submit", "button"):
        result["status"] = "skip"
        result["message"] = "submit/button field"
        return result

    # file は将来対応
    if field_type == "file":
        result["status"] = "skip"
        result["message"] = "file upload not supported yet"
        return result

    # 値がない場合はスキップ
    if not value:
        result["status"] = "skip"
        result["message"] = "no matching data"
        return result

    # セレクターを解決
    selector = _resolve_selector(field)

    try:
        if field_type in ("text", "email", "tel", "number", "url", "search"):
            await _fill_text(page, selector, field, value)
        elif field_type == "textarea":
            await _fill_text(page, selector, field, value)
        elif field_type == "select":
            await _fill_select(page, selector, field, value)
        elif field_type == "radio":
            await _fill_radio(page, field, value)
        elif field_type == "checkbox":
            await _fill_checkbox(page, field, value)
        else:
            await _fill_text(page, selector, field, value)

        result["status"] = "ok"
        result["message"] = f"filled with '{value}'"
    except Exception as e:
        result["status"] = "error"
        result["message"] = str(e)

    return result


def _resolve_selector(field: dict) -> str:
    """フィールド情報からPlaywrightセレクターを解決する"""
    name = field.get("name", "")
    field_id = field.get("id", "")
    field_type = field.get("type", "text")

    # 1. name属性
    if name:
        if field_type == "textarea":
            return f'textarea[name="{name}"]'
        elif field_type == "select":
            return f'select[name="{name}"]'
        else:
            return f'[name="{name}"]'

    # 2. id属性
    if field_id:
        return f'#{field_id}'

    # 3. label + placeholder でのフォールバック
    label = field.get("label", "")
    placeholder = field.get("placeholder", "")

    if label:
        tag = "textarea" if field_type == "textarea" else "select" if field_type == "select" else "input"
        return f'label:has-text("{label}") >> {tag}'

    if placeholder:
        return f'[placeholder*="{placeholder}"]'

    return ""


async def _fill_text(page, selector: str, field: dict, value: str):
    """テキスト系フィールドへの入力"""
    if selector:
        locator = page.locator(selector).first
    else:
        label = field.get("label", "")
        if label:
            locator = page.get_by_label(label).first
        else:
            raise ValueError(f"Cannot resolve selector for field: {field.get('name', 'unknown')}")

    await locator.scroll_into_view_if_needed()
    await locator.clear()
    await locator.fill(value)


async def _fill_select(page, selector: str, field: dict, value: str):
    """セレクトボックスへの入力（label優先、fallback to value）"""
    if selector:
        locator = page.locator(selector).first
    else:
        label = field.get("label", "")
        if label:
            locator = page.get_by_label(label).first
        else:
            raise ValueError(f"Cannot resolve selector for select: {field.get('name', 'unknown')}")

    await locator.scroll_into_view_if_needed()

    # まずlabel（表示テキスト）で選択を試みる
    try:
        await locator.select_option(label=value)
        return
    except Exception:
        pass

    # 次にvalueで選択を試みる
    try:
        await locator.select_option(value=value)
        return
    except Exception:
        pass

    # 部分一致で探す
    options = field.get("options", [])
    for opt in options:
        if isinstance(opt, str) and value in opt:
            try:
                await locator.select_option(label=opt)
                return
            except Exception:
                pass

    raise ValueError(f"Could not select '{value}' in {field.get('name', 'unknown')}")


async def _fill_radio(page, field: dict, value: str):
    """ラジオボタンの選択"""
    name = field.get("name", "")
    options = field.get("options", [])

    # 選択肢リストを構築（optionsがあればそこから、なければvalueをそのまま使う）
    candidates = []
    if options:
        for opt in options:
            opt_text = opt if isinstance(opt, str) else str(opt)
            if opt_text == value or value in opt_text:
                candidates.append(opt_text)
    if not candidates:
        candidates = [value]

    for candidate in candidates:
        if name:
            # name属性 + value で探す
            radio = page.locator(f'input[name="{name}"][value="{candidate}"]')
            if await radio.count() > 0:
                # labelがinputを覆っている場合が多いので、まずlabel[for]で試す
                radio_id = await radio.first.get_attribute("id")
                if radio_id:
                    label_for = page.locator(f'label[for="{radio_id}"]')
                    if await label_for.count() > 0:
                        await label_for.first.click()
                        return
                # label[for]がない場合はforce=Trueでチェック
                await radio.first.check(force=True)
                return

            # ラベルテキストで探す
            label_locator = page.locator(f'label:has-text("{candidate}")')
            if await label_locator.count() > 0:
                await label_locator.first.click()
                return

        # get_by_labelで探す
        try:
            await page.get_by_label(candidate).first.check()
            return
        except Exception:
            pass

    raise ValueError(f"Could not find radio option '{value}' for {name}")


async def _fill_checkbox(page, field: dict, value: str):
    """チェックボックスの選択"""
    name = field.get("name", "")

    if name:
        checkbox = page.locator(f'input[name="{name}"]')
        if await checkbox.count() > 0:
            if value.lower() in ("true", "1", "on", "yes", "checked"):
                await checkbox.first.check()
            return

    # ラベルテキストで探す
    label = field.get("label", "")
    if label:
        try:
            await page.get_by_label(label).first.check()
            return
        except Exception:
            pass

    raise ValueError(f"Could not find checkbox for {name}")
