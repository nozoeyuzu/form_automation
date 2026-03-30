"""
Airtable レコード読み込みモジュール

Airtable APIからバッチ処理対象のレコードを取得し、
run_batch.py の process_single() が期待する dict 形式に変換する。

テーブル構成:
  tblHAn3RGmqg6vUAr - キャンペーンマスタ（キャンペーン名で検索）
  tblvipFnhShnzbfW1 - 対象企業一覧（キャンペーンリンクでフィルタ、結果書き戻し先）
  tblQfOEB2vL3GFnq3 - Riskdog業界マスタ（リンク列の実値を取得）
"""
import os

import aiohttp
from dotenv import load_dotenv

from run_codegen import log

load_dotenv()

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "")

# テーブルID
TABLE_CAMPAIGN = "tblHAn3RGmqg6vUAr"
TABLE_TARGETS = "tblvipFnhShnzbfW1"
TABLE_RISKDOG = "tblQfOEB2vL3GFnq3"

# ビューID（このビューのソート順でレコードを取得する）
VIEW_TARGETS = "viwdtor8Ut1IeJlE8"

# フォームタイプの短縮名 → キャンペーン名
FORM_TYPE_MAP = {
    "A": "フォームA",
    "B": "フォームB",
    "C": "フォームC",
}

# Airtable フィールド名 → 内部キーのマッピング（tblvipFnhShnzbfW1）
# 注: 企業名・企業概要等はルックアップ列で配列として返る
FIELD_MAP = {
    "企業名（検索用）": "company_name",
    "会社サイトURL": "company_url",
    "問い合わせURL": "contact_url",
    "企業概要": "company_overview",
    "事業内容一言説明": "business_summary",
    "Riskdog業界": "riskdog_industry",
}


def _headers() -> dict:
    if not AIRTABLE_API_KEY:
        raise RuntimeError("AIRTABLE_API_KEY が設定されていません")
    return {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }


def _table_url(table_id: str) -> str:
    if not AIRTABLE_BASE_ID:
        raise RuntimeError("AIRTABLE_BASE_ID が設定されていません")
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_id}"


def _escape(value: str) -> str:
    """filterByFormula 用に値をエスケープする。"""
    return value.replace("\\", "\\\\").replace("'", "\\'")


async def fetch_campaign_record_id(
    session: aiohttp.ClientSession,
    campaign_name: str,
) -> str:
    """キャンペーンマスタテーブルからキャンペーン名でrecord IDを取得する。"""
    url = _table_url(TABLE_CAMPAIGN)
    params = {
        "filterByFormula": f"{{キャンペーン名}}='{_escape(campaign_name)}'",
        "pageSize": 1,
    }

    log(f"キャンペーン検索: {campaign_name}")

    async with session.get(
        url, headers=_headers(), params=params,
        timeout=aiohttp.ClientTimeout(total=15),
    ) as resp:
        if resp.status >= 300:
            body = await resp.text()
            raise RuntimeError(f"キャンペーン検索失敗 (HTTP {resp.status}): {body}")
        body = await resp.json()

    records = body.get("records", [])
    if not records:
        raise RuntimeError(f"キャンペーン '{campaign_name}' が見つかりません")

    record_id = records[0]["id"]
    log(f"キャンペーンレコードID: {record_id}", "OK")
    return record_id


async def fetch_target_records(
    session: aiohttp.ClientSession,
    campaign_name: str,
    limit: int | None = None,
    status_filter: str = "unsent",
) -> list[dict]:
    """対象企業テーブルからフィルタ条件に合うレコードを取得する。

    条件:
      - キャンペーンマスタ列（リンク列）の表示値がcampaign_nameに一致
      - status_filter に応じたフォーム送信状況フィルタ
      - お問い合わせURLが空でない

    status_filter:
      "unsent" — フォーム送信状況が空（デフォルト）
      "error"  — フォーム送信状況が "error"
      "all"    — フォーム送信状況を問わない
    """
    url = _table_url(TABLE_TARGETS)
    conditions = [
        f"{{キャンペーンマスタ}}='{_escape(campaign_name)}'",
        f"{{問い合わせURL}}!=''",
    ]
    if status_filter == "unsent":
        conditions.append("{フォーム送信状況}=''")
    elif status_filter == "error":
        conditions.append("{フォーム送信状況}='error'")
    # "all" の場合は送信状況フィルタなし

    formula = f"AND({','.join(conditions)})"

    log(f"対象企業取得中 (filter: {formula})")

    all_records = []
    offset = None
    page = 0

    while True:
        page += 1
        params = {
            "pageSize": 100,
            "filterByFormula": formula,
            "view": VIEW_TARGETS,
        }
        if offset:
            params["offset"] = offset

        async with session.get(
            url, headers=_headers(), params=params,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status >= 300:
                body = await resp.text()
                raise RuntimeError(f"対象企業取得失敗 (HTTP {resp.status}): {body}")
            body = await resp.json()

        records = body.get("records", [])
        all_records.extend(records)
        log(f"  page {page}: {len(records)} 件取得")

        # 件数制限チェック
        if limit and len(all_records) >= limit:
            all_records = all_records[:limit]
            break

        offset = body.get("offset")
        if not offset:
            break

    log(f"対象企業取得完了: {len(all_records)} 件", "OK")
    return all_records


async def resolve_riskdog_industries(
    session: aiohttp.ClientSession,
    record_ids: set[str],
) -> dict[str, str]:
    """Riskdog業界マスタテーブルからrecord IDに対応する業界名を取得する。

    Returns:
        {record_id: "業界名文字列"} のマッピング
    """
    if not record_ids:
        return {}

    url = _table_url(TABLE_RISKDOG)
    result = {}

    # Airtable の OR formula で一括取得（100件ずつ）
    id_list = list(record_ids)
    for i in range(0, len(id_list), 100):
        batch = id_list[i:i + 100]
        or_conditions = ",".join(
            f"RECORD_ID()='{rid}'" for rid in batch
        )
        formula = f"OR({or_conditions})" if len(batch) > 1 else f"RECORD_ID()='{batch[0]}'"
        params = {
            "filterByFormula": formula,
            "pageSize": 100,
        }

        async with session.get(
            url, headers=_headers(), params=params,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status >= 300:
                body = await resp.text()
                log(f"Riskdog業界取得失敗 (HTTP {resp.status}): {body}", "WARN")
                continue
            body = await resp.json()

        for record in body.get("records", []):
            rid = record["id"]
            fields = record.get("fields", {})
            industry = fields.get("Riskdog業界", "")
            if isinstance(industry, list):
                industry = industry[0] if industry else ""
            result[rid] = str(industry) if industry else ""

    log(f"Riskdog業界解決: {len(result)} 件", "OK")
    return result


def map_records_to_rows(
    records: list[dict],
    riskdog_map: dict[str, str],
) -> list[dict]:
    """Airtableレコードを内部のrow dict形式に変換する。"""
    rows = []
    for record in records:
        fields = record.get("fields", {})
        row = {"_airtable_record_id": record["id"]}

        for airtable_key, internal_key in FIELD_MAP.items():
            value = fields.get(airtable_key, "")

            # リンク列は配列で返る
            if isinstance(value, list):
                if internal_key == "riskdog_industry":
                    # Riskdog業界: リンク先のrecord IDを実値に変換
                    resolved = [riskdog_map.get(v, v) for v in value]
                    value = ",".join(resolved)
                else:
                    value = value[0] if value else ""

            row[internal_key] = str(value) if value else ""

        rows.append(row)

    return rows


async def fetch_records(
    session: aiohttp.ClientSession,
    form_type: str,
    limit: int | None = None,
    status_filter: str = "unsent",
) -> list[dict]:
    """Airtableからバッチ処理用のレコードを取得するメイン関数。

    Args:
        session: aiohttp セッション
        form_type: フォーム種別の短縮名（"A", "B", "C"）
        limit: 取得件数の上限（Noneで全件）
        status_filter: "unsent"=未送信のみ, "error"=エラーのみ, "all"=全件

    Returns:
        process_single() が期待する dict のリスト
    """
    # フォーム種別を正式名に変換
    campaign_name = FORM_TYPE_MAP.get(form_type.upper())
    if not campaign_name:
        raise RuntimeError(
            f"不明なフォーム種別: {form_type} "
            f"(有効な値: {', '.join(FORM_TYPE_MAP.keys())})"
        )

    # Step 1: キャンペーンの存在確認
    await fetch_campaign_record_id(session, campaign_name)

    # Step 2: 対象企業レコードを取得（リンク列の表示値で検索）
    records = await fetch_target_records(
        session, campaign_name, limit, status_filter,
    )

    if not records:
        log("対象レコードが0件です", "WARN")
        return []

    # Step 3: Riskdog業界のリンク列を実値に解決
    riskdog_ids = set()
    for record in records:
        riskdog_values = record.get("fields", {}).get("Riskdog業界", [])
        if isinstance(riskdog_values, list):
            riskdog_ids.update(riskdog_values)

    riskdog_map = await resolve_riskdog_industries(session, riskdog_ids)

    # Step 4: 内部形式に変換
    rows = map_records_to_rows(records, riskdog_map)

    skipped = sum(1 for r in rows if not r.get("contact_url"))
    if skipped:
        rows = [r for r in rows if r.get("contact_url")]
        log(f"問い合わせURLが空の {skipped} 件をスキップしました", "WARN")

    log(f"Airtable読み込み完了: {len(rows)} 件", "OK")
    return rows
