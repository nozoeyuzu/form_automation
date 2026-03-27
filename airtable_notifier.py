"""
Airtable 通知モジュール

Slack に送る結果の要点を Airtable の既存レコードにも反映する。
通知エラーが発生してもメインの送信フローは止めない。
"""
import os

import aiohttp
import requests
from dotenv import load_dotenv

load_dotenv()

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "appcAG0TeOXanO3Id")
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID", "tbljPPJj1FuGLRhyG")
AIRTABLE_COMPANY_SEARCH_FIELD = os.getenv("AIRTABLE_COMPANY_SEARCH_FIELD", "企業名（検索用）")
AIRTABLE_STATUS_FIELD = os.getenv("AIRTABLE_STATUS_FIELD", "フォーム送信状況")
AIRTABLE_CAMPAIGN_LINK_FIELD = os.getenv("AIRTABLE_CAMPAIGN_LINK_FIELD", "キャンペーンマスター")
AIRTABLE_CAMPAIGN_TABLE_ID = os.getenv("AIRTABLE_CAMPAIGN_TABLE_ID", "")
AIRTABLE_CAMPAIGN_SEARCH_FIELD = os.getenv("AIRTABLE_CAMPAIGN_SEARCH_FIELD", "キャンペーン名")

FORM_TYPE_TO_CAMPAIGN_NAME = {
    "A": "フォームA",
    "B": "フォームB",
}


def _build_headers() -> dict | None:
    """Airtable API 用ヘッダーを返す。"""
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID or not AIRTABLE_TABLE_ID:
        return None

    return {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }


def _build_table_url(table_id: str | None = None) -> str:
    """Airtable テーブル URL を返す。"""
    target_table_id = table_id or AIRTABLE_TABLE_ID
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{target_table_id}"


def _escape_formula_value(value: str) -> str:
    """filterByFormula 用に値をエスケープする。"""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _build_filter_formula(field_name: str, value: str) -> str:
    """指定フィールドの完全一致検索用 formula を返す。"""
    return f"{{{field_name}}}='{_escape_formula_value(value)}'"


def _resolve_campaign_name(form_type: str) -> str:
    """フォーム種別からキャンペーン名を返す。"""
    return FORM_TYPE_TO_CAMPAIGN_NAME.get(form_type.upper(), "")


def _build_update_payload(
    record_ids: list[str],
    status: str,
    campaign_record_id: str = "",
) -> dict:
    """PATCH 用ペイロードを返す。"""
    fields = {AIRTABLE_STATUS_FIELD: status}
    if campaign_record_id:
        fields[AIRTABLE_CAMPAIGN_LINK_FIELD] = [campaign_record_id]

    return {
        "records": [
            {
                "id": record_id,
                "fields": fields,
            }
            for record_id in record_ids
        ]
    }


def _find_matching_record_ids(
    company_name: str,
    headers: dict,
) -> list[str]:
    """企業名が一致するレコード ID を返す。"""
    url = _build_table_url()
    params = {
        "filterByFormula": _build_filter_formula(AIRTABLE_COMPANY_SEARCH_FIELD, company_name),
        "pageSize": 100,
    }
    resp = requests.get(url, headers=headers, params=params, timeout=10)
    if resp.status_code >= 300:
        raise RuntimeError(f"Airtable検索失敗 (HTTP {resp.status_code}): {resp.text}")

    body = resp.json()
    return [record["id"] for record in body.get("records", [])]


async def _find_matching_record_ids_async(
    company_name: str,
    headers: dict,
    session: aiohttp.ClientSession,
) -> list[str]:
    """企業名が一致するレコード ID を非同期で返す。"""
    url = _build_table_url()
    params = {
        "filterByFormula": _build_filter_formula(AIRTABLE_COMPANY_SEARCH_FIELD, company_name),
        "pageSize": 100,
    }
    async with session.get(
        url,
        headers=headers,
        params=params,
        timeout=aiohttp.ClientTimeout(total=10),
    ) as resp:
        if resp.status >= 300:
            body = await resp.text()
            raise RuntimeError(f"Airtable検索失敗 (HTTP {resp.status}): {body}")

        body = await resp.json()
        return [record["id"] for record in body.get("records", [])]


def _find_campaign_record_id(form_type: str, headers: dict) -> str:
    """フォーム種別に対応するキャンペーン record ID を返す。"""
    campaign_name = _resolve_campaign_name(form_type)
    if not campaign_name or not AIRTABLE_CAMPAIGN_TABLE_ID:
        return ""

    url = _build_table_url(AIRTABLE_CAMPAIGN_TABLE_ID)
    params = {
        "filterByFormula": _build_filter_formula(AIRTABLE_CAMPAIGN_SEARCH_FIELD, campaign_name),
        "pageSize": 1,
    }
    resp = requests.get(url, headers=headers, params=params, timeout=10)
    if resp.status_code >= 300:
        raise RuntimeError(f"キャンペーン検索失敗 (HTTP {resp.status_code}): {resp.text}")

    body = resp.json()
    records = body.get("records", [])
    return records[0]["id"] if records else ""


async def _find_campaign_record_id_async(
    form_type: str,
    headers: dict,
    session: aiohttp.ClientSession,
) -> str:
    """フォーム種別に対応するキャンペーン record ID を非同期で返す。"""
    campaign_name = _resolve_campaign_name(form_type)
    if not campaign_name or not AIRTABLE_CAMPAIGN_TABLE_ID:
        return ""

    url = _build_table_url(AIRTABLE_CAMPAIGN_TABLE_ID)
    params = {
        "filterByFormula": _build_filter_formula(AIRTABLE_CAMPAIGN_SEARCH_FIELD, campaign_name),
        "pageSize": 1,
    }
    async with session.get(
        url,
        headers=headers,
        params=params,
        timeout=aiohttp.ClientTimeout(total=10),
    ) as resp:
        if resp.status >= 300:
            body = await resp.text()
            raise RuntimeError(f"キャンペーン検索失敗 (HTTP {resp.status}): {body}")

        body = await resp.json()
        records = body.get("records", [])
        return records[0]["id"] if records else ""


def notify(company_name: str, status: str, form_type: str = "") -> None:
    """企業名一致の Airtable レコードを更新する（同期版）。"""
    headers = _build_headers()
    if headers is None or not company_name:
        return

    try:
        record_ids = _find_matching_record_ids(company_name, headers)
        if not record_ids:
            print(f"  [!] Airtable一致レコードなし: {company_name}")
            return

        url = _build_table_url()
        campaign_record_id = _find_campaign_record_id(form_type, headers)
        if form_type and not campaign_record_id:
            print(f"  [!] Airtableキャンペーン一致レコードなし: {form_type}")
        payload = _build_update_payload(record_ids, status, campaign_record_id)
        resp = requests.patch(url, headers=headers, json=payload, timeout=10)
        if resp.status_code >= 300:
            print(f"  [!] Airtable通知失敗 (HTTP {resp.status_code}): {resp.text}")
    except Exception as e:
        print(f"  [!] Airtable通知エラー: {e}")


async def async_notify(
    company_name: str,
    status: str,
    form_type: str = "",
    session: aiohttp.ClientSession | None = None,
) -> None:
    """企業名一致の Airtable レコードを更新する（非同期版）。"""
    headers = _build_headers()
    if headers is None or not company_name:
        return

    own_session = session is None
    if own_session:
        session = aiohttp.ClientSession()

    try:
        record_ids = await _find_matching_record_ids_async(company_name, headers, session)
        if not record_ids:
            print(f"  [!] Airtable一致レコードなし: {company_name}")
            return

        url = _build_table_url()
        campaign_record_id = await _find_campaign_record_id_async(form_type, headers, session)
        if form_type and not campaign_record_id:
            print(f"  [!] Airtableキャンペーン一致レコードなし: {form_type}")
        payload = _build_update_payload(record_ids, status, campaign_record_id)
        async with session.patch(
            url,
            headers=headers,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status >= 300:
                body = await resp.text()
                print(f"  [!] Airtable通知失敗 (HTTP {resp.status}): {body}")
    except Exception as e:
        print(f"  [!] Airtable通知エラー: {e}")
    finally:
        if own_session:
            await session.close()
