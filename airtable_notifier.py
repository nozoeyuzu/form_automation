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


def _build_headers() -> dict | None:
    """Airtable API 用ヘッダーを返す。"""
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID or not AIRTABLE_TABLE_ID:
        return None

    return {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }


def _build_table_url() -> str:
    """Airtable テーブル URL を返す。"""
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"


def _escape_formula_value(value: str) -> str:
    """filterByFormula 用に値をエスケープする。"""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _build_filter_formula(company_name: str) -> str:
    """企業名の完全一致検索用 formula を返す。"""
    return f"{{企業名}}='{_escape_formula_value(company_name)}'"


def _build_update_payload(record_ids: list[str], status: str) -> dict:
    """PATCH 用ペイロードを返す。"""
    return {
        "records": [
            {
                "id": record_id,
                "fields": {"フォーム送信状況": status},
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
        "filterByFormula": _build_filter_formula(company_name),
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
        "filterByFormula": _build_filter_formula(company_name),
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


def notify(company_name: str, status: str) -> None:
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
        payload = _build_update_payload(record_ids, status)
        resp = requests.patch(url, headers=headers, json=payload, timeout=10)
        if resp.status_code >= 300:
            print(f"  [!] Airtable通知失敗 (HTTP {resp.status_code}): {resp.text}")
    except Exception as e:
        print(f"  [!] Airtable通知エラー: {e}")


async def async_notify(
    company_name: str,
    status: str,
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
        payload = _build_update_payload(record_ids, status)
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
