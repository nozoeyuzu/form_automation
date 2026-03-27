"""
Slack Webhook 通知モジュール

フォーム送信結果をSlackチャンネルに通知する。
通知エラーが発生してもメインの送信フローを止めない。
"""
import os
from datetime import datetime

import aiohttp
import requests
from dotenv import load_dotenv

from airtable_notifier import async_notify as airtable_async_notify
from airtable_notifier import notify as airtable_notify

load_dotenv()

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


def _build_payload(
    company_name: str,
    contact_url: str,
    status: str,
    message: str = "",
    no_fit_reason: str = "",
    final_body: str = "",
) -> dict:
    """Slack通知用のペイロードを組み立てる。"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    is_ok = status == "ok"
    emoji = ":white_check_mark:" if is_ok else ":x:"
    status_label = "成功" if is_ok else "失敗"

    text = f"{emoji} *フォーム送信{status_label}*\n"
    text += f"• 会社名: {company_name or '(不明)'}\n"
    text += f"• URL: {contact_url}\n"
    text += f"• ステータス: {status}\n"
    if message:
        display_message = message.removeprefix("ERROR: ").removeprefix("ERROR:")
        text += f"• メッセージ: {display_message}\n"
    if no_fit_reason:
        text += f"• 理由: {no_fit_reason}\n"
    if final_body:
        text += f"• 送信内容:\n{final_body}\n"
    text += f"• 時刻: {now}"

    return {"text": text}


def notify(
    company_name: str,
    contact_url: str,
    status: str,
    message: str = "",
    no_fit_reason: str = "",
    final_body: str = "",
    airtable_record_id: str = "",
) -> None:
    """フォーム送信結果をSlackに通知する（同期版）。

    try/exceptで囲んでいるため、通知エラーが起きてもメインフローは止まらない。
    """
    payload = _build_payload(company_name, contact_url, status, message, no_fit_reason, final_body)

    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        if resp.status_code != 200:
            print(f"  [!] Slack通知失敗 (HTTP {resp.status_code}): {resp.text}")
    except Exception as e:
        print(f"  [!] Slack通知エラー: {e}")

    airtable_notify(
        company_name=company_name,
        status=status,
        record_id=airtable_record_id,
        final_body=final_body if status == "ok" else "",
    )


async def async_notify(
    company_name: str,
    contact_url: str,
    status: str,
    message: str = "",
    no_fit_reason: str = "",
    final_body: str = "",
    session: aiohttp.ClientSession | None = None,
    airtable_record_id: str = "",
) -> None:
    """フォーム送信結果をSlackに通知する（非同期版）。

    try/exceptで囲んでいるため、通知エラーが起きてもメインフローは止まらない。
    """
    payload = _build_payload(company_name, contact_url, status, message, no_fit_reason, final_body)

    own_session = session is None
    if own_session:
        session = aiohttp.ClientSession()

    try:
        async with session.post(
            SLACK_WEBHOOK_URL,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                print(f"  [!] Slack通知失敗 (HTTP {resp.status}): {body}")
    except Exception as e:
        print(f"  [!] Slack通知エラー: {e}")
    finally:
        try:
            await airtable_async_notify(
                company_name=company_name,
                status=status,
                session=session,
                record_id=airtable_record_id,
                final_body=final_body if status == "ok" else "",
            )
        finally:
            if own_session:
                await session.close()
