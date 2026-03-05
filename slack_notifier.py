"""
Slack Webhook 通知モジュール

フォーム送信結果をSlackチャンネルに通知する。
通知エラーが発生してもメインの送信フローを止めない。
"""
import os
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


def notify(
    company_name: str,
    contact_url: str,
    status: str,
    message: str = "",
) -> None:
    """フォーム送信結果をSlackに通知する。

    try/exceptで囲んでいるため、通知エラーが起きてもメインフローは止まらない。
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    is_ok = status == "ok"
    emoji = ":white_check_mark:" if is_ok else ":x:"
    status_label = "成功" if is_ok else "失敗"

    text = f"{emoji} *フォーム送信{status_label}*\n"
    text += f"• 会社名: {company_name or '(不明)'}\n"
    text += f"• URL: {contact_url}\n"
    text += f"• ステータス: {status}\n"
    if message:
        text += f"• メッセージ: {message}\n"
    text += f"• 時刻: {now}"

    payload = {"text": text}

    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        if resp.status_code != 200:
            print(f"  [!] Slack通知失敗 (HTTP {resp.status_code}): {resp.text}")
    except Exception as e:
        print(f"  [!] Slack通知エラー: {e}")
