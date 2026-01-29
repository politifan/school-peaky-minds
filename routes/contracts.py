import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.status import HTTP_302_FOUND

import core
from core import CONTRACT_DOCUMENTS, CONTRACT_KEY_POINTS, render

router = APIRouter()


def _abs_url(path: str, request: Optional[Request] = None) -> str:
    if not path:
        return ""
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if core.APP_BASE_URL:
        return f"{core.APP_BASE_URL.rstrip('/')}{path}"
    if request:
        base = str(request.base_url).rstrip("/")
        return f"{base}{path}"
    return path


def _build_contract_text(agreement: Dict[str, Any], request: Optional[Request], token: str) -> str:
    course = agreement.get("course") or "курс"
    full_name = agreement.get("full_name") or agreement.get("user", {}).get("name") or "участник"
    contract_url = core.build_contract_url(token, request)
    lines = [
        "📄 Договор обучения",
        f"Курс: {course}",
        f"Участник: {full_name}",
        f"Ссылка: {contract_url}",
        "",
        "Ключевые пункты:",
    ]
    for point in CONTRACT_KEY_POINTS:
        lines.append(f"- {point}")
    if CONTRACT_DOCUMENTS:
        lines.append("")
        lines.append("Документы:")
        for title, doc_path in CONTRACT_DOCUMENTS:
            lines.append(f"- {title}: {_abs_url(doc_path, request)}")
    return "\n".join(lines)


def _resolve_default_channel(channels: List[Dict[str, Any]], preferred: str) -> str:
    available = [item["key"] for item in channels if not item.get("disabled")]
    if preferred in available:
        return preferred
    if available:
        return available[0]
    return preferred or "email"


@router.get("/contract/{token}", include_in_schema=False)
def contract_view(request: Request, token: str):
    agreement, _ = core.find_agreement_by_token(token)
    if not agreement:
        return HTMLResponse("Ссылка недействительна или договор не найден.", status_code=404)

    user = agreement.get("user") or {}
    status_key, status_label, status_class = core.contract_status_from_item(agreement)
    contract_url = core.build_contract_url(agreement.get("contract_token"), request)
    sent_at = agreement.get("contract_sent_at")
    signed_at = agreement.get("contract_signed_at")

    contact_email = core.resolve_contact_email(user, agreement, "")
    telegram_chat_id = core.extract_telegram_chat_id(user, agreement)
    vk_user_id = core.extract_vk_user_id(user)
    telegram_label = agreement.get("telegram") or (f"ID {telegram_chat_id}" if telegram_chat_id else "")

    channels = [
        {"key": "email", "label": "Email", "detail": contact_email or "не указан", "disabled": False},
        {"key": "telegram", "label": "Telegram", "detail": telegram_label or "не указан", "disabled": not telegram_chat_id},
        {
            "key": "vk",
            "label": "VK",
            "detail": vk_user_id or "не указан",
            "disabled": not (vk_user_id and core.VK_MESSAGE_TOKEN),
            "note": "" if core.VK_MESSAGE_TOKEN else "VK не настроен",
        },
    ]
    default_channel = _resolve_default_channel(channels, core.default_contract_channel(user))

    message = request.query_params.get("message")
    error = request.query_params.get("error")

    return render(
        request,
        "contract.html",
        {
            "agreement": agreement,
            "contract_url": contract_url,
            "contract_status_key": status_key,
            "contract_status_label": status_label,
            "contract_status_class": status_class,
            "contract_sent_at": sent_at,
            "contract_signed_at": signed_at,
            "contract_channel_label": core.contract_channel_label(agreement.get("contract_channel")),
            "channels": channels,
            "default_channel": default_channel,
            "contract_key_points": CONTRACT_KEY_POINTS,
            "contract_documents": CONTRACT_DOCUMENTS,
            "message": message,
            "error": error,
            "manual_email": agreement.get("contract_email_override") or "",
        },
    )


@router.post("/contract/{token}/send", include_in_schema=False)
async def contract_send(request: Request, token: str):
    agreement, path = core.find_agreement_by_token(token)
    if not agreement or not path:
        return HTMLResponse("Ссылка недействительна или договор не найден.", status_code=404)

    form = await request.form()
    channel = str(form.get("channel") or "").strip()
    manual_email = str(form.get("email") or "").strip()
    user = agreement.get("user") or {}
    if not channel:
        channel = core.default_contract_channel(user)

    text = _build_contract_text(agreement, request, token)
    try:
        if channel == "email":
            recipient = core.resolve_contact_email(user, agreement, manual_email)
            if not recipient:
                return RedirectResponse(f"/contract/{token}?error=Укажите+email+для+отправки", status_code=HTTP_302_FOUND)
            core.send_email_message(recipient, "Ваш договор обучения", text)
            if manual_email:
                agreement["contract_email_override"] = manual_email
        elif channel == "telegram":
            chat_id = core.extract_telegram_chat_id(user, agreement)
            if not chat_id:
                return RedirectResponse(f"/contract/{token}?error=Telegram+не+привязан", status_code=HTTP_302_FOUND)
            await core.send_telegram_message(chat_id, text)
        elif channel == "vk":
            vk_id = core.extract_vk_user_id(user)
            if not vk_id:
                return RedirectResponse(f"/contract/{token}?error=VK+не+привязан", status_code=HTTP_302_FOUND)
            await core.send_vk_message(vk_id, text)
        else:
            return RedirectResponse(f"/contract/{token}?error=Неизвестный+канал", status_code=HTTP_302_FOUND)
    except Exception as exc:
        logging.getLogger("app.contract").error("Contract send failed: %s", exc)
        return RedirectResponse(f"/contract/{token}?error=Ошибка+отправки", status_code=HTTP_302_FOUND)

    if agreement.get("contract_status") != "signed":
        agreement["contract_status"] = "sent"
    agreement["contract_channel"] = channel
    agreement["contract_sent_at"] = int(time.time())
    agreement.pop("_file", None)
    core.save_json(path, agreement)
    return RedirectResponse(f"/contract/{token}?message=Договор+отправлен", status_code=HTTP_302_FOUND)


@router.post("/contract/{token}/sign", include_in_schema=False)
async def contract_sign(request: Request, token: str):
    agreement, path = core.find_agreement_by_token(token)
    if not agreement or not path:
        return HTMLResponse("Ссылка недействительна или договор не найден.", status_code=404)
    agreement["contract_status"] = "signed"
    agreement["contract_signed_at"] = int(time.time())
    agreement.pop("_file", None)
    core.save_json(path, agreement)
    return RedirectResponse(f"/contract/{token}?message=Договор+подтвержден", status_code=HTTP_302_FOUND)
