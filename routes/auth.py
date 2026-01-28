import logging
import re
import secrets
import time
from typing import Any, Dict, Optional, Tuple

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from starlette.status import HTTP_302_FOUND

from core import (
    OAuthError,
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_USERNAME_RE,
    TELETHON_AUTO_LOGIN,
    TELETHON_ENABLED,
    USERS_FILE,
    CODES_FILE,
    UsernameInvalidError,
    UsernameNotOccupiedError,
    build_redirect_uri,
    clear_user,
    ensure_telethon_login,
    get_current_user,
    get_telethon_client,
    load_json,
    login_context,
    oauth,
    providers,
    render,
    save_json,
    send_email_code,
    set_current_user,
    verify_telegram_auth,
)

router = APIRouter()


@router.get("/login", include_in_schema=False)
def login(request: Request):
    next_url = request.query_params.get("next") or "/"
    return render(request, "login.html", login_context(request, next_url=next_url))


@router.post("/login/email", include_in_schema=False)
async def login_email(request: Request):
    form = await request.form()
    email = str(form.get("email", "")).strip().lower()
    next_url = str(form.get("next", "/"))
    if not email:
        return render(request, "login.html", login_context(request, next_url=next_url, error="Введите email"))

    code = f"{secrets.randbelow(10 ** 6):06d}"
    codes = load_json(CODES_FILE, {})
    codes[email] = {"code": code, "expires": time.time() + 600}
    save_json(CODES_FILE, codes)
    send_email_code(email, code)

    return render(request, "verify.html", {"email": email, "next": next_url})


@router.post("/login/verify", include_in_schema=False)
async def login_verify(request: Request):
    form = await request.form()
    email = str(form.get("email", "")).strip().lower()
    code = str(form.get("code", "")).strip()
    next_url = str(form.get("next", "/"))

    codes = load_json(CODES_FILE, {})
    entry = codes.get(email)
    if not entry or entry.get("code") != code or entry.get("expires", 0) < time.time():
        return render(
            request,
            "verify.html",
            {"email": email, "next": next_url, "error": "Неверный или просроченный код"},
        )

    users = load_json(USERS_FILE, {})
    user_id = f"email:{email}"
    user = users.get(user_id) or {"id": user_id, "email": email, "name": email, "provider": "email"}
    users[user_id] = user
    save_json(USERS_FILE, users)

    set_current_user(request, user)
    codes.pop(email, None)
    save_json(CODES_FILE, codes)

    return RedirectResponse(next_url, status_code=HTTP_302_FOUND)


@router.get("/login/google", include_in_schema=False)
async def login_google(request: Request):
    if not (oauth and providers["google"]):
        return render(request, "login.html", login_context(request, error="Google OAuth не настроен"))
    logging.getLogger("app.auth").info(
        "Google login start: host=%s scheme=%s cookies=%s session_keys=%s",
        request.url.hostname,
        request.url.scheme,
        list(request.cookies.keys()),
        list(request.session.keys()),
    )
    redirect_uri = build_redirect_uri(request, "auth_google")
    return await oauth.google.authorize_redirect(request, redirect_uri)


@router.get("/auth/google/callback", include_in_schema=False, name="auth_google")
async def auth_google(request: Request):
    if not oauth:
        return render(request, "login.html", login_context(request, error="Google OAuth не настроен"))
    logging.getLogger("app.auth").info(
        "Google callback: host=%s scheme=%s query_state=%s cookies=%s session_keys=%s",
        request.url.hostname,
        request.url.scheme,
        request.query_params.get("state"),
        list(request.cookies.keys()),
        list(request.session.keys()),
    )

    async def exchange_google_token() -> Tuple[Optional[dict], Optional[str]]:
        code = request.query_params.get("code")
        if not code:
            return None, "missing_code"
        redirect_uri = build_redirect_uri(request, "auth_google")
        payload = {
            "code": code,
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post("https://oauth2.googleapis.com/token", data=payload)
        try:
            data = resp.json()
        except Exception:
            data = {}
        if not isinstance(data, dict) or "access_token" not in data:
            err = data.get("error") or f"http_{resp.status_code}"
            err_desc = data.get("error_description")
            message = f"{err}: {err_desc}" if err_desc else str(err)
            return None, message
        return data, None

    try:
        token = await oauth.google.authorize_access_token(request)
        if not isinstance(token, dict) or not token.get("access_token"):
            token, manual_error = await exchange_google_token()
            if not token:
                detail = "missing_token"
                extra = f" [manual_exchange: {manual_error}]" if manual_error else " [manual_exchange_failed]"
                safe_detail = re.sub(r"[\\r\\n]+", " ", f"{detail}{extra}")[:300]
                return render(
                    request,
                    "login.html",
                    login_context(request, error=f"Ошибка авторизации Google: {safe_detail}"),
                )
    except OAuthError as exc:
        detail = getattr(exc, "error", None) or str(exc) or "OAuthError"
        description = getattr(exc, "description", None) or ""
        response_hint = ""
        extra = ""
        response = getattr(exc, "response", None)
        if response is not None:
            try:
                response_hint = f" (status {response.status_code})"
            except Exception:
                response_hint = ""
            try:
                data = response.json()
                if isinstance(data, dict):
                    err = data.get("error")
                    err_desc = data.get("error_description")
                    if err or err_desc:
                        extra = f" [{err}: {err_desc}]" if err_desc else f" [{err}]"
            except Exception:
                extra = ""

        if "missing_token" in str(detail):
            token, manual_error = await exchange_google_token()
            if token:
                detail = ""
                description = ""
            else:
                extra = f" [manual_exchange: {manual_error}]" if manual_error else " [manual_exchange_failed]"
        if detail:
            message = f"{detail}{response_hint}{extra}"
            if description:
                message = f"{message}: {description}"
            safe_detail = re.sub(r"[\\r\\n]+", " ", message)[:300]
            return render(
                request,
                "login.html",
                login_context(request, error=f"Ошибка авторизации Google: {safe_detail}"),
            )
    except Exception as exc:
        safe_detail = re.sub(r"[\\r\\n]+", " ", str(exc) or "Unknown error")[:300]
        return render(
            request,
            "login.html",
            login_context(request, error=f"Ошибка авторизации Google: {safe_detail}"),
        )

    userinfo = None
    if isinstance(token, dict) and token.get("id_token"):
        try:
            userinfo = await oauth.google.parse_id_token(request, token)
        except Exception:
            userinfo = None
    if not userinfo:
        try:
            userinfo_resp = await oauth.google.get("userinfo")
            userinfo = userinfo_resp.json()
        except Exception:
            userinfo = None
    if not userinfo or not isinstance(userinfo, dict):
        try:
            access_token = token.get("access_token") if isinstance(token, dict) else None
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    "https://www.googleapis.com/oauth2/v3/userinfo",
                    headers={"Authorization": f"Bearer {access_token}"},
                )
            userinfo = resp.json()
        except Exception as exc:
            safe_detail = re.sub(r"[\\r\\n]+", " ", str(exc) or "userinfo_error")[:300]
            return render(
                request,
                "login.html",
                login_context(request, error=f"Ошибка авторизации Google: userinfo_failed {safe_detail}"),
            )

    if not userinfo or not isinstance(userinfo, dict):
        return render(request, "login.html", login_context(request, error="Ошибка авторизации Google"))

    users = load_json(USERS_FILE, {})
    user_sub = userinfo.get("sub") or userinfo.get("id") or userinfo.get("email")
    user_id = f"google:{user_sub}"
    user = {
        "id": user_id,
        "email": userinfo.get("email"),
        "name": userinfo.get("name") or userinfo.get("given_name") or userinfo.get("email"),
        "provider": "google",
    }
    users[user_id] = user
    save_json(USERS_FILE, users)

    set_current_user(request, user)
    return RedirectResponse("/", status_code=HTTP_302_FOUND)


@router.get("/login/vk", include_in_schema=False)
async def login_vk(request: Request):
    if not (oauth and providers["vk"]):
        return render(request, "login.html", login_context(request, error="VK OAuth не настроен"))
    redirect_uri = build_redirect_uri(request, "auth_vk")
    return await oauth.vk.authorize_redirect(request, redirect_uri)


@router.get("/auth/vk/callback", include_in_schema=False, name="auth_vk")
async def auth_vk(request: Request):
    if not oauth:
        return render(request, "login.html", login_context(request, error="VK OAuth не настроен"))
    try:
        token = await oauth.vk.authorize_access_token(request)
        user_resp = await oauth.vk.get("users.get", params={"v": "5.131", "fields": "photo_200"})
        profile = user_resp.json().get("response", [{}])[0]
    except OAuthError:
        return render(request, "login.html", login_context(request, error="Ошибка авторизации VK"))

    users = load_json(USERS_FILE, {})
    user_id = f"vk:{profile.get('id')}"
    user = {
        "id": user_id,
        "email": token.get("email"),
        "name": f"{profile.get('first_name', '')} {profile.get('last_name', '')}".strip(),
        "provider": "vk",
    }
    users[user_id] = user
    save_json(USERS_FILE, users)

    set_current_user(request, user)
    return RedirectResponse("/", status_code=HTTP_302_FOUND)


@router.get("/login/telegram", include_in_schema=False)
async def login_telegram(request: Request):
    data = dict(request.query_params)
    if not data or "hash" not in data:
        return render(request, "login.html", login_context(request, error="Нажмите и подтвердите вход через Telegram"))
    if not verify_telegram_auth(data):
        return render(request, "login.html", login_context(request, error="Ошибка авторизации Telegram"))

    users = load_json(USERS_FILE, {})
    user_id = f"telegram:{data.get('id')}"
    user = {
        "id": user_id,
        "email": None,
        "name": data.get("first_name") or data.get("username") or "Telegram",
        "provider": "telegram",
    }
    users[user_id] = user
    save_json(USERS_FILE, users)
    set_current_user(request, user)

    return RedirectResponse("/", status_code=HTTP_302_FOUND)


@router.get("/validate/telegram", include_in_schema=False)
async def validate_telegram(username: str) -> dict:
    handle = username.strip().lstrip("@")
    if not handle or not TELEGRAM_USERNAME_RE.match(handle):
        return {"ok": False, "reason": "invalid"}

    if TELETHON_ENABLED:
        client = await get_telethon_client()
        if not client:
            return {"ok": False, "reason": "not_configured"}
        if not await client.is_user_authorized():
            if TELETHON_AUTO_LOGIN:
                await ensure_telethon_login(client, interactive=True)
            if not await client.is_user_authorized():
                return {"ok": False, "reason": "telethon_login_required"}
        try:
            entity = await client.get_entity(handle)
            return {"ok": True, "description": getattr(entity, "first_name", "") or getattr(entity, "title", "")}
        except UsernameInvalidError:
            return {"ok": False, "reason": "invalid"}
        except UsernameNotOccupiedError:
            return {"ok": False, "reason": "not_found"}
        except ValueError:
            return {"ok": False, "reason": "invalid"}
        except Exception:
            return {"ok": False, "reason": "error"}

    if not TELEGRAM_BOT_TOKEN:
        return {"ok": False, "reason": "not_configured"}

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getChat"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(url, params={"chat_id": f\"@{handle}\"})
        data = response.json()
    except Exception:
        return {"ok": False, "reason": "error"}

    return {"ok": bool(data.get("ok")), "description": data.get("description")}


@router.get("/logout", include_in_schema=False)
def logout(request: Request):
    clear_user(request)
    return RedirectResponse("/", status_code=HTTP_302_FOUND)


@router.get("/account", include_in_schema=False)
def account(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=HTTP_302_FOUND)
    return render(request, "account.html")
