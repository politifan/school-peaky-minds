import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / "config" / ".env"
WHITELIST_FILE = BASE_DIR / "data" / "telegram_whitelist.json"
LEADS_DIR = BASE_DIR / "data" / "leads"
LEADS_DIR.mkdir(parents=True, exist_ok=True)


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


load_env(ENV_PATH)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DEFAULT_WHITELIST = [980343575, 1065558838, 1547353132]

STATUS_META = {
    "new": "Новая",
    "contacted": "Связались",
    "qualified": "Квалифицирован",
    "call_scheduled": "Созвон",
    "paid": "Оплачен",
    "lost": "Потерян",
    "in_progress": "В работе",
    "closed": "Закрыта",
    "archived": "Архив",
}

STATUS_BUTTON_ROWS = [
    ["new", "contacted", "qualified"],
    ["call_scheduled", "paid", "lost"],
    ["in_progress", "closed", "archived"],
    ["auto"],
]


def load_whitelist() -> Set[int]:
    if not WHITELIST_FILE.exists():
        return set(DEFAULT_WHITELIST)
    try:
        data = json.loads(WHITELIST_FILE.read_text(encoding="utf-8"))
    except Exception:
        return set(DEFAULT_WHITELIST)
    if not isinstance(data, list):
        return set(DEFAULT_WHITELIST)
    ids = []
    for item in data:
        try:
            ids.append(int(item))
        except Exception:
            continue
    return set(ids or DEFAULT_WHITELIST)


WHITELIST_CHAT_IDS = load_whitelist()


def current_whitelist() -> Set[int]:
    return load_whitelist()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("telegram_bot")


def is_configured() -> bool:
    return bool(BOT_TOKEN and current_whitelist())


def load_json(path: Path, default: Dict[str, object]) -> Dict[str, object]:
    if not path.exists():
        return default
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    if isinstance(data, dict):
        return data
    return default


def save_json(path: Path, data: Dict[str, object]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_leads() -> List[Dict[str, object]]:
    items: List[Dict[str, object]] = []
    for path in sorted(LEADS_DIR.glob("lead_*.json")):
        data = load_json(path, {})
        if data:
            data["_file"] = path.name
            items.append(data)
    return sorted(items, key=lambda item: int(item.get("timestamp", 0) or 0), reverse=True)


def lead_short_id(file_name: str) -> str:
    if not file_name:
        return ""
    stem = Path(file_name).stem
    parts = stem.split("_")
    if len(parts) >= 3:
        return parts[-1]
    return stem[-8:]


def format_ts(value: Optional[object]) -> str:
    if not value:
        return "—"
    try:
        return datetime.fromtimestamp(int(value)).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return "—"


def status_from_item(item: Dict[str, object]) -> Tuple[str, str]:
    manual = str(item.get("status") or "").strip()
    if manual in STATUS_META:
        return manual, STATUS_META[manual]
    ts = item.get("timestamp")
    if not ts:
        return "archived", STATUS_META["archived"]
    try:
        delta = datetime.now() - datetime.fromtimestamp(int(ts))
    except Exception:
        return "archived", STATUS_META["archived"]
    if delta <= timedelta(days=1):
        return "new", STATUS_META["new"]
    if delta <= timedelta(days=7):
        return "in_progress", STATUS_META["in_progress"]
    return "archived", STATUS_META["archived"]


def normalize_tags(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        raw = ",".join([str(item) for item in value])
    else:
        raw = str(value)
    parts = [part.strip() for part in raw.split(",") if part.strip()]
    return ", ".join(parts)


def build_status_keyboard(file_name: str, selected: Optional[str] = None) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for row in STATUS_BUTTON_ROWS:
        buttons = []
        for key in row:
            label = "Авто" if key == "auto" else STATUS_META.get(key, key)
            if selected and key == selected:
                label = f"✅ {label}"
            buttons.append(
                InlineKeyboardButton(text=label, callback_data=f"lead:{file_name}:{key}")
            )
        rows.append(buttons)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_lead_text(item: Dict[str, object]) -> str:
    name = str(item.get("name") or "—")
    contact = str(item.get("contact") or "—")
    course = str(item.get("course") or "—")
    page = str(item.get("page") or "—")
    note = str(item.get("note") or "").strip()
    tags = normalize_tags(item.get("tags"))
    next_contact = str(item.get("next_contact") or "").strip() or "—"
    lead_id = lead_short_id(str(item.get("_file") or ""))
    status_key, status_label = status_from_item(item)
    title = "🆕 <b>Новая заявка</b>" if status_key == "new" else "🧾 <b>Заявка</b>"
    lines = [
        title,
        f"🆔 <b>ID:</b> <code>{lead_id}</code>",
        f"📌 <b>Статус:</b> {status_label}",
        f"🕒 <b>Время:</b> {format_ts(item.get('timestamp'))}",
        f"👤 <b>Имя:</b> {name}",
        f"📱 <b>Контакт:</b> {contact}",
        f"🎯 <b>Курс:</b> {course}",
        f"🔗 <b>Страница:</b> {page}",
    ]
    if tags:
        lines.append(f"🏷 <b>Теги:</b> {tags}")
    if note:
        lines.append(f"📝 <b>Заметка:</b> {note}")
    if next_contact and next_contact != "—":
        lines.append(f"📅 <b>След. контакт:</b> {next_contact}")
    return "\n".join(lines)


def find_lead_file(token: str) -> Optional[Path]:
    if not token:
        return None
    value = token.strip()
    if value.endswith(".json"):
        path = LEADS_DIR / value
        return path if path.exists() else None
    if value.startswith("lead_"):
        path = LEADS_DIR / f"{value}.json"
        if path.exists():
            return path
        path = LEADS_DIR / value
        return path if path.exists() else None
    candidates = list(LEADS_DIR.glob(f"*{value}*.json"))
    if len(candidates) == 1:
        return candidates[0]
    return None


def update_lead_fields(file_name: str, updates: Dict[str, object]) -> bool:
    if not file_name:
        return False
    path = LEADS_DIR / file_name
    if not path.exists():
        return False
    data = load_json(path, {})
    if not data:
        return False
    data.update(updates)
    save_json(path, data)
    return True


def update_lead_status(file_name: str, status: str) -> bool:
    if not file_name:
        return False
    path = LEADS_DIR / file_name
    if not path.exists():
        return False
    data = load_json(path, {})
    if not data:
        return False
    if status:
        data["status"] = status
        data["status_updated_at"] = int(time.time())
    else:
        data.pop("status", None)
        data.pop("status_updated_at", None)
    save_json(path, data)
    return True


async def send_lead_message(text: str, lead_file: Optional[str] = None) -> bool:
    if not is_configured():
        logger.warning("Telegram bot not configured, skipping send.")
        return False

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    try:
        sent = False
        lead_item = None
        keyboard = None
        if lead_file:
            lead_path = LEADS_DIR / lead_file
            if lead_path.exists():
                lead_item = load_json(lead_path, {})
                if lead_item:
                    lead_item["_file"] = lead_file
                    text = build_lead_text(lead_item)
                    status_key, _ = status_from_item(lead_item)
                    keyboard = build_status_keyboard(lead_file, status_key)
        for chat_id in current_whitelist():
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    disable_web_page_preview=True,
                    reply_markup=keyboard,
                )
                logger.info("Lead message sent to chat_id=%s", chat_id)
                sent = True
            except Exception as exc:
                logger.error("Failed to send lead message to chat_id=%s: %s", chat_id, exc)
        return sent
    except Exception as exc:
        logger.error("Failed to send lead message: %s", exc)
        return False
    finally:
        await bot.session.close()


async def _start(message: Message) -> None:
    user_id = message.from_user.id if message.from_user else None
    if user_id not in current_whitelist():
        logger.info("Ignored /start from non-whitelisted user_id=%s", user_id)
        return
    logger.info("Received /start from user_id=%s", user_id)
    await message.answer(
        "Бот подключен. Заявки будут приходить сюда.\n\n"
        "Команды:\n"
        "/leads [N] — последние заявки\n"
        "/lead <id> — карточка заявки\n"
        "/find <текст> — поиск по заявкам\n"
        "/status <id> <статус> — сменить статус\n"
        "/note <id> <текст> — заметка\n"
        "/tags <id> <теги> — теги\n"
        "/next <id> <YYYY-MM-DD> — следующий контакт\n"
        "/stats — сводка статусов"
    )


async def _help(message: Message) -> None:
    user_id = message.from_user.id if message.from_user else None
    if user_id not in current_whitelist():
        return
    await message.answer(
        "Доступные команды:\n"
        "/leads [N] — последние заявки\n"
        "/lead <id> — карточка заявки\n"
        "/find <текст> — поиск по заявкам\n"
        "/status <id> <статус> — сменить статус\n"
        "/note <id> <текст> — заметка\n"
        "/tags <id> <теги> — теги\n"
        "/next <id> <YYYY-MM-DD> — следующий контакт\n"
        "/stats — сводка статусов\n\n"
        "Статусы: new, contacted, qualified, call_scheduled, paid, lost, in_progress, closed, archived, auto"
    )


async def _leads(message: Message) -> None:
    user_id = message.from_user.id if message.from_user else None
    if user_id not in current_whitelist():
        return
    parts = message.text.split(maxsplit=1) if message.text else []
    limit = 5
    if len(parts) > 1:
        try:
            limit = int(parts[1])
        except Exception:
            limit = 5
    limit = max(1, min(limit, 20))
    leads = load_leads()[:limit]
    if not leads:
        await message.answer("Заявок пока нет.")
        return
    for lead in leads:
        file_name = str(lead.get("_file") or "")
        status_key, _ = status_from_item(lead)
        await message.answer(
            build_lead_text(lead),
            reply_markup=build_status_keyboard(file_name, status_key),
            disable_web_page_preview=True,
        )


async def _lead(message: Message) -> None:
    user_id = message.from_user.id if message.from_user else None
    if user_id not in current_whitelist():
        return
    parts = message.text.split(maxsplit=1) if message.text else []
    if len(parts) < 2:
        await message.answer("Использование: /lead <id>")
        return
    token = parts[1].strip()
    path = find_lead_file(token)
    if not path:
        await message.answer("Заявка не найдена.")
        return
    lead = load_json(path, {})
    lead["_file"] = path.name
    status_key, _ = status_from_item(lead)
    await message.answer(
        build_lead_text(lead),
        reply_markup=build_status_keyboard(path.name, status_key),
        disable_web_page_preview=True,
    )


async def _find(message: Message) -> None:
    user_id = message.from_user.id if message.from_user else None
    if user_id not in current_whitelist():
        return
    parts = message.text.split(maxsplit=1) if message.text else []
    if len(parts) < 2:
        await message.answer("Использование: /find <текст>")
        return
    query = parts[1].strip().lower()
    results = []
    for lead in load_leads():
        haystack = " ".join(
            [
                str(lead.get("name") or ""),
                str(lead.get("contact") or ""),
                str(lead.get("course") or ""),
                str(lead.get("page") or ""),
            ]
        ).lower()
        if query in haystack:
            results.append(lead)
        if len(results) >= 10:
            break
    if not results:
        await message.answer("Ничего не найдено.")
        return
    lines = ["Найденные заявки:"]
    for lead in results:
        lead_id = lead_short_id(str(lead.get("_file") or ""))
        status_key, status_label = status_from_item(lead)
        name = str(lead.get("name") or "—")
        contact = str(lead.get("contact") or "—")
        lines.append(f"• <code>{lead_id}</code> — {name} ({contact}) — {status_label}")
    lines.append("\nОткрыть карточку: /lead <id>")
    await message.answer("\n".join(lines))


async def _status(message: Message) -> None:
    user_id = message.from_user.id if message.from_user else None
    if user_id not in current_whitelist():
        return
    parts = message.text.split(maxsplit=2) if message.text else []
    if len(parts) < 3:
        await message.answer("Использование: /status <id> <статус>")
        return
    token = parts[1].strip()
    status_key = parts[2].strip().lower()
    if status_key in ("auto", "clear", "reset"):
        status_key = ""
    elif status_key not in STATUS_META:
        await message.answer("Неизвестный статус. Пример: /status abcd1234 contacted")
        return
    path = find_lead_file(token)
    if not path:
        await message.answer("Заявка не найдена.")
        return
    if update_lead_status(path.name, status_key):
        label = "Авто" if not status_key else STATUS_META[status_key]
        await message.answer(f"Статус обновлён: {label}")
    else:
        await message.answer("Не удалось обновить статус.")


async def _note(message: Message) -> None:
    user_id = message.from_user.id if message.from_user else None
    if user_id not in current_whitelist():
        return
    parts = message.text.split(maxsplit=2) if message.text else []
    if len(parts) < 3:
        await message.answer("Использование: /note <id> <текст>")
        return
    path = find_lead_file(parts[1].strip())
    if not path:
        await message.answer("Заявка не найдена.")
        return
    note = parts[2].strip()
    if update_lead_fields(path.name, {"note": note}):
        await message.answer("Заметка обновлена.")
    else:
        await message.answer("Не удалось обновить заметку.")


async def _tags(message: Message) -> None:
    user_id = message.from_user.id if message.from_user else None
    if user_id not in current_whitelist():
        return
    parts = message.text.split(maxsplit=2) if message.text else []
    if len(parts) < 3:
        await message.answer("Использование: /tags <id> <теги через запятую>")
        return
    path = find_lead_file(parts[1].strip())
    if not path:
        await message.answer("Заявка не найдена.")
        return
    tags = normalize_tags(parts[2])
    if update_lead_fields(path.name, {"tags": tags}):
        await message.answer("Теги обновлены.")
    else:
        await message.answer("Не удалось обновить теги.")


async def _next(message: Message) -> None:
    user_id = message.from_user.id if message.from_user else None
    if user_id not in current_whitelist():
        return
    parts = message.text.split(maxsplit=2) if message.text else []
    if len(parts) < 3:
        await message.answer("Использование: /next <id> <YYYY-MM-DD>")
        return
    path = find_lead_file(parts[1].strip())
    if not path:
        await message.answer("Заявка не найдена.")
        return
    next_contact = parts[2].strip()
    if not next_contact or len(next_contact) != 10:
        await message.answer("Некорректная дата. Пример: 2026-02-05")
        return
    if update_lead_fields(path.name, {"next_contact": next_contact}):
        await message.answer("Дата следующего контакта обновлена.")
    else:
        await message.answer("Не удалось обновить дату.")


async def _stats(message: Message) -> None:
    user_id = message.from_user.id if message.from_user else None
    if user_id not in current_whitelist():
        return
    leads = load_leads()
    counts: Dict[str, int] = {key: 0 for key in STATUS_META}
    for lead in leads:
        status_key, _ = status_from_item(lead)
        counts[status_key] = counts.get(status_key, 0) + 1
    lines = [f"Всего заявок: {len(leads)}"]
    for key, label in STATUS_META.items():
        lines.append(f"{label}: {counts.get(key, 0)}")
    await message.answer("\n".join(lines))


async def _status_callback(callback: CallbackQuery) -> None:
    user_id = callback.from_user.id if callback.from_user else None
    if user_id not in current_whitelist():
        return
    if not callback.data:
        return
    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer("Ошибка данных.", show_alert=True)
        return
    lead_file = parts[1]
    status_key = parts[2]
    status_value = "" if status_key == "auto" else status_key
    if status_value and status_value not in STATUS_META:
        await callback.answer("Неизвестный статус.", show_alert=True)
        return
    if not update_lead_status(lead_file, status_value):
        await callback.answer("Заявка не найдена.", show_alert=True)
        return
    label = "Авто" if not status_value else STATUS_META[status_value]
    try:
        await callback.message.edit_reply_markup(
            reply_markup=build_status_keyboard(lead_file, status_key)
        )
    except Exception:
        pass
    await callback.answer(f"Статус обновлён: {label}")


def build_dispatcher() -> Dispatcher:
    dp = Dispatcher()
    dp.message.register(_start, CommandStart())
    dp.message.register(_help, Command("help"))
    dp.message.register(_leads, Command("leads"))
    dp.message.register(_lead, Command("lead"))
    dp.message.register(_find, Command("find"))
    dp.message.register(_status, Command("status"))
    dp.message.register(_note, Command("note"))
    dp.message.register(_tags, Command("tags"))
    dp.message.register(_next, Command("next"))
    dp.message.register(_stats, Command("stats"))
    dp.callback_query.register(_status_callback, F.data.startswith("lead:"))
    return dp


async def start_polling() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = build_dispatcher()
    try:
        logger.info("Telegram bot polling started.")
        await dp.start_polling(bot)
    except Exception as exc:
        logger.error("Telegram bot polling stopped with error: %s", exc)
        raise
    finally:
        logger.info("Telegram bot polling stopped.")
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(start_polling())
