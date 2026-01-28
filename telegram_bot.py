import asyncio
import logging
import os
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / "config" / ".env"


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
WHITELIST_CHAT_IDS = {980343575, 1065558838, 1547353132, 6721185787}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("telegram_bot")


def is_configured() -> bool:
    return bool(BOT_TOKEN and WHITELIST_CHAT_IDS)


async def send_lead_message(text: str) -> bool:
    if not is_configured():
        logger.warning("Telegram bot not configured, skipping send.")
        return False

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    try:
        sent = False
        for chat_id in WHITELIST_CHAT_IDS:
            try:
                await bot.send_message(chat_id=chat_id, text=text)
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
    if user_id not in WHITELIST_CHAT_IDS:
        logger.info("Ignored /start from non-whitelisted user_id=%s", user_id)
        return
    logger.info("Received /start from user_id=%s", user_id)
    await message.answer("Бот подключен. Заявки будут приходить сюда.")


def build_dispatcher() -> Dispatcher:
    dp = Dispatcher()
    dp.message.register(_start, CommandStart())
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
