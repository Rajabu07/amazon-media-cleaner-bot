import asyncio
import logging
import os
import re

from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message as PyroMessage
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("amazon_media_cleaner")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
USER_SESSION = os.environ["USER_SESSION"]
OWNER_IDS = {
    int(value.strip())
    for value in os.environ.get("OWNER_IDS", "").split(",")
    if value.strip().isdigit()
}
CHANNEL_ID = int(os.environ["CHANNEL_ID"])

AMAZON_RE = re.compile(
    r"(?i)(?:https?://)?(?:www\.)?(?:amazon\.[a-z.]{2,}|amzn\.to|a\.co)(?:[/?:#][^\s<>()\]]*)?"
)

# The user account reads channel history and deletes matching media posts.
user = Client(
    "amazon_cleaner_user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=USER_SESSION,
)

scan_lock = asyncio.Lock()
stop_event = asyncio.Event()
scan_state = {"running": False, "scanned": 0, "deleted": 0, "last_id": None}


def is_owner(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id in OWNER_IDS)


async def reply(update: Update, text: str):
    if update.effective_message:
        await update.effective_message.reply_text(text)


def contains_amazon_link(message: PyroMessage) -> bool:
    body = message.text or message.caption or ""
    if AMAZON_RE.search(body):
        return True

    entities = list(message.entities or []) + list(message.caption_entities or [])
    for entity in entities:
        hidden_url = getattr(entity, "url", None)
        if hidden_url and AMAZON_RE.search(hidden_url):
            return True
    return False


def has_photo_or_video(message: PyroMessage) -> bool:
    return message.photo is not None or message.video is not None


async def safe_delete(message: PyroMessage) -> bool:
    try:
        await user.delete_messages(CHANNEL_ID, message.id)
        return True
    except FloodWait as exc:
        wait_seconds = int(getattr(exc, "value", 5))
        log.warning("Telegram FloodWait; waiting %s seconds", wait_seconds)
        await asyncio.sleep(wait_seconds)
        try:
            await user.delete_messages(CHANNEL_ID, message.id)
            return True
        except RPCError:
            log.exception("Delete failed for message %s", message.id)
            return False
    except RPCError:
        log.exception("Delete failed for message %s", message.id)
        return False


async def scan_from(start_id: int, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    if scan_lock.locked():
        await context.bot.send_message(chat_id, "A scan is already running. Use /status or /stop.")
        return

    async with scan_lock:
        stop_event.clear()
        scan_state.update(running=True, scanned=0, deleted=0, last_id=start_id)
        await context.bot.send_message(
            chat_id,
            f"Scan started from message ID {start_id} to latest. "
            "Only photo/video posts containing Amazon links will be deleted.",
        )
        try:
            selected = []
            async for message in user.get_chat_history(CHANNEL_ID):
                if message.id >= start_id:
                    selected.append(message)
                else:
                    break

            selected.reverse()
            for message in selected:
                if stop_event.is_set():
                    break

                scan_state["scanned"] += 1
                scan_state["last_id"] = message.id

                if has_photo_or_video(message) and contains_amazon_link(message):
                    if await safe_delete(message):
                        scan_state["deleted"] += 1

                await asyncio.sleep(0.15)

            result = "Scan stopped." if stop_event.is_set() else "Scan completed."
            await context.bot.send_message(
                chat_id,
                f"{result}\\nScanned: {scan_state['scanned']}\\n"
                f"Deleted: {scan_state['deleted']}\\n"
                f"Last checked message ID: {scan_state['last_id']}",
            )
        except Exception as exc:
            log.exception("Scan failed")
            await context.bot.send_message(
                chat_id, f"Scan failed: {type(exc).__name__}: {exc}"
            )
        finally:
            scan_state["running"] = False


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else None
    log.info("Received /start from Telegram user_id=%s", uid)

    if not OWNER_IDS:
        await reply(
            update,
            "Bot is online, but OWNER_IDS is empty in Railway Variables. "
            "Add your numeric Telegram user ID to OWNER_IDS.",
        )
        return

    if not is_owner(update):
        await reply(update, "You are not authorized to use this bot.")
        return

    await reply(
        update,
        "Amazon Media Cleaner is online and ready.\\n\\n"
        "/from MESSAGE_ID - scan from that message to latest\\n"
        "/stop - stop current scan\\n"
        "/status - show progress\\n"
        "/stats - show last scan counts",
    )


async def from_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await reply(update, "You are not authorized to use this bot.")
        return
    if len(context.args) != 1 or not context.args[0].isdigit() or int(context.args[0]) < 1:
        await reply(update, "Usage: /from 500")
        return

    asyncio.create_task(
        scan_from(int(context.args[0]), update.effective_chat.id, context)
    )


async def all_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Scan the channel's available history from the oldest accessible message."""
    if not is_owner(update):
        await reply(update, "You are not authorized to use this bot.")
        return

    asyncio.create_task(scan_from(1, update.effective_chat.id, context))


async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await reply(update, "You are not authorized to use this bot.")
        return
    if scan_state["running"]:
        stop_event.set()
        await reply(update, "Stop requested. The scan will stop shortly.")
    else:
        await reply(update, "No scan is currently running.")


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await reply(update, "You are not authorized to use this bot.")
        return
    await reply(
        update,
        f"Running: {scan_state['running']}\\n"
        f"Scanned: {scan_state['scanned']}\\n"
        f"Deleted: {scan_state['deleted']}\\n"
        f"Last checked ID: {scan_state['last_id']}",
    )


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await reply(update, "You are not authorized to use this bot.")
        return
    await reply(
        update,
        f"Last scan stats\\nScanned: {scan_state['scanned']}\\n"
        f"Deleted: {scan_state['deleted']}",
    )


async def post_init(application: Application):
    await user.start()
    account = await user.get_me()
    log.info("History/deletion account connected: id=%s", account.id)

    # Clear any stale webhook so long polling can receive bot messages.
    await application.bot.delete_webhook(drop_pending_updates=False)
    log.info("Telegram webhook cleared; starting long polling")


async def post_shutdown(application: Application):
    if user.is_connected:
        await user.stop()


async def main():
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("from", from_cmd))
    application.add_handler(CommandHandler("all", all_cmd))
    application.add_handler(CommandHandler("stop", stop_cmd))
    application.add_handler(CommandHandler("status", status_cmd))
    application.add_handler(CommandHandler("stats", stats_cmd))

    log.info("Starting Telegram Bot API polling service")
    await user.start()
    account = await user.get_me()
    log.info("History/deletion account connected: id=%s", account.id)

    await application.initialize()
    await application.bot.delete_webhook(drop_pending_updates=False)
    await application.start()

    if application.updater is None:
        raise RuntimeError("Telegram polling updater is unavailable")

    await application.updater.start_polling(
        drop_pending_updates=False,
        allowed_updates=Update.ALL_TYPES,
    )
    me = await application.bot.get_me()
    log.info("Polling active for @%s (id=%s)", me.username, me.id)

    try:
        await asyncio.Event().wait()
    finally:
        if application.updater.running:
            await application.updater.stop()
        await application.stop()
        await application.shutdown()
        if user.is_connected:
            await user.stop()


if __name__ == "__main__":
    asyncio.run(main())
