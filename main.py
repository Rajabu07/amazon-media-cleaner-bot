import asyncio
import logging
import os
import re
from typing import Optional

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
USER_SESSION = os.environ.get("USER_SESSION", "").strip()
OWNER_IDS = {
    int(value.strip())
    for value in os.environ.get("OWNER_IDS", "").split(",")
    if value.strip().isdigit()
}
CHANNEL_ID = int(os.environ["CHANNEL_ID"])

AMAZON_RE = re.compile(
    r"(?i)(?:https?://)?(?:www\.)?(?:amazon\.[a-z.]{2,}|amzn\.to|a\.co)(?:[/?:#][^\s<>()\]]*)?"
)

user = Client(
    "amazon_cleaner_user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=USER_SESSION,
)

user_ready = False
scan_lock = asyncio.Lock()
stop_event = asyncio.Event()
scan_state = {"running": False, "mode": None, "scanned": 0, "deleted": 0, "last_id": None}


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


def has_photo(message: PyroMessage) -> bool:
    # Delamazon deliberately ignores text-only posts and videos without a photo.
    return message.photo is not None


async def delete_batch(message_ids: list[int]) -> int:
    if not message_ids:
        return 0
    try:
        result = await user.delete_messages(CHANNEL_ID, message_ids)
        return int(result or 0)
    except FloodWait as exc:
        wait_seconds = int(getattr(exc, "value", 5))
        log.warning("Telegram FloodWait; waiting %s seconds", wait_seconds)
        await asyncio.sleep(wait_seconds)
        try:
            result = await user.delete_messages(CHANNEL_ID, message_ids)
            return int(result or 0)
        except RPCError:
            log.exception("Batch delete failed for IDs %s..%s", message_ids[0], message_ids[-1])
            return 0
    except RPCError:
        log.exception("Batch delete failed for IDs %s..%s", message_ids[0], message_ids[-1])
        return 0


async def run_delete(mode: str, chat_id: int, context: ContextTypes.DEFAULT_TYPE, start_id: Optional[int] = None):
    global user_ready
    if not user_ready:
        await context.bot.send_message(
            chat_id,
            "User session is not connected. Check USER_SESSION in Railway Variables.",
        )
        return
    if scan_lock.locked():
        await context.bot.send_message(chat_id, "A deletion job is already running. Use /status or /stop.")
        return

    async with scan_lock:
        stop_event.clear()
        scan_state.update(running=True, mode=mode, scanned=0, deleted=0, last_id=start_id)
        labels = {
            "all": "all channel messages",
            "amazon": "photo posts containing Amazon links only",
            "from": f"messages from ID {start_id} through the latest post",
        }
        await context.bot.send_message(chat_id, f"Deletion started: {labels[mode]}.")
        batch: list[int] = []
        try:
            async for message in user.get_chat_history(CHANNEL_ID):
                if stop_event.is_set():
                    break
                if mode == "from" and start_id is not None and message.id < start_id:
                    break

                scan_state["scanned"] += 1
                scan_state["last_id"] = message.id

                should_delete = (
                    mode in ("all", "from")
                    or (mode == "amazon" and has_photo(message) and contains_amazon_link(message))
                )
                if should_delete:
                    batch.append(message.id)

                # Telegram accepts up to 100 message IDs per delete request.
                if len(batch) >= 100:
                    scan_state["deleted"] += await delete_batch(batch)
                    batch.clear()

            if batch:
                scan_state["deleted"] += await delete_batch(batch)
                batch.clear()

            result = "Deletion stopped." if stop_event.is_set() else "Deletion completed."
            await context.bot.send_message(
                chat_id,
                f"{result}\nMode: {mode}\nScanned: {scan_state['scanned']}\n"
                f"Deleted: {scan_state['deleted']}\nLast checked ID: {scan_state['last_id']}",
            )
        except Exception as exc:
            log.exception("Deletion job failed")
            await context.bot.send_message(chat_id, f"Deletion failed: {type(exc).__name__}: {exc}")
        finally:
            scan_state["running"] = False
            scan_state["mode"] = None


async def start_job(update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str, start_id: Optional[int] = None):
    if not is_owner(update):
        await reply(update, "You are not authorized to use this bot.")
        return
    if not update.effective_chat:
        return
    asyncio.create_task(run_delete(mode, update.effective_chat.id, context, start_id))


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await reply(update, "You are not authorized to use this bot.")
        return
    session_status = "Connected" if user_ready else "NOT connected — check USER_SESSION"
    await reply(
        update,
        "Amazon Media Cleaner is online.\n"
        f"History account: {session_status}\n\n"
        "/Delall - delete all messages in configured channel\n"
        "/Delamazon - delete only photo posts containing Amazon links\n"
        "/Delfrom MESSAGE_ID - delete from that ID to latest\n"
        "Or reply to a forwarded channel post with /Delfrom\n"
        "/status - show progress\n"
        "/stop - stop the current job",
    )


async def delall_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_job(update, context, "all")


async def delamazon_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_job(update, context, "amazon")


def forwarded_channel_message_id(message) -> Optional[int]:
    if not message or not message.forward_origin:
        return None
    origin = message.forward_origin
    # PTB MessageOriginChannel exposes the original channel message ID.
    return getattr(origin, "message_id", None)


async def delfrom_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await reply(update, "You are not authorized to use this bot.")
        return
    start_id = None
    if len(context.args) == 1 and context.args[0].isdigit():
        start_id = int(context.args[0])
    elif len(context.args) == 0 and update.effective_message:
        start_id = forwarded_channel_message_id(update.effective_message.reply_to_message)
    if not start_id or start_id < 1:
        await reply(
            update,
            "Use /Delfrom MESSAGE_ID, or forward the channel post to this bot and reply to that forwarded post with /Delfrom.",
        )
        return
    await start_job(update, context, "from", start_id)


async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await reply(update, "You are not authorized to use this bot.")
        return
    if scan_state["running"]:
        stop_event.set()
        await reply(update, "Stop requested.")
    else:
        await reply(update, "No deletion job is running.")


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await reply(update, "You are not authorized to use this bot.")
        return
    await reply(
        update,
        f"Running: {scan_state['running']}\nMode: {scan_state['mode']}\n"
        f"Scanned: {scan_state['scanned']}\nDeleted: {scan_state['deleted']}\n"
        f"Last checked ID: {scan_state['last_id']}",
    )


async def main():
    global user_ready
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("delall", delall_cmd))
    application.add_handler(CommandHandler("Delall", delall_cmd))
    application.add_handler(CommandHandler("delamazon", delamazon_cmd))
    application.add_handler(CommandHandler("Delamazon", delamazon_cmd))
    application.add_handler(CommandHandler("delfrom", delfrom_cmd))
    application.add_handler(CommandHandler("Delfrom", delfrom_cmd))
    application.add_handler(CommandHandler("stop", stop_cmd))
    application.add_handler(CommandHandler("status", status_cmd))

    log.info("Starting Telegram bot polling service")
    try:
        if not USER_SESSION:
            raise RuntimeError("USER_SESSION is empty in Railway Variables")
        await user.start()
        account = await user.get_me()
        user_ready = True
        log.info("History/deletion account connected: id=%s", account.id)
    except Exception:
        user_ready = False
        log.exception("User session failed; bot polling will start but deletion commands are paused")

    await application.initialize()
    await application.bot.delete_webhook(drop_pending_updates=False)
    await application.start()
    if application.updater is None:
        raise RuntimeError("Telegram polling updater is unavailable")
    await application.updater.start_polling(drop_pending_updates=False, allowed_updates=Update.ALL_TYPES)
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
