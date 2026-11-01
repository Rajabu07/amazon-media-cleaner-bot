import asyncio
import logging
import os
import re

from pyrogram import Client, filters
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("amazon_media_cleaner")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
USER_SESSION = os.environ["USER_SESSION"]
OWNER_IDS = {int(x.strip()) for x in os.environ.get("OWNER_IDS", "").split(",") if x.strip()}
CHANNEL_ID = int(os.environ["CHANNEL_ID"])
AMAZON_RE = re.compile(r"(?i)(?:https?://)?(?:www\.)?(?:amazon\.[a-z.]{2,}|amzn\.to|a\.co)(?:[/?:#][^\s<>()\]]*)?")

bot = Client("amazon_cleaner_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
user = Client("amazon_cleaner_user", api_id=API_ID, api_hash=API_HASH, session_string=USER_SESSION)
scan_lock = asyncio.Lock()
stop_event = asyncio.Event()
scan_state = {"running": False, "scanned": 0, "deleted": 0, "last_id": None}

def is_owner(message: Message) -> bool:
    return bool(message.from_user and message.from_user.id in OWNER_IDS)

def contains_amazon_link(message: Message) -> bool:
    body = message.text or message.caption or ""
    if AMAZON_RE.search(body):
        return True
    for entity in list(message.entities or []) + list(message.caption_entities or []):
        hidden_url = getattr(entity, "url", None)
        if hidden_url and AMAZON_RE.search(hidden_url):
            return True
    return False

def has_photo_or_video(message: Message) -> bool:
    return message.photo is not None or message.video is not None

async def safe_delete(message: Message) -> bool:
    try:
        await user.delete_messages(CHANNEL_ID, message.id)
        return True
    except FloodWait as exc:
        log.warning("FloodWait: waiting %s seconds", exc.value)
        await asyncio.sleep(exc.value)
        try:
            await user.delete_messages(CHANNEL_ID, message.id)
            return True
        except RPCError:
            log.exception("Delete failed for message %s", message.id)
            return False
    except RPCError:
        log.exception("Delete failed for message %s", message.id)
        return False

async def scan_from(start_id: int, status_chat_id: int):
    if scan_lock.locked():
        await bot.send_message(status_chat_id, "A scan is already running. Use /status or /stop.")
        return
    async with scan_lock:
        stop_event.clear()
        scan_state.update(running=True, scanned=0, deleted=0, last_id=start_id)
        await bot.send_message(status_chat_id, f"Scan started from message ID {start_id} to latest. Only photo/video posts containing Amazon links will be deleted.")
        try:
            selected = []
            async for msg in user.get_chat_history(CHANNEL_ID):
                if msg.id >= start_id:
                    selected.append(msg)
                else:
                    break
            selected.reverse()
            for msg in selected:
                if stop_event.is_set():
                    break
                scan_state["scanned"] += 1
                scan_state["last_id"] = msg.id
                if has_photo_or_video(msg) and contains_amazon_link(msg):
                    if await safe_delete(msg):
                        scan_state["deleted"] += 1
                await asyncio.sleep(0.15)
            result = "Scan stopped." if stop_event.is_set() else "Scan completed."
            await bot.send_message(status_chat_id, f"{result}\nScanned: {scan_state['scanned']}\nDeleted: {scan_state['deleted']}\nLast checked message ID: {scan_state['last_id']}")
        except Exception as exc:
            log.exception("Scan failed")
            await bot.send_message(status_chat_id, f"Scan failed: {type(exc).__name__}: {exc}")
        finally:
            scan_state["running"] = False

@bot.on_message(filters.private & filters.command("start"))
async def start_cmd(_, message: Message):
    log.info("Received /start from user_id=%s", message.from_user.id if message.from_user else None)
    if not is_owner(message):
        await message.reply_text("Bot is online, but this account is not authorized. Ask the bot owner to add your numeric Telegram ID to OWNER_IDS in Railway Variables, then redeploy.")
        return
    await message.reply_text("Amazon Media Cleaner is ready.\n\n/from MESSAGE_ID - scan from that message to latest\n/stop - stop current scan\n/status - show progress\n/stats - show last scan counts")

@bot.on_message(filters.private & filters.command("from"))
async def from_cmd(_, message: Message):
    if not is_owner(message):
        return
    if len(message.command) != 2 or not message.command[1].isdigit() or int(message.command[1]) < 1:
        await message.reply_text("Usage: /from 500")
        return
    asyncio.create_task(scan_from(int(message.command[1]), message.chat.id))

@bot.on_message(filters.private & filters.command("stop"))
async def stop_cmd(_, message: Message):
    if not is_owner(message):
        return
    if scan_state["running"]:
        stop_event.set()
        await message.reply_text("Stop requested. The scan will stop shortly.")
    else:
        await message.reply_text("No scan is currently running.")

@bot.on_message(filters.private & filters.command("status"))
async def status_cmd(_, message: Message):
    if not is_owner(message):
        return
    await message.reply_text(f"Running: {scan_state['running']}\nScanned: {scan_state['scanned']}\nDeleted: {scan_state['deleted']}\nLast checked ID: {scan_state['last_id']}")

@bot.on_message(filters.private & filters.command("stats"))
async def stats_cmd(_, message: Message):
    if not is_owner(message):
        return
    await message.reply_text(f"Last scan stats\nScanned: {scan_state['scanned']}\nDeleted: {scan_state['deleted']}")

async def main():
    await user.start()
    me = await user.get_me()
    log.info("History/deletion account started: %s (%s)", me.first_name, me.id)
    await bot.start()
    log.info("Bot started.")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
