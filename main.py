import asyncio
import logging
import os
import re
import sqlite3
from typing import Optional

from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message as PyroMessage
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application, CallbackQueryHandler, ChatMemberHandler,
    CommandHandler, ContextTypes,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logging.getLogger("httpx").disabled = True
logging.getLogger("httpcore").disabled = True
log = logging.getLogger("amazon_media_cleaner")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
USER_SESSION = os.environ.get("USER_SESSION", "").strip()
OWNER_IDS = {
    int(v.strip()) for v in os.environ.get("OWNER_IDS", "").split(",")
    if v.strip().isdigit()
}
LEGACY_CHANNEL_ID = os.environ.get("CHANNEL_ID", "").strip()
DB_PATH = "/data/channels.db"

AMAZON_RE = re.compile(
    r"(?i)(?:https?://)?(?:www\.)?(?:amazon\.[a-z.]{2,}|amzn\.to|a\.co)(?:[/?:#][^\s<>()\]]*)?"
)

user = Client("amazon_cleaner_user", api_id=API_ID, api_hash=API_HASH, session_string=USER_SESSION)
user_ready = False
bot_id = None
scan_lock = asyncio.Lock()
stop_event = asyncio.Event()
scan_state = {"running": False, "mode": None, "scanned": 0, "matched": 0, "deleted": 0, "last_id": None, "channel": None}
selected_channel: Optional[int] = None


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS channels (
        chat_id INTEGER PRIMARY KEY, title TEXT NOT NULL, added_by INTEGER, added_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY, value TEXT NOT NULL
    )""")
    conn.commit()
    return conn


def init_db():
    global selected_channel
    os.makedirs("/data", exist_ok=True)
    conn = db()
    if LEGACY_CHANNEL_ID:
        try:
            cid = int(LEGACY_CHANNEL_ID)
            conn.execute("INSERT OR IGNORE INTO channels(chat_id,title,added_by) VALUES(?,?,?)",
                         (cid, "Existing configured channel", 0))
        except ValueError:
            log.warning("Ignoring invalid CHANNEL_ID")
    row = conn.execute("SELECT value FROM settings WHERE key='selected_channel'").fetchone()
    if row:
        selected_channel = int(row[0])
    else:
        row = conn.execute("SELECT chat_id FROM channels ORDER BY added_at LIMIT 1").fetchone()
        if row:
            selected_channel = int(row[0])
            conn.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('selected_channel',?)", (str(selected_channel),))
    conn.commit()
    conn.close()


def is_owner(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id in OWNER_IDS)


async def reply(update: Update, text: str, reply_markup=None):
    if update.effective_message:
        await update.effective_message.reply_text(text, reply_markup=reply_markup)


def registered_channels():
    conn = db()
    rows = conn.execute("SELECT chat_id,title FROM channels ORDER BY title").fetchall()
    conn.close()
    return rows


def channel_is_registered(channel_id: int) -> bool:
    return any(int(cid) == int(channel_id) for cid, _ in registered_channels())


def set_selected_channel(channel_id: int):
    global selected_channel
    selected_channel = int(channel_id)
    conn = db()
    conn.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('selected_channel',?)", (str(channel_id),))
    conn.commit()
    conn.close()


async def add_channel(channel_id: int, title: str, added_by: int, bot):
    member = await bot.get_chat_member(channel_id, bot_id)
    if member.status not in ("administrator", "creator"):
        raise ValueError("Bot ko is channel ka admin banana zaroori hai.")
    conn = db()
    conn.execute("INSERT OR REPLACE INTO channels(chat_id,title,added_by) VALUES(?,?,?)",
                 (channel_id, title or str(channel_id), added_by))
    conn.commit()
    conn.close()
    if selected_channel is None:
        set_selected_channel(channel_id)


def contains_amazon_link(message: PyroMessage) -> bool:
    body = message.text or message.caption or ""
    if AMAZON_RE.search(body):
        return True

    for entity in list(message.entities or []) + list(message.caption_entities or []):
        url = getattr(entity, "url", None)
        if url and AMAZON_RE.search(url):
            return True

    markup = getattr(message, "reply_markup", None)
    for row in getattr(markup, "inline_keyboard", []) or []:
        for button in row:
            url = getattr(button, "url", None)
            if url and AMAZON_RE.search(url):
                return True
    return False


def has_photo(message: PyroMessage) -> bool:
    # Strict rule: only native Telegram photo posts qualify. Text-only posts,
    # video-only posts, and document-only posts are never deleted by Delamazon.
    return message.photo is not None


async def delete_batch(channel_id: int, message_ids: list[int]) -> int:
    if not message_ids:
        return 0
    try:
        return int(await user.delete_messages(channel_id, message_ids) or 0)
    except FloodWait as exc:
        await asyncio.sleep(int(getattr(exc, "value", 5)))
        try:
            return int(await user.delete_messages(channel_id, message_ids) or 0)
        except RPCError:
            log.exception("Batch delete failed in %s", channel_id)
            return 0
    except RPCError:
        log.exception("Batch delete failed in %s", channel_id)
        return 0


async def ensure_history_account_admin(channel_id: int, bot) -> bool:
    """Temporarily grant the connected USER_SESSION delete permission when needed.
    Returns True only when this function promoted the account, so it can be demoted later.
    """
    account = await user.get_me()
    member = await user.get_chat_member(channel_id, account.id)

    if member.status in ("administrator", "owner", "creator"):
        privileges = getattr(member, "privileges", None)
        if member.status in ("owner", "creator") or (privileges and privileges.can_delete_messages):
            return False
        raise PermissionError(
            "USER_SESSION pehle se admin hai, lekin Delete Messages permission nahi hai. "
            "Is existing admin ke permissions ko automatic change nahi kiya gaya."
        )

    bot_member = await bot.get_chat_member(channel_id, bot_id)
    bot_privileges = getattr(bot_member, "privileges", None)
    bot_can_promote = (
        bot_member.status in ("creator", "owner")
        or (bot_member.status == "administrator" and bot_privileges
            and bot_privileges.can_promote_members)
    )
    if not bot_can_promote:
        raise PermissionError(
            "Bot USER_SESSION ko admin nahi bana sakta. Channel mein bot ko "
            "'Add New Admins' / Promote Members permission dein."
        )

    await bot.promote_chat_member(
        chat_id=channel_id,
        user_id=account.id,
        can_delete_messages=True,
    )
    await asyncio.sleep(1)
    promoted = await user.get_chat_member(channel_id, account.id)
    privileges = getattr(promoted, "privileges", None)
    if promoted.status not in ("administrator", "owner", "creator") or (
        promoted.status == "administrator" and
        (not privileges or not privileges.can_delete_messages)
    ):
        raise PermissionError(
            "Telegram ne USER_SESSION ko Delete Messages permission ke saath admin nahi banaya."
        )
    log.info("Temporarily promoted USER_SESSION %s in channel %s", account.id, channel_id)
    return True


async def demote_history_account(channel_id: int, bot):
    account = await user.get_me()
    await bot.promote_chat_member(
        chat_id=channel_id,
        user_id=account.id,
        can_manage_chat=False,
        can_change_info=False,
        can_post_messages=False,
        can_edit_messages=False,
        can_delete_messages=False,
        can_invite_users=False,
        can_restrict_members=False,
        can_pin_messages=False,
        can_promote_members=False,
        can_manage_video_chats=False,
        can_manage_topics=False,
    )
    log.info("Demoted temporary USER_SESSION admin in channel %s", channel_id)


async def run_delete(mode: str, owner_chat_id: int, context: ContextTypes.DEFAULT_TYPE,
                     channel_id: int, start_id: Optional[int] = None):
    global user_ready
    if not user_ready:
        await context.bot.send_message(owner_chat_id, "USER_SESSION connect nahi hai. Railway Variables check karein.")
        return
    if not channel_is_registered(channel_id):
        await context.bot.send_message(owner_chat_id, "Ye channel saved list mein nahi hai. Pehle /addchannel karein.")
        return
    if scan_lock.locked():
        await context.bot.send_message(owner_chat_id, "Ek job pehle se chal raha hai. /status ya /stop use karein.")
        return

    async with scan_lock:
        stop_event.clear()
        scan_state.update(running=True, mode=mode, scanned=0, matched=0, deleted=0, last_id=start_id, channel=channel_id)
        labels = {"all": "all messages", "amazon": "photo posts with Amazon links",
                  "from": f"messages from ID {start_id} onward"}
        await context.bot.send_message(owner_chat_id, f"Started in {channel_id}: {labels[mode]}.")
        batch = []
        auto_promoted = False
        try:
            auto_promoted = await ensure_history_account_admin(channel_id, context.bot)
            async for message in user.get_chat_history(channel_id):
                if stop_event.is_set():
                    break
                if mode == "from" and start_id is not None and message.id < start_id:
                    break
                scan_state["scanned"] += 1
                scan_state["last_id"] = message.id

                # Delamazon ONLY matches photo + Amazon URL, including hidden entities
                # and inline keyboard URL buttons. No-photo posts are always preserved.
                match = has_photo(message) and contains_amazon_link(message)
                should_delete = mode in ("all", "from") or (mode == "amazon" and match)
                if mode == "amazon" and match:
                    scan_state["matched"] += 1
                if should_delete:
                    batch.append(message.id)
                if len(batch) >= 100:
                    scan_state["deleted"] += await delete_batch(channel_id, batch)
                    batch.clear()

            if batch:
                scan_state["deleted"] += await delete_batch(channel_id, batch)
            status = "Stopped." if stop_event.is_set() else "Completed."
            await context.bot.send_message(
                owner_chat_id,
                f"{status}\nChannel: {channel_id}\nMode: {mode}\nScanned: {scan_state['scanned']}\n"
                f"Amazon photo matches: {scan_state['matched']}\nDeleted: {scan_state['deleted']}\n"
                f"Last checked ID: {scan_state['last_id']}"
            )
        except Exception as exc:
            log.exception("Deletion job failed")
            await context.bot.send_message(owner_chat_id, f"Job failed: {type(exc).__name__}: {exc}")
        finally:
            if auto_promoted:
                try:
                    await demote_history_account(channel_id, context.bot)
                except Exception:
                    log.exception("Could not demote temporary USER_SESSION admin in %s", channel_id)
                    try:
                        await context.bot.send_message(
                            owner_chat_id,
                            "WARNING: Job ke baad USER_SESSION ko automatically demote nahi kar paya. "
                            "Channel admin list mein manually check karein."
                        )
                    except Exception:
                        pass
            scan_state["running"] = False
            scan_state["mode"] = None


async def start_job(update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str,
                    start_id: Optional[int] = None, channel_id: Optional[int] = None):
    if not is_owner(update):
        await reply(update, "You are not authorized to use this bot.")
        return
    cid = channel_id or selected_channel
    if not cid:
        await reply(update, "Pehle channel add karein: /addchannel -100xxxxxxxxxx")
        return
    asyncio.create_task(run_delete(mode, update.effective_chat.id, context, cid, start_id))


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await reply(update, "You are not authorized to use this bot.")
        return
    session_status = "Connected" if user_ready else "NOT connected"
    await reply(update,
        f"Amazon Media Cleaner online. History account: {session_status}\n"
        f"Selected channel: {selected_channel}\n\n"
        "/addchannel CHANNEL_ID - add a channel (bot must be admin)\n"
        "/channels - show saved channels\n"
        "/usechannel CHANNEL_ID - select channel\n"
        "/removechannel CHANNEL_ID - remove from bot list\n"
        "/Delamazon - delete only photo posts with Amazon URLs\n"
        "/previewamazon - count matches without deleting\n"
        "/Delall - delete all posts in selected channel\n"
        "/Delfrom MESSAGE_ID - delete from ID to latest\n"
        "/status - progress | /stop - stop job")


async def addchannel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await reply(update, "Not authorized.")
        return
    cid = None
    title = ""
    if context.args and context.args[0].lstrip("-").isdigit():
        cid = int(context.args[0])
    else:
        msg = update.effective_message.reply_to_message if update.effective_message else None
        origin = getattr(msg, "forward_origin", None)
        chat = getattr(origin, "chat", None)
        cid = getattr(chat, "id", None)
        title = getattr(chat, "title", "") if chat else ""
    if not cid:
        await reply(update, "Channel ID dein: /addchannel -100xxxxxxxxxx, ya channel post forward karke uske reply mein /addchannel bhejein.")
        return
    try:
        chat = await context.bot.get_chat(cid)
        await add_channel(cid, title or chat.title or str(cid), update.effective_user.id, context.bot)
        set_selected_channel(cid)
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Use this channel", callback_data=f"use:{cid}"),
            InlineKeyboardButton("📋 My channels", callback_data="list"),
        ]])
        await reply(update, f"Channel add successful: {chat.title or cid}", keyboard)
    except Exception as exc:
        await reply(update, f"Channel add nahi hua: {exc}")


async def channels_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await reply(update, "Not authorized.")
        return
    rows = registered_channels()
    if not rows:
        await reply(update, "Abhi koi channel saved nahi hai.")
        return
    buttons = [[InlineKeyboardButton(("✓ " if int(cid) == selected_channel else "") + title[:45],
                                     callback_data=f"use:{cid}")] for cid, title in rows]
    await reply(update, "Saved channels (button dabakar select karein):", InlineKeyboardMarkup(buttons))


async def usechannel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await reply(update, "Not authorized.")
        return
    if not context.args or not context.args[0].lstrip("-").isdigit():
        await reply(update, "Use: /usechannel CHANNEL_ID")
        return
    cid = int(context.args[0])
    if not channel_is_registered(cid):
        await reply(update, "Channel saved list mein nahi hai.")
        return
    set_selected_channel(cid)
    await reply(update, f"Selected channel: {cid}")


async def removechannel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await reply(update, "Not authorized.")
        return
    if not context.args or not context.args[0].lstrip("-").isdigit():
        await reply(update, "Use: /removechannel CHANNEL_ID")
        return
    cid = int(context.args[0])
    conn = db()
    conn.execute("DELETE FROM channels WHERE chat_id=?", (cid,))
    conn.commit()
    conn.close()
    rows = registered_channels()
    if selected_channel == cid:
        set_selected_channel(int(rows[0][0]) if rows else 0)
        if not rows:
            conn = db()
            conn.execute("DELETE FROM settings WHERE key='selected_channel'")
            conn.commit()
            conn.close()
    await reply(update, f"Channel removed from saved list: {cid}. Existing Telegram posts were not deleted.")


async def select_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or query.from_user.id not in OWNER_IDS:
        if query:
            await query.answer("Not authorized.", show_alert=True)
        return
    await query.answer()
    if query.data == "list":
        rows = registered_channels()
        buttons = [[InlineKeyboardButton(title[:45], callback_data=f"use:{cid}")] for cid, title in rows]
        await query.edit_message_reply_markup(InlineKeyboardMarkup(buttons) if buttons else None)
        return
    if query.data.startswith("use:"):
        cid = int(query.data.split(":", 1)[1])
        if not channel_is_registered(cid):
            await query.answer("Channel not found.", show_alert=True)
            return
        set_selected_channel(cid)
        await query.edit_message_text(f"✅ Selected channel: {cid}")


async def my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    change = update.my_chat_member
    if not change or change.new_chat_member.status != "administrator":
        return
    chat = change.chat
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("➕ Add this channel", callback_data=f"adminadd:{chat.id}"),
        InlineKeyboardButton("📋 My channels", callback_data="list"),
    ]])
    for owner_id in OWNER_IDS:
        try:
            await context.bot.send_message(owner_id,
                f"Bot ko channel admin banaya gaya: {chat.title or chat.id}\nChannel ID: {chat.id}",
                reply_markup=keyboard)
        except Exception:
            log.exception("Could not notify owner %s", owner_id)


async def admin_add_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or query.from_user.id not in OWNER_IDS:
        if query:
            await query.answer("Not authorized.", show_alert=True)
        return
    if not query.data.startswith("adminadd:"):
        return
    cid = int(query.data.split(":", 1)[1])
    try:
        chat = await context.bot.get_chat(cid)
        await add_channel(cid, chat.title or str(cid), query.from_user.id, context.bot)
        set_selected_channel(cid)
        await query.answer("Channel added.")
        await query.edit_message_text(f"✅ Channel add successful: {chat.title or cid}\nSelected for commands.")
    except Exception as exc:
        await query.answer("Could not add channel.", show_alert=True)
        await query.edit_message_text(f"Channel add nahi hua: {exc}")


async def previewamazon_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await reply(update, "Not authorized.")
        return
    cid = selected_channel
    if not cid:
        await reply(update, "Pehle /addchannel karein.")
        return
    if not user_ready:
        await reply(update, "USER_SESSION connect nahi hai.")
        return
    if scan_lock.locked():
        await reply(update, "Ek scan pehle se chal raha hai.")
        return
    async def scan():
        async with scan_lock:
            count = scanned = 0
            auto_promoted = False
            try:
                auto_promoted = await ensure_history_account_admin(cid, context.bot)
                async for message in user.get_chat_history(cid):
                    scanned += 1
                    if has_photo(message) and contains_amazon_link(message):
                        count += 1
                await context.bot.send_message(update.effective_chat.id,
                    f"Preview complete.\nChannel: {cid}\nScanned: {scanned}\nAmazon photo matches: {count}\nKoi post delete nahi ki gayi.")
            except Exception as exc:
                await context.bot.send_message(update.effective_chat.id, f"Preview failed: {exc}")
            finally:
                if auto_promoted:
                    try:
                        await demote_history_account(cid, context.bot)
                    except Exception:
                        log.exception("Could not demote temporary USER_SESSION admin after preview in %s", cid)
                        await context.bot.send_message(
                            update.effective_chat.id,
                            "WARNING: Preview ke baad USER_SESSION ko automatically demote nahi kar paya. "
                            "Channel admin list manually check karein."
                        )
    asyncio.create_task(scan())


async def delall_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_job(update, context, "all")


async def delamazon_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_job(update, context, "amazon")


def forwarded_origin(message):
    return getattr(message, "forward_origin", None) if message else None


async def delfrom_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await reply(update, "Not authorized.")
        return
    start_id = None
    cid = selected_channel
    if len(context.args) == 1 and context.args[0].isdigit():
        start_id = int(context.args[0])
    elif not context.args and update.effective_message:
        origin = forwarded_origin(update.effective_message.reply_to_message)
        start_id = getattr(origin, "message_id", None)
        origin_chat = getattr(origin, "chat", None)
        if origin_chat:
            cid = origin_chat.id
    if not start_id or start_id < 1:
        await reply(update, "Use /Delfrom MESSAGE_ID, or forward a post from a saved channel and reply /Delfrom.")
        return
    await start_job(update, context, "from", start_id, cid)


async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await reply(update, "Not authorized.")
        return
    if scan_state["running"]:
        stop_event.set()
        await reply(update, "Stop requested.")
    else:
        await reply(update, "No job is running.")


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await reply(update, "Not authorized.")
        return
    await reply(update,
        f"Running: {scan_state['running']}\nMode: {scan_state['mode']}\nChannel: {scan_state['channel']}\n"
        f"Scanned: {scan_state['scanned']}\nMatched: {scan_state['matched']}\nDeleted: {scan_state['deleted']}\n"
        f"Last checked ID: {scan_state['last_id']}")


async def main():
    global user_ready, bot_id
    init_db()
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("addchannel", addchannel_cmd))
    application.add_handler(CommandHandler("channels", channels_cmd))
    application.add_handler(CommandHandler("usechannel", usechannel_cmd))
    application.add_handler(CommandHandler("removechannel", removechannel_cmd))
    application.add_handler(CommandHandler("delall", delall_cmd))
    application.add_handler(CommandHandler("Delall", delall_cmd))
    application.add_handler(CommandHandler("delamazon", delamazon_cmd))
    application.add_handler(CommandHandler("Delamazon", delamazon_cmd))
    application.add_handler(CommandHandler("previewamazon", previewamazon_cmd))
    application.add_handler(CommandHandler("delfrom", delfrom_cmd))
    application.add_handler(CommandHandler("Delfrom", delfrom_cmd))
    application.add_handler(CommandHandler("stop", stop_cmd))
    application.add_handler(CommandHandler("status", status_cmd))
    application.add_handler(ChatMemberHandler(my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
    application.add_handler(CallbackQueryHandler(admin_add_callback, pattern=r"^adminadd:"))
    application.add_handler(CallbackQueryHandler(select_callback, pattern=r"^(use:|list$)"))

    log.info("Starting Telegram bot polling service")
    me = await application.bot.get_me()
    bot_id = me.id
    try:
        if not USER_SESSION:
            raise RuntimeError("USER_SESSION is empty")
        await user.start()
        account = await user.get_me()
        user_ready = True
        log.info("History/deletion account connected: id=%s", account.id)
    except Exception:
        user_ready = False
        log.exception("User session failed; deletion commands are paused")

    await application.initialize()
    await application.bot.delete_webhook(drop_pending_updates=False)
    await application.start()
    if application.updater is None:
        raise RuntimeError("Telegram polling updater unavailable")
    await application.updater.start_polling(drop_pending_updates=False, allowed_updates=Update.ALL_TYPES)
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
