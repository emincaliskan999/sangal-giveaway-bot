import asyncio
import csv
import io
import logging
import os
import sqlite3
from datetime import datetime
from typing import Optional

from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ChatMemberStatus
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    CallbackQuery,
)

BOT_TOKEN = os.getenv("BOT_TOKEN", "PUT_YOUR_BOT_TOKEN_HERE")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "@yourchannel")
ADMIN_IDS = {
    int(x) for x in os.getenv("ADMIN_IDS", "123456789").split(",") if x.strip()
}
DB_PATH = os.getenv("DB_PATH", "giveaway_bot.db")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


class JoinFlow(StatesGroup):
    waiting_for_1xbet_id = State()


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS giveaways (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                giveaway_code TEXT NOT NULL,
                telegram_user_id INTEGER NOT NULL,
                username TEXT,
                full_name TEXT,
                xbet_id TEXT NOT NULL,
                joined_at TEXT NOT NULL,
                UNIQUE(giveaway_code, telegram_user_id)
            )
            """
        )
        conn.commit()


def create_giveaway(code: str, title: str) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO giveaways (code, title, is_active, created_at) VALUES (?, ?, 1, ?)",
            (code, title, datetime.utcnow().isoformat()),
        )
        conn.commit()


def get_giveaway(code: str) -> Optional[tuple]:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT id, code, title, is_active, created_at FROM giveaways WHERE code = ?",
            (code,),
        ).fetchone()
        return row


def list_giveaways() -> list[tuple]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT code, title, is_active, created_at FROM giveaways ORDER BY id DESC"
        ).fetchall()
        return rows


def close_giveaway(code: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            "UPDATE giveaways SET is_active = 0 WHERE code = ? AND is_active = 1",
            (code,),
        )
        conn.commit()
        return cur.rowcount > 0


def entry_exists(giveaway_code: str, telegram_user_id: int) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM entries WHERE giveaway_code = ? AND telegram_user_id = ?",
            (giveaway_code, telegram_user_id),
        ).fetchone()
        return row is not None


def add_entry(
    giveaway_code: str,
    telegram_user_id: int,
    username: Optional[str],
    full_name: str,
    xbet_id: str,
) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO entries (
                giveaway_code, telegram_user_id, username, full_name, xbet_id, joined_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                giveaway_code,
                telegram_user_id,
                username,
                full_name,
                xbet_id,
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()


def export_entries_csv(giveaway_code: str) -> bytes:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT giveaway_code, telegram_user_id, username, full_name, xbet_id, joined_at
            FROM entries
            WHERE giveaway_code = ?
            ORDER BY id ASC
            """,
            (giveaway_code,),
        ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "giveaway_code",
            "telegram_user_id",
            "username",
            "full_name",
            "xbet_id",
            "joined_at",
        ]
    )
    writer.writerows(rows)
    return output.getvalue().encode("utf-8-sig")


def count_entries(giveaway_code: str) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM entries WHERE giveaway_code = ?",
            (giveaway_code,),
        ).fetchone()
        return row[0] if row else 0


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def build_check_keyboard(giveaway_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="I Joined The Channel",
                    callback_data=f"check_join:{giveaway_code}",
                )
            ]
        ]
    )


def build_join_url(giveaway_code: str, bot_username: str) -> str:
    return f"https://t.me/{bot_username}?start={giveaway_code}"


def build_channel_post_keyboard(
    giveaway_code: str, bot_username: str
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="JOIN",
                    url=build_join_url(giveaway_code, bot_username),
                )
            ]
        ]
    )


def normalize_xbet_id(text: str) -> str:
    return text.strip()


def is_valid_xbet_id(text: str) -> bool:
    text = normalize_xbet_id(text)
    return text.isdigit() and 4 <= len(text) <= 20


async def is_user_in_channel(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_USERNAME, user_id=user_id)
        return member.status in {
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.CREATOR,
        }
    except Exception as e:
        logger.exception("Channel membership check failed: %s", e)
        return False


@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext) -> None:
    args = message.text.split(maxsplit=1)
    giveaway_code = args[1].strip() if len(args) > 1 else None

    await state.clear()

    if not giveaway_code:
        await message.answer(
            "Use the giveaway link from the latest channel post to join."
        )
        return

    giveaway = get_giveaway(giveaway_code)
    if not giveaway:
        await message.answer(
            "This giveaway link is no longer active. Please return to the latest channel post."
        )
        return

    _, code, title, is_active, _ = giveaway
    if not is_active:
        await message.answer("This giveaway is closed.")
        return

    if entry_exists(code, message.from_user.id):
        await message.answer("You already joined this giveaway.")
        return

    joined = await is_user_in_channel(message.from_user.id)
    if not joined:
        await state.update_data(giveaway_code=code)
        await message.answer(
            f"To join {title}, first subscribe to {CHANNEL_USERNAME}. After joining, press the button below.",
            reply_markup=build_check_keyboard(code),
        )
        return

    await state.update_data(giveaway_code=code)
    await state.set_state(JoinFlow.waiting_for_1xbet_id)
    await message.answer(f"You are joining {title}. Please enter your 1xBet ID.")


@dp.callback_query(F.data.startswith("check_join:"))
async def check_join_callback(callback: CallbackQuery, state: FSMContext) -> None:
    giveaway_code = callback.data.split(":", 1)[1]
    joined = await is_user_in_channel(callback.from_user.id)

    if not joined:
        await callback.answer("You still need to join the channel first.", show_alert=True)
        return

    await state.update_data(giveaway_code=giveaway_code)
    await state.set_state(JoinFlow.waiting_for_1xbet_id)
    await callback.message.answer("Great. Now send your 1xBet ID.")
    await callback.answer()


@dp.message(JoinFlow.waiting_for_1xbet_id)
async def handle_1xbet_id(message: Message, state: FSMContext) -> None:
    xbet_id = normalize_xbet_id(message.text)
    if not is_valid_xbet_id(xbet_id):
        await message.answer("Please enter a valid numeric 1xBet ID.")
        return

    data = await state.get_data()
    giveaway_code = data.get("giveaway_code")
    if not giveaway_code:
        await state.clear()
        await message.answer("Session expired. Please use the giveaway link again.")
        return

    giveaway = get_giveaway(giveaway_code)
    if not giveaway or not giveaway[3]:
        await state.clear()
        await message.answer("This giveaway is no longer active.")
        return

    if entry_exists(giveaway_code, message.from_user.id):
        await state.clear()
        await message.answer("You already joined this giveaway.")
        return

    full_name = " ".join(
        x for x in [message.from_user.first_name, message.from_user.last_name] if x
    ).strip()

    try:
        add_entry(
            giveaway_code=giveaway_code,
            telegram_user_id=message.from_user.id,
            username=message.from_user.username,
            full_name=full_name,
            xbet_id=xbet_id,
        )
    except sqlite3.IntegrityError:
        await message.answer("You already joined this giveaway.")
        await state.clear()
        return

    await state.clear()
    await message.answer("Your participation has been recorded successfully.")


@dp.message(Command("new_giveaway"))
async def new_giveaway_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("You are not authorized.")
        return

    parts = message.text.split(" ", 2)
    if len(parts) < 3:
        await message.answer("Usage:\n/new_giveaway giveaway_code Giveaway Title")
        return

    code = parts[1].strip()
    title = parts[2].strip()

    try:
        create_giveaway(code, title)
    except sqlite3.IntegrityError:
        await message.answer("A giveaway with this code already exists.")
        return

    me = await bot.get_me()
    deep_link = f"https://t.me/{me.username}?start={code}"
    await message.answer(
        f"Giveaway created successfully.\n\nCode: {code}\nTitle: {title}\nJoin link: {deep_link}"
    )


@dp.message(Command("giveaways"))
async def giveaways_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("You are not authorized.")
        return

    rows = list_giveaways()
    if not rows:
        await message.answer("No giveaways found.")
        return

    text_parts = []
    for code, title, is_active, created_at in rows:
        total = count_entries(code)
        status = "active" if is_active else "closed"
        text_parts.append(
            f"• {title}\ncode: {code}\nstatus: {status}\nentries: {total}\ncreated: {created_at}"
        )

    await message.answer("\n\n".join(text_parts))


@dp.message(Command("close_giveaway"))
async def close_giveaway_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("You are not authorized.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Usage:\n/close_giveaway giveaway_code")
        return

    code = parts[1].strip()
    ok = close_giveaway(code)
    await message.answer(
        "Giveaway closed." if ok else "Giveaway not found or already closed."
    )


@dp.message(Command("export"))
async def export_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("You are not authorized.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Usage:\n/export giveaway_code")
        return

    code = parts[1].strip()
    giveaway = get_giveaway(code)
    if not giveaway:
        await message.answer("Giveaway not found.")
        return

    csv_bytes = export_entries_csv(code)
    file = BufferedInputFile(csv_bytes, filename=f"{code}_entries.csv")
    await message.answer_document(file, caption=f"Export for {code}")


@dp.message(Command("post_giveaway"))
async def post_giveaway_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("You are not authorized.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer(
            "Usage:\n/post_giveaway giveaway_code\n\nOptional: reply to a photo with this command to publish a visual post."
        )
        return

    code = parts[1].strip()
    giveaway = get_giveaway(code)
    if not giveaway:
        await message.answer("Giveaway not found.")
        return

    _, giveaway_code, title, is_active, _ = giveaway
    if not is_active:
        await message.answer("This giveaway is closed.")
        return

    me = await bot.get_me()
    join_url = build_join_url(giveaway_code, me.username)
    keyboard = build_channel_post_keyboard(giveaway_code, me.username)

    caption = (
        "DERBY EXCITEMENT IS IN THE AIR 🔥\n\n"
        "We’re giving away 3 Galatasaray or Fenerbahçe jerseys so you can watch the upcoming derbies in your team’s colors 🎁\n\n"
        "How to participate:\n\n"
        "🔸 Make sure you're subscribed to Sangal Telegram\n"
        "🔸 Place a bet on the derby\n"
        "🔸 Click JOIN and enter your 1xBet ID\n\n"
        "🎯 Promocode: SANGAL1X\n"
        "🔗 https://crplnk.net/BUd1jg\n\n"
        "📆 Deadline: 26/04/2026\n\n"
        "Good luck 🍀\n\n"
        "❗️18+, bet responsibly. Check legal regulations in your country."
    )

    if message.reply_to_message and message.reply_to_message.photo:
        photo = message.reply_to_message.photo[-1]
        await bot.send_photo(
            chat_id=CHANNEL_USERNAME,
            photo=photo.file_id,
            caption=caption,
            reply_markup=keyboard,
        )
    else:
        await bot.send_message(
            chat_id=CHANNEL_USERNAME,
            text=caption,
            reply_markup=keyboard,
        )

    await message.answer(
        "Giveaway post published successfully.\n\n"
        f"Join link: {join_url}"
    )


@dp.message(Command("help_admin"))
async def help_admin_handler(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("You are not authorized.")
        return

    await message.answer(
        "Admin commands:\n\n"
        "/new_giveaway code title\n"
        "/giveaways\n"
        "/close_giveaway code\n"
        "/export code\n"
        "/post_giveaway code"
    )


async def healthcheck(request):
    return web.Response(text="ok")


async def start_health_server():
    app = web.Application()
    app.router.add_get("/", healthcheck)
    app.router.add_get("/healthz", healthcheck)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.getenv("PORT", "10000"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()


async def main() -> None:
    if BOT_TOKEN == "PUT_YOUR_BOT_TOKEN_HERE":
        raise RuntimeError("Please set BOT_TOKEN before running the bot.")

    init_db()
    await start_health_server()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
