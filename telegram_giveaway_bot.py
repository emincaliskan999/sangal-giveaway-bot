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

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
DB_PATH = os.getenv("DB_PATH", "giveaway_bot.db")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


class JoinFlow(StatesGroup):
    waiting_for_1xbet_id = State()


# ---------------- DB ----------------

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS giveaways (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE,
            title TEXT,
            is_active INTEGER,
            created_at TEXT
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            giveaway_code TEXT,
            telegram_user_id INTEGER,
            username TEXT,
            xbet_id TEXT
        )
        """)
        conn.commit()


def create_giveaway(code, title):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO giveaways (code, title, is_active, created_at) VALUES (?, ?, 1, ?)",
            (code, title, datetime.utcnow().isoformat()),
        )
        conn.commit()


def get_giveaway(code):
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute(
            "SELECT code, title, is_active FROM giveaways WHERE code = ?",
            (code,),
        ).fetchone()


def close_giveaway(code):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE giveaways SET is_active=0 WHERE code=?", (code,))
        conn.commit()


def add_entry(code, user_id, username, xbet_id):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO entries (giveaway_code, telegram_user_id, username, xbet_id) VALUES (?, ?, ?, ?)",
            (code, user_id, username, xbet_id),
        )
        conn.commit()


def export_csv(code):
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT giveaway_code, telegram_user_id, username, xbet_id FROM entries WHERE giveaway_code=?",
            (code,),
        ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["code", "user_id", "username", "xbet_id"])
    writer.writerows(rows)
    return output.getvalue().encode()


# ---------------- UI ----------------

def build_keyboard(code, bot_username):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="JOIN",
                    url=f"https://t.me/{bot_username}?start={code}",
                )
            ]
        ]
    )


async def is_user_in_channel(user_id):
    try:
        member = await bot.get_chat_member(CHANNEL_USERNAME, user_id)
        return member.status in {
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.CREATOR,
        }
    except:
        return False


# ---------------- HANDLERS ----------------

@dp.message(CommandStart())
async def start(message: Message, state: FSMContext):
    args = message.text.split(maxsplit=1)
    code = args[1] if len(args) > 1 else None

    if not code:
        await message.answer("Use the giveaway link.")
        return

    giveaway = get_giveaway(code)
    if not giveaway:
        await message.answer("This giveaway is no longer active.")
        return

    if not await is_user_in_channel(message.from_user.id):
        await message.answer("Join channel first.")
        return

    await state.update_data(code=code)
    await state.set_state(JoinFlow.waiting_for_1xbet_id)
    await message.answer("Enter your 1xBet ID")


@dp.message(JoinFlow.waiting_for_1xbet_id)
async def id_handler(message: Message, state: FSMContext):
    data = await state.get_data()
    code = data["code"]

    add_entry(code, message.from_user.id, message.from_user.username, message.text)
    await message.answer("Saved.")
    await state.clear()


@dp.message(Command("new_giveaway"))
async def new_giveaway_handler(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    _, code, title = message.text.split(" ", 2)
    create_giveaway(code, title)
    await message.answer("Created")


@dp.message(Command("close_giveaway"))
async def close_handler(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    code = message.text.split()[1]
    close_giveaway(code)
    await message.answer("Closed")


@dp.message(Command("export"))
async def export_handler(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    code = message.text.split()[1]
    file = BufferedInputFile(export_csv(code), filename="export.csv")
    await message.answer_document(file)


@dp.message(Command("post_giveaway"))
async def post_giveaway(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    code = message.text.split()[1]
    me = await bot.get_me()

    caption = (
        "🔥 TEST123 FINAL BOT 🔥\n\n"
        "DERBY EXCITEMENT IS IN THE AIR 🔥\n\n"
        "We’re giving away 3 Galatasaray or Fenerbahçe jerseys 🎁\n\n"
        "How to participate:\n\n"
        "🔸 Subscribe\n"
        "🔸 Place a bet\n"
        "🔸 Click JOIN and enter your 1xBet ID\n\n"
        "🎯 Promocode: SANGAL1X\n"
        "📆 Deadline: 26/04/2026\n\n"
        "Good luck 🍀"
    )

    keyboard = build_keyboard(code, me.username)

    if message.reply_to_message and message.reply_to_message.photo:
        await bot.send_photo(
            chat_id=CHANNEL_USERNAME,
            photo=message.reply_to_message.photo[-1].file_id,
            caption=caption,
            reply_markup=keyboard,
        )
    else:
        await bot.send_message(
            chat_id=CHANNEL_USERNAME,
            text=caption,
            reply_markup=keyboard,
        )


# ---------------- RUN ----------------

async def main():
    init_db()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
