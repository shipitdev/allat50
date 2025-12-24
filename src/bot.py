import asyncio
import json
import os
import time
from pathlib import Path

from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    ApplicationHandlerStop,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    TypeHandler,
    filters,
)

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.json"
LOGO_PATH = Path(__file__).resolve().parent.parent / "allat50.png"

if not CONFIG_PATH.exists():
    raise SystemExit("Missing config.json.")

with CONFIG_PATH.open("r", encoding="utf-8") as handle:
    CONFIG = json.load(handle)

FOOD_TOKEN = CONFIG.get("botToken") or os.environ.get("BOT_TOKEN")
if not FOOD_TOKEN:
    raise SystemExit("Missing botToken in config.json or BOT_TOKEN in environment.")

FLIGHT_TOKEN = CONFIG.get("flightBotToken")
HOTEL_TOKEN = CONFIG.get("hotelBotToken")

raw_admin_ids = CONFIG.get("adminChatIds")
if not isinstance(raw_admin_ids, list):
    admin_single = CONFIG.get("adminChatId")
    raw_admin_ids = [admin_single] if admin_single is not None else []
ADMIN_CHAT_IDS = []
for item in raw_admin_ids:
    try:
        ADMIN_CHAT_IDS.append(int(item))
    except (TypeError, ValueError):
        continue
if not ADMIN_CHAT_IDS:
    raise SystemExit("Missing adminChatIds array in config.json.")

BANNED_CHAT_IDS = set()
for item in CONFIG.get("bannedChatIds", []):
    try:
        BANNED_CHAT_IDS.add(int(item))
    except (TypeError, ValueError):
        continue

TICKET_RECORDS = {}
records = CONFIG.get("ticketRecords")
if isinstance(records, dict):
    for key, record in records.items():
        try:
            ticket_id = int(key)
        except (TypeError, ValueError):
            continue
        if isinstance(record, dict):
            record["ticketId"] = ticket_id
            TICKET_RECORDS[ticket_id] = record

ticket_counter = int(CONFIG.get("ticketCounter", 60))

session_timeout_minutes = CONFIG.get("sessionTimeoutMinutes")
try:
    session_timeout_minutes = float(session_timeout_minutes)
except (TypeError, ValueError):
    session_timeout_minutes = 15
SESSION_TIMEOUT_SECONDS = max(session_timeout_minutes, 0) * 60

rate_limit = CONFIG.get("rateLimit") or {}
rate_limit_window = rate_limit.get("windowMinutes", 30)
rate_limit_max = rate_limit.get("maxTickets", 2)
try:
    rate_limit_window = float(rate_limit_window)
except (TypeError, ValueError):
    rate_limit_window = 30
try:
    rate_limit_max = int(rate_limit_max)
except (TypeError, ValueError):
    rate_limit_max = 2
RATE_LIMIT_SECONDS = max(rate_limit_window, 0) * 60
RATE_LIMIT_ENABLED = RATE_LIMIT_SECONDS > 0 and rate_limit_max > 0

BOT_USERNAMES = {
    "food": CONFIG.get("foodBotUsername", ""),
    "flight": CONFIG.get("flightBotUsername", ""),
    "hotel": CONFIG.get("hotelBotUsername", ""),
}

CHANNEL_URL = "https://t.me/Allat50"
GROUP_URL = "https://t.me/Allat50_group"

FOOD_PROMO = (
    "🔥 <b>50% OFF</b>\n🚗 <b>DELIVERY only</b>\n💵 <b>$40 MIN - $100 MAX</b>"
)
FOOD_QUESTIONS = [
    {"key": "name", "label": "Name", "prompt": "👤 <b>First and last name?</b>"},
    {
        "key": "address",
        "label": "Address",
        "prompt": (
            "🏠 <b>Full address</b> (must include apt#, zip, state, etc)\n\n"
            "Format example: 4455 Landing Lange, APT 4, Louisville, KY 40018"
        ),
    },
    {
        "key": "phone",
        "label": "Phone",
        "prompt": "📞 <b>Phone number?</b> (will be used to receive updates about your order)",
    },
]
FOOD_CONTINUE_PROMPT = (
    "✅ <b>Would you like to continue?</b>\nType <b>yes</b> or /cancel"
)

FLIGHT_PROMO = (
    "✈️ <b>Flights</b>\n"
    "🔻 <b>40% OFF</b>\n"
    "🔻 <b>Domestic & International</b>\n"
    "🔻 <b>JetBlue, Spirit, Frontier,</b>\n"
    "<b>Southwest, American Airlines and</b>\n"
    "<b>International custom airlines</b>\n"
    "🔻 <b>100% Safe</b>\n"
    "🔻 <b>Book up-to 5 days in advance!</b>\n\n"
    "⏱️ <b>upto 24hr response time</b>"
)
FLIGHT_QUESTIONS = [
    {"key": "trip_dates", "label": "Trip Dates", "prompt": "📅 <b>Trip Dates?</b>"},
    {
        "key": "passenger_form",
        "label": "Passenger Info",
        "prompt": (
            "🧾 <b>Please fill out this form</b>\n"
            "Per Passenger\nIn the Format Below :-\n"
            "<b>First Name</b> :\n"
            "<b>Middle Name</b> : if have\n"
            "<b>Last Name</b> : <b>DOB (MM/DD/YYYY)</b> :\n"
            "<b>Male/Female</b> : <b>Email</b> : <b>Phone</b> :"
        ),
    },
    {
        "key": "residence",
        "label": "State of residence",
        "prompt": "📍 <b>State of residence?</b>",
    },
    {"key": "order_total", "label": "Total value", "prompt": "💵 <b>Total value of order?</b>"},
    {"key": "airlines", "label": "Airlines", "prompt": "✈️ <b>What airlines?</b>"},
]
FLIGHT_CONTINUE_PROMPT = (
    "✅ <b>Would you like to continue?</b>\nType <b>yes</b> to continue or /cancel"
)

HOTEL_PROMO = (
    "🏨 <b>Hotels</b>\n"
    "💎 <b>Premium stays & verified bookings</b>\n\n"
    "⏱️ <b>upto 24hr response time</b>"
)
HOTEL_QUESTIONS = [
    {"key": "destination", "label": "Destination", "prompt": "📍 <b>Destination city?</b>"},
    {"key": "dates", "label": "Dates", "prompt": "📅 <b>Check-in and check-out dates?</b>"},
    {"key": "budget", "label": "Budget", "prompt": "💵 <b>Budget range?</b>"},
    {"key": "email", "label": "Email", "prompt": "📧 <b>Customer email for booking?</b>"},
    {"key": "booking_link", "label": "Booking link", "prompt": "🔗 <b>Booking.com link (if any)?</b>"},
    {
        "key": "preferred_chain",
        "label": "Preferred chain",
        "prompt": "🏨 <b>Preferred hotel chain?</b>\nExamples: Marriot / Hilton / IHG",
    },
]
HOTEL_CONTINUE_PROMPT = (
    "✅ <b>Would you like to continue?</b>\nType <b>yes</b> to continue or /cancel"
)

START_PROMPT = "🧭 <b>Send /start</b> to begin or choose a service from the menu."
FLIGHT_START_PROMPT = "🧭 <b>Send /start</b> to begin your flight request."
HOTEL_START_PROMPT = "🧭 <b>Send /start</b> to begin your hotel request."

FOOD_CATEGORIES = [
    {"id": "fast_food", "label": "🔴 Fast Food Pickup 55% off"},
    {"id": "meal_kits", "label": "🥑 Meal Kits"},
    {"id": "sonic_combo", "label": "🔴 Sonic | 🍗 Zaxby's | 🥤 Smoothie King"},
    {"id": "ihop_dennys", "label": "🥞 IHOP/Dennys"},
    {"id": "panera", "label": "🥪 Panera"},
    {"id": "wingstop", "label": "🍗 WingStop"},
    {"id": "panda", "label": "🐼 Panda Express"},
    {"id": "five_guys", "label": "🍔 Five Guys"},
    {"id": "pizza", "label": "🍕 Pizza"},
    {"id": "chipotle", "label": "🌯 Chipotle"},
    {"id": "cava", "label": "🥗 Cava"},
    {"id": "shake_shack", "label": "🍔 Shake Shack"},
    {"id": "canes", "label": "🔴 Canes"},
    {"id": "restaurants", "label": "🍽️ Restaurants"},
    {"id": "dine_in", "label": "🍴 Dine-In"},
    {"id": "ubereats", "label": "🚗 UberEats"},
    {"id": "doordash", "label": "🚗 Doordash"},
    {"id": "grubhub", "label": "🌭 Grubhub Delivery"},
    {"id": "groceries", "label": "🛒 Groceries"},
    {"id": "movies", "label": "🎬 Movies"},
    {"id": "uber_rides", "label": "🔴 Uber Rides"},
]

TICKETS = {}
ADMIN_TICKET_MESSAGES = {}
CUSTOMER_TICKETS = {}
TICKET_HISTORY = {}


def normalize_username(value: str) -> str:
    return value.replace("@", "") if value else ""


def format_hybrid_link(label: str, url: str) -> str:
    return f'<a href="{url}">| {label} |</a>'


def format_bot_link(label: str, username: str) -> Optional[str]:
    clean = normalize_username(username)
    if not clean:
        return None
    return format_hybrid_link(label, f"https://t.me/{clean}")


def quick_links_section() -> str:
    links = [
        format_bot_link("Food BOT", BOT_USERNAMES["food"]),
        format_bot_link("Flight BOT", BOT_USERNAMES["flight"]),
        format_bot_link("Hotel BOT", BOT_USERNAMES["hotel"]),
        format_hybrid_link("All at 50 Channel", CHANNEL_URL),
        format_hybrid_link("All at 50 Group", GROUP_URL),
    ]
    links = [item for item in links if item]
    if not links:
        return ""
    first_line = " ".join(links[:3])
    second_line = " ".join(links[3:])
    extra = f"\n{second_line}" if second_line else ""
    return f"\n\n<b>Quick links</b>\n{first_line}{extra}"


def food_home() -> str:
    return (
        "👋 <b>Foodbot Concierge</b>\n"
        "<i>Food Orders • Flight Bookings • Hotel Bookings</i>\n\n"
        "🟢 <b>Status:</b> Up\n"
        "🎯 <b>How it works:</b> Pick a service → answer a few questions → connect with an agent\n"
        "🔥 <b>Promos:</b> Food 50% off · Flights 40% off\n\n"
        "👇 <b>Tap a food category to begin</b>"
        + quick_links_section()
    )


def flight_home() -> str:
    return (
        "✈️ <b>Flight Concierge</b>\n"
        "<i>Domestic & International • 40% OFF</i>\n\n"
        "🧭 <b>How it works:</b> Answer a few quick questions → connect with an agent\n"
        "⏱️ <b>Response:</b> up to 24h\n\n"
        "👇 <b>Tap to start</b>"
        + quick_links_section()
    )


def hotel_home() -> str:
    return (
        "🏨 <b>Hotel Concierge</b>\n"
        "<i>Verified stays • Premium deals</i>\n\n"
        "🧭 <b>How it works:</b> Share your trip details → connect with an agent\n"
        "⏱️ <b>Response:</b> up to 24h\n\n"
        "👇 <b>Tap to start</b>"
        + quick_links_section()
    )


def main_menu() -> InlineKeyboardMarkup:
    return food_menu(include_back=False)


def flight_start_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("✈️ Start Flight Booking", callback_data="flight:start")]]
    )


def hotel_start_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🏨 Start Hotel Booking", callback_data="hotel:start")]]
    )


def food_menu(include_back: bool = True) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(FOOD_CATEGORIES[0]["label"], callback_data="food:fast_food")],
        [InlineKeyboardButton(FOOD_CATEGORIES[1]["label"], callback_data="food:meal_kits")],
        [InlineKeyboardButton(FOOD_CATEGORIES[2]["label"], callback_data="food:sonic_combo")],
        [
            InlineKeyboardButton(FOOD_CATEGORIES[3]["label"], callback_data="food:ihop_dennys"),
            InlineKeyboardButton(FOOD_CATEGORIES[4]["label"], callback_data="food:panera"),
        ],
        [
            InlineKeyboardButton(FOOD_CATEGORIES[5]["label"], callback_data="food:wingstop"),
            InlineKeyboardButton(FOOD_CATEGORIES[6]["label"], callback_data="food:panda"),
        ],
        [
            InlineKeyboardButton(FOOD_CATEGORIES[7]["label"], callback_data="food:five_guys"),
            InlineKeyboardButton(FOOD_CATEGORIES[8]["label"], callback_data="food:pizza"),
        ],
        [
            InlineKeyboardButton(FOOD_CATEGORIES[9]["label"], callback_data="food:chipotle"),
            InlineKeyboardButton(FOOD_CATEGORIES[10]["label"], callback_data="food:cava"),
        ],
        [
            InlineKeyboardButton(FOOD_CATEGORIES[11]["label"], callback_data="food:shake_shack"),
            InlineKeyboardButton(FOOD_CATEGORIES[12]["label"], callback_data="food:canes"),
        ],
        [
            InlineKeyboardButton(FOOD_CATEGORIES[13]["label"], callback_data="food:restaurants"),
            InlineKeyboardButton(FOOD_CATEGORIES[14]["label"], callback_data="food:dine_in"),
        ],
        [
            InlineKeyboardButton(FOOD_CATEGORIES[15]["label"], callback_data="food:ubereats"),
            InlineKeyboardButton(FOOD_CATEGORIES[16]["label"], callback_data="food:doordash"),
        ],
        [InlineKeyboardButton(FOOD_CATEGORIES[17]["label"], callback_data="food:grubhub")],
        [InlineKeyboardButton(FOOD_CATEGORIES[18]["label"], callback_data="food:groceries")],
        [
            InlineKeyboardButton(FOOD_CATEGORIES[19]["label"], callback_data="food:movies"),
            InlineKeyboardButton(FOOD_CATEGORIES[20]["label"], callback_data="food:uber_rides"),
        ],
    ]
    if include_back:
        rows.append([InlineKeyboardButton("⬅️ Back to main menu", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def admin_ticket_keyboard(ticket_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🛑 Close ticket", callback_data=f"close:{ticket_id}"),
                InlineKeyboardButton("🚫 Ban customer", callback_data=f"ban:{ticket_id}"),
            ]
        ]
    )


def save_config() -> None:
    CONFIG["adminChatIds"] = ADMIN_CHAT_IDS
    CONFIG["bannedChatIds"] = list(BANNED_CHAT_IDS)
    CONFIG["ticketCounter"] = ticket_counter
    CONFIG["ticketRecords"] = {str(k): v for k, v in TICKET_RECORDS.items()}
    with CONFIG_PATH.open("w", encoding="utf-8") as handle:
        json.dump(CONFIG, handle, indent=2)


def next_ticket_id() -> int:
    global ticket_counter
    ticket_counter += 1
    save_config()
    return ticket_counter


def create_ticket_record(ticket_id: int, data: dict) -> None:
    TICKET_RECORDS[ticket_id] = {
        "ticketId": ticket_id,
        "status": "open",
        "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        **data,
    }
    save_config()


def close_ticket_record(ticket_id: int, updates: dict) -> bool:
    record = TICKET_RECORDS.get(ticket_id)
    if not record:
        return False
    record.update(
        {
            "status": "closed",
            "closedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            **updates,
        }
    )
    TICKET_RECORDS[ticket_id] = record
    save_config()
    return True


def summarize_report() -> dict:
    totals = {
        "total": 0,
        "open": 0,
        "closed_with_remarks": 0,
        "closed_no_order": 0,
        "banned": 0,
    }
    for record in TICKET_RECORDS.values():
        totals["total"] += 1
        if record.get("status") == "open":
            totals["open"] += 1
            continue
        if record.get("closeType") == "admin_close":
            totals["closed_with_remarks"] += 1
            continue
        if record.get("closeType") == "banned":
            totals["banned"] += 1
            continue
        totals["closed_no_order"] += 1
    return totals


def get_open_ticket(chat_id: int):
    ticket_id = CUSTOMER_TICKETS.get(chat_id)
    if not ticket_id:
        return None
    ticket = TICKETS.get(ticket_id)
    if not ticket or ticket.get("status") != "open":
        CUSTOMER_TICKETS.pop(chat_id, None)
        return None
    return ticket_id, ticket


def close_tickets_for_chat(chat_id: int, close_type: str, closed_by: str) -> None:
    for ticket_id, ticket in list(TICKETS.items()):
        if ticket.get("chatId") == chat_id and ticket.get("status") == "open":
            ticket["status"] = "closed"
            TICKETS[ticket_id] = ticket
            close_ticket_record(ticket_id, {"closeType": close_type, "closedBy": closed_by})
    CUSTOMER_TICKETS.pop(chat_id, None)


def check_rate_limit(user_id: int):
    if not RATE_LIMIT_ENABLED:
        return {"limited": False}
    now = time.time()
    window_start = now - RATE_LIMIT_SECONDS
    history = [t for t in TICKET_HISTORY.get(user_id, []) if t >= window_start]
    if len(history) >= rate_limit_max:
        retry_after = history[0] + RATE_LIMIT_SECONDS - now
        TICKET_HISTORY[user_id] = history
        return {"limited": True, "retry_after": retry_after}
    history.append(now)
    TICKET_HISTORY[user_id] = history
    return {"limited": False}


class SessionStore:
    def __init__(self, bot):
        self.bot = bot
        self.sessions = {}

    def get(self, chat_id: int):
        return self.sessions.get(chat_id)

    def set(self, chat_id: int, session: dict):
        existing = self.sessions.get(chat_id)
        if existing and existing.get("timeout_task"):
            existing["timeout_task"].cancel()
        if SESSION_TIMEOUT_SECONDS > 0:
            last_active = time.time()
            session["last_active"] = last_active
            session["timeout_task"] = asyncio.create_task(
                self._timeout(chat_id, last_active)
            )
        self.sessions[chat_id] = session

    def reset(self, chat_id: int):
        existing = self.sessions.get(chat_id)
        if existing and existing.get("timeout_task"):
            existing["timeout_task"].cancel()
        self.sessions.pop(chat_id, None)

    async def _timeout(self, chat_id: int, last_active: float):
        await asyncio.sleep(SESSION_TIMEOUT_SECONDS)
        current = self.sessions.get(chat_id)
        if not current or current.get("last_active") != last_active:
            return
        self.sessions.pop(chat_id, None)
        await self.bot.send_message(
            chat_id,
            "⏰ <b>Session timed out</b> due to inactivity.\nSend /start to begin again.",
            parse_mode=ParseMode.HTML,
        )


async def send_home(update: Update, context: ContextTypes.DEFAULT_TYPE, caption: str, keyboard):
    if update.message:
        if LOGO_PATH.exists():
            try:
                with LOGO_PATH.open("rb") as handle:
                    await update.message.reply_photo(
                        photo=handle,
                        caption=caption,
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard,
                    )
                    return
            except Exception:
                pass
        await update.message.reply_text(
            caption, parse_mode=ParseMode.HTML, reply_markup=keyboard
        )


async def ban_guard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat:
        return
    if chat.id in ADMIN_CHAT_IDS:
        return
    if chat.id in BANNED_CHAT_IDS:
        if update.callback_query:
            try:
                await update.callback_query.answer("Access restricted.")
            except Exception:
                pass
        if update.effective_message:
            await update.effective_message.reply_text(
                "🚫 <b>Access restricted.</b>", parse_mode=ParseMode.HTML
            )
        raise ApplicationHandlerStop()


async def admin_reply_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat or chat.id not in ADMIN_CHAT_IDS:
        return
    message = update.effective_message
    if not message or not message.reply_to_message:
        return
    entry = ADMIN_TICKET_MESSAGES.get((chat.id, message.reply_to_message.message_id))
    if not entry:
        return
    if entry["botKey"] != context.application.bot_data.get("bot_key"):
        return
    ticket = TICKETS.get(entry["ticketId"])
    if not ticket or ticket.get("status") != "open":
        await message.reply_text("Ticket is closed or no longer exists.")
        raise ApplicationHandlerStop()

    admin_label = update.effective_user.first_name or "Admin"
    if message.text:
        await context.bot.send_message(
            ticket["chatId"], f"{admin_label}: {message.text}"
        )
    else:
        await context.bot.send_message(ticket["chatId"], f"{admin_label} sent:")
        await context.bot.copy_message(
            ticket["chatId"], chat.id, message.message_id
        )
    raise ApplicationHandlerStop()


async def forward_customer_message(
    bot, chat_id: int, ticket_id: int, ticket: dict, message
):
    customer_label = message.from_user.first_name or "Customer"
    header = f"Customer ({customer_label}) on ticket #{ticket_id}:"
    targets = ticket.get("adminMessages") or [
        {"chatId": admin_id} for admin_id in ADMIN_CHAT_IDS
    ]
    for target in targets:
        reply_id = target.get("messageId")
        if message.text:
            sent = await bot.send_message(
                target["chatId"],
                f"{header} {message.text}",
                reply_to_message_id=reply_id,
            )
            ADMIN_TICKET_MESSAGES[(target["chatId"], sent.message_id)] = {
                "ticketId": ticket_id,
                "botKey": ticket["botKey"],
            }
            continue
        header_message = await bot.send_message(
            target["chatId"], header, reply_to_message_id=reply_id
        )
        ADMIN_TICKET_MESSAGES[(target["chatId"], header_message.message_id)] = {
            "ticketId": ticket_id,
            "botKey": ticket["botKey"],
        }
        copied = await bot.copy_message(
            target["chatId"],
            chat_id,
            message.message_id,
            reply_to_message_id=reply_id,
        )
        ADMIN_TICKET_MESSAGES[(target["chatId"], copied.message_id)] = {
            "ticketId": ticket_id,
            "botKey": ticket["botKey"],
        }


async def send_admin_ticket(bot, ticket_id: int, summary: str, user_tag: str, label: str, bot_key: str):
    text = (
        f"New {label} ticket #{ticket_id}\n{summary}\nCustomer: {user_tag}\n\n"
        "Reply to this message to chat with the customer."
    )
    for chat_id in ADMIN_CHAT_IDS:
        sent = await bot.send_message(
            chat_id, text, reply_markup=admin_ticket_keyboard(ticket_id)
        )
        ADMIN_TICKET_MESSAGES[(chat_id, sent.message_id)] = {
            "ticketId": ticket_id,
            "botKey": bot_key,
        }
        ticket = TICKETS.get(ticket_id)
        if ticket:
            ticket.setdefault("adminMessages", []).append(
                {"chatId": chat_id, "messageId": sent.message_id}
            )
            TICKETS[ticket_id] = ticket


async def close_ticket_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    if query.message.chat_id not in ADMIN_CHAT_IDS:
        await query.answer("Not authorized.")
        return
    ticket_id = int(query.data.split(":")[1])
    ticket = TICKETS.get(ticket_id)
    if not ticket or ticket.get("status") == "closed":
        await query.answer("Ticket already closed.")
        return
    ticket["status"] = "closed"
    TICKETS[ticket_id] = ticket
    CUSTOMER_TICKETS.pop(ticket["chatId"], None)
    close_ticket_record(ticket_id, {"closeType": "manual_close", "closedBy": update.effective_user.first_name})
    await query.answer("Ticket closed.")
    await query.message.reply_text(f"Ticket #{ticket_id} closed.")
    await context.bot.send_message(
        ticket["chatId"], f"Ticket #{ticket_id} has been closed by an admin."
    )


async def ban_ticket_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    if query.message.chat_id not in ADMIN_CHAT_IDS:
        await query.answer("Not authorized.")
        return
    ticket_id = int(query.data.split(":")[1])
    ticket = TICKETS.get(ticket_id)
    if not ticket:
        await query.answer("Ticket not found.")
        return
    BANNED_CHAT_IDS.add(ticket["chatId"])
    save_config()
    ticket["status"] = "closed"
    TICKETS[ticket_id] = ticket
    CUSTOMER_TICKETS.pop(ticket["chatId"], None)
    close_ticket_record(ticket_id, {"closeType": "banned", "closedBy": update.effective_user.first_name})
    await query.answer("Customer banned.")
    await query.message.reply_text(f"Ticket #{ticket_id} closed and customer banned.")
    await context.bot.send_message(
        ticket["chatId"], "🚫 <b>Access restricted.</b>", parse_mode=ParseMode.HTML
    )


async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in ADMIN_CHAT_IDS:
        return
    totals = summarize_report()
    report = (
        "📊 <b>Ticket Report</b>\n"
        f"Total created: <b>{totals['total']}</b>\n"
        f"Open: <b>{totals['open']}</b>\n"
        f"Closed w/ remarks: <b>{totals['closed_with_remarks']}</b>\n"
        f"Closed no order: <b>{totals['closed_no_order']}</b>\n"
        f"Banned: <b>{totals['banned']}</b>\n"
        f"Last ticket #: <b>{ticket_counter}</b>"
    )
    await update.message.reply_text(report, parse_mode=ParseMode.HTML)


async def close_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in ADMIN_CHAT_IDS:
        return
    parts = update.message.text.split(maxsplit=2)
    if len(parts) < 3:
        await update.message.reply_text('Usage: /close <ticket_id> "Profit/remarks"')
        return
    try:
        ticket_id = int(parts[1])
    except ValueError:
        await update.message.reply_text('Usage: /close <ticket_id> "Profit/remarks"')
        return
    remarks = parts[2].strip()
    if (remarks.startswith('"') and remarks.endswith('"')) or (
        remarks.startswith("'") and remarks.endswith("'")
    ):
        remarks = remarks[1:-1].strip()
    if not remarks:
        await update.message.reply_text('Usage: /close <ticket_id> "Profit/remarks"')
        return

    ticket = TICKETS.get(ticket_id)
    if ticket and ticket.get("status") == "closed":
        await update.message.reply_text("Ticket already closed.")
        return

    if not close_ticket_record(
        ticket_id,
        {"closeType": "admin_close", "remarks": remarks, "closedBy": update.effective_user.first_name},
    ):
        await update.message.reply_text("Ticket not found.")
        return

    chat_id = ticket["chatId"] if ticket else TICKET_RECORDS.get(ticket_id, {}).get("chatId")
    if ticket:
        ticket["status"] = "closed"
        TICKETS[ticket_id] = ticket
    if chat_id:
        CUSTOMER_TICKETS.pop(chat_id, None)
        await context.bot.send_message(chat_id, f"Ticket #{ticket_id} has been closed.")
    await update.message.reply_text(f"Ticket #{ticket_id} closed with remarks.")


async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in ADMIN_CHAT_IDS:
        return
    parts = update.message.text.split(maxsplit=1)
    if len(parts) != 2 or not parts[1].isdigit():
        await update.message.reply_text("Usage: /ban <chat_id>")
        return
    chat_id = int(parts[1])
    BANNED_CHAT_IDS.add(chat_id)
    close_tickets_for_chat(chat_id, "banned", update.effective_user.first_name or "Admin")
    save_config()
    await update.message.reply_text(f"Chat {chat_id} banned.")


async def unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in ADMIN_CHAT_IDS:
        return
    parts = update.message.text.split(maxsplit=1)
    if len(parts) != 2 or not parts[1].isdigit():
        await update.message.reply_text("Usage: /unban <chat_id>")
        return
    chat_id = int(parts[1])
    if chat_id not in BANNED_CHAT_IDS:
        await update.message.reply_text("Chat is not banned.")
        return
    BANNED_CHAT_IDS.discard(chat_id)
    save_config()
    await update.message.reply_text(f"Chat {chat_id} unbanned.")


def register_food_bot(application, session_store: SessionStore):
    application.bot_data["bot_key"] = "food"
    application.add_handler(TypeHandler(Update, ban_guard), group=0)
    application.add_handler(MessageHandler(filters.ALL, admin_reply_handler), group=1)

    application.add_handler(CommandHandler("start", food_start))
    application.add_handler(CommandHandler("help", food_help))
    application.add_handler(CommandHandler("cancel", food_cancel))
    application.add_handler(CommandHandler("report", report_command))
    application.add_handler(CommandHandler("close", close_command))
    application.add_handler(CommandHandler("ban", ban_command))
    application.add_handler(CommandHandler("unban", unban_command))

    application.add_handler(CallbackQueryHandler(menu_main, pattern="^menu:main$"))
    application.add_handler(CallbackQueryHandler(menu_food, pattern="^menu:food$"))
    application.add_handler(CallbackQueryHandler(food_category, pattern="^food:"))
    application.add_handler(CallbackQueryHandler(close_ticket_callback, pattern="^close:"))
    application.add_handler(CallbackQueryHandler(ban_ticket_callback, pattern="^ban:"))

    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, food_text))
    application.add_handler(MessageHandler(~filters.TEXT & ~filters.COMMAND, food_other))

    application.bot_data["session_store"] = session_store


def register_flight_bot(application, session_store: SessionStore):
    application.bot_data["bot_key"] = "flight"
    application.add_handler(TypeHandler(Update, ban_guard), group=0)
    application.add_handler(MessageHandler(filters.ALL, admin_reply_handler), group=1)

    application.add_handler(CommandHandler("start", flight_start))
    application.add_handler(CommandHandler("help", flight_help))
    application.add_handler(CommandHandler("cancel", flight_cancel))
    application.add_handler(CallbackQueryHandler(flight_start_action, pattern="^flight:start$"))
    application.add_handler(CallbackQueryHandler(close_ticket_callback, pattern="^close:"))
    application.add_handler(CallbackQueryHandler(ban_ticket_callback, pattern="^ban:"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, flight_text))
    application.add_handler(MessageHandler(~filters.TEXT & ~filters.COMMAND, flight_other))
    application.bot_data["session_store"] = session_store


def register_hotel_bot(application, session_store: SessionStore):
    application.bot_data["bot_key"] = "hotel"
    application.add_handler(TypeHandler(Update, ban_guard), group=0)
    application.add_handler(MessageHandler(filters.ALL, admin_reply_handler), group=1)

    application.add_handler(CommandHandler("start", hotel_start))
    application.add_handler(CommandHandler("help", hotel_help))
    application.add_handler(CommandHandler("cancel", hotel_cancel))
    application.add_handler(CallbackQueryHandler(hotel_start_action, pattern="^hotel:start$"))
    application.add_handler(CallbackQueryHandler(close_ticket_callback, pattern="^close:"))
    application.add_handler(CallbackQueryHandler(ban_ticket_callback, pattern="^ban:"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, hotel_text))
    application.add_handler(MessageHandler(~filters.TEXT & ~filters.COMMAND, hotel_other))
    application.bot_data["session_store"] = session_store


async def food_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.application.bot_data["session_store"].reset(update.effective_chat.id)
    await send_home(update, context, food_home(), main_menu())


async def food_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "ℹ️ <b>How it works</b>\nChoose a service and answer each question.\n"
        "Send /start to begin, /cancel to reset.",
        parse_mode=ParseMode.HTML,
    )


async def food_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.application.bot_data["session_store"].reset(update.effective_chat.id)
    await update.message.reply_text(
        "🛑 <b>Canceled.</b> Send /start when you're ready.", parse_mode=ParseMode.HTML
    )


async def menu_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.application.bot_data["session_store"].reset(update.effective_chat.id)
    await update.callback_query.answer()
    await update.callback_query.message.reply_text(
        "🍔 <b>Food menu</b>", parse_mode=ParseMode.HTML, reply_markup=main_menu()
    )


async def menu_food(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.application.bot_data["session_store"].reset(update.effective_chat.id)
    await update.callback_query.answer()
    await update.callback_query.message.reply_text(
        "🍔 <b>Choose a food category</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=food_menu(include_back=False),
    )


async def food_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    category_id = query.data.split(":")[1]
    category = next((item for item in FOOD_CATEGORIES if item["id"] == category_id), None)
    if not category:
        await query.answer("Category not found.")
        return
    session = {
        "service": "food",
        "stage": "food_questions",
        "stepIndex": 0,
        "answers": {},
        "foodCategory": category["label"],
    }
    context.application.bot_data["session_store"].set(query.message.chat_id, session)
    await query.message.reply_text(FOOD_PROMO, parse_mode=ParseMode.HTML)
    await query.message.reply_text(FOOD_QUESTIONS[0]["prompt"], parse_mode=ParseMode.HTML)


async def food_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id in ADMIN_CHAT_IDS:
        return
    session_store = context.application.bot_data["session_store"]
    session = session_store.get(update.effective_chat.id)
    if not session:
        open_ticket = get_open_ticket(update.effective_chat.id)
        if open_ticket:
            await forward_customer_message(
                context.bot, update.effective_chat.id, open_ticket[0], open_ticket[1], update.message
            )
            return
        await update.message.reply_text(START_PROMPT, parse_mode=ParseMode.HTML)
        return

    if session["stage"] == "food_questions":
        step = FOOD_QUESTIONS[session["stepIndex"]]
        session["answers"][step["key"]] = update.message.text.strip()
        if session["stepIndex"] < len(FOOD_QUESTIONS) - 1:
            session["stepIndex"] += 1
            session_store.set(update.effective_chat.id, session)
            await update.message.reply_text(
                FOOD_QUESTIONS[session["stepIndex"]]["prompt"], parse_mode=ParseMode.HTML
            )
            return
        session["stage"] = "food_continue"
        session_store.set(update.effective_chat.id, session)
        await update.message.reply_text(FOOD_CONTINUE_PROMPT, parse_mode=ParseMode.HTML)
        return

    if session["stage"] == "food_continue":
        if update.message.text.strip().lower() != "yes":
            await update.message.reply_text(FOOD_CONTINUE_PROMPT, parse_mode=ParseMode.HTML)
            return
        rate = check_rate_limit(update.effective_user.id)
        if rate.get("limited"):
            minutes = int((rate.get("retry_after", 0) / 60) + 1)
            session_store.reset(update.effective_chat.id)
            await update.message.reply_text(
                f"You're sending too many requests. Please try again in {minutes} minute(s)."
            )
            return
        ticket_id = next_ticket_id()
        TICKETS[ticket_id] = {
            "chatId": update.effective_chat.id,
            "category": session["foodCategory"],
            "answers": session["answers"],
            "status": "open",
            "adminMessages": [],
            "botKey": "food",
        }
        create_ticket_record(
            ticket_id,
            {"service": "Food", "category": session["foodCategory"], "chatId": update.effective_chat.id, "botKey": "food"},
        )
        CUSTOMER_TICKETS[update.effective_chat.id] = ticket_id
        summary = "\n".join(
            [
                f"Category: {session['foodCategory']}",
                f"Name: {session['answers'].get('name', '-')}",
                f"Address: {session['answers'].get('address', '-')}",
                f"Phone: {session['answers'].get('phone', '-')}",
            ]
        )
        user_tag = f"@{update.effective_user.username}" if update.effective_user.username else f"ID {update.effective_user.id}"
        await send_admin_ticket(context.bot, ticket_id, summary, user_tag, "food order", "food")
        session_store.reset(update.effective_chat.id)
        await update.message.reply_text(
            "🕘 You're being connected over to our workers! This could take a few moments..."
        )
        await update.message.reply_text(
            f"✅ Your ticket has been created, and you're now connected with our workers! This is ticket #{ticket_id}"
        )
        return

    await update.message.reply_text(START_PROMPT, parse_mode=ParseMode.HTML)


async def food_other(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id in ADMIN_CHAT_IDS:
        return
    open_ticket = get_open_ticket(update.effective_chat.id)
    session_store = context.application.bot_data["session_store"]
    if open_ticket and not session_store.get(update.effective_chat.id):
        await forward_customer_message(
            context.bot, update.effective_chat.id, open_ticket[0], open_ticket[1], update.message
        )
        return
    await update.message.reply_text(
        "✍️ <b>Please send text details</b> or use /start.",
        parse_mode=ParseMode.HTML,
    )


async def flight_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.application.bot_data["session_store"].reset(update.effective_chat.id)
    await send_home(update, context, flight_home(), flight_start_menu())


async def flight_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "ℹ️ <b>How it works</b>\nAnswer a few questions and we will connect you with an agent.",
        parse_mode=ParseMode.HTML,
    )


async def flight_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.application.bot_data["session_store"].reset(update.effective_chat.id)
    await update.message.reply_text(
        "🛑 <b>Canceled.</b> Send /start when you're ready.", parse_mode=ParseMode.HTML
    )


async def flight_start_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    session = {"service": "flight", "stage": "flight_questions", "stepIndex": 0, "answers": {}}
    context.application.bot_data["session_store"].set(update.effective_chat.id, session)
    await update.callback_query.message.reply_text(FLIGHT_PROMO, parse_mode=ParseMode.HTML)
    await update.callback_query.message.reply_text(FLIGHT_QUESTIONS[0]["prompt"], parse_mode=ParseMode.HTML)


async def flight_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id in ADMIN_CHAT_IDS:
        return
    session_store = context.application.bot_data["session_store"]
    session = session_store.get(update.effective_chat.id)
    if not session:
        open_ticket = get_open_ticket(update.effective_chat.id)
        if open_ticket:
            await forward_customer_message(
                context.bot, update.effective_chat.id, open_ticket[0], open_ticket[1], update.message
            )
            return
        await update.message.reply_text(FLIGHT_START_PROMPT, parse_mode=ParseMode.HTML)
        return

    if session["stage"] == "flight_questions":
        step = FLIGHT_QUESTIONS[session["stepIndex"]]
        session["answers"][step["key"]] = update.message.text.strip()
        if session["stepIndex"] < len(FLIGHT_QUESTIONS) - 1:
            session["stepIndex"] += 1
            session_store.set(update.effective_chat.id, session)
            await update.message.reply_text(
                FLIGHT_QUESTIONS[session["stepIndex"]]["prompt"], parse_mode=ParseMode.HTML
            )
            return
        session["stage"] = "flight_continue"
        session_store.set(update.effective_chat.id, session)
        await update.message.reply_text(FLIGHT_CONTINUE_PROMPT, parse_mode=ParseMode.HTML)
        return

    if session["stage"] == "flight_continue":
        if update.message.text.strip().lower() != "yes":
            await update.message.reply_text(FLIGHT_CONTINUE_PROMPT, parse_mode=ParseMode.HTML)
            return
        rate = check_rate_limit(update.effective_user.id)
        if rate.get("limited"):
            minutes = int((rate.get("retry_after", 0) / 60) + 1)
            session_store.reset(update.effective_chat.id)
            await update.message.reply_text(
                f"You're sending too many requests. Please try again in {minutes} minute(s)."
            )
            return
        ticket_id = next_ticket_id()
        TICKETS[ticket_id] = {
            "chatId": update.effective_chat.id,
            "category": "Flights",
            "answers": session["answers"],
            "status": "open",
            "adminMessages": [],
            "botKey": "flight",
        }
        create_ticket_record(
            ticket_id,
            {"service": "Flights", "category": "Flights", "chatId": update.effective_chat.id, "botKey": "flight"},
        )
        CUSTOMER_TICKETS[update.effective_chat.id] = ticket_id
        summary = "\n".join(
            [
                f"Trip Dates: {session['answers'].get('trip_dates', '-')}",
                f"Passenger Info: {session['answers'].get('passenger_form', '-')}",
                f"State: {session['answers'].get('residence', '-')}",
                f"Total Value: {session['answers'].get('order_total', '-')}",
                f"Airlines: {session['answers'].get('airlines', '-')}",
            ]
        )
        user_tag = f"@{update.effective_user.username}" if update.effective_user.username else f"ID {update.effective_user.id}"
        await send_admin_ticket(context.bot, ticket_id, summary, user_tag, "flight", "flight")
        session_store.reset(update.effective_chat.id)
        await update.message.reply_text(
            "🕘 You're being connected over to our workers! This could take a few moments..."
        )
        await update.message.reply_text(
            f"✅ Your ticket has been created, and you're now connected with our workers! This is ticket #{ticket_id}"
        )
        return

    await update.message.reply_text(FLIGHT_START_PROMPT, parse_mode=ParseMode.HTML)


async def flight_other(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id in ADMIN_CHAT_IDS:
        return
    open_ticket = get_open_ticket(update.effective_chat.id)
    session_store = context.application.bot_data["session_store"]
    if open_ticket and not session_store.get(update.effective_chat.id):
        await forward_customer_message(
            context.bot, update.effective_chat.id, open_ticket[0], open_ticket[1], update.message
        )
        return
    await update.message.reply_text(
        "✍️ <b>Please send text details</b> or use /start.",
        parse_mode=ParseMode.HTML,
    )


async def hotel_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.application.bot_data["session_store"].reset(update.effective_chat.id)
    await send_home(update, context, hotel_home(), hotel_start_menu())


async def hotel_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "ℹ️ <b>How it works</b>\nShare your trip details and we will connect you with an agent.",
        parse_mode=ParseMode.HTML,
    )


async def hotel_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.application.bot_data["session_store"].reset(update.effective_chat.id)
    await update.message.reply_text(
        "🛑 <b>Canceled.</b> Send /start when you're ready.", parse_mode=ParseMode.HTML
    )


async def hotel_start_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    session = {"service": "hotel", "stage": "hotel_questions", "stepIndex": 0, "answers": {}}
    context.application.bot_data["session_store"].set(update.effective_chat.id, session)
    await update.callback_query.message.reply_text(HOTEL_PROMO, parse_mode=ParseMode.HTML)
    await update.callback_query.message.reply_text(HOTEL_QUESTIONS[0]["prompt"], parse_mode=ParseMode.HTML)


async def hotel_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id in ADMIN_CHAT_IDS:
        return
    session_store = context.application.bot_data["session_store"]
    session = session_store.get(update.effective_chat.id)
    if not session:
        open_ticket = get_open_ticket(update.effective_chat.id)
        if open_ticket:
            await forward_customer_message(
                context.bot, update.effective_chat.id, open_ticket[0], open_ticket[1], update.message
            )
            return
        await update.message.reply_text(HOTEL_START_PROMPT, parse_mode=ParseMode.HTML)
        return

    if session["stage"] == "hotel_questions":
        step = HOTEL_QUESTIONS[session["stepIndex"]]
        session["answers"][step["key"]] = update.message.text.strip()
        if session["stepIndex"] < len(HOTEL_QUESTIONS) - 1:
            session["stepIndex"] += 1
            session_store.set(update.effective_chat.id, session)
            await update.message.reply_text(
                HOTEL_QUESTIONS[session["stepIndex"]]["prompt"], parse_mode=ParseMode.HTML
            )
            return
        session["stage"] = "hotel_continue"
        session_store.set(update.effective_chat.id, session)
        await update.message.reply_text(HOTEL_CONTINUE_PROMPT, parse_mode=ParseMode.HTML)
        return

    if session["stage"] == "hotel_continue":
        if update.message.text.strip().lower() != "yes":
            await update.message.reply_text(HOTEL_CONTINUE_PROMPT, parse_mode=ParseMode.HTML)
            return
        rate = check_rate_limit(update.effective_user.id)
        if rate.get("limited"):
            minutes = int((rate.get("retry_after", 0) / 60) + 1)
            session_store.reset(update.effective_chat.id)
            await update.message.reply_text(
                f"You're sending too many requests. Please try again in {minutes} minute(s)."
            )
            return
        ticket_id = next_ticket_id()
        TICKETS[ticket_id] = {
            "chatId": update.effective_chat.id,
            "category": "Hotels",
            "answers": session["answers"],
            "status": "open",
            "adminMessages": [],
            "botKey": "hotel",
        }
        create_ticket_record(
            ticket_id,
            {"service": "Hotels", "category": "Hotels", "chatId": update.effective_chat.id, "botKey": "hotel"},
        )
        CUSTOMER_TICKETS[update.effective_chat.id] = ticket_id
        summary = "\n".join(
            [
                f"Destination: {session['answers'].get('destination', '-')}",
                f"Dates: {session['answers'].get('dates', '-')}",
                f"Budget: {session['answers'].get('budget', '-')}",
                f"Email: {session['answers'].get('email', '-')}",
                f"Booking.com: {session['answers'].get('booking_link', '-')}",
                f"Preferred Chain: {session['answers'].get('preferred_chain', '-')}",
            ]
        )
        user_tag = f"@{update.effective_user.username}" if update.effective_user.username else f"ID {update.effective_user.id}"
        await send_admin_ticket(context.bot, ticket_id, summary, user_tag, "hotel", "hotel")
        session_store.reset(update.effective_chat.id)
        await update.message.reply_text(
            "🕘 You're being connected over to our workers! This could take a few moments..."
        )
        await update.message.reply_text(
            f"✅ Your ticket has been created, and you're now connected with our workers! This is ticket #{ticket_id}"
        )
        return

    await update.message.reply_text(HOTEL_START_PROMPT, parse_mode=ParseMode.HTML)


async def hotel_other(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id in ADMIN_CHAT_IDS:
        return
    open_ticket = get_open_ticket(update.effective_chat.id)
    session_store = context.application.bot_data["session_store"]
    if open_ticket and not session_store.get(update.effective_chat.id):
        await forward_customer_message(
            context.bot, update.effective_chat.id, open_ticket[0], open_ticket[1], update.message
        )
        return
    await update.message.reply_text(
        "✍️ <b>Please send text details</b> or use /start.",
        parse_mode=ParseMode.HTML,
    )


async def main():
    bots = []

    food_app = ApplicationBuilder().token(FOOD_TOKEN).build()
    register_food_bot(food_app, SessionStore(food_app.bot))
    bots.append(("food", food_app))

    if FLIGHT_TOKEN:
        flight_app = ApplicationBuilder().token(FLIGHT_TOKEN).build()
        register_flight_bot(flight_app, SessionStore(flight_app.bot))
        bots.append(("flight", flight_app))

    if HOTEL_TOKEN:
        hotel_app = ApplicationBuilder().token(HOTEL_TOKEN).build()
        register_hotel_bot(hotel_app, SessionStore(hotel_app.bot))
        bots.append(("hotel", hotel_app))

    async def start_app(name, app):
        try:
            await app.initialize()
            await app.start()
            await app.updater.start_polling()
            print(f"[{name}] bot launched")
        except Exception as exc:
            print(f"[{name}] bot launch failed: {exc}")
            await asyncio.sleep(5)
            await start_app(name, app)

    await asyncio.gather(*(start_app(name, app) for name, app in bots))
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
