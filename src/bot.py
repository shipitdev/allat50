import asyncio
import html
import json
import logging
import os
import re
import time
from pathlib import Path

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

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger("foodbot")

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.json"
LOGO_PATH = Path(__file__).resolve().parent.parent / "allat50.png"

if not CONFIG_PATH.exists():
    raise SystemExit("Missing config.json.")

try:
    with CONFIG_PATH.open("r", encoding="utf-8") as handle:
        CONFIG = json.load(handle)
except json.JSONDecodeError:
    raise SystemExit("Invalid config.json (JSON parse failed).")

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

raw_duff_ids = CONFIG.get("duffChatIds")
if not isinstance(raw_duff_ids, list):
    duff_single = CONFIG.get("duffChatId")
    raw_duff_ids = [duff_single] if duff_single is not None else []
DUFF_CHAT_IDS = set()
for item in raw_duff_ids:
    try:
        DUFF_CHAT_IDS.add(int(item))
    except (TypeError, ValueError):
        continue

ADMIN_ALIASES = {}
alias_map = CONFIG.get("adminAliases")
if isinstance(alias_map, dict):
    for key, value in alias_map.items():
        try:
            admin_id = int(key)
        except (TypeError, ValueError):
            continue
        if isinstance(value, str) and value.strip():
            ADMIN_ALIASES[admin_id] = value.strip()

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

try:
    ticket_counter = int(CONFIG.get("ticketCounter", 60))
except (TypeError, ValueError):
    ticket_counter = 60

try:
    session_timeout_minutes = float(CONFIG.get("sessionTimeoutMinutes", 15))
except (TypeError, ValueError):
    session_timeout_minutes = 15
SESSION_TIMEOUT_SECONDS = max(session_timeout_minutes, 0) * 60

rate_limit = CONFIG.get("rateLimit") or {}
try:
    rate_limit_window = float(rate_limit.get("windowMinutes", 30))
except (TypeError, ValueError):
    rate_limit_window = 30
try:
    rate_limit_max = int(rate_limit.get("maxTickets", 2))
except (TypeError, ValueError):
    rate_limit_max = 2
RATE_LIMIT_SECONDS = max(rate_limit_window, 0) * 60
RATE_LIMIT_ENABLED = RATE_LIMIT_SECONDS > 0 and rate_limit_max > 0
DUFF_CUT_RATE = 0.25

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
FOOD_CATEGORY_MAP = {item["id"]: item["label"] for item in FOOD_CATEGORIES}
FOOD_MENU_ROWS = [
    ["fast_food"],
    ["meal_kits"],
    ["sonic_combo"],
    ["ihop_dennys", "panera"],
    ["wingstop", "panda"],
    ["five_guys", "pizza"],
    ["chipotle", "cava"],
    ["shake_shack", "canes"],
    ["restaurants", "dine_in"],
    ["ubereats", "doordash"],
    ["grubhub"],
    ["groceries"],
    ["movies", "uber_rides"],
]

TICKETS = {}
CUSTOMER_TICKETS = {}
ADMIN_TICKET_MESSAGES = {}
TICKET_HISTORY = {}
DUFF_REQUEST_MESSAGES = {}


class SessionStore:
    def __init__(self, bot):
        self.bot = bot
        self.sessions = {}

    def get(self, chat_id: int):
        return self.sessions.get(chat_id)

    def reset(self, chat_id: int):
        session = self.sessions.pop(chat_id, None)
        if session and session.get("timeout_task"):
            session["timeout_task"].cancel()

    def set(self, chat_id: int, session: dict):
        self.reset(chat_id)
        if SESSION_TIMEOUT_SECONDS > 0:
            last_active = time.time()
            session["last_active"] = last_active
            session["timeout_task"] = asyncio.create_task(
                self._timeout(chat_id, last_active)
            )
        self.sessions[chat_id] = session

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


def normalize_username(value: str) -> str:
    return value.replace("@", "") if value else ""


def is_admin(chat_id: int) -> bool:
    return chat_id in ADMIN_CHAT_IDS


def is_duff(chat_id: int) -> bool:
    return chat_id in DUFF_CHAT_IDS


def admin_alias(chat_id: int, fallback: str) -> str:
    return ADMIN_ALIASES.get(chat_id) or fallback or "Worker"


def format_hybrid_link(label: str, url: str) -> str:
    return f'<a href="{url}">| {label} |</a>'


def format_bot_link(label: str, username: str):
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
    return f"\n\n<b>Quick links</b>\n{first_line}{'\n' + second_line if second_line else ''}"


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


def food_menu(include_back: bool = True) -> InlineKeyboardMarkup:
    rows = []
    for row in FOOD_MENU_ROWS:
        rows.append(
            [
                InlineKeyboardButton(
                    FOOD_CATEGORY_MAP[item], callback_data=f"food:{item}"
                )
                for item in row
            ]
        )
    if include_back:
        rows.append(
            [InlineKeyboardButton("⬅️ Back to main menu", callback_data="menu:main")]
        )
    return InlineKeyboardMarkup(rows)


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


def admin_ticket_keyboard(ticket_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🛑 Close ticket", callback_data=f"close:{ticket_id}"),
                InlineKeyboardButton("🚫 Ban customer", callback_data=f"ban:{ticket_id}"),
            ],
            [InlineKeyboardButton("🎟️ Request coupon", callback_data=f"coupon:{ticket_id}")],
        ]
    )


def save_config() -> None:
    CONFIG["adminChatIds"] = ADMIN_CHAT_IDS
    CONFIG["duffChatIds"] = list(DUFF_CHAT_IDS)
    CONFIG["adminAliases"] = {str(k): v for k, v in ADMIN_ALIASES.items()}
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


def update_ticket_record(ticket_id: int, updates: dict) -> bool:
    record = TICKET_RECORDS.get(ticket_id)
    if not record:
        return False
    record.update(updates)
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
        "profit_total": 0.0,
        "duff_total": 0.0,
    }
    for record in TICKET_RECORDS.values():
        totals["total"] += 1
        if record.get("status") == "open":
            totals["open"] += 1
            continue
        if record.get("closeType") == "admin_close":
            totals["closed_with_remarks"] += 1
            profit_value = float(record.get("profit") or 0)
            totals["profit_total"] += profit_value
            totals["duff_total"] += float(record.get("duffCut") or profit_value * DUFF_CUT_RATE)
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


def service_summary(service_key: str, session: dict) -> str:
    if service_key == "food":
        return "\n".join(
            [
                f"Category: {session.get('category', '-')}",
                f"Name: {session['answers'].get('name', '-')}",
                f"Address: {session['answers'].get('address', '-')}",
                f"Phone: {session['answers'].get('phone', '-')}",
            ]
        )
    if service_key == "flight":
        return "\n".join(
            [
                f"Trip Dates: {session['answers'].get('trip_dates', '-')}",
                f"Passenger Info: {session['answers'].get('passenger_form', '-')}",
                f"State: {session['answers'].get('residence', '-')}",
                f"Total Value: {session['answers'].get('order_total', '-')}",
                f"Airlines: {session['answers'].get('airlines', '-')}",
            ]
        )
    return "\n".join(
        [
            f"Destination: {session['answers'].get('destination', '-')}",
            f"Dates: {session['answers'].get('dates', '-')}",
            f"Budget: {session['answers'].get('budget', '-')}",
            f"Email: {session['answers'].get('email', '-')}",
            f"Booking.com: {session['answers'].get('booking_link', '-')}",
            f"Preferred Chain: {session['answers'].get('preferred_chain', '-')}",
        ]
    )


def ticket_summary_from_record(ticket: dict) -> str:
    session = {"answers": ticket.get("answers", {}), "category": ticket.get("category", "-")}
    service_key = ticket.get("botKey", "food")
    if ticket.get("service") == "Flights":
        service_key = "flight"
    elif ticket.get("service") == "Hotels":
        service_key = "hotel"
    return service_summary(service_key, session)


def format_ticket_line(ticket_id: int, ticket: dict) -> str:
    service_label = html.escape(str(ticket.get("service") or ticket.get("category") or "Order"))
    assigned = html.escape(str(ticket.get("assignedAlias") or "Unassigned"))
    coupon_state = "Coupon requested" if ticket.get("couponRequested") else "No coupon"
    return f"#{ticket_id} {service_label} · {assigned} · {coupon_state}"


def format_closed_line(ticket_id: int, record: dict) -> str:
    profit = float(record.get("profit") or 0)
    duff_cut = float(record.get("duffCut") or profit * DUFF_CUT_RATE)
    closed_by = html.escape(str(record.get("closedBy") or "-"))
    return f"#{ticket_id} Profit: ${profit:.2f} · Duff: ${duff_cut:.2f} · {closed_by}"


SERVICE_CONFIG = {
    "food": {
        "promo": FOOD_PROMO,
        "questions": FOOD_QUESTIONS,
        "continue_prompt": FOOD_CONTINUE_PROMPT,
        "start_prompt": START_PROMPT,
        "ticket_service": "Food",
        "ticket_label": "food order",
    },
    "flight": {
        "promo": FLIGHT_PROMO,
        "questions": FLIGHT_QUESTIONS,
        "continue_prompt": FLIGHT_CONTINUE_PROMPT,
        "start_prompt": FLIGHT_START_PROMPT,
        "ticket_service": "Flights",
        "ticket_label": "flight",
    },
    "hotel": {
        "promo": HOTEL_PROMO,
        "questions": HOTEL_QUESTIONS,
        "continue_prompt": HOTEL_CONTINUE_PROMPT,
        "start_prompt": HOTEL_START_PROMPT,
        "ticket_service": "Hotels",
        "ticket_label": "hotel",
    },
}

HOME_TEXT = {"food": food_home, "flight": flight_home, "hotel": hotel_home}
HOME_MENU = {"food": main_menu, "flight": flight_start_menu, "hotel": hotel_start_menu}
HELP_TEXT = {
    "food": "ℹ️ <b>How it works</b>\nChoose a service and answer each question.\n"
    "Send /start to begin, /cancel to reset.",
    "flight": "ℹ️ <b>How it works</b>\nAnswer a few questions and we will connect you with an agent.",
    "hotel": "ℹ️ <b>How it works</b>\nShare your trip details and we will connect you with an agent.",
}


async def send_home(update: Update, context: ContextTypes.DEFAULT_TYPE, caption: str, keyboard):
    if not update.message:
        return
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
    if chat.id in ADMIN_CHAT_IDS or chat.id in DUFF_CHAT_IDS:
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
    if not chat or not is_admin(chat.id):
        return
    message = update.effective_message
    if not message or not message.reply_to_message:
        return
    entry = ADMIN_TICKET_MESSAGES.get((chat.id, message.reply_to_message.message_id))
    if not entry or entry["botKey"] != context.application.bot_data.get("bot_key"):
        return
    ticket = TICKETS.get(entry["ticketId"])
    if not ticket or ticket.get("status") != "open":
        await message.reply_text("Ticket is closed or no longer exists.")
        raise ApplicationHandlerStop()

    admin_label = admin_alias(chat.id, update.effective_user.first_name or "Admin")
    if message.text:
        await context.bot.send_message(ticket["chatId"], f"{admin_label}: {message.text}")
    else:
        await context.bot.send_message(ticket["chatId"], f"{admin_label} sent:")
        await context.bot.copy_message(ticket["chatId"], chat.id, message.message_id)
    raise ApplicationHandlerStop()


async def forward_customer_message(bot, chat_id: int, ticket_id: int, ticket: dict, message):
    customer_label = message.from_user.first_name or "Customer"
    header = f"Customer ({customer_label}) on ticket #{ticket_id}:"
    targets = ticket.get("adminMessages") or [
        {"chatId": admin_id} for admin_id in ADMIN_CHAT_IDS
    ]

    for target in targets:
        reply_id = target.get("messageId")
        try:
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
                target["chatId"], chat_id, message.message_id, reply_to_message_id=reply_id
            )
            ADMIN_TICKET_MESSAGES[(target["chatId"], copied.message_id)] = {
                "ticketId": ticket_id,
                "botKey": ticket["botKey"],
            }
        except Exception:
            continue


async def send_admin_ticket(bot, ticket_id: int, summary: str, user_tag: str, label: str, bot_key: str):
    text = (
        f"New {label} ticket #{ticket_id}\n{summary}\nCustomer: {user_tag}\n\n"
        "Reply to this message to chat with the customer."
    )
    for chat_id in ADMIN_CHAT_IDS:
        try:
            sent = await bot.send_message(
                chat_id, text, reply_markup=admin_ticket_keyboard(ticket_id)
            )
        except Exception:
            continue
        ADMIN_TICKET_MESSAGES[(chat_id, sent.message_id)] = {
            "ticketId": ticket_id,
            "botKey": bot_key,
        }
        ticket = TICKETS.get(ticket_id)
        if ticket is not None:
            ticket.setdefault("adminMessages", []).append(
                {"chatId": chat_id, "messageId": sent.message_id}
            )
            TICKETS[ticket_id] = ticket


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    store = context.application.bot_data["store"]
    store.reset(update.effective_chat.id)
    key = context.application.bot_data["bot_key"]
    await send_home(update, context, HOME_TEXT[key](), HOME_MENU[key]())


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    key = context.application.bot_data["bot_key"]
    await update.message.reply_text(HELP_TEXT[key], parse_mode=ParseMode.HTML)


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.application.bot_data["store"].reset(update.effective_chat.id)
    await update.message.reply_text(
        "🛑 <b>Canceled.</b> Send /start when you're ready.", parse_mode=ParseMode.HTML
    )


async def menu_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.application.bot_data["store"].reset(update.effective_chat.id)
    await update.callback_query.answer()
    await update.callback_query.message.reply_text(
        "🍔 <b>Food menu</b>", parse_mode=ParseMode.HTML, reply_markup=main_menu()
    )


async def menu_food(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.application.bot_data["store"].reset(update.effective_chat.id)
    await update.callback_query.answer()
    await update.callback_query.message.reply_text(
        "🍔 <b>Choose a food category</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=food_menu(include_back=False),
    )


async def food_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    category_id = query.data.split(":", 1)[1]
    label = FOOD_CATEGORY_MAP.get(category_id)
    if not label:
        await query.answer("Category not found.")
        return
    session = {
        "service": "food",
        "stage": "questions",
        "step": 0,
        "answers": {},
        "category": label,
    }
    context.application.bot_data["store"].set(query.message.chat_id, session)
    await query.message.reply_text(FOOD_PROMO, parse_mode=ParseMode.HTML)
    await query.message.reply_text(FOOD_QUESTIONS[0]["prompt"], parse_mode=ParseMode.HTML)


async def start_flow_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    service_key = query.data.split(":", 1)[0]
    if service_key not in ("flight", "hotel"):
        return
    session = {
        "service": service_key,
        "stage": "questions",
        "step": 0,
        "answers": {},
        "category": "Flights" if service_key == "flight" else "Hotels",
    }
    context.application.bot_data["store"].set(query.message.chat_id, session)
    await query.message.reply_text(SERVICE_CONFIG[service_key]["promo"], parse_mode=ParseMode.HTML)
    await query.message.reply_text(
        SERVICE_CONFIG[service_key]["questions"][0]["prompt"], parse_mode=ParseMode.HTML
    )


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_admin(update.effective_chat.id):
        return
    key = context.application.bot_data["bot_key"]
    config = SERVICE_CONFIG[key]
    store = context.application.bot_data["store"]
    session = store.get(update.effective_chat.id)

    if not session:
        open_ticket = get_open_ticket(update.effective_chat.id)
        if open_ticket:
            await forward_customer_message(
                context.bot,
                update.effective_chat.id,
                open_ticket[0],
                open_ticket[1],
                update.message,
            )
            return
        await update.message.reply_text(config["start_prompt"], parse_mode=ParseMode.HTML)
        return

    if session.get("stage") == "questions":
        questions = config["questions"]
        step = questions[session["step"]]
        session["answers"][step["key"]] = update.message.text.strip()
        if session["step"] < len(questions) - 1:
            session["step"] += 1
            store.set(update.effective_chat.id, session)
            await update.message.reply_text(
                questions[session["step"]]["prompt"], parse_mode=ParseMode.HTML
            )
            return
        session["stage"] = "continue"
        store.set(update.effective_chat.id, session)
        await update.message.reply_text(config["continue_prompt"], parse_mode=ParseMode.HTML)
        return

    if session.get("stage") == "continue":
        if update.message.text.strip().lower() != "yes":
            await update.message.reply_text(config["continue_prompt"], parse_mode=ParseMode.HTML)
            return
        rate = check_rate_limit(update.effective_user.id)
        if rate.get("limited"):
            minutes = int((rate.get("retry_after", 0) / 60) + 1)
            store.reset(update.effective_chat.id)
            await update.message.reply_text(
                f"You're sending too many requests. Please try again in {minutes} minute(s)."
            )
            return

        ticket_id = next_ticket_id()
        bot_key = context.application.bot_data["bot_key"]
        category = session.get("category", "-")
        TICKETS[ticket_id] = {
            "chatId": update.effective_chat.id,
            "category": category,
            "answers": session["answers"],
            "status": "open",
            "adminMessages": [],
            "botKey": bot_key,
            "service": config["ticket_service"],
        }
        create_ticket_record(
            ticket_id,
            {
                "service": config["ticket_service"],
                "category": category,
                "chatId": update.effective_chat.id,
                "botKey": bot_key,
            },
        )
        CUSTOMER_TICKETS[update.effective_chat.id] = ticket_id

        summary = service_summary(key, session)
        user_tag = (
            f"@{update.effective_user.username}"
            if update.effective_user.username
            else f"ID {update.effective_user.id}"
        )
        await send_admin_ticket(
            context.bot, ticket_id, summary, user_tag, config["ticket_label"], bot_key
        )

        store.reset(update.effective_chat.id)
        await update.message.reply_text(
            "🕘 You're being connected over to our workers! This could take a few moments..."
        )
        await update.message.reply_text(
            f"✅ Your ticket has been created, and you're now connected with our workers! This is ticket #{ticket_id}"
        )
        return

    await update.message.reply_text(config["start_prompt"], parse_mode=ParseMode.HTML)


async def other_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_admin(update.effective_chat.id):
        return
    store = context.application.bot_data["store"]
    open_ticket = get_open_ticket(update.effective_chat.id)
    if open_ticket and not store.get(update.effective_chat.id):
        await forward_customer_message(
            context.bot,
            update.effective_chat.id,
            open_ticket[0],
            open_ticket[1],
            update.message,
        )
        return
    await update.message.reply_text(
        "✍️ <b>Please send text details</b> or use /start.", parse_mode=ParseMode.HTML
    )


async def close_ticket_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    if not is_admin(query.message.chat_id):
        await query.answer("Not authorized.")
        return
    ticket_id = int(query.data.split(":", 1)[1])
    ticket = TICKETS.get(ticket_id)
    if not ticket or ticket.get("status") == "closed":
        await query.answer("Ticket already closed.")
        return
    ticket["status"] = "closed"
    TICKETS[ticket_id] = ticket
    CUSTOMER_TICKETS.pop(ticket["chatId"], None)
    close_ticket_record(
        ticket_id, {"closeType": "manual_close", "closedBy": update.effective_user.first_name}
    )
    await query.answer("Ticket closed.")
    await query.message.reply_text(f"Ticket #{ticket_id} closed.")
    await context.bot.send_message(
        ticket["chatId"], f"Ticket #{ticket_id} has been closed by an admin."
    )


async def ban_ticket_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    if not is_admin(query.message.chat_id):
        await query.answer("Not authorized.")
        return
    ticket_id = int(query.data.split(":", 1)[1])
    ticket = TICKETS.get(ticket_id)
    if not ticket:
        await query.answer("Ticket not found.")
        return
    BANNED_CHAT_IDS.add(ticket["chatId"])
    save_config()
    ticket["status"] = "closed"
    TICKETS[ticket_id] = ticket
    CUSTOMER_TICKETS.pop(ticket["chatId"], None)
    close_ticket_record(
        ticket_id, {"closeType": "banned", "closedBy": update.effective_user.first_name}
    )
    await query.answer("Customer banned.")
    await query.message.reply_text(f"Ticket #{ticket_id} closed and customer banned.")
    await context.bot.send_message(
        ticket["chatId"], "🚫 <b>Access restricted.</b>", parse_mode=ParseMode.HTML
    )


async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_chat.id):
        return
    totals = summarize_report()
    report = (
        "📊 <b>Ticket Report</b>\n"
        f"Total created: <b>{totals['total']}</b>\n"
        f"Open: <b>{totals['open']}</b>\n"
        f"Closed w/ remarks: <b>{totals['closed_with_remarks']}</b>\n"
        f"Closed no order: <b>{totals['closed_no_order']}</b>\n"
        f"Banned: <b>{totals['banned']}</b>\n"
        f"Profit total: <b>${totals['profit_total']:.2f}</b>\n"
        f"Duff 25%: <b>${totals['duff_total']:.2f}</b>\n"
        f"Last ticket #: <b>{ticket_counter}</b>"
    )
    await update.message.reply_text(report, parse_mode=ParseMode.HTML)


def parse_close_payload(text: str):
    match = re.match(r"^/close\s+(\d+)\s+([\s\S]+)$", text.strip())
    if not match:
        return None
    ticket_id = int(match.group(1))
    rest = match.group(2).strip()
    profit = 0.0
    remarks = rest
    if (rest.startswith('"') and rest.endswith('"')) or (rest.startswith("'") and rest.endswith("'")):
        remarks = rest[1:-1].strip()
    else:
        token_match = re.match(r"^([+-]?\d+(?:\.\d+)?)\s*(.*)$", rest)
        if token_match:
            profit = float(token_match.group(1))
            remarks = token_match.group(2).strip() or f"Profit recorded: ${profit:.2f}"
    return ticket_id, profit, remarks


async def close_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_chat.id):
        return
    parsed = parse_close_payload(update.message.text)
    if not parsed:
        await update.message.reply_text('Usage: /close <ticket_id> <profit> "remarks"')
        return
    ticket_id, profit, remarks = parsed

    ticket = TICKETS.get(ticket_id)
    if ticket and ticket.get("status") == "closed":
        await update.message.reply_text("Ticket already closed.")
        return

    duff_cut = round(profit * DUFF_CUT_RATE, 2)
    if not close_ticket_record(
        ticket_id,
        {
            "closeType": "admin_close",
            "remarks": remarks,
            "closedBy": admin_alias(update.effective_chat.id, update.effective_user.first_name or "Admin"),
            "profit": profit,
            "duffCut": duff_cut,
        },
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
    await update.message.reply_text(
        f"Ticket #{ticket_id} closed. Profit: ${profit:.2f} · Duff: ${duff_cut:.2f}"
    )

    if DUFF_CHAT_IDS:
        for duff_id in DUFF_CHAT_IDS:
            try:
                await context.bot.send_message(
                    duff_id,
                    f"✅ Ticket #{ticket_id} closed.\nProfit: ${profit:.2f}\nDuff 25%: ${duff_cut:.2f}",
                )
            except Exception:
                continue


async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_chat.id):
        return
    parts = update.message.text.strip().split(maxsplit=1)
    if len(parts) != 2 or not parts[1].isdigit():
        await update.message.reply_text("Usage: /ban <chat_id>")
        return
    chat_id = int(parts[1])
    BANNED_CHAT_IDS.add(chat_id)
    close_tickets_for_chat(chat_id, "banned", update.effective_user.first_name or "Admin")
    save_config()
    await update.message.reply_text(f"Chat {chat_id} banned.")


async def unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_chat.id):
        return
    parts = update.message.text.strip().split(maxsplit=1)
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


async def work_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_chat.id):
        return
    alias = admin_alias(update.effective_chat.id, update.effective_user.first_name or "Worker")
    alias_display = html.escape(alias)
    open_tickets = [
        (ticket_id, ticket)
        for ticket_id, ticket in TICKETS.items()
        if ticket.get("status") == "open"
    ]
    assigned = [
        format_ticket_line(ticket_id, ticket)
        for ticket_id, ticket in open_tickets
        if ticket.get("assignedAdminId") == update.effective_chat.id
    ]
    unassigned = [
        format_ticket_line(ticket_id, ticket)
        for ticket_id, ticket in open_tickets
        if not ticket.get("assignedAdminId")
    ]
    lines = [
        "👷 <b>Worker Panel</b>",
        f"Alias: <b>{alias_display}</b>",
        "",
        "Commands:",
        "• <code>/setname &lt;alias&gt;</code>",
        "• <code>/accept &lt;ticket_id&gt;</code>",
        "• <code>/close &lt;ticket_id&gt; &lt;profit&gt; \"remarks\"</code>",
        "• Use <b>Request coupon</b> on a ticket to ping Duff",
        "",
    ]
    if assigned:
        lines.append("<b>Your open tickets</b>")
        lines.extend(f"• {line}" for line in assigned[:10])
        if len(assigned) > 10:
            lines.append(f"...and {len(assigned) - 10} more")
        lines.append("")
    if unassigned:
        lines.append("<b>Unassigned tickets</b>")
        lines.extend(f"• {line}" for line in unassigned[:10])
        if len(unassigned) > 10:
            lines.append(f"...and {len(unassigned) - 10} more")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def setname_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_chat.id):
        return
    parts = update.message.text.strip().split(maxsplit=1)
    if len(parts) != 2 or not parts[1].strip():
        await update.message.reply_text("Usage: /setname <alias>")
        return
    ADMIN_ALIASES[update.effective_chat.id] = parts[1].strip()
    save_config()
    await update.message.reply_text(f"Alias set to: {ADMIN_ALIASES[update.effective_chat.id]}")


async def accept_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_chat.id):
        return
    parts = update.message.text.strip().split(maxsplit=1)
    if len(parts) != 2 or not parts[1].isdigit():
        await update.message.reply_text("Usage: /accept <ticket_id>")
        return
    ticket_id = int(parts[1])
    ticket = TICKETS.get(ticket_id)
    if not ticket or ticket.get("status") != "open":
        await update.message.reply_text("Ticket not found or already closed.")
        return
    current_owner = ticket.get("assignedAdminId")
    if current_owner and current_owner != update.effective_chat.id:
        await update.message.reply_text("Ticket already accepted by another worker.")
        return
    alias = admin_alias(update.effective_chat.id, update.effective_user.first_name or "Worker")
    ticket["assignedAdminId"] = update.effective_chat.id
    ticket["assignedAlias"] = alias
    ticket["acceptedAt"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    TICKETS[ticket_id] = ticket
    update_ticket_record(
        ticket_id,
        {
            "assignedAdminId": update.effective_chat.id,
            "assignedAlias": alias,
            "acceptedAt": ticket["acceptedAt"],
        },
    )
    await update.message.reply_text(f"Ticket #{ticket_id} accepted as {alias}.")


async def coupon_request_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    if not is_admin(query.message.chat_id):
        await query.answer("Not authorized.")
        return
    ticket_id = int(query.data.split(":", 1)[1])
    ticket = TICKETS.get(ticket_id)
    if not ticket or ticket.get("status") != "open":
        await query.answer("Ticket not found or closed.")
        return
    if not ticket.get("assignedAdminId"):
        await query.answer("Accept first with /accept <ticket_id>.")
        return
    if ticket.get("assignedAdminId") != query.message.chat_id:
        await query.answer("Only the assigned worker can request coupons.")
        return
    if not DUFF_CHAT_IDS:
        await query.answer("Duff panel is not configured.")
        return
    alias = admin_alias(query.message.chat_id, query.from_user.first_name or "Worker")
    ticket["couponRequested"] = True
    ticket["couponRequestedBy"] = alias
    ticket["couponRequestedAt"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    TICKETS[ticket_id] = ticket
    update_ticket_record(
        ticket_id,
        {
            "couponRequested": True,
            "couponRequestedBy": alias,
            "couponRequestedAt": ticket["couponRequestedAt"],
        },
    )

    summary = ticket_summary_from_record(ticket)
    text = (
        f"🎟️ Coupon request for ticket #{ticket_id}\n"
        f"Service: {ticket.get('service', '-')}")
    if ticket.get("category"):
        text += f"\nCategory: {ticket['category']}"
    text += f"\nWorker: {alias}\n\n{summary}\n\n"
    text += "Reply to this message with the coupon details, or use /coupon <ticket_id> <code>."

    for duff_id in DUFF_CHAT_IDS:
        try:
            sent = await context.bot.send_message(duff_id, text)
        except Exception:
            continue
        DUFF_REQUEST_MESSAGES[(duff_id, sent.message_id)] = {
            "ticketId": ticket_id,
            "botKey": ticket.get("botKey", "food"),
        }
    await query.answer("Coupon request sent to Duff.")
    await query.message.reply_text("🎟️ Coupon request sent to Duff manager.")


async def duff_reply_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat or not is_duff(chat.id):
        return
    message = update.effective_message
    if not message or not message.reply_to_message:
        return
    entry = DUFF_REQUEST_MESSAGES.get((chat.id, message.reply_to_message.message_id))
    if not entry:
        return
    ticket = TICKETS.get(entry["ticketId"])
    if not ticket or ticket.get("status") != "open":
        await message.reply_text("Ticket is closed or no longer exists.")
        raise ApplicationHandlerStop()
    admin_id = ticket.get("assignedAdminId")
    if not admin_id:
        await message.reply_text("No worker assigned yet. Ask them to /accept first.")
        raise ApplicationHandlerStop()
    coupon_text = message.text or ""
    if not coupon_text:
        await message.reply_text("Please send coupon text.")
        raise ApplicationHandlerStop()
    duff_name = update.effective_user.first_name or "Duff"
    ticket["couponProvidedBy"] = duff_name
    ticket["couponProvidedAt"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    ticket["couponText"] = coupon_text
    TICKETS[entry["ticketId"]] = ticket
    update_ticket_record(
        entry["ticketId"],
        {
            "couponProvidedBy": duff_name,
            "couponProvidedAt": ticket["couponProvidedAt"],
        },
    )
    await context.bot.send_message(
        admin_id,
        f"🎟️ Duff coupon for ticket #{entry['ticketId']}:\n{coupon_text}",
    )
    await message.reply_text("Coupon sent to the worker.")
    raise ApplicationHandlerStop()


async def coupon_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_duff(update.effective_chat.id):
        return
    match = re.match(r"^/coupon\s+(\d+)\s+([\s\S]+)$", update.message.text.strip())
    if not match:
        await update.message.reply_text("Usage: /coupon <ticket_id> <coupon text>")
        return
    ticket_id = int(match.group(1))
    coupon_text = match.group(2).strip()
    ticket = TICKETS.get(ticket_id)
    if not ticket or ticket.get("status") != "open":
        await update.message.reply_text("Ticket not found or closed.")
        return
    admin_id = ticket.get("assignedAdminId")
    if not admin_id:
        await update.message.reply_text("No worker assigned yet. Ask them to /accept first.")
        return
    duff_name = update.effective_user.first_name or "Duff"
    ticket["couponProvidedBy"] = duff_name
    ticket["couponProvidedAt"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    ticket["couponText"] = coupon_text
    TICKETS[ticket_id] = ticket
    update_ticket_record(
        ticket_id,
        {
            "couponProvidedBy": duff_name,
            "couponProvidedAt": ticket["couponProvidedAt"],
        },
    )
    await context.bot.send_message(
        admin_id, f"🎟️ Duff coupon for ticket #{ticket_id}:\n{coupon_text}"
    )
    await update.message.reply_text("Coupon sent to the worker.")


async def duff_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_duff(update.effective_chat.id):
        return
    open_tickets = [
        (ticket_id, ticket)
        for ticket_id, ticket in TICKETS.items()
        if ticket.get("status") == "open"
    ]
    live = [
        format_ticket_line(ticket_id, ticket)
        for ticket_id, ticket in open_tickets
        if ticket.get("couponRequested")
    ]
    pending = [
        format_ticket_line(ticket_id, ticket)
        for ticket_id, ticket in open_tickets
        if not ticket.get("couponRequested")
    ]
    closed_records = [
        (ticket_id, record)
        for ticket_id, record in TICKET_RECORDS.items()
        if record.get("status") == "closed" and record.get("closeType") == "admin_close"
    ]
    closed_records.sort(key=lambda item: item[0], reverse=True)
    totals = summarize_report()

    lines = [
        "🧩 <b>Duff Panel</b>",
        f"🟢 Live requests: <b>{len(live)}</b>",
        f"🟡 Open tickets: <b>{len(open_tickets)}</b>",
        f"💰 Profit total: <b>${totals['profit_total']:.2f}</b>",
        f"🧮 Duff 25%: <b>${totals['duff_total']:.2f}</b>",
        "",
        "Commands:",
        "• <code>/coupon &lt;ticket_id&gt; &lt;coupon text&gt;</code>",
        "• Reply to a coupon request to send the coupon",
        "",
    ]
    if live:
        lines.append("🟢 <b>Live coupon requests</b>")
        lines.extend(f"• {line}" for line in live[:10])
        if len(live) > 10:
            lines.append(f"...and {len(live) - 10} more")
        lines.append("")
    if pending:
        lines.append("🟡 <b>Open tickets</b>")
        lines.extend(f"• {line}" for line in pending[:10])
        if len(pending) > 10:
            lines.append(f"...and {len(pending) - 10} more")
        lines.append("")
    if closed_records:
        lines.append("✅ <b>Recent completed</b>")
        for ticket_id, record in closed_records[:10]:
            lines.append(f"• {format_closed_line(ticket_id, record)}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled error: %s", context.error)


def register_bot(application, bot_key: str):
    application.bot_data["bot_key"] = bot_key
    application.bot_data["store"] = SessionStore(application.bot)

    application.add_handler(TypeHandler(Update, ban_guard), group=0)
    application.add_handler(MessageHandler(filters.ALL, admin_reply_handler), group=1)
    application.add_handler(MessageHandler(filters.REPLY, duff_reply_handler), group=2)

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(CommandHandler("work", work_command))
    application.add_handler(CommandHandler("setname", setname_command))
    application.add_handler(CommandHandler("accept", accept_command))
    application.add_handler(CommandHandler("duff", duff_command))
    application.add_handler(CommandHandler("coupon", coupon_command))

    if bot_key == "food":
        application.add_handler(CommandHandler("report", report_command))
        application.add_handler(CommandHandler("close", close_command))
        application.add_handler(CommandHandler("ban", ban_command))
        application.add_handler(CommandHandler("unban", unban_command))
        application.add_handler(CallbackQueryHandler(menu_main, pattern="^menu:main$"))
        application.add_handler(CallbackQueryHandler(menu_food, pattern="^menu:food$"))
        application.add_handler(CallbackQueryHandler(food_category, pattern="^food:"))
    else:
        pattern = f"^{bot_key}:start$"
        application.add_handler(CallbackQueryHandler(start_flow_callback, pattern=pattern))

    application.add_handler(CallbackQueryHandler(close_ticket_callback, pattern="^close:"))
    application.add_handler(CallbackQueryHandler(ban_ticket_callback, pattern="^ban:"))
    application.add_handler(CallbackQueryHandler(coupon_request_callback, pattern="^coupon:"))

    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    application.add_handler(MessageHandler(~filters.TEXT & ~filters.COMMAND, other_handler))
    application.add_error_handler(error_handler)


async def start_app(name: str, app):
    while True:
        try:
            await app.initialize()
            await app.start()
            await app.updater.start_polling()
            logger.info("[%s] bot launched", name)
            await asyncio.Event().wait()
        except Exception as exc:
            logger.exception("[%s] bot launch failed: %s", name, exc)
            await asyncio.sleep(5)
        finally:
            try:
                await app.stop()
            except Exception:
                pass
            try:
                await app.shutdown()
            except Exception:
                pass


async def main():
    bots = []

    food_app = ApplicationBuilder().token(FOOD_TOKEN).build()
    register_bot(food_app, "food")
    bots.append(("food", food_app))

    if FLIGHT_TOKEN:
        flight_app = ApplicationBuilder().token(FLIGHT_TOKEN).build()
        register_bot(flight_app, "flight")
        bots.append(("flight", flight_app))

    if HOTEL_TOKEN:
        hotel_app = ApplicationBuilder().token(HOTEL_TOKEN).build()
        register_bot(hotel_app, "hotel")
        bots.append(("hotel", hotel_app))

    await asyncio.gather(*(start_app(name, app) for name, app in bots))


if __name__ == "__main__":
    asyncio.run(main())
