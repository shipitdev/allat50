const fs = require("fs");
const path = require("path");
try {
  const dns = require("dns");
  if (typeof dns.setDefaultResultOrder === "function") {
    dns.setDefaultResultOrder("ipv4first");
  }
} catch (_) {}
const { Telegraf, Markup } = require("telegraf");
const pTimeout = require("p-timeout");
require("dotenv").config();

const configPath = path.join(__dirname, "..", "config.json");
let config = {};
try {
  config = JSON.parse(fs.readFileSync(configPath, "utf8"));
} catch (err) {
  console.error("Missing or invalid config.json.");
  process.exit(1);
}

const dataDir = path.join(__dirname, "..", "data");
const usersPath = path.join(dataDir, "users.json");
const sessionsPath = path.join(dataDir, "sessions.json");
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const loadJsonFile = (filePath, fallback) => {
  if (!fs.existsSync(filePath)) {
    return fallback;
  }
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (err) {
    console.error(`[storage] Invalid JSON in ${filePath}`);
    return fallback;
  }
};

const usersStore = loadJsonFile(usersPath, {});
const sessionsStore = loadJsonFile(sessionsPath, {});
const SESSION_TTL_MS = 30 * 60 * 1000;
let storageWriteQueue = Promise.resolve();
let usersFlushTimer = null;
let sessionsFlushTimer = null;
const backupState = new Map();

const maybeBackup = (filePath) => {
  const stamp = new Date().toISOString().slice(0, 10);
  if (backupState.get(filePath) === stamp) {
    return;
  }
  backupState.set(filePath, stamp);
  if (!fs.existsSync(filePath)) {
    return;
  }
  try {
    fs.copyFileSync(filePath, `${filePath}.bak`);
  } catch (err) {
    logError("storage backup", err);
  }
};

const atomicWriteJson = async (filePath, data) => {
  const tmpPath = `${filePath}.tmp.${process.pid}`;
  maybeBackup(filePath);
  await fs.promises.writeFile(tmpPath, JSON.stringify(data, null, 2));
  await fs.promises.rename(tmpPath, filePath);
};

const queueWrite = async (filePath, data) => {
  storageWriteQueue = storageWriteQueue
    .then(() => atomicWriteJson(filePath, data))
    .catch((err) => logError("storage write", err));
  return storageWriteQueue;
};

const scheduleFlushUsers = () => {
  if (usersFlushTimer) {
    return;
  }
  usersFlushTimer = setTimeout(() => {
    usersFlushTimer = null;
    queueWrite(usersPath, usersStore);
  }, 300);
  if (typeof usersFlushTimer.unref === "function") {
    usersFlushTimer.unref();
  }
};

const scheduleFlushSessions = () => {
  if (sessionsFlushTimer) {
    return;
  }
  sessionsFlushTimer = setTimeout(() => {
    sessionsFlushTimer = null;
    queueWrite(sessionsPath, sessionsStore);
  }, 300);
  if (typeof sessionsFlushTimer.unref === "function") {
    sessionsFlushTimer.unref();
  }
};

const cleanupSessions = () => {
  const now = Date.now();
  let changed = false;
  Object.entries(sessionsStore).forEach(([userId, session]) => {
    const updatedAt = Number(session?.updatedAt) || 0;
    if (!updatedAt || now - updatedAt > SESSION_TTL_MS) {
      delete sessionsStore[userId];
      changed = true;
    }
  });
  if (changed) {
    queueWrite(sessionsPath, sessionsStore);
  }
};

setInterval(cleanupSessions, 5 * 60 * 1000).unref();

const getUser = (userId) => usersStore[String(userId)] || null;
const getAddresses = (user) =>
  Array.isArray(user?.addresses) ? user.addresses : [];
const saveUser = async (userId, user) => {
  const key = String(userId);
  const now = Date.now();
  user.createdAt = user.createdAt || now;
  user.updatedAt = now;
  usersStore[key] = user;
  scheduleFlushUsers();
};
const deleteUser = async (userId) => {
  delete usersStore[String(userId)];
  scheduleFlushUsers();
};
const getSessionData = (userId) => sessionsStore[String(userId)] || null;
const saveSessionData = async (userId, session) => {
  const key = String(userId);
  const existing = sessionsStore[key];
  if (!Array.isArray(session.nav) && Array.isArray(existing?.nav)) {
    session.nav = existing.nav;
  }
  session.updatedAt = Date.now();
  sessionsStore[key] = session;
  scheduleFlushSessions();
};
const deleteSessionData = async (userId) => {
  delete sessionsStore[String(userId)];
  scheduleFlushSessions();
};

const ticketRecordsPath = (() => {
  const configured = config.ticketRecordsPath;
  if (!configured) {
    return path.join(__dirname, "..", "ticket_records.json");
  }
  return path.isAbsolute(configured)
    ? configured
    : path.join(__dirname, "..", configured);
})();

const token = config.botToken || process.env.BOT_TOKEN;
if (!token) {
  console.error("Missing botToken in config.json or BOT_TOKEN in environment.");
  process.exit(1);
}

const flightBotToken = config.flightBotToken;
const hotelBotToken = config.hotelBotToken;

const rawAdminChatIds = Array.isArray(config.adminChatIds)
  ? config.adminChatIds
  : config.adminChatId
    ? [config.adminChatId]
    : [];
const adminChatIds = rawAdminChatIds
  .map((id) => Number(id))
  .filter((id) => Number.isFinite(id));
if (adminChatIds.length === 0) {
  console.error("Missing adminChatIds array in config.json.");
  process.exit(1);
}

const rawDuffChatIds = Array.isArray(config.duffChatIds)
  ? config.duffChatIds
  : config.duffChatId
    ? [config.duffChatId]
    : [];
const duffChatIds = rawDuffChatIds
  .map((id) => Number(id))
  .filter((id) => Number.isFinite(id));

const adminAliases = {};
if (config.adminAliases && typeof config.adminAliases === "object") {
  Object.entries(config.adminAliases).forEach(([id, alias]) => {
    const adminId = Number(id);
    if (!Number.isFinite(adminId) || typeof alias !== "string") {
      return;
    }
    const trimmed = alias.trim();
    if (trimmed) {
      adminAliases[adminId] = trimmed;
    }
  });
}

const bannedChatIds = new Set(
  (config.bannedChatIds || [])
    .map((id) => Number(id))
    .filter((id) => Number.isFinite(id))
);

const ticketRecords = new Map();
let ticketRecordsFlushTimer = null;

const scheduleFlushTicketRecords = () => {
  if (ticketRecordsFlushTimer) {
    return;
  }
  ticketRecordsFlushTimer = setTimeout(() => {
    ticketRecordsFlushTimer = null;
    queueWrite(ticketRecordsPath, Object.fromEntries(ticketRecords.entries()));
  }, 300);
  if (typeof ticketRecordsFlushTimer.unref === "function") {
    ticketRecordsFlushTimer.unref();
  }
};

const saveTicketRecords = () => {
  try {
    scheduleFlushTicketRecords();
    return true;
  } catch (err) {
    logError("ticket records save", err);
    return false;
  }
};

const loadTicketRecords = () => {
  let records = null;
  if (fs.existsSync(ticketRecordsPath)) {
    try {
      records = JSON.parse(fs.readFileSync(ticketRecordsPath, "utf8"));
    } catch (_) {
      console.warn("Invalid ticket_records.json (JSON parse failed).");
    }
  } else if (config.ticketRecords && typeof config.ticketRecords === "object") {
    records = config.ticketRecords;
  }

  if (!records || typeof records !== "object") {
    return;
  }

  Object.entries(records).forEach(([id, record]) => {
    const ticketId = Number(id);
    if (!Number.isFinite(ticketId) || !record || typeof record !== "object") {
      return;
    }
    ticketRecords.set(ticketId, { ...record, ticketId });
  });

  if (!fs.existsSync(ticketRecordsPath) && ticketRecords.size > 0) {
    saveTicketRecords();
  }
  delete config.ticketRecords;
};

loadTicketRecords();

const sessionTimeoutMinutesValue = Number(config.sessionTimeoutMinutes);
const sessionTimeoutMinutes = Number.isFinite(sessionTimeoutMinutesValue)
  ? sessionTimeoutMinutesValue
  : 15;
const sessionTimeoutMs =
  sessionTimeoutMinutes > 0 ? sessionTimeoutMinutes * 60 * 1000 : 0;

const rateLimitWindowMinutesValue = Number(config?.rateLimit?.windowMinutes);
const rateLimitMaxTicketsValue = Number(config?.rateLimit?.maxTickets);
const rateLimitWindowMinutes = Number.isFinite(rateLimitWindowMinutesValue)
  ? rateLimitWindowMinutesValue
  : 30;
const rateLimitMaxTickets = Number.isFinite(rateLimitMaxTicketsValue)
  ? rateLimitMaxTicketsValue
  : 2;
const rateLimitWindowMs =
  rateLimitWindowMinutes > 0 ? rateLimitWindowMinutes * 60 * 1000 : 0;
const rateLimitEnabled = rateLimitWindowMs > 0 && rateLimitMaxTickets > 0;
const duffCutRate = 0.25;
const startupTimeoutMs = Number(config.startupTimeoutMs) || 30000;
const startupRetryBaseMs = Number(config.startupRetryBaseMs) || 5000;
const startupRetryMaxMs = Number(config.startupRetryMaxMs) || 60000;
const startupRetryJitterMs = Number(config.startupRetryJitterMs) || 750;
const startupGetMeRetries = Number(config.startupGetMeRetries) || 2;
const startupStaggerMs = Number(config.startupStaggerMs) || 500;
const startupSkipGetMe = config.startupSkipGetMe === true;
const startupRequireGetMe = config.startupRequireGetMe === true;

const botUsernames = {
  food: config.foodBotUsername || "",
  flight: config.flightBotUsername || "",
  hotel: config.hotelBotUsername || "",
};

const CHANNEL_URL = "https://t.me/Allat50";
const GROUP_URL = "https://t.me/Allat50_group";
const LOGO_PATH = path.join(__dirname, "..", "allat50.png");
let LOGO_BYTES = null;
try {
  if (fs.existsSync(LOGO_PATH)) {
    LOGO_BYTES = fs.readFileSync(LOGO_PATH);
  }
} catch (_) {
  LOGO_BYTES = null;
}

const logError = (label, err) => {
  const message = err?.stack || err?.message || String(err);
  console.error(`[${label}] ${message}`);
};

process.on("unhandledRejection", (err) =>
  logError("unhandledRejection", err)
);
process.on("uncaughtException", (err) =>
  logError("uncaughtException", err)
);

const ensureText = (value, fallback = "OK") => {
  if (typeof value !== "string") {
    return fallback;
  }
  if (!value.trim()) {
    return fallback;
  }
  return value;
};

const withTimeout = (label, promise, ms = 12000) =>
  pTimeout(promise, { milliseconds: ms }).catch((err) => {
    logError(`timeout:${label}`, err);
    return null;
  });

const safeReply = (ctx, text, extra) =>
  withTimeout("ctx.reply", ctx.reply(ensureText(text), extra));

const safeReplyHtml = (ctx, html, extra) =>
  withTimeout("ctx.replyWithHTML", ctx.replyWithHTML(ensureText(html), extra));

const safeReplyPhoto = (ctx, photo, extra) =>
  withTimeout("ctx.replyWithPhoto", ctx.replyWithPhoto(photo, extra));

const step = async (label, fn) => {
  const startedAt = Date.now();
  try {
    return await fn();
  } finally {
    const ms = Date.now() - startedAt;
    if (ms > 800) {
      console.warn(`[slow-step] ${label} ${ms}ms`);
    }
  }
};

const normalizeUsername = (value) =>
  value ? String(value).replace(/^@/, "") : "";

const escapeHtml = (value) =>
  String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

const emphasizeHtml = (text) => `<b><i>${text}</i></b>`;

const formatHybridLink = (label, url) =>
  `<a href="${url}">| ${label} |</a>`;

const formatBotLink = (label, username) => {
  const clean = normalizeUsername(username);
  if (!clean) {
    return null;
  }
  return formatHybridLink(label, `https://t.me/${clean}`);
};

const quickLinksSection = () => {
  const links = [
    formatBotLink("Food BOT", botUsernames.food),
    formatBotLink("Flight BOT", botUsernames.flight),
    formatBotLink("Hotel BOT", botUsernames.hotel),
    formatHybridLink("All at 50 Channel", CHANNEL_URL),
    formatHybridLink("All at 50 Group", GROUP_URL),
  ].filter(Boolean);

  if (links.length === 0) {
    return "";
  }

  const firstLine = links.slice(0, 3).join(" ");
  const secondLine = links.slice(3).join(" ");
  return `\n\n<b>Quick links</b>\n${firstLine}${
    secondLine ? `\n${secondLine}` : ""
  }`;
};

const attachBotMiddlewares = (botInstance, label) => {
  botInstance.use(async (ctx, next) => {
    if (ctx.callbackQuery) {
      ctx.answerCbQuery().catch(() => {});
    }
    return next();
  });
  botInstance.use(async (ctx, next) => {
    const startedAt = Date.now();
    try {
      return await next();
    } finally {
      const ms = Date.now() - startedAt;
      if (ms > 800) {
        console.warn(`[slow] ${ms}ms ${label}:${ctx.updateType}`);
      }
    }
  });
  botInstance.use(async (ctx, next) => {
    const warnAfterMs = 15000;
    const timer = setTimeout(() => {
      const userId = ctx.from?.id;
      const chatId = ctx.chat?.id;
      console.warn(
        `[watchdog] ${label} still running after ${warnAfterMs}ms update=${ctx.updateType} chat=${chatId} user=${userId}`
      );
    }, warnAfterMs);
    if (typeof timer.unref === "function") {
      timer.unref();
    }
    try {
      return await next();
    } finally {
      clearTimeout(timer);
    }
  });
  botInstance.catch((err) => logError(`${label} telegraf`, err));
};

const bot = new Telegraf(token);
attachBotMiddlewares(bot, "food");
const sessions = new Map();
const tickets = new Map();
const adminTicketMessages = new Map();
const duffRequestMessages = new Map();
const customerTickets = new Map();
const ticketHistory = new Map();
const adminPrompts = new Map();
const workerListMessages = new Map();
const MAX_OPEN_TICKETS_PER_CHAT = 4;
let ticketCounter = Number.isFinite(Number(config.ticketCounter))
  ? Number(config.ticketCounter)
  : 60;

const rebuildOpenTickets = () => {
  for (const [ticketId, record] of ticketRecords.entries()) {
    if (!record || record.status !== "open") {
      continue;
    }
    const chatId = Number(record.chatId);
    if (!Number.isFinite(chatId)) {
      continue;
    }
    const ticket = {
      chatId,
      status: "open",
      service: record.service || record.category || "Order",
      category: record.category,
      botKey: record.botKey,
      answers: record.answers || {},
      adminMessages: [],
    };
    if (record.assignedAdminId) {
      ticket.assignedAdminId = record.assignedAdminId;
    }
    if (record.assignedAlias) {
      ticket.assignedAlias = record.assignedAlias;
    }
    if (record.couponRequested) {
      ticket.couponRequested = record.couponRequested;
      ticket.couponRequestedBy = record.couponRequestedBy;
      ticket.couponRequestedAt = record.couponRequestedAt;
    }
    if (record.couponProvidedBy) {
      ticket.couponProvidedBy = record.couponProvidedBy;
      ticket.couponProvidedAt = record.couponProvidedAt;
    }
    if (record.paymentStatus) {
      ticket.paymentStatus = record.paymentStatus;
      ticket.paidAt = record.paidAt;
      ticket.paidBy = record.paidBy;
    }
    tickets.set(ticketId, ticket);
    addOpenTicketForChat(chatId, ticketId);
  }
};

rebuildOpenTickets();

const SERVICES = {};

const FOOD_PROMO =
  "🔥 <b>50% OFF</b>\n🚗 <b>DELIVERY only</b>\n💵 <b>$40 MIN - $100 MAX</b>";

const FOOD_QUESTIONS = [
  {
    key: "name",
    label: "Name",
    prompt: "👤 <b>First and last name?</b>",
  },
  {
    key: "address",
    label: "Address",
    prompt:
      "🏠 <b>Full address</b> (must include apt#, zip, state, etc)\n\nFormat example: 4455 Landing Lange, APT 4, Louisville, KY 40018",
  },
  {
    key: "phone",
    label: "Phone",
    prompt:
      "📞 <b>Phone number?</b> (US only)\nExample: 555-123-4567",
  },
];

const FOOD_CONTINUE_PROMPT =
  "✅ <b>Would you like to continue?</b>\nType <b>yes</b> or /cancel";

const FLIGHT_PROMO =
  "✈️ <b>Flights</b>\n" +
  "🔻 <b>40% OFF</b>\n" +
  "🔻 <b>Domestic & International</b>\n" +
  "🔻 <b>JetBlue, Spirit, Frontier,</b>\n<b>Southwest, American Airlines and</b>\n<b>International custom airlines</b>\n" +
  "🔻 <b>100% Safe</b>\n" +
  "🔻 <b>Book up-to 5 days in advance!</b>\n\n" +
  "⏱️ <b>upto 24hr response time</b>";

const FLIGHT_QUESTIONS = [
  {
    key: "trip_dates",
    label: "Trip Dates",
    prompt: "📅 <b>Trip Dates?</b>",
  },
  {
    key: "passenger_form",
    label: "Passenger Info",
    prompt:
      "🧾 <b>Please fill out this form</b>\nPer Passenger\nIn the Format Below :-\n<b>First Name</b> :\n<b>Middle Name</b> : if have\n<b>Last Name</b> : <b>DOB (MM/DD/YYYY)</b> :\n<b>Male/Female</b> : <b>Email</b> : <b>Phone</b> :",
  },
  {
    key: "residence",
    label: "State of residence",
    prompt: "📍 <b>State of residence?</b>",
  },
  {
    key: "order_total",
    label: "Total value",
    prompt: "💵 <b>Total value of order?</b>",
  },
  {
    key: "airlines",
    label: "Airlines",
    prompt: "✈️ <b>What airlines?</b>",
  },
];

const FLIGHT_CONTINUE_PROMPT =
  "✅ <b>Would you like to continue?</b>\nType <b>yes</b> to continue or /cancel";

const HOTEL_PROMO =
  "🏨 <b>Hotels</b>\n" +
  "💎 <b>Premium stays & verified bookings</b>\n\n" +
  "⏱️ <b>upto 24hr response time</b>";

const HOTEL_QUESTIONS = [
  {
    key: "destination",
    label: "Destination",
    prompt: "📍 <b>Destination city?</b>",
  },
  {
    key: "dates",
    label: "Dates",
    prompt: "📅 <b>Check-in and check-out dates?</b>",
  },
  {
    key: "budget",
    label: "Budget",
    prompt: "💵 <b>Budget range?</b>",
  },
  {
    key: "email",
    label: "Email",
    prompt: "📧 <b>Customer email for booking?</b>",
  },
  {
    key: "booking_link",
    label: "Booking link",
    prompt: "🔗 <b>Booking.com link (if any)?</b>",
  },
  {
    key: "preferred_chain",
    label: "Preferred chain",
    prompt:
      "🏨 <b>Preferred hotel chain?</b>\nExamples: Marriot / Hilton / IHG",
  },
];

const HOTEL_CONTINUE_PROMPT =
  "✅ <b>Would you like to continue?</b>\nType <b>yes</b> to continue or /cancel";

const START_PROMPT =
  "🧭 <b>Send /start</b> to begin or choose a service from the menu.";
const FLIGHT_START_PROMPT =
  "🧭 <b>Send /start</b> to begin your flight request.";
const HOTEL_START_PROMPT =
  "🧭 <b>Send /start</b> to begin your hotel request.";

const FLIGHT_HOME = () =>
  "✈️ <b>Flight Concierge</b>\n" +
  "<i>Domestic & International • 40% OFF</i>\n\n" +
  "🧭 <b>How it works:</b> Answer a few quick questions → connect with an agent\n" +
  "⏱️ <b>Response:</b> up to 24h\n\n" +
  "👇 <b>Tap to start</b>";

const HOTEL_HOME = () =>
  "🏨 <b>Hotel Concierge</b>\n" +
  "<i>Verified stays • Premium deals</i>\n\n" +
  "🧭 <b>How it works:</b> Share your trip details → connect with an agent\n" +
  "⏱️ <b>Response:</b> up to 24h\n\n" +
  "👇 <b>Tap to start</b>";

const FOOD_CATEGORIES = [
  {
    id: "fast_food",
    label: "🔴 Fast Food Pickup 55% off",
  },
  {
    id: "meal_kits",
    label: "🥑 Meal Kits",
  },
  {
    id: "sonic_combo",
    label: "🔴 Sonic | 🍗 Zaxby's | 🥤 Smoothie King",
  },
  {
    id: "ihop_dennys",
    label: "🥞 IHOP/Dennys",
  },
  {
    id: "panera",
    label: "🥪 Panera",
  },
  {
    id: "wingstop",
    label: "🍗 WingStop",
  },
  {
    id: "panda",
    label: "🐼 Panda Express",
  },
  {
    id: "five_guys",
    label: "🍔 Five Guys",
  },
  {
    id: "pizza",
    label: "🍕 Pizza",
  },
  {
    id: "chipotle",
    label: "🌯 Chipotle",
  },
  {
    id: "cava",
    label: "🥗 Cava",
  },
  {
    id: "shake_shack",
    label: "🍔 Shake Shack",
  },
  {
    id: "canes",
    label: "🔴 Canes",
  },
  {
    id: "ubereats",
    label: "🚗 UberEats",
  },
  {
    id: "doordash",
    label: "🚗 Doordash",
  },
  {
    id: "grubhub",
    label: "🌭 Grubhub Delivery",
  },
  {
    id: "restaurants",
    label: "🍽️ Restaurants",
  },
  {
    id: "dine_in",
    label: "🍽️ Dine-In",
  },
  {
    id: "groceries",
    label: "🛒 Groceries",
  },
  {
    id: "movies",
    label: "🎬 Movies",
  },
  {
    id: "uber_rides",
    label: "🔴 Uber Rides",
  },
];

const FOOD_CATEGORY_MAP = new Map(
  FOOD_CATEGORIES.map((item) => [item.id, item])
);

const FOOD_MENU_ROWS = [
  ["fast_food"],
  ["meal_kits"],
  ["sonic_combo"],
  ["ihop_dennys", "panera"],
  ["wingstop", "panda"],
  ["five_guys", "pizza"],
  ["chipotle", "cava"],
  ["shake_shack", "canes"],
  ["ubereats", "doordash"],
  ["grubhub"],
  ["restaurants", "dine_in"],
  ["groceries"],
  ["movies", "uber_rides"],
];

const FLOW_STATES = {
  IDLE: "IDLE",
  AWAIT_PROFILE_CHOICE: "AWAIT_PROFILE_CHOICE",
  AWAIT_NAME: "AWAIT_NAME",
  AWAIT_PHONE: "AWAIT_PHONE",
  AWAIT_ADDRESS_TEXT: "AWAIT_ADDRESS_TEXT",
  AWAIT_ADDRESS_LABEL: "AWAIT_ADDRESS_LABEL",
  AWAIT_ADDRESS_LABEL_CUSTOM: "AWAIT_ADDRESS_LABEL_CUSTOM",
  AWAIT_PROFILE_POST_SAVE: "AWAIT_PROFILE_POST_SAVE",
  AWAIT_ADDRESS_PICK: "AWAIT_ADDRESS_PICK",
  AWAIT_SUBTOTAL: "AWAIT_SUBTOTAL",
  AWAIT_ADD_NAME: "AWAIT_ADD_NAME",
  AWAIT_ADD_PHONE: "AWAIT_ADD_PHONE",
  AWAIT_ADD_ADDRESS_TEXT: "AWAIT_ADD_ADDRESS_TEXT",
  AWAIT_ADD_ADDRESS_LABEL: "AWAIT_ADD_ADDRESS_LABEL",
  AWAIT_ADD_ADDRESS_LABEL_CUSTOM: "AWAIT_ADD_ADDRESS_LABEL_CUSTOM",
  AWAIT_MANAGE_PICK: "AWAIT_MANAGE_PICK",
  AWAIT_MANAGE_ACTION: "AWAIT_MANAGE_ACTION",
  AWAIT_EDIT_ADDRESS: "AWAIT_EDIT_ADDRESS",
  AWAIT_RENAME_LABEL: "AWAIT_RENAME_LABEL",
  AWAIT_RENAME_LABEL_CUSTOM: "AWAIT_RENAME_LABEL_CUSTOM",
  AWAIT_DELETE_CONFIRM: "AWAIT_DELETE_CONFIRM",
  AWAIT_CONFIRM: "AWAIT_CONFIRM",
  AWAIT_SUPPORT: "AWAIT_SUPPORT",
  AWAIT_LAST_ORDER: "AWAIT_LAST_ORDER",
  AWAIT_NEW_TICKET_CONFIRM: "AWAIT_NEW_TICKET_CONFIRM",
  AWAIT_TICKET_PICK: "AWAIT_TICKET_PICK",
};

const BTN_ADD_ADDRESS = "➕ Add Address";
const BTN_MANAGE = "⚙️ Manage";
const BTN_MANAGE_ADDR = "⚙️ Manage Addresses";
const BTN_BACK = "⬅️ Back";
const BTN_HOME = "🏠 Main Menu";
const BTN_USE_DEFAULT = "✅ Use this address";
const BTN_CHOOSE_ANOTHER = "🔁 Choose another";
const BTN_CREATE_PROFILE = "💾 Create Profile";
const BTN_SKIP = "Skip (this time)";
const BTN_CHANGE_ADDRESS = "🔁 Change address";
const BTN_PROFILE = "👤 My Profile";
const BTN_CHOOSE_ADDRESS = "📍 Choose Address";
const BTN_DELETE_PROFILE = "🗑️ Delete Profile";
const BTN_SET_DEFAULT = "✅ Set Default";
const BTN_EDIT_ADDRESS = "✏️ Edit Address";
const BTN_RENAME_LABEL = "🏷️ Rename Label";
const BTN_DELETE_ADDRESS = "🗑️ Delete";
const BTN_DELETE_CONFIRM = "✅ Yes, delete";
const BTN_DELETE_CANCEL = "❌ Cancel";
const BTN_SUBMIT_ORDER = "✅ Submit Order";
const BTN_EDIT_SUBTOTAL = "✏️ Edit Subtotal";
const BTN_CANCEL_ORDER = "❌ Cancel";
const BTN_NEW_ORDER = "🆕 New Order";
const BTN_LAST_ORDER = "🧾 Last Order";
const BTN_SUPPORT = "🆘 Support";
const BTN_CHANNEL = "📢 Channel";
const BTN_REORDER_SAME = "🔁 Reorder (same address)";
const BTN_REORDER_CHOOSE = "🔁 Reorder (choose address)";
const BTN_CHANGE_SUBTOTAL = "✏️ Edit subtotal";
const BTN_ADDRESSES = "📍 Addresses";
const BTN_MENU = "☰ Menu";
const BTN_ADD_PROFILE_ADDR = "➕ Add Profile/Address";
const BTN_EDIT_PROFILE_ADDR = "✏️ Edit/Delete Profile/Address";
const BTN_MY_TICKETS = "🎟 My Tickets";
const BTN_OPEN_ANOTHER = "✅ Open another";
const BTN_OPEN_THIS = "✅ Open this request";
const BTN_CANCEL_GENERIC = "❌ Cancel";

const SCREEN = {
  HOME: "HOME",
  QUICK_ACTIONS: "QUICK_ACTIONS",
  PROFILE_MENU: "PROFILE_MENU",
  PROFILE_CREATE: "PROFILE_CREATE",
  ADDRESS_PICKER: "ADDRESS_PICKER",
  MANAGE_ADDRESSES: "MANAGE_ADDRESSES",
  MANAGE_ADDRESS: "MANAGE_ADDRESS",
  SUBTOTAL: "SUBTOTAL",
  CONFIRM: "CONFIRM",
  LAST_ORDER: "LAST_ORDER",
  SUPPORT: "SUPPORT",
  TICKETS: "TICKETS",
};

const NAV_MAX = 10;

const normalizeNav = (nav) => (Array.isArray(nav) ? nav.slice() : []);

const updateNavStack = (nav, screenId) => {
  let stack = normalizeNav(nav);
  if (stack.length === 0) {
    stack = [SCREEN.HOME];
  }
  if (!screenId) {
    return stack;
  }
  if (screenId === SCREEN.HOME) {
    return [SCREEN.HOME];
  }
  if (stack[stack.length - 1] !== screenId) {
    stack.push(screenId);
  }
  if (stack.length > NAV_MAX) {
    stack = stack.slice(stack.length - NAV_MAX);
  }
  return stack;
};

const recordScreen = async (ctx, screenId) => {
  if (ctx.state?.skipNav) {
    return;
  }
  const userId = ctx.from?.id;
  if (!userId) {
    return;
  }
  const existing = getSessionData(userId);
  const session =
    existing && typeof existing === "object"
      ? existing
      : { state: FLOW_STATES.IDLE, temp: {} };
  const nav = updateNavStack(session.nav, screenId);
  await saveSessionData(userId, { ...session, nav });
};

const ensureFlowState = async (userId, nextState) => {
  if (!userId || !nextState) {
    return;
  }
  const existing = getSessionData(userId);
  const session =
    existing && typeof existing === "object"
      ? existing
      : { state: FLOW_STATES.IDLE, temp: {} };
  if (session.state !== nextState) {
    await saveSessionData(userId, { ...session, state: nextState });
  }
};

const LABEL_HOME = "🏠 Home";
const LABEL_WORK = "🏢 Work";
const LABEL_OTHER = "📍 Other";
const LABEL_CUSTOM = "✍️ Custom";
const LABEL_CANCEL = "⬅️ Cancel";

const mainMenu = () => foodMenu(false);

const flightStartMenu = () =>
  Markup.inlineKeyboard([
    [Markup.button.callback("✈️ Start Flight Booking", "flight:start")],
  ]);

const hotelStartMenu = () =>
  Markup.inlineKeyboard([
    [Markup.button.callback("🏨 Start Hotel Booking", "hotel:start")],
  ]);

const foodMenu = (includeBack = true) => {
  const rows = FOOD_MENU_ROWS.map((row) =>
    row
      .map((id) => FOOD_CATEGORY_MAP.get(id))
      .filter(Boolean)
      .map((item) => Markup.button.callback(item.label, `food:${item.id}`))
  );

  rows.push([Markup.button.callback(BTN_MENU, "menu:quick")]);
  if (includeBack) {
    rows.push([Markup.button.callback("⬅️ Back to main menu", "menu:main")]);
  }

  return Markup.inlineKeyboard(rows);
};

const shortText = (value, max = 40) => {
  if (!value) {
    return "";
  }
  if (value.length <= max) {
    return value;
  }
  return `${value.slice(0, max - 3).trim()}...`;
};

const normalizeLabel = (value) => (value ? value.trim() : "");
const isHomeLabel = (label) => /^home$/i.test(label);
const isWorkLabel = (label) => /^work$/i.test(label);

const addressSlots = (user) => {
  const addresses = Array.isArray(user?.addresses) ? user.addresses : [];
  const home = addresses.find((addr) => isHomeLabel(addr.label));
  const work = addresses.find((addr) => isWorkLabel(addr.label));
  const others = addresses.filter(
    (addr) => addr !== home && addr !== work
  );
  return { home, work, others };
};

const findAddressByLabel = (user, label) => {
  const clean = normalizeLabel(label).toLowerCase();
  const { home, work, others } = addressSlots(user);
  const candidates = [home, work, ...others].filter(Boolean);
  return candidates.find(
    (addr) => normalizeLabel(addr.label).toLowerCase() === clean
  );
};

const addressPickerKeyboard = (user) => {
  const { home, work, others } = addressSlots(user);
  const rows = [];
  if (home || work) {
    rows.push(
      [home, work]
        .filter(Boolean)
        .map((addr) =>
          Markup.button.text(
            addr === home ? `🏠 ${addr.label}` : `🏢 ${addr.label}`
          )
        )
    );
  }
  if (others.length) {
    for (let i = 0; i < others.length; i += 2) {
      rows.push(
        others
          .slice(i, i + 2)
          .map((addr) => Markup.button.text(`📍 ${addr.label}`))
      );
    }
  }
  rows.push([Markup.button.text(BTN_ADD_ADDRESS), Markup.button.text(BTN_MANAGE_ADDR)]);
  rows.push([Markup.button.text(BTN_BACK), Markup.button.text(BTN_HOME)]);
  return Markup.keyboard(rows).resize();
};

const singleAddressKeyboard = () =>
  Markup.keyboard([
    [Markup.button.text(BTN_USE_DEFAULT)],
    [Markup.button.text(BTN_CHOOSE_ANOTHER), Markup.button.text(BTN_ADD_ADDRESS)],
    [Markup.button.text(BTN_MANAGE_ADDR)],
    [Markup.button.text(BTN_BACK), Markup.button.text(BTN_HOME)],
  ]).resize();

const profilePromptKeyboard = () =>
  Markup.keyboard([
    [Markup.button.text(BTN_CREATE_PROFILE)],
    [Markup.button.text(BTN_SKIP)],
    [Markup.button.text(BTN_BACK), Markup.button.text(BTN_HOME)],
  ]).resize();

const profileSavedKeyboard = (includeContinue = true) => {
  const rows = [[Markup.button.text(BTN_ADD_ADDRESS)]];
  if (includeContinue) {
    rows.push([Markup.button.text("Continue Order")]);
  }
  rows.push([Markup.button.text(BTN_BACK), Markup.button.text(BTN_HOME)]);
  return Markup.keyboard(rows).resize();
};

const addressLabelKeyboard = () =>
  Markup.keyboard([
    [Markup.button.text(LABEL_HOME), Markup.button.text(LABEL_WORK)],
    [Markup.button.text(LABEL_OTHER), Markup.button.text(LABEL_CUSTOM)],
    [Markup.button.text(LABEL_CANCEL)],
    [Markup.button.text(BTN_BACK), Markup.button.text(BTN_HOME)],
  ]).resize();

const subtotalKeyboard = () =>
  Markup.keyboard([
    [Markup.button.text(BTN_CHANGE_ADDRESS), Markup.button.text(BTN_MANAGE_ADDR)],
    [Markup.button.text(BTN_BACK), Markup.button.text(BTN_HOME)],
  ]).resize();

const confirmOrderKeyboard = () =>
  Markup.keyboard([
    [Markup.button.text(BTN_SUBMIT_ORDER)],
    [Markup.button.text(BTN_EDIT_SUBTOTAL)],
    [Markup.button.text(BTN_CHANGE_ADDRESS)],
    [Markup.button.text(BTN_CANCEL_ORDER)],
    [Markup.button.text(BTN_BACK), Markup.button.text(BTN_HOME)],
  ]).resize();

const quickActionsKeyboard = () =>
  Markup.keyboard([
    [Markup.button.text(BTN_MY_TICKETS)],
    [Markup.button.text(BTN_ADD_PROFILE_ADDR)],
    [Markup.button.text(BTN_EDIT_PROFILE_ADDR)],
    [Markup.button.text(BTN_BACK), Markup.button.text(BTN_HOME)],
  ]).resize();

const lastOrderKeyboard = () =>
  Markup.keyboard([
    [Markup.button.text(BTN_REORDER_SAME)],
    [Markup.button.text(BTN_REORDER_CHOOSE)],
    [Markup.button.text(BTN_CHANGE_SUBTOTAL)],
    [Markup.button.text(BTN_CANCEL_ORDER)],
    [Markup.button.text(BTN_BACK), Markup.button.text(BTN_HOME)],
  ]).resize();

const openAnotherKeyboardGeneric = () =>
  Markup.keyboard([
    [Markup.button.text(BTN_OPEN_ANOTHER)],
    [Markup.button.text(BTN_MY_TICKETS), Markup.button.text(BTN_SUPPORT)],
    [Markup.button.text(BTN_CANCEL_GENERIC), Markup.button.text(BTN_HOME)],
  ]).resize();

const openAnotherKeyboardService = () =>
  Markup.keyboard([
    [Markup.button.text(BTN_OPEN_THIS)],
    [Markup.button.text(BTN_MY_TICKETS)],
    [Markup.button.text(BTN_CANCEL_GENERIC), Markup.button.text(BTN_HOME)],
  ]).resize();

const maxLimitKeyboard = () =>
  Markup.keyboard([
    [Markup.button.text(BTN_MY_TICKETS), Markup.button.text(BTN_SUPPORT)],
    [Markup.button.text(BTN_HOME)],
  ]).resize();

const ticketsKeyboard = (ids = []) => {
  const rows = ids.map((id) => [Markup.button.text(`🎟 Ticket #${id}`)]);
  rows.push([Markup.button.text(BTN_BACK), Markup.button.text(BTN_HOME)]);
  return Markup.keyboard(rows).resize();
};

const manageListKeyboard = (user) => {
  const { home, work, others } = addressSlots(user);
  const rows = [];
  if (home || work) {
    rows.push(
      [home, work]
        .filter(Boolean)
        .map((addr) =>
          Markup.button.text(
            addr === home ? `🏠 ${addr.label}` : `🏢 ${addr.label}`
          )
        )
    );
  }
  if (others.length) {
    for (let i = 0; i < others.length; i += 2) {
      rows.push(
        others
          .slice(i, i + 2)
          .map((addr) => Markup.button.text(`📍 ${addr.label}`))
      );
    }
  }
  rows.push([Markup.button.text(BTN_ADD_ADDRESS)]);
  rows.push([Markup.button.text(BTN_BACK), Markup.button.text(BTN_HOME)]);
  return Markup.keyboard(rows).resize();
};

const manageActionsKeyboard = () =>
  Markup.keyboard([
    [Markup.button.text(BTN_SET_DEFAULT)],
    [Markup.button.text(BTN_EDIT_ADDRESS)],
    [Markup.button.text(BTN_RENAME_LABEL)],
    [Markup.button.text(BTN_DELETE_ADDRESS)],
    [Markup.button.text(BTN_BACK), Markup.button.text(BTN_HOME)],
  ]).resize();

const deleteConfirmKeyboard = () =>
  Markup.keyboard([
    [Markup.button.text(BTN_DELETE_CONFIRM)],
    [Markup.button.text(BTN_DELETE_CANCEL)],
    [Markup.button.text(BTN_BACK), Markup.button.text(BTN_HOME)],
  ]).resize();

const profileKeyboard = () =>
  Markup.keyboard([
    [Markup.button.text(BTN_CHOOSE_ADDRESS)],
    [Markup.button.text(BTN_ADD_ADDRESS), Markup.button.text(BTN_MANAGE_ADDR)],
    [Markup.button.text(BTN_DELETE_PROFILE)],
    [Markup.button.text(BTN_BACK), Markup.button.text(BTN_HOME)],
  ]).resize();

const backHomeKeyboard = () =>
  Markup.keyboard([[Markup.button.text(BTN_BACK), Markup.button.text(BTN_HOME)]]).resize();

const createAddressId = (user) => {
  const existing = getAddresses(user);
  const base = `addr${Date.now()}`;
  const suffix = Math.floor(Math.random() * 1000);
  const id = `${base}${suffix}`;
  if (!existing.some((addr) => addr && addr.id === id)) {
    return id;
  }
  return `${base}${suffix}${Math.floor(Math.random() * 10)}`;
};

const resolveLabelChoice = (choice, user) => {
  const label = normalizeLabel(choice);
  if (!label) {
    return null;
  }
  if (label === LABEL_HOME) {
    return "Home";
  }
  if (label === LABEL_WORK) {
    return "Work";
  }
  if (label === LABEL_OTHER) {
    const addresses = getAddresses(user);
    const existing = new Set(
      addresses
        .map((addr) => normalizeLabel(addr.label).toLowerCase())
        .filter(Boolean)
    );
    if (!existing.has("other")) {
      return "Other";
    }
    for (let i = 2; i <= 99; i += 1) {
      const candidate = `other ${i}`;
      if (!existing.has(candidate)) {
        return `Other ${i}`;
      }
    }
    return "Other";
  }
  return null;
};

const stripButtonLabel = (text) => {
  if (!text) {
    return "";
  }
  const parts = text.trim().split(" ");
  if (parts.length > 1 && ["🏠", "🏢", "📍"].includes(parts[0])) {
    return parts.slice(1).join(" ").trim();
  }
  return text.trim();
};

const startLegacyFoodFlow = (ctx, optionLabel) => {
  setSession(ctx.chat.id, {
    service: "food",
    stage: "food_questions",
    stepIndex: 0,
    answers: {},
    foodCategory: optionLabel,
  });
  replyHtml(ctx, FOOD_PROMO, Markup.removeKeyboard());
  return replyHtml(ctx, FOOD_QUESTIONS[0].prompt);
};

const sendMainMenu = async (ctx) => {
  if (openTicketCountForChat(ctx.chat.id) > 0) {
    return promptOpenAnother(ctx, { type: "menu" });
  }
  const userId = ctx.from?.id;
  if (userId) {
    const session = getSessionData(userId);
    if (!session || session.state !== FLOW_STATES.IDLE) {
      await saveSessionData(userId, {
        state: FLOW_STATES.IDLE,
        temp: {},
        nav: session?.nav,
      });
    }
  }
  await step("recordScreen:home", () => recordScreen(ctx, SCREEN.HOME));
  await step("reply:mainMenuTitle", () =>
    safeReply(ctx, "🏠 Main Menu", Markup.removeKeyboard())
  );
  return step("reply:mainMenu", () =>
    replyHtml(ctx, "🍔 <b>Pick a category to start</b>", mainMenu())
  );
};

const sendAddressPicker = async (ctx, optionLabel, user) => {
  const message =
    `📍 Address for: ${optionLabel}\n` +
    "Choose where we're delivering.";
  const prev = getSessionData(ctx.from.id) || {};
  await step("saveSession:addressPicker", () =>
    saveSessionData(ctx.from.id, {
      ...prev,
      state: FLOW_STATES.AWAIT_ADDRESS_PICK,
      selectedOption: optionLabel,
      temp: { ...(prev.temp || {}), mode: "picker" },
    })
  );
  await step("recordScreen:addressPicker", () =>
    recordScreen(ctx, SCREEN.ADDRESS_PICKER)
  );
  return step("reply:addressPicker", () =>
    safeReply(ctx, message, addressPickerKeyboard(user))
  );
};

const sendSingleAddressPrompt = async (ctx, optionLabel, user, address) => {
  const shortAddr = shortText(address.text, 50);
  const contactName = address.name || user.name || "-";
  const contactPhone = address.phone || user.phone || "-";
  const message =
    "📍 Confirm delivery details\n" +
    `${optionLabel}\n\n` +
    `👤 ${contactName} · 📞 ${contactPhone}\n` +
    `📍 ${address.label}: ${shortAddr}\n\n` +
    "Tap ✅ Use this address to continue.";
  const prev = getSessionData(ctx.from.id) || {};
  await step("saveSession:singleAddress", () =>
    saveSessionData(ctx.from.id, {
      ...prev,
      state: FLOW_STATES.AWAIT_ADDRESS_PICK,
      selectedOption: optionLabel,
      temp: { ...(prev.temp || {}), mode: "single", addressId: address.id },
    })
  );
  await step("recordScreen:singleAddress", () =>
    recordScreen(ctx, SCREEN.ADDRESS_PICKER)
  );
  return step("reply:singleAddress", () =>
    safeReply(ctx, message, singleAddressKeyboard())
  );
};

const sendProfilePrompt = async (ctx, optionLabel) => {
  const message =
    "⚡ Faster checkout next time\n" +
    "Save your details once (name + phone + up to 4 addresses).\n\n" +
    "Want to set it up now?";
  await step("saveSession:profilePrompt", () =>
    saveSessionData(ctx.from.id, {
      state: FLOW_STATES.AWAIT_PROFILE_CHOICE,
      selectedOption: optionLabel,
      temp: {},
    })
  );
  await step("recordScreen:profilePrompt", () =>
    recordScreen(ctx, SCREEN.PROFILE_CREATE)
  );
  return step("reply:profilePrompt", () =>
    safeReply(ctx, message, profilePromptKeyboard())
  );
};

const sendSubtotalPrompt = async (ctx, label, addressText, suggestedSubtotal) => {
  const shortAddr = shortText(addressText, 50);
  const message =
    "💰 Subtotal (before tax/fees)\n" +
    `📍 ${label}: ${shortAddr}\n` +
    (Number.isFinite(suggestedSubtotal)
      ? `Last time: $${Number(suggestedSubtotal).toFixed(2)}\n`
      : "") +
    "\nSend a number (example: 55)";
  await step("recordScreen:subtotal", () =>
    recordScreen(ctx, SCREEN.SUBTOTAL)
  );
  return step("reply:subtotal", () =>
    safeReply(ctx, message, subtotalKeyboard())
  );
};

const sendConfirmCard = async (
  ctx,
  optionLabel,
  addressLabel,
  addressText,
  subtotal
) => {
  const addressLine = addressText || addressLabel || "Address";
  const message =
    "🧾 Review request\n" +
    `${optionLabel}\n\n` +
    `📍 ${addressLine}\n` +
    `💰 Subtotal: $${subtotal.toFixed(2)}\n\n` +
    "Tap ✅ Submit Order to open the request.";
  await step("recordScreen:confirm", () =>
    recordScreen(ctx, SCREEN.CONFIRM)
  );
  return step("reply:confirm", () =>
    safeReply(ctx, message, confirmOrderKeyboard())
  );
};

const sendQuickActions = async (ctx) => {
  const userId = ctx.from?.id;
  if (userId) {
    const session = getSessionData(userId);
    if (session && session.state !== FLOW_STATES.IDLE) {
      await saveSessionData(userId, {
        state: FLOW_STATES.IDLE,
        temp: {},
        nav: session.nav,
      });
    }
  }
  await recordScreen(ctx, SCREEN.QUICK_ACTIONS);
  return safeReply(ctx, "Menu", quickActionsKeyboard());
};

const promptOpenAnother = async (ctx, pending) => {
  const openCount = openTicketCountForChat(ctx.chat.id);
  if (openCount >= MAX_OPEN_TICKETS_PER_CHAT) {
    return safeReply(ctx, 
      "⚠️ You’ve reached the 4 active request limit.\nPlease close one to open a new request.",
      maxLimitKeyboard()
    );
  }

  await saveSessionData(ctx.from.id, {
    state: FLOW_STATES.AWAIT_NEW_TICKET_CONFIRM,
    temp: { pending: pending || { type: "menu" } },
  });

  if (pending?.type === "food" || pending?.type === "flight" || pending?.type === "hotel") {
    return safeReply(ctx, 
      `🎟 You already have ${openCount} active request(s).\nOpen a new request for ${pending.label}?`,
      openAnotherKeyboardService()
    );
  }

  return safeReply(ctx, 
    `🎟 You already have ${openCount} active request(s).\nWhat would you like to do?`,
    openAnotherKeyboardGeneric()
  );
};

const sendMyTickets = async (ctx) => {
  const ids = getOpenTicketIdsForChat(ctx.chat.id);
  if (!ids.length) {
    await safeReply(ctx, "No active tickets right now.");
    return sendMainMenu(ctx);
  }

  await saveSessionData(ctx.from.id, {
    state: FLOW_STATES.AWAIT_TICKET_PICK,
    temp: {},
  });

  await recordScreen(ctx, SCREEN.TICKETS);

  return safeReply(
    ctx,
    `🎟 Your active requests (${ids.length}/${MAX_OPEN_TICKETS_PER_CHAT})\nTap a ticket to view:`,
    ticketsKeyboard(ids)
  );
};

const sendSupportPrompt = async (ctx) => {
  await saveSessionData(ctx.from.id, {
    state: FLOW_STATES.AWAIT_SUPPORT,
    temp: {},
  });
  await recordScreen(ctx, SCREEN.SUPPORT);
  return safeReply(
    ctx,
    "🆘 Support\nDescribe your issue in one message.\nA human will reply."
  );
};

const showLastOrder = async (ctx) => {
  const user = getUser(ctx.from.id);
  const lastOrder = user?.lastOrder;
  if (!lastOrder) {
    await safeReply(ctx, "No recent orders yet.");
    return sendQuickActions(ctx);
  }
  const addresses = getAddresses(user);
  let address = null;
  if (lastOrder.addressId) {
    address = addresses.find((addr) => addr.id === lastOrder.addressId);
  }
  const label = address?.label || lastOrder.addressLabel || "Address";
  const addressText = address?.text || lastOrder.addressText || "";
  if (!addressText && addresses.length) {
    await saveSessionData(ctx.from.id, {
      state: FLOW_STATES.AWAIT_ADDRESS_PICK,
      selectedOption: lastOrder.option,
      temp: {
        lastOrder: {
          option: lastOrder.option,
          addressId: lastOrder.addressId || null,
          addressLabel: label,
          addressText: addressText,
          subtotal: Number(lastOrder.subtotal || 0),
        },
        suggestedSubtotal: Number(lastOrder.subtotal || 0),
      },
    });
    return sendAddressPicker(ctx, lastOrder.option, user);
  }
  const shortAddr = shortText(addressText, 50);
  const message =
    "🧾 Last order\n\n" +
    `${lastOrder.option}\n` +
    `📍 ${label}: ${shortAddr}\n` +
    `💰 Subtotal: $${Number(lastOrder.subtotal || 0).toFixed(2)}`;
  await saveSessionData(ctx.from.id, {
    state: FLOW_STATES.AWAIT_LAST_ORDER,
    selectedOption: lastOrder.option,
    temp: {
      lastOrder: {
        option: lastOrder.option,
        addressId: lastOrder.addressId || address?.id || null,
        addressLabel: label,
        addressText: addressText,
        subtotal: Number(lastOrder.subtotal || 0),
      },
    },
  });
  await recordScreen(ctx, SCREEN.LAST_ORDER);
  return safeReply(ctx, message, lastOrderKeyboard());
};

const sendManageList = async (ctx, user) => {
  const userId = ctx.from?.id;
  if (userId) {
    const existing = getSessionData(userId);
    const session =
      existing && typeof existing === "object"
        ? existing
        : { state: FLOW_STATES.IDLE, temp: {} };
    if (session.state !== FLOW_STATES.AWAIT_MANAGE_PICK) {
      await saveSessionData(userId, { ...session, state: FLOW_STATES.AWAIT_MANAGE_PICK });
    }
  }
  const message = "⚙️ Manage your saved addresses\n(select one)";
  await step("recordScreen:manageList", () =>
    recordScreen(ctx, SCREEN.MANAGE_ADDRESSES)
  );
  return step("reply:manageList", () =>
    safeReply(ctx, message, manageListKeyboard(user))
  );
};

const sendManageAddressCard = async (ctx, address) => {
  const contactName = address.name || "-";
  const contactPhone = address.phone || "-";
  const message =
    `📍 ${address.label}\n` +
    `${address.text}\n\n` +
    `👤 ${contactName}\n` +
    `📞 ${contactPhone}`;
  await step("recordScreen:manageCard", () =>
    recordScreen(ctx, SCREEN.MANAGE_ADDRESS)
  );
  return step("reply:manageCard", () =>
    safeReply(ctx, message, manageActionsKeyboard())
  );
};

const handleFoodOption = async (ctx, optionLabel, options = {}) => {
  const userId = ctx.from?.id;
  if (!userId) {
    return;
  }
  if (!options.bypassTicketPrompt && openTicketCountForChat(ctx.chat.id) > 0) {
    return promptOpenAnother(ctx, { type: "food", label: optionLabel, optionLabel });
  }
  const activeSession = getSessionData(userId);
  if (activeSession && activeSession.state && activeSession.state !== FLOW_STATES.IDLE) {
    await deleteSessionData(userId);
  }
  const user = getUser(userId);
  if (!user) {
    return sendProfilePrompt(ctx, optionLabel);
  }
  const addresses = getAddresses(user);
  if (addresses.length === 0) {
    await saveSessionData(ctx.from.id, {
      state: FLOW_STATES.AWAIT_ADD_NAME,
      selectedOption: optionLabel,
      temp: { returnTo: "picker" },
    });
    return safeReply(ctx, "👤 Name for this address?", backHomeKeyboard());
  }
  if (addresses.length === 1) {
    return sendSingleAddressPrompt(ctx, optionLabel, user, addresses[0]);
  }
  return sendAddressPicker(ctx, optionLabel, user);
};

const startFlightFlow = async (ctx, setSessionFn) => {
  setSessionFn(ctx.chat.id, {
    service: "flight",
    stage: "flight_questions",
    stepIndex: 0,
    answers: {},
  });
  await replyHtml(ctx, FLIGHT_PROMO);
  return replyHtml(ctx, FLIGHT_QUESTIONS[0].prompt);
};

const startHotelFlow = async (ctx, setSessionFn) => {
  setSessionFn(ctx.chat.id, {
    service: "hotel",
    stage: "hotel_questions",
    stepIndex: 0,
    answers: {},
  });
  await replyHtml(ctx, HOTEL_PROMO);
  return replyHtml(ctx, HOTEL_QUESTIONS[0].prompt);
};

const parseSubtotal = (text) => {
  if (!text) {
    return null;
  }
  const match = text.replace(/,/g, "").match(/(\d+(\.\d+)?)/);
  if (!match) {
    return null;
  }
  const value = Number(match[1]);
  return Number.isFinite(value) ? value : null;
};

const showProfile = async (ctx) => {
  const user = getUser(ctx.from.id);
  if (!user) {
    return safeReply(
      ctx,
      "No profile saved yet.\nTap a food option and choose Create Profile."
    );
  }
  const addresses = getAddresses(user);
  const addressCount = addresses.length;
  const defaultAddress = addresses.find(
    (addr) => addr.id === user.defaultAddressId
  );
  const contactName = defaultAddress?.name || user.name || "-";
  const contactPhone = defaultAddress?.phone || user.phone || "-";
  const message =
    `👤 ${contactName}\n` +
    `📞 ${contactPhone}\n` +
    `📍 Saved addresses: ${addressCount}/4`;
  await recordScreen(ctx, SCREEN.PROFILE_MENU);
  return safeReply(ctx, message, profileKeyboard());
};

const finalizeOrder = async (ctx, optionLabel, user, address, subtotal) => {
  if (!canOpenAnotherTicket(ctx.chat.id)) {
    await safeReply(ctx, 
      "⚠️ You’ve reached the 4 active request limit.\nPlease close one to open a new request.",
      Markup.keyboard([
        [Markup.button.text(BTN_MY_TICKETS), Markup.button.text(BTN_SUPPORT)],
        [Markup.button.text(BTN_HOME)],
      ]).resize()
    );
    return null;
  }
  const contactName = address.name || user.name || "-";
  const contactPhone = address.phone || user.phone || "-";
  const ticketId = nextTicketId();
  tickets.set(ticketId, {
    chatId: ctx.chat.id,
    category: optionLabel,
    answers: {
      name: contactName,
      phone: contactPhone,
      address: address.text,
      addressLabel: address.label,
      subtotal: subtotal,
    },
    status: "open",
    adminMessages: [],
    botKey: "food",
    service: "Food",
  });
  createTicketRecord(ticketId, {
    service: "Food",
    category: optionLabel,
    chatId: ctx.chat.id,
    botKey: "food",
    answers: {
      name: contactName,
      phone: contactPhone,
      address: address.text,
      addressLabel: address.label,
      subtotal: subtotal,
    },
  });
  addOpenTicketForChat(ctx.chat.id, ticketId);

  const summary = [
    `Option: ${optionLabel}`,
    `Name: ${contactName}`,
    `Phone: ${contactPhone}`,
    `Address: ${address.text}`,
    `Subtotal: $${subtotal.toFixed(2)}`,
  ].join("\n");
  const userTag = ctx.from.username ? `@${ctx.from.username}` : `ID ${ctx.from.id}`;
  const adminMessage =
    `New food order ticket #${ticketId}\n${summary}\nCustomer: ${userTag}\n\n` +
    "Reply to this message to chat with the customer.";

  adminChatIds.forEach((chatId) => {
    withTimeout(
      "admin.sendMessage",
      bot.telegram.sendMessage(chatId, adminMessage, adminTicketKeyboard(ticketId)),
      12000
    )
      .then((message) => {
        if (!message) {
          return;
        }
        adminTicketMessages.set(
          adminMessageKey(chatId, message.message_id),
          { ticketId, botKey: "food" }
        );
        const ticket = tickets.get(ticketId);
        if (ticket) {
          ticket.adminMessages.push({
            chatId,
            messageId: message.message_id,
          });
          tickets.set(ticketId, ticket);
        }
      })
      .catch((err) => logError(`food admin notify #${ticketId}`, err));
  });

  withTimeout("refreshWorkerLists", refreshWorkerLists(ctx.telegram), 12000)
    .catch((err) => logError("food refresh worker list", err));

  if (user) {
    const updatedUser = {
      ...user,
      lastOrder: {
        option: optionLabel,
        addressId: address.id || null,
        subtotal: subtotal,
        updatedAt: Date.now(),
      },
    };
    await saveUser(ctx.from.id, updatedUser);
  }

  await deleteSessionData(ctx.from.id);
  return ticketId;
};

const resolveSessionAddress = (user, session) => {
  if (!user || !session) {
    return null;
  }
  const addresses = getAddresses(user);
  if (session.selectedAddressId) {
    const match = addresses.find((addr) => addr.id === session.selectedAddressId);
    if (match) {
      return match;
    }
  }
  if (session.temp?.lastOrder) {
    return {
      id: session.temp.lastOrder.addressId || null,
      label: session.temp.lastOrder.addressLabel,
      text: session.temp.lastOrder.addressText,
    };
  }
  return null;
};

const renderScreen = async (ctx, screenId) => {
  const userId = ctx.from?.id;
  const session = userId
    ? getSessionData(userId) || { state: FLOW_STATES.IDLE, temp: {} }
    : null;
  const user = userId ? getUser(userId) : null;

  if (!session || !userId) {
    return sendMainMenu(ctx);
  }

  switch (screenId) {
    case SCREEN.HOME:
      return sendMainMenu(ctx);
    case SCREEN.QUICK_ACTIONS:
      return sendQuickActions(ctx);
    case SCREEN.PROFILE_MENU:
      await ensureFlowState(userId, FLOW_STATES.IDLE);
      return showProfile(ctx);
    case SCREEN.PROFILE_CREATE:
      if (session.selectedOption) {
        return sendProfilePrompt(ctx, session.selectedOption);
      }
      return sendMainMenu(ctx);
    case SCREEN.ADDRESS_PICKER: {
      await ensureFlowState(userId, FLOW_STATES.AWAIT_ADDRESS_PICK);
      if (!user || !session.selectedOption) {
        return sendMainMenu(ctx);
      }
      const addresses = getAddresses(user);
      if (session.temp?.mode === "single") {
        const addressId =
          session.temp?.addressId || session.selectedAddressId || user.defaultAddressId;
        const address = addresses.find((addr) => addr.id === addressId);
        if (address) {
          return sendSingleAddressPrompt(ctx, session.selectedOption, user, address);
        }
      }
      return sendAddressPicker(ctx, session.selectedOption, user);
    }
    case SCREEN.SUBTOTAL: {
      await ensureFlowState(userId, FLOW_STATES.AWAIT_SUBTOTAL);
      const address = resolveSessionAddress(user, session);
      if (!address) {
        return sendMainMenu(ctx);
      }
      if (!session.selectedOption) {
        return sendMainMenu(ctx);
      }
      return sendSubtotalPrompt(
        ctx,
        address.label,
        address.text,
        session.temp?.suggestedSubtotal ?? session.temp?.subtotal
      );
    }
    case SCREEN.CONFIRM: {
      await ensureFlowState(userId, FLOW_STATES.AWAIT_CONFIRM);
      const address = resolveSessionAddress(user, session);
      if (!address) {
        return sendMainMenu(ctx);
      }
      if (!session.selectedOption) {
        return sendMainMenu(ctx);
      }
      const subtotal = Number(session.temp?.subtotal || 0);
      if (!Number.isFinite(subtotal) || subtotal <= 0) {
        return sendSubtotalPrompt(
          ctx,
          address.label,
          address.text,
          session.temp?.suggestedSubtotal
        );
      }
      return sendConfirmCard(
        ctx,
        session.selectedOption,
        address.label,
        address.text,
        subtotal
      );
    }
    case SCREEN.MANAGE_ADDRESSES:
      if (user) {
        await ensureFlowState(userId, FLOW_STATES.AWAIT_MANAGE_PICK);
        return sendManageList(ctx, user);
      }
      return showProfile(ctx);
    case SCREEN.MANAGE_ADDRESS: {
      if (!user || !session.selectedAddressId) {
        await ensureFlowState(userId, FLOW_STATES.AWAIT_MANAGE_PICK);
        return sendManageList(ctx, user || { addresses: [] });
      }
      const address = getAddresses(user).find(
        (addr) => addr.id === session.selectedAddressId
      );
      if (address) {
        await ensureFlowState(userId, FLOW_STATES.AWAIT_MANAGE_ACTION);
        return sendManageAddressCard(ctx, address);
      }
      await ensureFlowState(userId, FLOW_STATES.AWAIT_MANAGE_PICK);
      return sendManageList(ctx, user);
    }
    case SCREEN.LAST_ORDER:
      return showLastOrder(ctx);
    case SCREEN.SUPPORT:
      return sendSupportPrompt(ctx);
    case SCREEN.TICKETS:
      await ensureFlowState(userId, FLOW_STATES.AWAIT_TICKET_PICK);
      return sendMyTickets(ctx);
    default:
      return sendMainMenu(ctx);
  }
};

const handleHome = async (ctx) => {
  await deleteSessionData(ctx.from.id);
  return sendMainMenu(ctx);
};

const handleBack = async (ctx) => {
  const userId = ctx.from.id;
  const session = getSessionData(userId) || { state: FLOW_STATES.IDLE, temp: {} };
  const nav = normalizeNav(session.nav);
  if (nav.length <= 1) {
    return handleHome(ctx);
  }
  nav.pop();
  await saveSessionData(userId, { ...session, nav });
  ctx.state = ctx.state || {};
  ctx.state.skipNav = true;
  const target = nav[nav.length - 1] || SCREEN.HOME;
  return renderScreen(ctx, target);
};

async function handleNewTicketConfirm(ctx, session, handlers) {
  const pending = session.temp?.pending;
  if (ctx.message?.text === BTN_MY_TICKETS) {
    return sendMyTickets(ctx);
  }
  if (ctx.message?.text === BTN_SUPPORT) {
    return sendSupportPrompt(ctx);
  }
  if (ctx.message?.text === BTN_CANCEL_GENERIC) {
    return sendMainMenu(ctx);
  }
  if (ctx.message?.text === BTN_OPEN_ANOTHER) {
    await saveSessionData(ctx.from.id, { state: FLOW_STATES.IDLE, temp: {} });
    return sendMainMenu(ctx);
  }
  if (ctx.message?.text === BTN_OPEN_THIS) {
    await saveSessionData(ctx.from.id, { state: FLOW_STATES.IDLE, temp: {} });
    if (pending?.type === "food" && pending.optionLabel && handlers?.startFood) {
      return handlers.startFood(pending.optionLabel);
    }
    if (pending?.type === "flight" && handlers?.startFlight) {
      return handlers.startFlight();
    }
    if (pending?.type === "hotel" && handlers?.startHotel) {
      return handlers.startHotel();
    }
    return sendMainMenu(ctx);
  }
  return promptOpenAnother(ctx, pending || { type: "menu" });
}

async function handleTicketPick(ctx) {
  const text = ctx.message?.text || "";
  const match = text.match(/Ticket\s*#(\d+)/i);
  if (!match) {
    return sendMyTickets(ctx);
  }
  const ticketId = Number(match[1]);
  const ticket = tickets.get(ticketId);
  if (!ticket || ticket.status !== "open" || ticket.chatId !== ctx.chat.id) {
    return safeReply(ctx, "That ticket is no longer active.");
  }
  const summary = formatTicketSummary(ticket);
  return safeReply(ctx, `🎟 Ticket #${ticketId}\n\n${summary}\n\nStatus: OPEN`);
}

const handleProfileFlow = async (ctx, session) => {
  const rawText =
    ctx.message && typeof ctx.message.text === "string" ? ctx.message.text : "";
  const text = rawText.trim();
  const userId = ctx.from.id;
  const user = getUser(userId) || { addresses: [] };

  if (text === BTN_HOME) {
    return handleHome(ctx);
  }
  if (text === BTN_BACK) {
    return handleBack(ctx);
  }

  if (text === BTN_PROFILE) {
    return showProfile(ctx);
  }
  if (text === BTN_MANAGE || text === BTN_MANAGE_ADDR) {
    await saveSessionData(userId, {
      ...session,
      state: FLOW_STATES.AWAIT_MANAGE_PICK,
      temp: {
        ...(session.temp || {}),
        returnTo: session.temp?.returnTo || "profile",
      },
    });
    return sendManageList(ctx, user);
  }
  if (text === BTN_MY_TICKETS) {
    return sendMyTickets(ctx);
  }

  switch (session.state) {
    case FLOW_STATES.AWAIT_PROFILE_CHOICE: {
      if (text === BTN_CREATE_PROFILE) {
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_NAME,
          temp: {},
        });
        return safeReply(ctx, "👤 Full name?", backHomeKeyboard());
      }
      if (text === BTN_SKIP) {
        await deleteSessionData(userId);
        return startLegacyFoodFlow(ctx, session.selectedOption);
      }
      if (text && text !== BTN_CREATE_PROFILE && text !== BTN_SKIP) {
        return safeReply(ctx, 
          "Tap 💾 Create Profile to save your details (or Skip for now).",
          profilePromptKeyboard()
        );
      }
      return sendProfilePrompt(ctx, session.selectedOption);
    }
    case FLOW_STATES.AWAIT_NEW_TICKET_CONFIRM: {
      return handleNewTicketConfirm(ctx, session, {
        startFood: (optionLabel) =>
          handleFoodOption(ctx, optionLabel, { bypassTicketPrompt: true }),
        startFlight: () => startFlightFlow(ctx, setSession),
        startHotel: () => startHotelFlow(ctx, setSession),
      });
    }
    case FLOW_STATES.AWAIT_TICKET_PICK: {
      return handleTicketPick(ctx);
    }
    case FLOW_STATES.AWAIT_NAME: {
      if (!text) {
        return safeReply(ctx, "👤 Full name?", backHomeKeyboard());
      }
      await saveSessionData(userId, {
        ...session,
        state: FLOW_STATES.AWAIT_PHONE,
        temp: { name: text },
      });
      return safeReply(ctx, 
        "📞 Your phone number? (US only)\nExample: 555-123-4567",
        backHomeKeyboard()
      );
    }
    case FLOW_STATES.AWAIT_PHONE: {
      if (!text) {
        return safeReply(ctx, 
          "📞 Your phone number? (US only)\nExample: 555-123-4567",
          backHomeKeyboard()
        );
      }
      await saveSessionData(userId, {
        ...session,
        state: FLOW_STATES.AWAIT_ADDRESS_TEXT,
        temp: { ...session.temp, phone: text },
      });
      return safeReply(ctx, 
        "📍 Delivery address (include Apt/Zip/Gate code if any)",
        backHomeKeyboard()
      );
    }
    case FLOW_STATES.AWAIT_ADDRESS_TEXT: {
      if (!text) {
        return safeReply(ctx, 
          "📍 Delivery address (include Apt/Zip/Gate code if any)",
          backHomeKeyboard()
        );
      }
      await saveSessionData(userId, {
        ...session,
        state: FLOW_STATES.AWAIT_ADDRESS_LABEL,
        temp: { ...session.temp, addressText: text },
      });
      return safeReply(ctx, "🏷️ Label this address:", addressLabelKeyboard());
    }
    case FLOW_STATES.AWAIT_ADDRESS_LABEL: {
      if (text === LABEL_CANCEL) {
        await deleteSessionData(userId);
        return sendMainMenu(ctx);
      }
      if (text === LABEL_CUSTOM) {
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_ADDRESS_LABEL_CUSTOM,
        });
        return safeReply(ctx, "Type a label (example: Hostel / Office2 / GF Home)", backHomeKeyboard());
      }
      const label = resolveLabelChoice(text, user);
      if (!label) {
        return safeReply(ctx, "🏷️ Label this address:", addressLabelKeyboard());
      }
      const addressId = createAddressId(user);
      const contactName = session.temp?.name || user.name || "-";
      const contactPhone = session.temp?.phone || user.phone || "-";
      const baseUser = getUser(userId) || {};
      const baseAddresses = Array.isArray(baseUser.addresses)
        ? baseUser.addresses
        : [];
      const newAddress = {
        id: addressId,
        label: label,
        text: session.temp?.addressText || "",
        name: contactName,
        phone: contactPhone,
      };
      const mergedAddresses = [...baseAddresses, newAddress].slice(0, 4);
      const updated = {
        ...baseUser,
        name: session.temp?.name || baseUser.name,
        phone: session.temp?.phone || baseUser.phone,
        addresses: mergedAddresses,
        defaultAddressId: baseUser.defaultAddressId || addressId,
      };
      await saveUser(userId, updated);
      await saveSessionData(userId, {
        ...session,
        state: FLOW_STATES.AWAIT_PROFILE_POST_SAVE,
        temp: {},
      });
      return safeReply(ctx, 
        "✅ Saved.\nYou can store up to 4 addresses.\nAdd another now?",
        profileSavedKeyboard(Boolean(session.selectedOption))
      );
    }
    case FLOW_STATES.AWAIT_ADDRESS_LABEL_CUSTOM: {
      if (!text) {
        return safeReply(ctx, "Type a label (example: Hostel / Office2 / GF Home)", backHomeKeyboard());
      }
      const addressId = createAddressId(user);
      const contactName = session.temp?.name || user.name || "-";
      const contactPhone = session.temp?.phone || user.phone || "-";
      const baseUser = getUser(userId) || {};
      const baseAddresses = Array.isArray(baseUser.addresses)
        ? baseUser.addresses
        : [];
      const newAddress = {
        id: addressId,
        label: text.trim(),
        text: session.temp?.addressText || "",
        name: contactName,
        phone: contactPhone,
      };
      const mergedAddresses = [...baseAddresses, newAddress].slice(0, 4);
      const updated = {
        ...baseUser,
        name: session.temp?.name || baseUser.name,
        phone: session.temp?.phone || baseUser.phone,
        addresses: mergedAddresses,
        defaultAddressId: baseUser.defaultAddressId || addressId,
      };
      await saveUser(userId, updated);
      await saveSessionData(userId, {
        ...session,
        state: FLOW_STATES.AWAIT_PROFILE_POST_SAVE,
        temp: {},
      });
      return safeReply(ctx, 
        "✅ Saved.\nYou can store up to 4 addresses.\nAdd another now?",
        profileSavedKeyboard(Boolean(session.selectedOption))
      );
    }
    case FLOW_STATES.AWAIT_PROFILE_POST_SAVE: {
      if (text === BTN_ADD_ADDRESS) {
        const existing = getAddresses(user);
        if (existing.length >= 4) {
          await saveSessionData(userId, {
            ...session,
            state: FLOW_STATES.AWAIT_MANAGE_PICK,
            temp: {
              ...(session.temp || {}),
              returnTo: session.selectedOption ? "picker" : "profile",
            },
          });
          await safeReply(ctx, 
            "⚠️ You already have 4 saved addresses.\nEdit or delete one to add a new address."
          );
          return sendManageList(ctx, user);
        }
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_ADD_NAME,
          temp: { returnTo: "order" },
        });
        return safeReply(ctx, "👤 Name for this address?", backHomeKeyboard());
      }
      if (text === "Continue Order") {
        if (!session.selectedOption) {
          await deleteSessionData(userId);
          return sendMainMenu(ctx);
        }
        const refreshed = getUser(userId);
        if (refreshed && getAddresses(refreshed).length) {
          if (getAddresses(refreshed).length === 1) {
            return sendSingleAddressPrompt(
              ctx,
              session.selectedOption,
              refreshed,
              getAddresses(refreshed)[0]
            );
          }
          return sendAddressPicker(ctx, session.selectedOption, refreshed);
        }
      }
      return safeReply(ctx, 
        "Choose an option to continue.",
        profileSavedKeyboard(Boolean(session.selectedOption))
      );
    }
    case FLOW_STATES.AWAIT_ADDRESS_PICK: {
      if (text === BTN_ADD_ADDRESS) {
        const existing = getAddresses(user);
        if (existing.length >= 4) {
          await saveSessionData(userId, {
            ...session,
            state: FLOW_STATES.AWAIT_MANAGE_PICK,
            temp: { ...(session.temp || {}), returnTo: "picker" },
          });
          await safeReply(ctx, 
            "⚠️ You already have 4 saved addresses.\nEdit or delete one to add a new address."
          );
          return sendManageList(ctx, user);
        }
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_ADD_NAME,
          temp: { returnTo: "picker" },
        });
        return safeReply(ctx, "👤 Name for this address?", backHomeKeyboard());
      }
      if (text === BTN_MANAGE || text === BTN_MANAGE_ADDR) {
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_MANAGE_PICK,
          temp: { returnTo: "picker" },
        });
        return sendManageList(ctx, user);
      }
      if (text === BTN_USE_DEFAULT || text === BTN_CHOOSE_ANOTHER) {
        const addresses = getAddresses(user);
        if (!addresses.length) {
          return safeReply(ctx, "No saved addresses yet.");
        }
        if (text === BTN_USE_DEFAULT) {
          const addr =
            addresses.find((addr) => addr.id === user.defaultAddressId) ||
            addresses[0];
          await saveSessionData(userId, {
            ...session,
            state: FLOW_STATES.AWAIT_SUBTOTAL,
            selectedAddressId: addr.id,
          });
          return sendSubtotalPrompt(
            ctx,
            addr.label,
            addr.text,
            session.temp?.suggestedSubtotal
          );
        }
        return sendAddressPicker(ctx, session.selectedOption, user);
      }
      const chosenLabel = stripButtonLabel(text);
      const address = findAddressByLabel(user, chosenLabel);
      if (!address) {
        return safeReply(ctx, "📍 Choose an address:", addressPickerKeyboard(user));
      }
      await saveSessionData(userId, {
        ...session,
        state: FLOW_STATES.AWAIT_SUBTOTAL,
        selectedAddressId: address.id,
      });
      return sendSubtotalPrompt(
        ctx,
        address.label,
        address.text,
        session.temp?.suggestedSubtotal
      );
    }
    case FLOW_STATES.AWAIT_ADD_NAME: {
      if (!text) {
        return safeReply(ctx, "👤 Name for this address?", backHomeKeyboard());
      }
      await saveSessionData(userId, {
        ...session,
        state: FLOW_STATES.AWAIT_ADD_PHONE,
        temp: { ...session.temp, addressName: text },
      });
      return safeReply(ctx, 
        "📞 Phone for this address? (US only)\nExample: 555-123-4567",
        backHomeKeyboard()
      );
    }
    case FLOW_STATES.AWAIT_ADD_PHONE: {
      if (!text) {
        return safeReply(ctx, 
          "📞 Phone for this address? (US only)\nExample: 555-123-4567",
          backHomeKeyboard()
        );
      }
      await saveSessionData(userId, {
        ...session,
        state: FLOW_STATES.AWAIT_ADD_ADDRESS_TEXT,
        temp: { ...session.temp, addressPhone: text },
      });
      return safeReply(ctx, 
        "📍 Send the new address\n(include Apt/Zip/Gate code)",
        backHomeKeyboard()
      );
    }
    case FLOW_STATES.AWAIT_ADD_ADDRESS_TEXT: {
      const existing = getAddresses(user);
      if (existing.length >= 4) {
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_MANAGE_PICK,
          temp: { ...(session.temp || {}), returnTo: session.temp?.returnTo || "profile" },
        });
        await safeReply(ctx, 
          "⚠️ You already have 4 saved addresses.\nEdit or delete one to add a new address."
        );
        return sendManageList(ctx, user);
      }
      if (!text) {
        return safeReply(ctx, 
          "📍 Send the new address\n(include Apt/Zip/Gate code)",
          backHomeKeyboard()
        );
      }
      await saveSessionData(userId, {
        ...session,
        state: FLOW_STATES.AWAIT_ADD_ADDRESS_LABEL,
        temp: { ...session.temp, addressText: text },
      });
      return safeReply(ctx, "🏷️ Label this address:", addressLabelKeyboard());
    }
    case FLOW_STATES.AWAIT_ADD_ADDRESS_LABEL: {
      if (text === LABEL_CANCEL) {
        await deleteSessionData(userId);
        return sendMainMenu(ctx);
      }
      if (text === LABEL_CUSTOM) {
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_ADD_ADDRESS_LABEL_CUSTOM,
        });
        return safeReply(ctx, "Type a label (example: Hostel / Office2 / GF Home)", backHomeKeyboard());
      }
      const label = resolveLabelChoice(text, user);
      if (!label) {
        return safeReply(ctx, "🏷️ Label this address:", addressLabelKeyboard());
      }
      const addressId = createAddressId(user);
      const contactName =
        session.temp?.addressName || session.temp?.name || user.name || "-";
      const contactPhone =
        session.temp?.addressPhone || session.temp?.phone || user.phone || "-";
      const addresses = getAddresses(user).concat({
        id: addressId,
        label: label,
        text: session.temp?.addressText || "",
        name: contactName,
        phone: contactPhone,
      }).slice(0, 4);
      const updated = {
        ...user,
        addresses,
        defaultAddressId: user.defaultAddressId || addressId,
      };
      await saveUser(userId, updated);
      const refreshed = getUser(userId);
      await saveSessionData(userId, {
        ...session,
        state: FLOW_STATES.AWAIT_ADDRESS_PICK,
      });
      await safeReply(ctx, "✅ Address added.\nPick an address to continue:");
      if (refreshed) {
        return sendAddressPicker(ctx, session.selectedOption, refreshed);
      }
      return sendMainMenu(ctx);
    }
    case FLOW_STATES.AWAIT_ADD_ADDRESS_LABEL_CUSTOM: {
      if (!text) {
        return safeReply(ctx, "Type a label (example: Hostel / Office2 / GF Home)", backHomeKeyboard());
      }
      const addressId = createAddressId(user);
      const contactName =
        session.temp?.addressName || session.temp?.name || user.name || "-";
      const contactPhone =
        session.temp?.addressPhone || session.temp?.phone || user.phone || "-";
      const addresses = getAddresses(user).concat({
        id: addressId,
        label: text.trim(),
        text: session.temp?.addressText || "",
        name: contactName,
        phone: contactPhone,
      }).slice(0, 4);
      const updated = {
        ...user,
        addresses,
        defaultAddressId: user.defaultAddressId || addressId,
      };
      await saveUser(userId, updated);
      const refreshed = getUser(userId);
      await saveSessionData(userId, {
        ...session,
        state: FLOW_STATES.AWAIT_ADDRESS_PICK,
      });
      await safeReply(ctx, "✅ Address added.\nPick an address to continue:");
      if (refreshed) {
        return sendAddressPicker(ctx, session.selectedOption, refreshed);
      }
      return sendMainMenu(ctx);
    }
    case FLOW_STATES.AWAIT_MANAGE_PICK: {
      if (text === BTN_MANAGE || text === BTN_MANAGE_ADDR) {
        return sendManageList(ctx, user);
      }
      if (text === BTN_ADD_ADDRESS) {
        const existing = getAddresses(user);
        if (existing.length >= 4) {
          await saveSessionData(userId, {
            ...session,
            state: FLOW_STATES.AWAIT_MANAGE_PICK,
            temp: { ...(session.temp || {}), returnTo: session.temp?.returnTo || "profile" },
          });
          await safeReply(ctx, 
            "⚠️ You already have 4 saved addresses.\nEdit or delete one to add a new address."
          );
          return sendManageList(ctx, user);
        }
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_ADD_NAME,
          temp: { returnTo: session.temp?.returnTo || "profile" },
        });
        return safeReply(ctx, "👤 Name for this address?", backHomeKeyboard());
      }
      const chosenLabel = stripButtonLabel(text);
      const address = findAddressByLabel(user, chosenLabel);
      if (!address) {
        return sendManageList(ctx, user);
      }
      await saveSessionData(userId, {
        ...session,
        state: FLOW_STATES.AWAIT_MANAGE_ACTION,
        selectedAddressId: address.id,
      });
      return sendManageAddressCard(ctx, address);
    }
    case FLOW_STATES.AWAIT_MANAGE_ACTION: {
      const address = getAddresses(user).find(
        (addr) => addr.id === session.selectedAddressId
      );
      if (!address) {
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_MANAGE_PICK,
        });
        return sendManageList(ctx, user);
      }
      if (text === BTN_SET_DEFAULT) {
        await saveUser(userId, { ...user, defaultAddressId: address.id });
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_MANAGE_PICK,
        });
        await safeReply(ctx, "✅ Default address updated.");
        return sendManageList(ctx, getUser(userId));
      }
      if (text === BTN_EDIT_ADDRESS) {
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_EDIT_ADDRESS,
        });
        return safeReply(ctx, "✏️ Send the updated address text:", backHomeKeyboard());
      }
      if (text === BTN_RENAME_LABEL) {
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_RENAME_LABEL,
        });
        return safeReply(ctx, "🏷️ Choose a new label:", addressLabelKeyboard());
      }
      if (text === BTN_DELETE_ADDRESS) {
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_DELETE_CONFIRM,
        });
        return safeReply(ctx, "🗑️ Delete this address?", deleteConfirmKeyboard());
      }
      return sendManageAddressCard(ctx, address);
    }
    case FLOW_STATES.AWAIT_EDIT_ADDRESS: {
      if (!text) {
        return safeReply(ctx, "✏️ Send the updated address text:", backHomeKeyboard());
      }
      const addresses = getAddresses(user).map((addr) =>
        addr.id === session.selectedAddressId
          ? { ...addr, text: text.trim() }
          : addr
      );
      await saveUser(userId, { ...user, addresses });
      await saveSessionData(userId, {
        ...session,
        state: FLOW_STATES.AWAIT_MANAGE_PICK,
      });
      await safeReply(ctx, "✅ Address updated.");
      return sendManageList(ctx, getUser(userId));
    }
    case FLOW_STATES.AWAIT_RENAME_LABEL: {
      if (text === LABEL_CANCEL) {
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_MANAGE_ACTION,
        });
        return sendManageAddressCard(
          ctx,
          getAddresses(user).find(
            (addr) => addr.id === session.selectedAddressId
          )
        );
      }
      if (text === LABEL_CUSTOM) {
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_RENAME_LABEL_CUSTOM,
        });
        return safeReply(ctx, "Type a label (example: Hostel / Office2 / GF Home)", backHomeKeyboard());
      }
      const label = resolveLabelChoice(text, user);
      if (!label) {
        return safeReply(ctx, "🏷️ Choose a new label:", addressLabelKeyboard());
      }
      const addresses = getAddresses(user).map((addr) =>
        addr.id === session.selectedAddressId
          ? { ...addr, label }
          : addr
      );
      await saveUser(userId, { ...user, addresses });
      await saveSessionData(userId, {
        ...session,
        state: FLOW_STATES.AWAIT_MANAGE_PICK,
      });
      await safeReply(ctx, "✅ Label updated.");
      return sendManageList(ctx, getUser(userId));
    }
    case FLOW_STATES.AWAIT_RENAME_LABEL_CUSTOM: {
      if (!text) {
        return safeReply(ctx, "Type a label (example: Hostel / Office2 / GF Home)", backHomeKeyboard());
      }
      const addresses = getAddresses(user).map((addr) =>
        addr.id === session.selectedAddressId
          ? { ...addr, label: text.trim() }
          : addr
      );
      await saveUser(userId, { ...user, addresses });
      await saveSessionData(userId, {
        ...session,
        state: FLOW_STATES.AWAIT_MANAGE_PICK,
      });
      await safeReply(ctx, "✅ Label updated.");
      return sendManageList(ctx, getUser(userId));
    }
    case FLOW_STATES.AWAIT_DELETE_CONFIRM: {
      if (text === BTN_DELETE_CANCEL) {
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_MANAGE_PICK,
        });
        return sendManageList(ctx, user);
      }
      if (text !== BTN_DELETE_CONFIRM) {
        return safeReply(ctx, "🗑️ Delete this address?", deleteConfirmKeyboard());
      }
      const addresses = getAddresses(user).filter(
        (addr) => addr.id !== session.selectedAddressId
      );
      const defaultAddressId =
        user.defaultAddressId === session.selectedAddressId
          ? addresses[0]?.id || null
          : user.defaultAddressId;
      await saveUser(userId, { ...user, addresses, defaultAddressId });
      await saveSessionData(userId, {
        ...session,
        state: FLOW_STATES.AWAIT_MANAGE_PICK,
      });
      await safeReply(ctx, "✅ Address deleted.");
      return sendManageList(ctx, getUser(userId));
    }
    case FLOW_STATES.AWAIT_SUBTOTAL: {
      if (text === BTN_CHANGE_ADDRESS) {
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_ADDRESS_PICK,
        });
        return sendAddressPicker(ctx, session.selectedOption, user);
      }
      if (text === BTN_MANAGE || text === BTN_MANAGE_ADDR) {
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_MANAGE_PICK,
          temp: { returnTo: "picker" },
        });
        return sendManageList(ctx, user);
      }
      const subtotal = parseSubtotal(text);
      if (subtotal === null) {
        return safeReply(ctx, 
          "💰 Send subtotal (before tax/fees)\nExample: 55",
          subtotalKeyboard()
        );
      }
      if (subtotal < 40) {
        return safeReply(ctx, 
          "⚠️ Min subtotal is $40.\nSend a new subtotal to continue.",
          subtotalKeyboard()
        );
      }
      if (subtotal > 100) {
        return safeReply(ctx, 
          "⚠️ Max subtotal is $100.\nSend a new subtotal to continue.",
          subtotalKeyboard()
        );
      }
      let address = null;
      if (session.selectedAddressId) {
        address = getAddresses(user).find(
          (addr) => addr.id === session.selectedAddressId
        );
      }
      if (!address && session.temp?.lastOrder) {
        address = {
          id: session.temp.lastOrder.addressId,
          label: session.temp.lastOrder.addressLabel,
          text: session.temp.lastOrder.addressText,
        };
      }
      if (!address) {
        await deleteSessionData(userId);
        return sendMainMenu(ctx);
      }
      await saveSessionData(userId, {
        ...session,
        state: FLOW_STATES.AWAIT_CONFIRM,
        temp: {
          ...session.temp,
          subtotal,
        },
      });
      return sendConfirmCard(
        ctx,
        session.selectedOption,
        address.label,
        address.text,
        subtotal
      );
    }
    case FLOW_STATES.AWAIT_CONFIRM: {
      let address = null;
      if (session.selectedAddressId) {
        address = getAddresses(user).find(
          (addr) => addr.id === session.selectedAddressId
        );
      }
      if (!address && session.temp?.lastOrder) {
        address = {
          id: session.temp.lastOrder.addressId,
          label: session.temp.lastOrder.addressLabel,
          text: session.temp.lastOrder.addressText,
        };
      }
      if (!address) {
        await deleteSessionData(userId);
        return sendMainMenu(ctx);
      }
      if (text === BTN_SUBMIT_ORDER) {
        const subtotal = Number(session.temp?.subtotal || 0);
        const ticketId = await finalizeOrder(
          ctx,
          session.selectedOption,
          user,
          address,
          subtotal
        );
        if (!ticketId) {
          return;
        }
        await safeReply(ctx, 
          "✅ Request opened\n⏳ Connecting you to a concierge…\n⏱ Typical wait: 1–5 min",
          Markup.removeKeyboard()
        );
        return;
      }
      if (text === BTN_EDIT_SUBTOTAL) {
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_SUBTOTAL,
        });
        return sendSubtotalPrompt(
          ctx,
          address.label,
          address.text,
          session.temp?.subtotal
        );
      }
      if (text === BTN_CHANGE_ADDRESS) {
        await saveSessionData(userId, {
          ...session,
          state: FLOW_STATES.AWAIT_ADDRESS_PICK,
        });
        return sendAddressPicker(ctx, session.selectedOption, user);
      }
      if (text === BTN_CANCEL_ORDER) {
        await deleteSessionData(userId);
        await safeReply(ctx, 
          "✅ Cancelled. No request was submitted.",
          Markup.removeKeyboard()
        );
        return sendQuickActions(ctx);
      }
      return sendConfirmCard(
        ctx,
        session.selectedOption,
        address.label,
        address.text,
        Number(session.temp?.subtotal || 0)
      );
    }
    case FLOW_STATES.AWAIT_LAST_ORDER: {
      const lastOrder = session.temp?.lastOrder;
      if (!lastOrder) {
        await deleteSessionData(userId);
        return sendQuickActions(ctx);
      }
      if (text === BTN_REORDER_SAME) {
        await saveSessionData(userId, {
          state: FLOW_STATES.AWAIT_SUBTOTAL,
          selectedOption: lastOrder.option,
          selectedAddressId: lastOrder.addressId || null,
          temp: { lastOrder: lastOrder, suggestedSubtotal: lastOrder.subtotal },
        });
        return sendSubtotalPrompt(
          ctx,
          lastOrder.addressLabel,
          lastOrder.addressText,
          lastOrder.subtotal
        );
      }
      if (text === BTN_REORDER_CHOOSE) {
        await saveSessionData(userId, {
          state: FLOW_STATES.AWAIT_ADDRESS_PICK,
          selectedOption: lastOrder.option,
          temp: { lastOrder: lastOrder, suggestedSubtotal: lastOrder.subtotal },
        });
        return sendAddressPicker(ctx, lastOrder.option, user);
      }
      if (text === BTN_CHANGE_SUBTOTAL) {
        await saveSessionData(userId, {
          state: FLOW_STATES.AWAIT_SUBTOTAL,
          selectedOption: lastOrder.option,
          selectedAddressId: lastOrder.addressId || null,
          temp: { lastOrder: lastOrder, suggestedSubtotal: lastOrder.subtotal },
        });
        return sendSubtotalPrompt(
          ctx,
          lastOrder.addressLabel,
          lastOrder.addressText,
          lastOrder.subtotal
        );
      }
      if (text === BTN_CANCEL_ORDER) {
        await deleteSessionData(userId);
        return sendQuickActions(ctx);
      }
      return showLastOrder(ctx);
    }
    case FLOW_STATES.AWAIT_SUPPORT: {
      if (!text) {
        return safeReply(ctx, "Describe your issue in one message.");
      }
      const userTag = ctx.from.username
        ? `@${ctx.from.username}`
        : `ID ${ctx.from.id}`;
      adminChatIds.forEach((chatId) => {
        bot.telegram
          .sendMessage(chatId, `🆘 Support request from ${userTag}\n${text}`)
          .catch((err) => logError("support notify", err));
      });
      await deleteSessionData(userId);
      await safeReply(ctx, "✅ Thanks! A human will reply shortly.");
      return sendQuickActions(ctx);
    }
    default:
      return null;
  }
};

const handleTicketFlow = async (ctx, handlers) => {
  const flowSession = getSessionData(ctx.from.id);
  if (!flowSession) {
    return false;
  }
  if (flowSession.state === FLOW_STATES.AWAIT_NEW_TICKET_CONFIRM) {
    await handleNewTicketConfirm(ctx, flowSession, handlers);
    return true;
  }
  if (flowSession.state === FLOW_STATES.AWAIT_TICKET_PICK) {
    await handleTicketPick(ctx);
    return true;
  }
  return false;
};

const confirmMenu = () =>
  Markup.inlineKeyboard([
    [
      Markup.button.callback("✅ Submit request", "confirm"),
      Markup.button.callback("✏️ Edit details", "edit"),
    ],
  ]);

const formatSummary = (serviceKey, answers) => {
  const service = SERVICES[serviceKey];
  return service.steps
    .map((step) => `${step.label}: ${answers[step.key] || "-"}`)
    .join("\n");
};

const formatFoodSummary = (session) => {
  return [
    `Category: ${session.foodCategory}`,
    `Name: ${session.answers.name || "-"}`,
    `Address: ${session.answers.address || "-"}`,
    `Phone: ${session.answers.phone || "-"}`,
  ].join("\n");
};

const formatFlightSummary = (session) => {
  return [
    `Trip Dates: ${session.answers.trip_dates || "-"}`,
    `Passenger Info: ${session.answers.passenger_form || "-"}`,
    `State: ${session.answers.residence || "-"}`,
    `Total Value: ${session.answers.order_total || "-"}`,
    `Airlines: ${session.answers.airlines || "-"}`,
  ].join("\n");
};

const formatHotelSummary = (session) => {
  return [
    `Destination: ${session.answers.destination || "-"}`,
    `Dates: ${session.answers.dates || "-"}`,
    `Budget: ${session.answers.budget || "-"}`,
    `Email: ${session.answers.email || "-"}`,
    `Booking.com: ${session.answers.booking_link || "-"}`,
    `Preferred Chain: ${session.answers.preferred_chain || "-"}`,
  ].join("\n");
};

const formatTicketSummary = (ticket) => {
  if (ticket.service === "Flights" || ticket.botKey === "flight") {
    return formatFlightSummary({ answers: ticket.answers || {} });
  }
  if (ticket.service === "Hotels" || ticket.botKey === "hotel") {
    return formatHotelSummary({ answers: ticket.answers || {} });
  }
  return formatFoodSummary({
    foodCategory: ticket.category || "-",
    answers: ticket.answers || {},
  });
};

const truncateLabel = (value, max = 42) => {
  if (!value) {
    return "";
  }
  if (value.length <= max) {
    return value;
  }
  return `${value.slice(0, max - 3).trim()}...`;
};

const ticketBrief = (ticket) => {
  const answers = ticket.answers || {};
  if (ticket.botKey === "flight" || ticket.service === "Flights") {
    const parts = [answers.trip_dates, answers.airlines].filter(Boolean);
    return parts.join(" · ") || "Flight";
  }
  if (ticket.botKey === "hotel" || ticket.service === "Hotels") {
    const parts = [answers.destination, answers.dates].filter(Boolean);
    return parts.join(" · ") || "Hotel";
  }
  if (ticket.botKey === "food" || ticket.service === "Food") {
    return ticket.category || "Food";
  }
  return ticket.category || ticket.service || "Order";
};

const formatTicketLine = (ticketId, ticket) => {
  const serviceLabel = escapeHtml(ticket.service || ticket.category || "Order");
  const assigned = escapeHtml(ticket.assignedAlias || "Unassigned");
  const logState = ticket.couponRequested ? "Log requested" : "No log";
  return `#${ticketId} ${serviceLabel} · ${assigned} · ${logState}`;
};

const completionThankYou = (ticketId) =>
  `Option A (premium + simple)\n✅ Order #${ticketId} completed\nThanks for choosing Allat50 — appreciate you! 🙌`;

const formatClosedLine = (ticketId, record) => {
  const profit = Number(record.profit) || 0;
  const duffCut = Number(record.duffCut) || profit * duffCutRate;
  const closedBy = escapeHtml(record.closedBy || "-");
  return `#${ticketId} Profit: $${profit.toFixed(2)} · Duff: $${duffCut.toFixed(
    2
  )} · ${closedBy}`;
};

const isAdminChat = (ctx) => adminChatIds.includes(ctx.chat.id);
const isDuffChat = (ctx) => duffChatIds.includes(ctx.chat.id);
const adminAlias = (ctx) =>
  adminAliases[ctx.chat.id] || ctx.from?.first_name || "Worker";

const adminMessageKey = (chatId, messageId) => `${chatId}:${messageId}`;
const duffMessageKey = (chatId, messageId) => `${chatId}:${messageId}`;

const adminTicketKeyboard = (ticketId, includeAccept = true) => {
  const rows = [];
  if (includeAccept) {
    rows.push([Markup.button.callback("✅ Accept order", `accept:${ticketId}`)]);
  }
  rows.push([
    Markup.button.callback("💳 Mark paid", `paid:${ticketId}`),
    Markup.button.callback("🛑 Close ticket", `close:${ticketId}`),
    Markup.button.callback("🚫 Ban customer", `ban:${ticketId}`),
  ]);
  rows.push([Markup.button.callback("📝 Request log", `coupon:${ticketId}`)]);
  return Markup.inlineKeyboard(rows);
};

const workerPanelKeyboard = () =>
  Markup.inlineKeyboard([
    [Markup.button.callback("📋 View open tickets", "worker:view")],
    [Markup.button.callback("✅ Close order", "worker:close")],
    [Markup.button.callback("✍️ Set alias", "worker:alias")],
  ]);

const workerListKeyboard = (openTickets = []) => {
  const rows = openTickets.map(([ticketId, ticket]) => {
    const summary = truncateLabel(ticketBrief(ticket)) || "Order";
    const label = `Accept #${ticketId} · ${summary}`;
    return [Markup.button.callback(label, `accept:${ticketId}`)];
  });
  rows.push([Markup.button.callback("⬅️ Back", "worker:panel")]);
  return Markup.inlineKeyboard(rows);
};

const nextTicketId = () => {
  ticketCounter += 1;
  saveConfig();
  return ticketCounter;
};

const createTicketRecord = (ticketId, data) => {
  ticketRecords.set(ticketId, {
    ticketId,
    status: "open",
    createdAt: new Date().toISOString(),
    ...data,
  });
  saveTicketRecords();
};

const closeTicketRecord = (ticketId, updates) => {
  const record = ticketRecords.get(ticketId);
  if (!record) {
    return false;
  }
  ticketRecords.set(ticketId, {
    ...record,
    status: "closed",
    closedAt: new Date().toISOString(),
    ...updates,
  });
  saveTicketRecords();
  return true;
};

const updateTicketRecord = (ticketId, updates) => {
  const record = ticketRecords.get(ticketId);
  if (!record) {
    return false;
  }
  ticketRecords.set(ticketId, { ...record, ...updates });
  saveTicketRecords();
  return true;
};

const summarizeReport = () => {
  const totals = {
    total: 0,
    open: 0,
    closedWithRemarks: 0,
    closedNoOrder: 0,
    banned: 0,
    profitTotal: 0,
    duffTotal: 0,
  };

  for (const record of ticketRecords.values()) {
    totals.total += 1;
    if (record.status === "open") {
      totals.open += 1;
      continue;
    }
    if (record.closeType === "admin_close") {
      totals.closedWithRemarks += 1;
      const profit = Number(record.profit) || 0;
      totals.profitTotal += profit;
      totals.duffTotal += Number(record.duffCut) || profit * duffCutRate;
      continue;
    }
    if (record.closeType === "banned") {
      totals.banned += 1;
      continue;
    }
    totals.closedNoOrder += 1;
  }

  return totals;
};

function getOpenTicketIdsForChat(chatId) {
  const ids = customerTickets.get(chatId);
  if (!Array.isArray(ids)) {
    return [];
  }
  return ids
    .map((id) => Number(id))
    .filter((id) => Number.isFinite(id))
    .filter((id) => {
      const ticket = tickets.get(id);
      return ticket && ticket.status === "open";
    });
}

function addOpenTicketForChat(chatId, ticketId) {
  const ids = getOpenTicketIdsForChat(chatId);
  if (!ids.includes(ticketId)) {
    ids.push(ticketId);
  }
  customerTickets.set(chatId, ids);
}

function removeOpenTicketForChat(chatId, ticketId) {
  const ids = getOpenTicketIdsForChat(chatId).filter((id) => id !== ticketId);
  if (ids.length) {
    customerTickets.set(chatId, ids);
  } else {
    customerTickets.delete(chatId);
  }
}

const openTicketCountForChat = (chatId) => getOpenTicketIdsForChat(chatId).length;

const canOpenAnotherTicket = (chatId) =>
  openTicketCountForChat(chatId) < MAX_OPEN_TICKETS_PER_CHAT;

const getOpenTicketByChat = (chatId) => {
  const ids = getOpenTicketIdsForChat(chatId);
  if (!ids.length) {
    return null;
  }
  const latest = Math.max(...ids);
  const ticket = tickets.get(latest);
  if (!ticket || ticket.status !== "open") {
    return null;
  }
  return { ticketId: latest, ticket };
};

const closeTicketsForChat = (chatId, closeType, closedBy) => {
  for (const [ticketId, ticket] of tickets.entries()) {
    if (ticket.chatId === chatId && ticket.status === "open") {
      ticket.status = "closed";
      tickets.set(ticketId, ticket);
      closeTicketRecord(ticketId, { closeType, closedBy });
      removeOpenTicketForChat(chatId, ticketId);
    }
  }
};

const forwardCustomerMessage = async (telegram, ctx, ticketId, ticket) => {
  const customerLabel = ctx.from.first_name || "Customer";
  const safeHeader = emphasizeHtml(
    `Customer (${escapeHtml(customerLabel)}) on ticket #${ticketId}`
  );
  const assignedId = ticket.assignedAdminId;
  let targets = [];
  if (assignedId) {
    targets =
      ticket.adminMessages?.filter((entry) => entry.chatId === assignedId) || [];
    if (targets.length === 0) {
      targets = [{ chatId: assignedId }];
    }
  } else {
    targets =
      ticket.adminMessages?.length > 0
        ? ticket.adminMessages
        : adminChatIds.map((chatId) => ({ chatId }));
  }

  for (const target of targets) {
    if (!target?.chatId) {
      continue;
    }
    const replyId = target.messageId;
    if (ctx.message.text) {
      try {
        const payload = { parse_mode: "HTML" };
        if (replyId) {
          payload.reply_to_message_id = replyId;
        }
        const message = await telegram.sendMessage(
          target.chatId,
          `${safeHeader} ${escapeHtml(ctx.message.text)}`,
          payload
        );
        adminTicketMessages.set(
          adminMessageKey(target.chatId, message.message_id),
          { ticketId, botKey: ticket.botKey }
        );
      } catch (_) {
        if (replyId) {
          try {
            const payload = { parse_mode: "HTML" };
            const message = await telegram.sendMessage(
              target.chatId,
              `${safeHeader} ${escapeHtml(ctx.message.text)}`,
              payload
            );
            adminTicketMessages.set(
              adminMessageKey(target.chatId, message.message_id),
              { ticketId, botKey: ticket.botKey }
            );
          } catch (_) {}
        }
      }
      continue;
    }

    try {
      const payload = { parse_mode: "HTML" };
      if (replyId) {
        payload.reply_to_message_id = replyId;
      }
      const headerMessage = await telegram.sendMessage(
        target.chatId,
        safeHeader,
        payload
      );
      adminTicketMessages.set(
        adminMessageKey(target.chatId, headerMessage.message_id),
        { ticketId, botKey: ticket.botKey }
      );
      const copiedMessage = await telegram.copyMessage(
        target.chatId,
        ctx.chat.id,
        ctx.message.message_id,
        replyId ? { reply_to_message_id: replyId } : undefined
      );
      adminTicketMessages.set(
        adminMessageKey(target.chatId, copiedMessage.message_id),
        { ticketId, botKey: ticket.botKey }
      );
    } catch (_) {
      if (replyId) {
        try {
          const payload = { parse_mode: "HTML" };
          const headerMessage = await telegram.sendMessage(
            target.chatId,
            safeHeader,
            payload
          );
          adminTicketMessages.set(
            adminMessageKey(target.chatId, headerMessage.message_id),
            { ticketId, botKey: ticket.botKey }
          );
          const copiedMessage = await telegram.copyMessage(
            target.chatId,
            ctx.chat.id,
            ctx.message.message_id
          );
          adminTicketMessages.set(
            adminMessageKey(target.chatId, copiedMessage.message_id),
            { ticketId, botKey: ticket.botKey }
          );
        } catch (_) {}
      }
    }
  }
};

const scheduleSessionTimeout = (chatId, session) => {
  if (!sessionTimeoutMs) {
    return;
  }
  if (session.timeoutId) {
    clearTimeout(session.timeoutId);
  }
  const lastActive = Date.now();
  session.lastActive = lastActive;
  session.timeoutId = setTimeout(() => {
    const current = sessions.get(chatId);
    if (!current || current.lastActive !== lastActive) {
      return;
    }
    sessions.delete(chatId);
    bot.telegram.sendMessage(
      chatId,
      "⏰ <b>Session timed out</b> due to inactivity.\nSend /start to begin again.",
      { parse_mode: "HTML" }
    );
  }, sessionTimeoutMs);
};

const createSessionStore = (telegram) => {
  const store = new Map();
  const scheduleTimeout = (chatId, session) => {
    if (!sessionTimeoutMs) {
      return;
    }
    if (session.timeoutId) {
      clearTimeout(session.timeoutId);
    }
    const lastActive = Date.now();
    session.lastActive = lastActive;
    session.timeoutId = setTimeout(() => {
      const current = store.get(chatId);
      if (!current || current.lastActive !== lastActive) {
        return;
      }
      store.delete(chatId);
      telegram.sendMessage(
        chatId,
        "⏰ <b>Session timed out</b> due to inactivity.\nSend /start to begin again.",
        { parse_mode: "HTML" }
      );
    }, sessionTimeoutMs);
  };

  const setSession = (chatId, session) => {
    store.set(chatId, session);
    scheduleTimeout(chatId, session);
  };

  const resetSession = (chatId) => {
    const session = store.get(chatId);
    if (session?.timeoutId) {
      clearTimeout(session.timeoutId);
    }
    store.delete(chatId);
  };

  return { sessions: store, setSession, resetSession };
};

const replyHtml = (ctx, text, extra = {}) =>
  safeReplyHtml(ctx, text, extra);

const sendHome = async (ctx, caption, keyboard) => {
  if (LOGO_BYTES) {
    try {
      const sent = await safeReplyPhoto(
        ctx,
        { source: LOGO_BYTES },
        { caption, parse_mode: "HTML", ...keyboard }
      );
      if (sent) {
        return sent;
      }
    } catch (_) {
      return replyHtml(ctx, caption, keyboard);
    }
  }
  return replyHtml(ctx, caption, keyboard);
};

const saveConfig = () => {
  config.adminChatIds = adminChatIds;
  config.duffChatIds = duffChatIds;
  config.adminAliases = adminAliases;
  config.bannedChatIds = Array.from(bannedChatIds);
  config.ticketCounter = ticketCounter;
  delete config.ticketRecords;
  fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
};

const isBannedChat = (chatId) => bannedChatIds.has(chatId);

const banGuard = async (ctx, next) => {
  if (isAdminChat(ctx) || isDuffChat(ctx)) {
    return next();
  }
  const chatId = ctx.chat?.id;
  if (chatId && isBannedChat(chatId)) {
    if (ctx.callbackQuery) {
      try {
        await ctx.answerCbQuery("Access restricted.");
      } catch (_) {}
    }
    await replyHtml(ctx, "🚫 <b>Access restricted.</b>");
    return;
  }
  return next();
};

bot.use(banGuard);

const setSession = (chatId, session) => {
  sessions.set(chatId, session);
  scheduleSessionTimeout(chatId, session);
};

const resetSession = (chatId) => {
  const session = sessions.get(chatId);
  if (session?.timeoutId) {
    clearTimeout(session.timeoutId);
  }
  sessions.delete(chatId);
};

const checkRateLimit = (userId) => {
  if (!rateLimitEnabled) {
    return { limited: false };
  }
  const now = Date.now();
  const windowStart = now - rateLimitWindowMs;
  const history = (ticketHistory.get(userId) || []).filter(
    (timestamp) => timestamp >= windowStart
  );
  if (history.length >= rateLimitMaxTickets) {
    const retryAfterMs = history[0] + rateLimitWindowMs - now;
    ticketHistory.set(userId, history);
    return { limited: true, retryAfterMs };
  }
  history.push(now);
  ticketHistory.set(userId, history);
  return { limited: false };
};

bot.start((ctx) => {
  if (openTicketCountForChat(ctx.chat.id) > 0) {
    return promptOpenAnother(ctx, { type: "menu" });
  }
  resetSession(ctx.chat.id);
  saveSessionData(ctx.from.id, {
    state: FLOW_STATES.IDLE,
    temp: {},
    nav: [SCREEN.HOME],
  }).catch((err) => logError("clear profile session", err));
  return sendHome(
    ctx,
    "👋 Allat50 Foodbot\n\n" +
      "🍔 Food • ✈️ Flights • 🏨 Hotels\n\n" +
      "🟢 Online — real agents only\n" +
      "⏱ Avg response: 1-5 mins\n" +
      "💸 Savings: easy 50%\n\n" +
    "👇 Pick a service to begin",
    mainMenu()
  );
});

bot.command("help", (ctx) =>
  replyHtml(
    ctx,
    "ℹ️ <b>How it works</b>\nChoose a service and answer each question.\nSend /start to begin, /cancel to reset."
  )
);

bot.command("cancel", (ctx) => {
  resetSession(ctx.chat.id);
  deleteSessionData(ctx.from.id).catch((err) =>
    logError("clear profile session", err)
  );
  return replyHtml(
    ctx,
    "🛑 <b>Canceled.</b> Send /start when you're ready."
  );
});


bot.command("report", (ctx) => {
  if (!isAdminChat(ctx)) {
    return;
  }
  const totals = summarizeReport();
  const report =
    "📊 <b>Ticket Report</b>\n" +
    `Total created: <b>${totals.total}</b>\n` +
    `Open: <b>${totals.open}</b>\n` +
    `Closed w/ remarks: <b>${totals.closedWithRemarks}</b>\n` +
    `Closed no order: <b>${totals.closedNoOrder}</b>\n` +
    `Banned: <b>${totals.banned}</b>\n` +
    `Profit total: <b>$${totals.profitTotal.toFixed(2)}</b>\n` +
    `Duff 25%: <b>$${totals.duffTotal.toFixed(2)}</b>\n` +
    `Last ticket #: <b>${ticketCounter}</b>`;
  return replyHtml(ctx, report);
});

const parseCloseInput = (text, hasCommand = false) => {
  const trimmed = text.trim();
  const payload = hasCommand ? trimmed.replace(/^\/close\s+/i, "") : trimmed;
  const match = payload.match(/^(\d+)\s+([\s\S]+)$/);
  if (!match) {
    return null;
  }
  const ticketId = Number(match[1]);
  let remarks = match[2].trim();
  let profit = 0;
  if (
    (remarks.startsWith('"') && remarks.endsWith('"')) ||
    (remarks.startsWith("'") && remarks.endsWith("'"))
  ) {
    remarks = remarks.slice(1, -1).trim();
  } else {
    const profitMatch = remarks.match(/^([+-]?\d+(?:\.\d+)?)\s*(.*)$/);
    if (profitMatch) {
      profit = Number(profitMatch[1]);
      remarks = profitMatch[2].trim() || `Profit recorded: $${profit.toFixed(2)}`;
    }
  }
  if (!Number.isFinite(ticketId) || !remarks) {
    return null;
  }
  return { ticketId, profit, remarks };
};

const closeTicketWithValues = (ctx, ticketId, profit, remarks) => {
  const ticket = tickets.get(ticketId);
  if (ticket && ticket.status === "closed") {
    return safeReply(ctx, "Ticket already closed.");
  }

  const duffCut = Number((profit * duffCutRate).toFixed(2));
  const updated = closeTicketRecord(ticketId, {
    closeType: "admin_close",
    remarks,
    closedBy: adminAlias(ctx),
    profit,
    duffCut,
  });

  if (!updated) {
    return safeReply(ctx, "Ticket not found.");
  }

  const record = ticketRecords.get(ticketId);
  const chatId = ticket?.chatId || record?.chatId;
  if (ticket) {
    ticket.status = "closed";
    tickets.set(ticketId, ticket);
  }
  if (chatId) {
    removeOpenTicketForChat(chatId, ticketId);
    ctx.telegram
      .sendMessage(chatId, completionThankYou(ticketId), { parse_mode: "HTML" })
      .catch(() => {});
  }

  if (duffChatIds.length > 0) {
    duffChatIds.forEach((duffId) => {
      ctx.telegram
        .sendMessage(
          duffId,
          `✅ Ticket #${ticketId} closed.\nProfit: $${profit.toFixed(
            2
          )}\nDuff 25%: $${duffCut.toFixed(2)}`
        )
        .catch(() => {});
    });
  }

  refreshWorkerLists(ctx.telegram);
  return safeReply(ctx, 
    `Ticket #${ticketId} closed. Profit: $${profit.toFixed(
      2
    )} · Duff: $${duffCut.toFixed(2)}`
  );
};

bot.command("close", (ctx) => {
  if (!isAdminChat(ctx)) {
    return;
  }
  const parsed = parseCloseInput(ctx.message.text, true);
  if (!parsed) {
    return safeReply(ctx, 'Usage: /close <ticket_id> <profit> "remarks"');
  }
  return closeTicketWithValues(ctx, parsed.ticketId, parsed.profit, parsed.remarks);
});

bot.command("ban", (ctx) => {
  if (!isAdminChat(ctx)) {
    return;
  }
  const parts = ctx.message.text.trim().split(/\s+/);
  const chatId = Number(parts[1]);
  if (!Number.isFinite(chatId)) {
    return safeReply(ctx, "Usage: /ban <chat_id>");
  }
  bannedChatIds.add(chatId);
  closeTicketsForChat(chatId, "banned", adminAlias(ctx));
  saveConfig();
  return safeReply(ctx, `Chat ${chatId} banned.`);
});

bot.command("unban", (ctx) => {
  if (!isAdminChat(ctx)) {
    return;
  }
  const parts = ctx.message.text.trim().split(/\s+/);
  const chatId = Number(parts[1]);
  if (!Number.isFinite(chatId)) {
    return safeReply(ctx, "Usage: /unban <chat_id>");
  }
  if (!bannedChatIds.has(chatId)) {
    return safeReply(ctx, "Chat is not banned.");
  }
  bannedChatIds.delete(chatId);
  saveConfig();
  return safeReply(ctx, `Chat ${chatId} unbanned.`);
});

const registerAdminReplyHandler = (botInstance, botKey) => {
  botInstance.on("message", async (ctx, next) => {
    if (!isAdminChat(ctx)) {
      return next();
    }

    const reply = ctx.message?.reply_to_message;
    if (!reply) {
      return next();
    }

    const entry = adminTicketMessages.get(
      adminMessageKey(ctx.chat.id, reply.message_id)
    );
    if (!entry || entry.botKey !== botKey) {
      return next();
    }

    const ticket = tickets.get(entry.ticketId);
    if (!ticket || ticket.status !== "open") {
      await safeReply(ctx, "Ticket is closed or no longer exists.");
      return;
    }

    const adminLabel = adminAlias(ctx);

    if (ctx.message.text) {
      await ctx.telegram.sendMessage(
        ticket.chatId,
        `${emphasizeHtml(escapeHtml(adminLabel))}: ${escapeHtml(
          ctx.message.text
        )}`,
        { parse_mode: "HTML" }
      );
      return;
    }

    await ctx.telegram.sendMessage(
      ticket.chatId,
      `${emphasizeHtml(escapeHtml(adminLabel))} sent:`,
      { parse_mode: "HTML" }
    );
    return ctx.telegram.copyMessage(
      ticket.chatId,
      ctx.chat.id,
      ctx.message.message_id
    );
  });
};

const registerDuffReplyHandler = (botInstance) => {
  botInstance.on("message", async (ctx, next) => {
    if (!isDuffChat(ctx)) {
      return next();
    }

    const reply = ctx.message?.reply_to_message;
    if (!reply) {
      return next();
    }

    const entry = duffRequestMessages.get(
      duffMessageKey(ctx.chat.id, reply.message_id)
    );
    if (!entry) {
      return next();
    }

    const ticket = tickets.get(entry.ticketId);
    if (!ticket || ticket.status !== "open") {
      await safeReply(ctx, "Ticket is closed or no longer exists.");
      return;
    }

    const adminId = ticket.assignedAdminId;
    if (!adminId) {
      await safeReply(ctx, "No worker assigned yet. Ask them to /accept first.");
      return;
    }

    const logText = ctx.message?.text;
    if (!logText) {
      await safeReply(ctx, "Please send log text.");
      return;
    }

    const duffName = ctx.from?.first_name || "Duff";
    ticket.couponProvidedBy = duffName;
    ticket.couponProvidedAt = new Date().toISOString();
    ticket.couponText = logText;
    tickets.set(entry.ticketId, ticket);
    updateTicketRecord(entry.ticketId, {
      couponProvidedBy: duffName,
      couponProvidedAt: ticket.couponProvidedAt,
    });

    const safeLog = escapeHtml(logText);
    await ctx.telegram.sendMessage(
      adminId,
      `${emphasizeHtml(`Log for ticket #${entry.ticketId}`)}\n<code>${safeLog}</code>`,
      { parse_mode: "HTML" }
    );
    await safeReply(ctx, emphasizeHtml("Log sent to the worker."), {
      parse_mode: "HTML",
    });
  });
};

const workPanel = (ctx) => {
  if (!isAdminChat(ctx)) {
    return;
  }
  const alias = escapeHtml(adminAlias(ctx));
  const lines = [
    "👷 <b>Worker Panel</b>",
    `Alias: <b>${alias}</b>`,
    "",
    "Choose an action below:",
  ];
  return replyHtml(ctx, lines.join("\n"), workerPanelKeyboard());
};

const workerPanelAction = async (ctx) => {
  if (!isAdminChat(ctx)) {
    await ctx.answerCbQuery("Not authorized.");
    return;
  }
  const alias = escapeHtml(adminAlias(ctx));
  const text = `👷 <b>Worker Panel</b>\nAlias: <b>${alias}</b>\n\nChoose an action below:`;
  await ctx.answerCbQuery();
  try {
    await ctx.editMessageText(text, {
      parse_mode: "HTML",
      reply_markup: workerPanelKeyboard().reply_markup,
    });
  } catch (_) {
    await safeReply(ctx, text, { parse_mode: "HTML", reply_markup: workerPanelKeyboard().reply_markup });
  }
};

const workerViewAction = async (ctx) => {
  if (!isAdminChat(ctx)) {
    await ctx.answerCbQuery("Not authorized.");
    return;
  }
  const openTickets = Array.from(tickets.entries()).filter(
    ([, ticket]) => ticket.status === "open" && !ticket.assignedAdminId
  );
  const text = openTickets.length
    ? "📋 <b>Open tickets</b>\nTap a ticket to accept."
    : "📋 <b>Open tickets</b>\nNo unassigned tickets right now.";
  await ctx.answerCbQuery();
  try {
    await ctx.editMessageText(text, {
      parse_mode: "HTML",
      reply_markup: workerListKeyboard(openTickets).reply_markup,
    });
    workerListMessages.set(ctx.chat.id, ctx.callbackQuery.message.message_id);
  } catch (_) {
    const sent = await safeReply(ctx, text, {
      parse_mode: "HTML",
      reply_markup: workerListKeyboard(openTickets).reply_markup,
    });
    workerListMessages.set(ctx.chat.id, sent.message_id);
  }
};

const workerCloseAction = async (ctx) => {
  if (!isAdminChat(ctx)) {
    await ctx.answerCbQuery("Not authorized.");
    return;
  }
  adminPrompts.set(ctx.chat.id, { action: "close" });
  await ctx.answerCbQuery();
  await safeReply(ctx, 'Send: <code>&lt;ticket_id&gt; &lt;profit&gt; "remarks"</code>', {
    parse_mode: "HTML",
  });
};

const workerAliasAction = async (ctx) => {
  if (!isAdminChat(ctx)) {
    await ctx.answerCbQuery("Not authorized.");
    return;
  }
  adminPrompts.set(ctx.chat.id, { action: "alias" });
  await ctx.answerCbQuery();
  await safeReply(ctx, "Send your new alias.");
};

const adminPromptHandler = async (ctx, next) => {
  if (!isAdminChat(ctx)) {
    return next();
  }
  const flowSession = getSessionData(ctx.from.id);
  const legacySession = sessions.get(ctx.chat.id);
  if ((flowSession && flowSession.state !== FLOW_STATES.IDLE) || legacySession) {
    return next();
  }
  const prompt = adminPrompts.get(ctx.chat.id);
  if (!prompt) {
    return next();
  }
  const reply = ctx.message?.reply_to_message;
  if (reply) {
    const key = adminMessageKey(ctx.chat.id, reply.message_id);
    if (adminTicketMessages.has(key)) {
      return next();
    }
  }
  if (prompt.action === "alias") {
    const alias = ctx.message.text.trim();
    if (!alias) {
      await safeReply(ctx, "Alias cannot be empty.");
      return;
    }
    adminAliases[ctx.chat.id] = alias;
    saveConfig();
    adminPrompts.delete(ctx.chat.id);
    await safeReply(ctx, `Alias set to: ${alias}`);
    return;
  }
  if (prompt.action === "close") {
    const parsed = parseCloseInput(ctx.message.text, false);
    if (!parsed) {
      await safeReply(ctx, 'Usage: <ticket_id> <profit> "remarks"');
      return;
    }
    adminPrompts.delete(ctx.chat.id);
    await closeTicketWithValues(ctx, parsed.ticketId, parsed.profit, parsed.remarks);
  }
};

const setnameCommand = (ctx) => {
  if (!isAdminChat(ctx)) {
    return;
  }
  const parts = ctx.message.text.trim().split(/\s+/);
  parts.shift();
  const alias = parts.join(" ").trim();
  if (!alias) {
    return safeReply(ctx, "Usage: /setname <alias>");
  }
  adminAliases[ctx.chat.id] = alias;
  saveConfig();
  return safeReply(ctx, `Alias set to: ${alias}`);
};

const assignTicket = (ticketId, adminId, alias) => {
  const ticket = tickets.get(ticketId);
  if (!ticket || ticket.status !== "open") {
    return { ok: false, reason: "not_found" };
  }
  if (ticket.assignedAdminId && ticket.assignedAdminId !== adminId) {
    return { ok: false, reason: "assigned" };
  }
  ticket.assignedAdminId = adminId;
  ticket.assignedAlias = alias;
  ticket.acceptedAt = new Date().toISOString();
  tickets.set(ticketId, ticket);
  updateTicketRecord(ticketId, {
    assignedAdminId: adminId,
    assignedAlias: alias,
    acceptedAt: ticket.acceptedAt,
  });
  return { ok: true, ticket };
};

const updateAdminTicketMessages = async (ticketId, acceptedChatId, telegram) => {
  const ticket = tickets.get(ticketId);
  if (!ticket || !ticket.adminMessages) {
    return;
  }
  for (const entry of ticket.adminMessages) {
    if (!entry?.chatId || !entry?.messageId) {
      continue;
    }
    if (entry.chatId !== acceptedChatId) {
      try {
        await telegram.deleteMessage(entry.chatId, entry.messageId);
        continue;
      } catch (_) {}
    }
    try {
      await telegram.editMessageReplyMarkup(
        entry.chatId,
        entry.messageId,
        undefined,
        adminTicketKeyboard(ticketId, false).reply_markup
      );
    } catch (_) {}
  }
  ticket.adminMessages = ticket.adminMessages.filter(
    (entry) => entry.chatId === acceptedChatId
  );
  tickets.set(ticketId, ticket);
};

const refreshWorkerLists = async (telegram) => {
  const openTickets = Array.from(tickets.entries()).filter(
    ([, ticket]) => ticket.status === "open" && !ticket.assignedAdminId
  );
  for (const [chatId, messageId] of workerListMessages.entries()) {
    const text = openTickets.length
      ? "📋 <b>Open tickets</b>\nTap a ticket to accept."
      : "📋 <b>Open tickets</b>\nNo unassigned tickets right now.";
    try {
      await telegram.editMessageText(chatId, messageId, undefined, text, {
        parse_mode: "HTML",
        reply_markup: workerListKeyboard(openTickets).reply_markup,
      });
    } catch (_) {}
  }
};

const acceptCommand = (ctx) => {
  if (!isAdminChat(ctx)) {
    return;
  }
  const parts = ctx.message.text.trim().split(/\s+/);
  const ticketId = Number(parts[1]);
  if (!Number.isFinite(ticketId)) {
    return safeReply(ctx, "Usage: /accept <ticket_id>");
  }
  const alias = adminAlias(ctx);
  const result = assignTicket(ticketId, ctx.chat.id, alias);
  if (!result.ok) {
    return safeReply(ctx, 
      result.reason === "assigned"
        ? "Ticket already accepted by another worker."
        : "Ticket not found or already closed."
    );
  }
  updateAdminTicketMessages(ticketId, ctx.chat.id, ctx.telegram);
  refreshWorkerLists(ctx.telegram);
  return safeReply(ctx, `Ticket #${ticketId} accepted as ${alias}.`);
};

const duffPanel = (ctx) => {
  if (!isDuffChat(ctx)) {
    return;
  }
  const openTickets = Array.from(tickets.entries()).filter(
    ([, ticket]) => ticket.status === "open"
  );
  const live = openTickets
    .filter(([, ticket]) => ticket.couponRequested)
    .map(([ticketId, ticket]) => formatTicketLine(ticketId, ticket));
  const pending = openTickets
    .filter(([, ticket]) => !ticket.couponRequested)
    .map(([ticketId, ticket]) => formatTicketLine(ticketId, ticket));
  const closedRecords = Array.from(ticketRecords.entries())
    .filter(([, record]) => record.status === "closed" && record.closeType === "admin_close")
    .sort((a, b) => b[0] - a[0]);
  const totals = summarizeReport();

  const lines = [
    "🧩 <b>Duff Panel</b>",
    `🟢 Live requests: <b>${live.length}</b>`,
    `🟡 Open tickets: <b>${openTickets.length}</b>`,
    `💰 Profit total: <b>$${totals.profitTotal.toFixed(2)}</b>`,
    `🧮 Duff 25%: <b>$${totals.duffTotal.toFixed(2)}</b>`,
    "",
    "Commands:",
    "• <code>/log &lt;ticket_id&gt; &lt;log text&gt;</code>",
    "• Reply to a log request to send the log",
    "",
  ];

  if (live.length) {
    lines.push("🟢 <b>Live log requests</b>");
    lines.push(...live.slice(0, 10).map((line) => `• ${line}`));
    if (live.length > 10) {
      lines.push(`...and ${live.length - 10} more`);
    }
    lines.push("");
  }

  if (pending.length) {
    lines.push("🟡 <b>Open tickets</b>");
    lines.push(...pending.slice(0, 10).map((line) => `• ${line}`));
    if (pending.length > 10) {
      lines.push(`...and ${pending.length - 10} more`);
    }
    lines.push("");
  }

  if (closedRecords.length) {
    lines.push("✅ <b>Recent completed</b>");
    closedRecords.slice(0, 10).forEach(([ticketId, record]) => {
      lines.push(`• ${formatClosedLine(ticketId, record)}`);
    });
  }

  return replyHtml(ctx, lines.join("\n"));
};

const logCommand = (ctx) => {
  if (!isDuffChat(ctx)) {
    return;
  }
  const match = ctx.message.text.trim().match(/^\/log\s+(\d+)\s+([\s\S]+)$/i);
  if (!match) {
    return safeReply(ctx, "Usage: /log <ticket_id> <log text>");
  }
  const ticketId = Number(match[1]);
  const logText = match[2].trim();
  const ticket = tickets.get(ticketId);
  if (!ticket || ticket.status !== "open") {
    return safeReply(ctx, "Ticket not found or closed.");
  }
  if (!ticket.assignedAdminId) {
    return safeReply(ctx, "No worker assigned yet. Ask them to /accept first.");
  }
  const duffName = ctx.from?.first_name || "Duff";
  ticket.couponProvidedBy = duffName;
  ticket.couponProvidedAt = new Date().toISOString();
  ticket.couponText = logText;
  tickets.set(ticketId, ticket);
  updateTicketRecord(ticketId, {
    couponProvidedBy: duffName,
    couponProvidedAt: ticket.couponProvidedAt,
  });
  ctx.telegram
    .sendMessage(
      ticket.assignedAdminId,
      `${emphasizeHtml(`Log for ticket #${ticketId}`)}\n<code>${escapeHtml(
        logText
      )}</code>`,
      { parse_mode: "HTML" }
    )
    .catch(() => {});
  return safeReply(ctx, emphasizeHtml("Log sent to the worker."), {
    parse_mode: "HTML",
  });
};

const requestCoupon = async (ctx, ticketId) => {
  if (!isAdminChat(ctx)) {
    await ctx.answerCbQuery("Not authorized.");
    return;
  }
  const ticket = tickets.get(ticketId);
  if (!ticket || ticket.status !== "open") {
    await ctx.answerCbQuery("Ticket not found or closed.");
    return;
  }
  if (!ticket.assignedAdminId) {
    await ctx.answerCbQuery("Accept first with /accept <ticket_id>.");
    return;
  }
  if (ticket.assignedAdminId !== ctx.chat.id) {
    await ctx.answerCbQuery("Only the assigned worker can request logs.");
    return;
  }
  if (!duffChatIds.length) {
    await ctx.answerCbQuery("Duff panel is not configured.");
    return;
  }

  const alias = adminAlias(ctx);
  ticket.couponRequested = true;
  ticket.couponRequestedBy = alias;
  ticket.couponRequestedAt = new Date().toISOString();
  tickets.set(ticketId, ticket);
  updateTicketRecord(ticketId, {
    couponRequested: true,
    couponRequestedBy: alias,
    couponRequestedAt: ticket.couponRequestedAt,
  });

  const summary = formatTicketSummary(ticket);
  const header = emphasizeHtml(`Log request for ticket #${ticketId}`);
  const serviceLine = emphasizeHtml(
    `Service: ${escapeHtml(ticket.service || ticket.category || "Order")}`
  );
  const categoryLine = ticket.category
    ? emphasizeHtml(`Category: ${escapeHtml(ticket.category)}`)
    : null;
  const workerLine = emphasizeHtml(`Worker: ${escapeHtml(alias)}`);
  const summaryBlock = emphasizeHtml(escapeHtml(summary));
  const instructions = `${emphasizeHtml(
    "Reply to this message with the log details, or use"
  )} <code>/log &lt;ticket_id&gt; &lt;log&gt;</code>.`;
  let text = `${header}\n${serviceLine}`;
  if (categoryLine) {
    text += `\n${categoryLine}`;
  }
  text += `\n${workerLine}\n\n${summaryBlock}\n\n${instructions}`;

  duffChatIds.forEach((chatId) => {
    ctx.telegram
      .sendMessage(chatId, text, { parse_mode: "HTML" })
      .then((message) => {
        duffRequestMessages.set(
          duffMessageKey(chatId, message.message_id),
          { ticketId, botKey: ticket.botKey }
        );
      })
      .catch(() => {});
  });

  await ctx.answerCbQuery("Log request sent to Duff.");
  await safeReply(ctx, emphasizeHtml("Log request sent to Duff manager."), {
    parse_mode: "HTML",
  });
};

const handleAcceptAction = async (ctx) => {
  const ticketId = Number(ctx.match?.[1] || ctx.callbackQuery?.data?.split(":")[1]);
  if (!isAdminChat(ctx)) {
    await ctx.answerCbQuery("Not authorized.");
    return;
  }
  if (!Number.isFinite(ticketId)) {
    await ctx.answerCbQuery("Ticket not found.");
    return;
  }
  const alias = adminAlias(ctx);
  const result = assignTicket(ticketId, ctx.chat.id, alias);
  if (!result.ok) {
    await ctx.answerCbQuery(
      result.reason === "assigned" ? "Ticket already accepted." : "Ticket not found."
    );
    return;
  }
  await ctx.answerCbQuery("Ticket accepted.");
  await updateAdminTicketMessages(ticketId, ctx.chat.id, ctx.telegram);
  await refreshWorkerLists(ctx.telegram);
  try {
    await ctx.editMessageReplyMarkup(adminTicketKeyboard(ticketId, false).reply_markup);
  } catch (_) {}
};

const handlePaidAction = async (ctx) => {
  const ticketId = Number(ctx.match?.[1] || ctx.callbackQuery?.data?.split(":")[1]);
  if (!isAdminChat(ctx)) {
    await ctx.answerCbQuery("Not authorized.");
    return;
  }
  if (!Number.isFinite(ticketId)) {
    await ctx.answerCbQuery("Ticket not found.");
    return;
  }
  const ticket = tickets.get(ticketId);
  if (!ticket || ticket.status !== "open") {
    await ctx.answerCbQuery("Ticket not found or closed.");
    return;
  }
  if (ticket.assignedAdminId && ticket.assignedAdminId !== ctx.chat.id) {
    await ctx.answerCbQuery("Only the assigned worker can mark paid.");
    return;
  }
  const alias = adminAlias(ctx);
  ticket.paymentStatus = "paid";
  ticket.paidAt = new Date().toISOString();
  ticket.paidBy = alias;
  tickets.set(ticketId, ticket);
  updateTicketRecord(ticketId, {
    paymentStatus: "paid",
    paidAt: ticket.paidAt,
    paidBy: alias,
  });
  await ctx.answerCbQuery("Marked paid.");
  await safeReply(ctx, `Ticket #${ticketId} marked paid.`);
};

const handleCloseAction = async (ctx) => {
  const ticketId = Number(ctx.match?.[1] || ctx.callbackQuery?.data?.split(":")[1]);
  if (!isAdminChat(ctx)) {
    await ctx.answerCbQuery("Not authorized.");
    return;
  }

  const ticket = tickets.get(ticketId);
  if (!ticket || ticket.status === "closed") {
    await ctx.answerCbQuery("Ticket already closed.");
    return;
  }

  ticket.status = "closed";
  tickets.set(ticketId, ticket);
  removeOpenTicketForChat(ticket.chatId, ticketId);
  closeTicketRecord(ticketId, {
    closeType: "manual_close",
    closedBy: adminAlias(ctx),
  });

  await ctx.answerCbQuery("Ticket closed.");
  await safeReply(ctx, `Ticket #${ticketId} closed.`);
  await refreshWorkerLists(ctx.telegram);
  return ctx.telegram.sendMessage(
    ticket.chatId,
    completionThankYou(ticketId),
    { parse_mode: "HTML" }
  );
};

const handleBanAction = async (ctx) => {
  const ticketId = Number(ctx.match?.[1] || ctx.callbackQuery?.data?.split(":")[1]);
  if (!isAdminChat(ctx)) {
    await ctx.answerCbQuery("Not authorized.");
    return;
  }

  const ticket = tickets.get(ticketId);
  if (!ticket) {
    await ctx.answerCbQuery("Ticket not found.");
    return;
  }

  bannedChatIds.add(ticket.chatId);
  saveConfig();

  ticket.status = "closed";
  tickets.set(ticketId, ticket);
  removeOpenTicketForChat(ticket.chatId, ticketId);
  closeTicketRecord(ticketId, {
    closeType: "banned",
    closedBy: adminAlias(ctx),
  });

  await ctx.answerCbQuery("Customer banned.");
  await safeReply(ctx, `Ticket #${ticketId} closed and customer banned.`);
  await refreshWorkerLists(ctx.telegram);
  return ctx.telegram.sendMessage(
    ticket.chatId,
    "🚫 <b>Access restricted.</b>",
    { parse_mode: "HTML" }
  );
};

bot.command("work", workPanel);
bot.command("setname", setnameCommand);
bot.command("accept", acceptCommand);
bot.command("duff", duffPanel);
bot.command("log", logCommand);
bot.command("coupon", logCommand);

registerAdminReplyHandler(bot, "food");
registerDuffReplyHandler(bot);

bot.on("text", adminPromptHandler);

bot.action("menu:main", async (ctx) => {
  resetSession(ctx.chat.id);
  await saveSessionData(ctx.from.id, {
    state: FLOW_STATES.IDLE,
    temp: {},
    nav: [SCREEN.HOME],
  });
  await ctx.answerCbQuery();
  return sendMainMenu(ctx);
});

bot.action("menu:food", async (ctx) => {
  resetSession(ctx.chat.id);
  await ctx.answerCbQuery();
  return sendMainMenu(ctx);
});

bot.action("menu:quick", async (ctx) => {
  await ctx.answerCbQuery();
  return sendQuickActions(ctx);
});

bot.action("worker:panel", workerPanelAction);
bot.action("worker:view", workerViewAction);
bot.action("worker:close", workerCloseAction);
bot.action("worker:alias", workerAliasAction);
bot.action("profile:show", async (ctx) => {
  await ctx.answerCbQuery();
  return showProfile(ctx);
});

bot.action(/^service:(.+)/, async (ctx) => {
  const serviceKey = ctx.match[1];
  if (serviceKey === "flight") {
    setSession(ctx.chat.id, {
      service: "flight",
      stage: "flight_questions",
      stepIndex: 0,
      answers: {},
    });

    await ctx.answerCbQuery();
    await replyHtml(ctx, FLIGHT_PROMO);
    return replyHtml(ctx, FLIGHT_QUESTIONS[0].prompt);
  }

  if (serviceKey === "hotel") {
    setSession(ctx.chat.id, {
      service: "hotel",
      stage: "hotel_questions",
      stepIndex: 0,
      answers: {},
    });

    await ctx.answerCbQuery();
    await replyHtml(ctx, HOTEL_PROMO);
    return replyHtml(ctx, HOTEL_QUESTIONS[0].prompt);
  }

  const service = SERVICES[serviceKey];
  if (!service) {
    await ctx.answerCbQuery("Service not found.");
    return;
  }

  setSession(ctx.chat.id, {
    service: serviceKey,
    stage: "collecting",
    stepIndex: 0,
    answers: {},
  });

  await ctx.answerCbQuery();
  return replyHtml(
    ctx,
    `<b>${service.label} selected.</b>\n${service.steps[0].prompt}`
  );
});

bot.action(/^food:(.+)/, async (ctx) => {
  const categoryId = ctx.match[1];
  const category = FOOD_CATEGORIES.find((item) => item.id === categoryId);
  if (!category) {
    await ctx.answerCbQuery("Category not found.");
    return;
  }
  await ctx.answerCbQuery();
  await deleteSessionData(ctx.from.id);
  return handleFoodOption(ctx, category.label);
});

bot.action("edit", async (ctx) => {
  const session = sessions.get(ctx.chat.id);
  if (!session?.service) {
    await ctx.answerCbQuery("Start a request first.");
    return;
  }

  if (session.service === "food") {
    session.stage = "food_questions";
    session.stepIndex = 0;
    session.answers = {};
    setSession(ctx.chat.id, session);
    await ctx.answerCbQuery();
    await replyHtml(ctx, FOOD_PROMO);
    return replyHtml(ctx, FOOD_QUESTIONS[0].prompt);
  }

  if (session.service === "flight") {
    session.stage = "flight_questions";
    session.stepIndex = 0;
    session.answers = {};
    setSession(ctx.chat.id, session);
    await ctx.answerCbQuery();
    await replyHtml(ctx, FLIGHT_PROMO);
    return replyHtml(ctx, FLIGHT_QUESTIONS[0].prompt);
  }

  if (session.service === "hotel") {
    session.stage = "hotel_questions";
    session.stepIndex = 0;
    session.answers = {};
    setSession(ctx.chat.id, session);
    await ctx.answerCbQuery();
    await replyHtml(ctx, HOTEL_PROMO);
    return replyHtml(ctx, HOTEL_QUESTIONS[0].prompt);
  }

  session.stage = "collecting";
  session.stepIndex = 0;
  session.answers = {};
  setSession(ctx.chat.id, session);
  await ctx.answerCbQuery();
  return replyHtml(ctx, SERVICES[session.service].steps[0].prompt);
});

bot.action("confirm", async (ctx) => {
  const session = sessions.get(ctx.chat.id);
  if (!session?.answers || session.stage !== "confirm") {
    await ctx.answerCbQuery("Finish the questions first.");
    return;
  }

  await ctx.answerCbQuery("Submitted");
  await safeReply(ctx, 
    "Thanks! Your request is queued. We'll follow up shortly."
  );
  resetSession(ctx.chat.id);
});

bot.on("text", async (ctx) => {
  const isAdmin = isAdminChat(ctx);
  const messageText = ctx.message.text.trim();
  const lowerText = messageText.toLowerCase();
  const flowSession = getSessionData(ctx.from.id);
  const legacySession = sessions.get(ctx.chat.id);
  const quickActionSet = new Set([
    BTN_NEW_ORDER,
    BTN_PROFILE,
    BTN_ADDRESSES,
    BTN_CHOOSE_ADDRESS,
    BTN_LAST_ORDER,
    BTN_SUPPORT,
    BTN_CHANNEL,
    BTN_MENU,
    BTN_ADD_PROFILE_ADDR,
    BTN_EDIT_PROFILE_ADDR,
    BTN_BACK,
    BTN_HOME,
    BTN_CANCEL_ORDER,
    BTN_MANAGE_ADDR,
    BTN_MANAGE,
    BTN_MY_TICKETS,
    BTN_OPEN_ANOTHER,
    BTN_OPEN_THIS,
    BTN_CANCEL_GENERIC,
  ]);
  const isQuickAction = quickActionSet.has(messageText) || lowerText === "menu";
  if (
    isAdmin &&
    (!flowSession || flowSession.state === FLOW_STATES.IDLE) &&
    !legacySession &&
    !isQuickAction
  ) {
    return;
  }
  if (messageText === BTN_MENU) {
    resetSession(ctx.chat.id);
    await saveSessionData(ctx.from.id, {
      state: FLOW_STATES.IDLE,
      temp: {},
      nav: [SCREEN.HOME],
    });
    return sendQuickActions(ctx);
  }
  if (lowerText === "menu") {
    resetSession(ctx.chat.id);
    await saveSessionData(ctx.from.id, {
      state: FLOW_STATES.IDLE,
      temp: {},
      nav: [SCREEN.HOME],
    });
    return sendQuickActions(ctx);
  }
  if (flowSession && flowSession.state && flowSession.state !== FLOW_STATES.IDLE) {
    const handled = await handleProfileFlow(ctx, flowSession);
    if (handled !== null) {
      return handled;
    }
  }
  if (messageText === BTN_CANCEL_ORDER) {
    if (flowSession) {
      await deleteSessionData(ctx.from.id);
    }
    if (legacySession) {
      resetSession(ctx.chat.id);
    }
    await safeReply(ctx, 
      "✅ Cancelled. No request was submitted.",
      Markup.removeKeyboard()
    );
    return;
  }
  if (messageText === BTN_HOME) {
    return handleHome(ctx);
  }
  if (messageText === BTN_BACK) {
    return handleBack(ctx);
  }

  if (messageText === BTN_NEW_ORDER) {
    return sendMainMenu(ctx);
  }
  if (messageText === BTN_ADD_PROFILE_ADDR) {
    const existing = getUser(ctx.from.id);
    if (!existing) {
      await saveSessionData(ctx.from.id, {
        state: FLOW_STATES.AWAIT_NAME,
        temp: { returnTo: "menu" },
      });
      return safeReply(ctx, "👤 Full name?", backHomeKeyboard());
    }
    const addresses = existing.addresses || [];
    if (addresses.length >= 4) {
      await saveSessionData(ctx.from.id, {
        state: FLOW_STATES.AWAIT_MANAGE_PICK,
        temp: { returnTo: "profile" },
      });
      await safeReply(ctx, 
        "⚠️ You already have 4 saved addresses.\nEdit or delete one to add a new address."
      );
      return sendManageList(ctx, existing);
    }
    await saveSessionData(ctx.from.id, {
      state: FLOW_STATES.AWAIT_ADD_NAME,
      temp: { returnTo: "profile" },
    });
    return safeReply(ctx, "👤 Name for this address?", backHomeKeyboard());
  }
  if (messageText === BTN_EDIT_PROFILE_ADDR) {
    const existing = getUser(ctx.from.id);
    if (!existing) {
      await safeReply(ctx, "No profile saved yet.");
      return sendQuickActions(ctx);
    }
    return showProfile(ctx);
  }
  if (messageText === BTN_PROFILE) {
    return showProfile(ctx);
  }
  if (messageText === BTN_ADDRESSES || messageText === BTN_CHOOSE_ADDRESS) {
    const user = getUser(ctx.from.id);
    if (!user) {
      return showProfile(ctx);
    }
    await saveSessionData(ctx.from.id, {
      state: FLOW_STATES.AWAIT_MANAGE_PICK,
      temp: { returnTo: "profile" },
    });
    return sendManageList(ctx, user);
  }
  if (messageText === BTN_ADD_ADDRESS) {
    const user = getUser(ctx.from.id);
    if (!user) {
      return showProfile(ctx);
    }
    const addresses = getAddresses(user);
    if (addresses.length >= 4) {
      await saveSessionData(ctx.from.id, {
        state: FLOW_STATES.AWAIT_MANAGE_PICK,
        temp: { returnTo: "profile" },
      });
      await safeReply(ctx, 
        "⚠️ You already have 4 saved addresses.\nEdit or delete one to add a new address."
      );
      return sendManageList(ctx, user);
    }
    await saveSessionData(ctx.from.id, {
      state: FLOW_STATES.AWAIT_ADD_NAME,
      temp: { returnTo: "profile" },
    });
    return safeReply(ctx, "👤 Name for this address?", backHomeKeyboard());
  }
  if (messageText === BTN_MANAGE || messageText === BTN_MANAGE_ADDR) {
    const user = getUser(ctx.from.id);
    if (!user) {
      return showProfile(ctx);
    }
    await saveSessionData(ctx.from.id, {
      state: FLOW_STATES.AWAIT_MANAGE_PICK,
      temp: { returnTo: "profile" },
    });
    return sendManageList(ctx, user);
  }
  if (messageText === BTN_LAST_ORDER) {
    return showLastOrder(ctx);
  }
  if (messageText === BTN_MY_TICKETS) {
    return sendMyTickets(ctx);
  }
  if (messageText === BTN_SUPPORT) {
    return sendSupportPrompt(ctx);
  }
  if (messageText === BTN_CHANNEL) {
    await safeReply(ctx, `📢 Channel: ${CHANNEL_URL}`);
    return sendQuickActions(ctx);
  }
  if (messageText === BTN_DELETE_PROFILE) {
    await deleteUser(ctx.from.id);
    await deleteSessionData(ctx.from.id);
    return safeReply(ctx, "✅ Profile deleted.", Markup.removeKeyboard());
  }

  const session = legacySession;
  if (!session) {
    const openTicket = getOpenTicketByChat(ctx.chat.id);
    if (openTicket) {
      return forwardCustomerMessage(
        ctx.telegram,
        ctx,
        openTicket.ticketId,
        openTicket.ticket
      );
    }
    return sendQuickActions(ctx);
  }

  if (session.service === "food") {
    if (session.stage === "food_questions") {
      const step = FOOD_QUESTIONS[session.stepIndex];
      session.answers[step.key] = ctx.message.text.trim();

      if (session.stepIndex < FOOD_QUESTIONS.length - 1) {
        session.stepIndex += 1;
        setSession(ctx.chat.id, session);
        return replyHtml(ctx, FOOD_QUESTIONS[session.stepIndex].prompt);
      }

      session.stage = "food_continue";
      setSession(ctx.chat.id, session);
      return replyHtml(ctx, FOOD_CONTINUE_PROMPT);
    }

    if (session.stage === "food_continue") {
      const response = ctx.message.text.trim().toLowerCase();
      if (response !== "yes") {
        return replyHtml(ctx, FOOD_CONTINUE_PROMPT);
      }

      const rateLimit = checkRateLimit(ctx.from.id);
      if (rateLimit.limited) {
        const minutes = Math.ceil(rateLimit.retryAfterMs / 60000);
        resetSession(ctx.chat.id);
        return safeReply(ctx, 
          `You're sending too many requests. Please try again in ${minutes} minute(s).`
        );
      }

      const ticketId = nextTicketId();
      tickets.set(ticketId, {
        chatId: ctx.chat.id,
        category: session.foodCategory,
        answers: session.answers,
        status: "open",
        adminMessages: [],
        botKey: "food",
        service: "Food",
      });
      createTicketRecord(ticketId, {
        service: "Food",
        category: session.foodCategory,
        chatId: ctx.chat.id,
        botKey: "food",
        answers: session.answers,
      });
      addOpenTicketForChat(ctx.chat.id, ticketId);

      const summary = formatFoodSummary(session);
      const userTag = ctx.from.username
        ? `@${ctx.from.username}`
        : `ID ${ctx.from.id}`;
      const adminMessage =
        `New food order ticket #${ticketId}\n${summary}\nCustomer: ${userTag}\n\n` +
        "Reply to this message to chat with the customer.";

      adminChatIds.forEach((chatId) => {
        bot.telegram
          .sendMessage(chatId, adminMessage, adminTicketKeyboard(ticketId))
          .then((message) => {
            adminTicketMessages.set(
              adminMessageKey(chatId, message.message_id),
              { ticketId, botKey: "food" }
            );
            const ticket = tickets.get(ticketId);
            if (ticket) {
              ticket.adminMessages.push({
                chatId,
                messageId: message.message_id,
              });
              tickets.set(ticketId, ticket);
            }
          })
          .catch((err) =>
            logError(`food admin notify #${ticketId}`, err)
          );
      });
      refreshWorkerLists(ctx.telegram).catch((err) =>
        logError("food refresh worker list", err)
      );

      resetSession(ctx.chat.id);
      safeReply(ctx, "⏳ Connecting you to a concierge…\n⏱ Typical wait: 1–5 min");
      await safeReply(ctx, 
        `✅ Request opened — Ticket #${ticketId}\nA concierge will reply here shortly.`
      );
      return sendQuickActions(ctx);
    }

    return replyHtml(ctx, START_PROMPT);
  }

  if (session.service === "flight") {
    if (session.stage === "flight_questions") {
      const step = FLIGHT_QUESTIONS[session.stepIndex];
      session.answers[step.key] = ctx.message.text.trim();

      if (session.stepIndex < FLIGHT_QUESTIONS.length - 1) {
        session.stepIndex += 1;
        setSession(ctx.chat.id, session);
        return replyHtml(ctx, FLIGHT_QUESTIONS[session.stepIndex].prompt);
      }

      session.stage = "flight_continue";
      setSession(ctx.chat.id, session);
      return replyHtml(ctx, FLIGHT_CONTINUE_PROMPT);
    }

    if (session.stage === "flight_continue") {
      const response = ctx.message.text.trim().toLowerCase();
      if (response !== "yes") {
        return replyHtml(ctx, FLIGHT_CONTINUE_PROMPT);
      }

      const rateLimit = checkRateLimit(ctx.from.id);
      if (rateLimit.limited) {
        const minutes = Math.ceil(rateLimit.retryAfterMs / 60000);
        resetSession(ctx.chat.id);
        return safeReply(ctx, 
          `You're sending too many requests. Please try again in ${minutes} minute(s).`
        );
      }

      const ticketId = nextTicketId();
      tickets.set(ticketId, {
        chatId: ctx.chat.id,
        category: "Flights",
        answers: session.answers,
        status: "open",
        adminMessages: [],
        botKey: "food",
        service: "Flights",
      });
      createTicketRecord(ticketId, {
        service: "Flights",
        category: "Flights",
        chatId: ctx.chat.id,
        botKey: "food",
        answers: session.answers,
      });
      addOpenTicketForChat(ctx.chat.id, ticketId);

      const summary = formatFlightSummary(session);
      const userTag = ctx.from.username
        ? `@${ctx.from.username}`
        : `ID ${ctx.from.id}`;
      const adminMessage =
        `New flight ticket #${ticketId}\n${summary}\nCustomer: ${userTag}\n\n` +
        "Reply to this message to chat with the customer.";

      adminChatIds.forEach((chatId) => {
        bot.telegram
          .sendMessage(chatId, adminMessage, adminTicketKeyboard(ticketId))
          .then((message) => {
            adminTicketMessages.set(
              adminMessageKey(chatId, message.message_id),
              { ticketId, botKey: "food" }
            );
            const ticket = tickets.get(ticketId);
            if (ticket) {
              ticket.adminMessages.push({
                chatId,
                messageId: message.message_id,
              });
              tickets.set(ticketId, ticket);
            }
          })
          .catch((err) =>
            logError(`food flight admin notify #${ticketId}`, err)
          );
      });
      refreshWorkerLists(ctx.telegram).catch((err) =>
        logError("food flight refresh worker list", err)
      );

      resetSession(ctx.chat.id);
      safeReply(ctx, "⏳ Connecting you to a concierge…\n⏱ Typical wait: 1–5 min");
      return safeReply(ctx, 
        `✅ Request opened — Ticket #${ticketId}\nA concierge will reply here shortly.`
      );
    }

    return replyHtml(ctx, START_PROMPT);
  }

  if (session.service === "hotel") {
    if (session.stage === "hotel_questions") {
      const step = HOTEL_QUESTIONS[session.stepIndex];
      session.answers[step.key] = ctx.message.text.trim();

      if (session.stepIndex < HOTEL_QUESTIONS.length - 1) {
        session.stepIndex += 1;
        setSession(ctx.chat.id, session);
        return replyHtml(ctx, HOTEL_QUESTIONS[session.stepIndex].prompt);
      }

      session.stage = "hotel_continue";
      setSession(ctx.chat.id, session);
      return replyHtml(ctx, HOTEL_CONTINUE_PROMPT);
    }

    if (session.stage === "hotel_continue") {
      const response = ctx.message.text.trim().toLowerCase();
      if (response !== "yes") {
        return replyHtml(ctx, HOTEL_CONTINUE_PROMPT);
      }

      const rateLimit = checkRateLimit(ctx.from.id);
      if (rateLimit.limited) {
        const minutes = Math.ceil(rateLimit.retryAfterMs / 60000);
        resetSession(ctx.chat.id);
        return safeReply(ctx, 
          `You're sending too many requests. Please try again in ${minutes} minute(s).`
        );
      }

      const ticketId = nextTicketId();
      tickets.set(ticketId, {
        chatId: ctx.chat.id,
        category: "Hotels",
        answers: session.answers,
        status: "open",
        adminMessages: [],
        botKey: "food",
        service: "Hotels",
      });
      createTicketRecord(ticketId, {
        service: "Hotels",
        category: "Hotels",
        chatId: ctx.chat.id,
        botKey: "food",
        answers: session.answers,
      });
      addOpenTicketForChat(ctx.chat.id, ticketId);

      const summary = formatHotelSummary(session);
      const userTag = ctx.from.username
        ? `@${ctx.from.username}`
        : `ID ${ctx.from.id}`;
      const adminMessage =
        `New hotel ticket #${ticketId}\n${summary}\nCustomer: ${userTag}\n\n` +
        "Reply to this message to chat with the customer.";

      adminChatIds.forEach((chatId) => {
        bot.telegram
          .sendMessage(chatId, adminMessage, adminTicketKeyboard(ticketId))
          .then((message) => {
            adminTicketMessages.set(
              adminMessageKey(chatId, message.message_id),
              { ticketId, botKey: "food" }
            );
            const ticket = tickets.get(ticketId);
            if (ticket) {
              ticket.adminMessages.push({
                chatId,
                messageId: message.message_id,
              });
              tickets.set(ticketId, ticket);
            }
          })
          .catch((err) =>
            logError(`food hotel admin notify #${ticketId}`, err)
          );
      });
      refreshWorkerLists(ctx.telegram).catch((err) =>
        logError("food hotel refresh worker list", err)
      );

      resetSession(ctx.chat.id);
      safeReply(ctx, "⏳ Connecting you to a concierge…\n⏱ Typical wait: 1–5 min");
      return safeReply(ctx, 
        `✅ Request opened — Ticket #${ticketId}\nA concierge will reply here shortly.`
      );
    }

    return sendQuickActions(ctx);
  }

  if (session.stage === "confirm") {
    return replyHtml(
      ctx,
      "⚠️ <b>Use the buttons</b> to submit or edit your request."
    );
  }

  if (session.stage !== "collecting") {
    return sendQuickActions(ctx);
  }

  const service = SERVICES[session.service];
  const step = service.steps[session.stepIndex];
  session.answers[step.key] = ctx.message.text.trim();

  if (session.stepIndex < service.steps.length - 1) {
    session.stepIndex += 1;
    setSession(ctx.chat.id, session);
    return replyHtml(ctx, service.steps[session.stepIndex].prompt);
  }

  session.stage = "confirm";
  setSession(ctx.chat.id, session);

  const summary = formatSummary(session.service, session.answers);
  return safeReply(ctx, 
    `Got it for ${service.label}:\n${summary}\n\nSubmit when ready.`,
    confirmMenu()
  );
});

bot.on("message", (ctx) => {
  if (isAdminChat(ctx)) {
    return;
  }
  if (ctx.message?.text) {
    return;
  }
  const openTicket = getOpenTicketByChat(ctx.chat.id);
  if (openTicket && !sessions.has(ctx.chat.id)) {
    return forwardCustomerMessage(
      ctx.telegram,
      ctx,
      openTicket.ticketId,
      openTicket.ticket
    );
  }
  return replyHtml(
    ctx,
    "✍️ <b>Please send text details</b> or use /start."
  );
});

bot.action(/^accept:(\d+)/, handleAcceptAction);
bot.action(/^paid:(\d+)/, handlePaidAction);
bot.action(/^close:(\d+)/, handleCloseAction);
bot.action(/^ban:(\d+)/, handleBanAction);
bot.action(/^coupon:(\d+)/, async (ctx) => {
  const ticketId = Number(ctx.match[1]);
  if (!Number.isFinite(ticketId)) {
    await ctx.answerCbQuery("Ticket not found.");
    return;
  }
  await requestCoupon(ctx, ticketId);
});

const botEntries = [{ bot, name: "food" }];

if (flightBotToken) {
  const flightBot = new Telegraf(flightBotToken);
  attachBotMiddlewares(flightBot, "flight");
  const {
    sessions: flightSessions,
    setSession: setFlightSession,
    resetSession: resetFlightSession,
  } = createSessionStore(flightBot.telegram);

  flightBot.use(banGuard);
  registerAdminReplyHandler(flightBot, "flight");
  registerDuffReplyHandler(flightBot);

  flightBot.start((ctx) => {
    if (openTicketCountForChat(ctx.chat.id) > 0) {
      return promptOpenAnother(ctx, { type: "flight", label: "Flights" });
    }
    resetFlightSession(ctx.chat.id);
    return sendHome(ctx, FLIGHT_HOME(), flightStartMenu());
  });

  flightBot.command("help", (ctx) =>
    replyHtml(
      ctx,
      "ℹ️ <b>How it works</b>\nAnswer a few questions and we will connect you with an agent."
    )
  );

  flightBot.command("cancel", (ctx) => {
    resetFlightSession(ctx.chat.id);
    return replyHtml(
      ctx,
      "🛑 <b>Canceled.</b> Send /start when you're ready."
    );
  });

  flightBot.command("work", workPanel);
  flightBot.command("setname", setnameCommand);
  flightBot.command("accept", acceptCommand);
  flightBot.command("duff", duffPanel);
  flightBot.command("log", logCommand);
  flightBot.command("coupon", logCommand);

  flightBot.action("flight:start", async (ctx) => {
    if (openTicketCountForChat(ctx.chat.id) > 0) {
      return promptOpenAnother(ctx, { type: "flight", label: "Flights" });
    }
    await ctx.answerCbQuery();
    return startFlightFlow(ctx, setFlightSession);
  });

  flightBot.action("worker:panel", workerPanelAction);
  flightBot.action("worker:view", workerViewAction);
  flightBot.action("worker:close", workerCloseAction);
  flightBot.action("worker:alias", workerAliasAction);

  flightBot.action(/^accept:(\d+)/, handleAcceptAction);
  flightBot.action(/^paid:(\d+)/, handlePaidAction);
  flightBot.action(/^close:(\d+)/, handleCloseAction);
  flightBot.action(/^ban:(\d+)/, handleBanAction);
  flightBot.action(/^coupon:(\d+)/, async (ctx) => {
    const ticketId = Number(ctx.match[1]);
    if (!Number.isFinite(ticketId)) {
      await ctx.answerCbQuery("Ticket not found.");
      return;
    }
    await requestCoupon(ctx, ticketId);
  });

  flightBot.on("text", adminPromptHandler);

  flightBot.on("text", async (ctx) => {
    if (isAdminChat(ctx)) {
      return;
    }
    const ticketHandled = await handleTicketFlow(ctx, {
      startFlight: () => startFlightFlow(ctx, setFlightSession),
    });
    if (ticketHandled) {
      return;
    }
    const session = flightSessions.get(ctx.chat.id);
    if (!session) {
      const openTicket = getOpenTicketByChat(ctx.chat.id);
      if (openTicket) {
        return forwardCustomerMessage(
          ctx.telegram,
          ctx,
          openTicket.ticketId,
          openTicket.ticket
        );
      }
      return replyHtml(ctx, FLIGHT_START_PROMPT);
    }

    if (session.stage === "flight_questions") {
      const step = FLIGHT_QUESTIONS[session.stepIndex];
      session.answers[step.key] = ctx.message.text.trim();

      if (session.stepIndex < FLIGHT_QUESTIONS.length - 1) {
        session.stepIndex += 1;
        setFlightSession(ctx.chat.id, session);
        return replyHtml(ctx, FLIGHT_QUESTIONS[session.stepIndex].prompt);
      }

      session.stage = "flight_continue";
      setFlightSession(ctx.chat.id, session);
      return replyHtml(ctx, FLIGHT_CONTINUE_PROMPT);
    }

    if (session.stage === "flight_continue") {
      const response = ctx.message.text.trim().toLowerCase();
      if (response !== "yes") {
        return replyHtml(ctx, FLIGHT_CONTINUE_PROMPT);
      }

      const rateLimit = checkRateLimit(ctx.from.id);
      if (rateLimit.limited) {
        const minutes = Math.ceil(rateLimit.retryAfterMs / 60000);
        resetFlightSession(ctx.chat.id);
        return safeReply(ctx, 
          `You're sending too many requests. Please try again in ${minutes} minute(s).`
        );
      }

      const ticketId = nextTicketId();
      tickets.set(ticketId, {
        chatId: ctx.chat.id,
        category: "Flights",
        answers: session.answers,
        status: "open",
        adminMessages: [],
        botKey: "flight",
        service: "Flights",
      });
      createTicketRecord(ticketId, {
        service: "Flights",
        category: "Flights",
        chatId: ctx.chat.id,
        botKey: "flight",
        answers: session.answers,
      });
      addOpenTicketForChat(ctx.chat.id, ticketId);

      const summary = formatFlightSummary(session);
      const userTag = ctx.from.username
        ? `@${ctx.from.username}`
        : `ID ${ctx.from.id}`;
      const adminMessage =
        `New flight ticket #${ticketId}\n${summary}\nCustomer: ${userTag}\n\n` +
        "Reply to this message to chat with the customer.";

      adminChatIds.forEach((chatId) => {
        flightBot.telegram
          .sendMessage(chatId, adminMessage, adminTicketKeyboard(ticketId))
          .then((message) => {
            adminTicketMessages.set(
              adminMessageKey(chatId, message.message_id),
              { ticketId, botKey: "flight" }
            );
            const ticket = tickets.get(ticketId);
            if (ticket) {
              ticket.adminMessages.push({
                chatId,
                messageId: message.message_id,
              });
              tickets.set(ticketId, ticket);
            }
          })
          .catch((err) =>
            logError(`flight admin notify #${ticketId}`, err)
          );
      });
      refreshWorkerLists(ctx.telegram).catch((err) =>
        logError("flight refresh worker list", err)
      );

      resetFlightSession(ctx.chat.id);
      safeReply(ctx, "⏳ Connecting you to a concierge…\n⏱ Typical wait: 1–5 min");
      return safeReply(ctx, 
        `✅ Request opened — Ticket #${ticketId}\nA concierge will reply here shortly.`
      );
    }

    return replyHtml(ctx, FLIGHT_START_PROMPT);
  });

  flightBot.on("message", (ctx) => {
    if (isAdminChat(ctx)) {
      return;
    }
    if (ctx.message?.text) {
      return;
    }
    const openTicket = getOpenTicketByChat(ctx.chat.id);
    if (openTicket && !flightSessions.has(ctx.chat.id)) {
      return forwardCustomerMessage(
        ctx.telegram,
        ctx,
        openTicket.ticketId,
        openTicket.ticket
      );
    }
    return replyHtml(
      ctx,
      "✍️ <b>Please send text details</b> or use /start."
    );
  });

  botEntries.push({ bot: flightBot, name: "flight" });
}

if (hotelBotToken) {
  const hotelBot = new Telegraf(hotelBotToken);
  attachBotMiddlewares(hotelBot, "hotel");
  const {
    sessions: hotelSessions,
    setSession: setHotelSession,
    resetSession: resetHotelSession,
  } = createSessionStore(hotelBot.telegram);

  hotelBot.use(banGuard);
  registerAdminReplyHandler(hotelBot, "hotel");
  registerDuffReplyHandler(hotelBot);

  hotelBot.start((ctx) => {
    if (openTicketCountForChat(ctx.chat.id) > 0) {
      return promptOpenAnother(ctx, { type: "hotel", label: "Hotels" });
    }
    resetHotelSession(ctx.chat.id);
    return sendHome(ctx, HOTEL_HOME(), hotelStartMenu());
  });

  hotelBot.command("help", (ctx) =>
    replyHtml(
      ctx,
      "ℹ️ <b>How it works</b>\nShare your trip details and we will connect you with an agent."
    )
  );

  hotelBot.command("cancel", (ctx) => {
    resetHotelSession(ctx.chat.id);
    return replyHtml(
      ctx,
      "🛑 <b>Canceled.</b> Send /start when you're ready."
    );
  });

  hotelBot.command("work", workPanel);
  hotelBot.command("setname", setnameCommand);
  hotelBot.command("accept", acceptCommand);
  hotelBot.command("duff", duffPanel);
  hotelBot.command("log", logCommand);
  hotelBot.command("coupon", logCommand);

  hotelBot.action("hotel:start", async (ctx) => {
    if (openTicketCountForChat(ctx.chat.id) > 0) {
      return promptOpenAnother(ctx, { type: "hotel", label: "Hotels" });
    }
    await ctx.answerCbQuery();
    return startHotelFlow(ctx, setHotelSession);
  });

  hotelBot.action("worker:panel", workerPanelAction);
  hotelBot.action("worker:view", workerViewAction);
  hotelBot.action("worker:close", workerCloseAction);
  hotelBot.action("worker:alias", workerAliasAction);

  hotelBot.action(/^accept:(\d+)/, handleAcceptAction);
  hotelBot.action(/^paid:(\d+)/, handlePaidAction);
  hotelBot.action(/^close:(\d+)/, handleCloseAction);
  hotelBot.action(/^ban:(\d+)/, handleBanAction);
  hotelBot.action(/^coupon:(\d+)/, async (ctx) => {
    const ticketId = Number(ctx.match[1]);
    if (!Number.isFinite(ticketId)) {
      await ctx.answerCbQuery("Ticket not found.");
      return;
    }
    await requestCoupon(ctx, ticketId);
  });

  hotelBot.on("text", adminPromptHandler);

  hotelBot.on("text", async (ctx) => {
    if (isAdminChat(ctx)) {
      return;
    }
    const ticketHandled = await handleTicketFlow(ctx, {
      startHotel: () => startHotelFlow(ctx, setHotelSession),
    });
    if (ticketHandled) {
      return;
    }
    const session = hotelSessions.get(ctx.chat.id);
    if (!session) {
      const openTicket = getOpenTicketByChat(ctx.chat.id);
      if (openTicket) {
        return forwardCustomerMessage(
          ctx.telegram,
          ctx,
          openTicket.ticketId,
          openTicket.ticket
        );
      }
      return replyHtml(ctx, HOTEL_START_PROMPT);
    }

    if (session.stage === "hotel_questions") {
      const step = HOTEL_QUESTIONS[session.stepIndex];
      session.answers[step.key] = ctx.message.text.trim();

      if (session.stepIndex < HOTEL_QUESTIONS.length - 1) {
        session.stepIndex += 1;
        setHotelSession(ctx.chat.id, session);
        return replyHtml(ctx, HOTEL_QUESTIONS[session.stepIndex].prompt);
      }

      session.stage = "hotel_continue";
      setHotelSession(ctx.chat.id, session);
      return replyHtml(ctx, HOTEL_CONTINUE_PROMPT);
    }

    if (session.stage === "hotel_continue") {
      const response = ctx.message.text.trim().toLowerCase();
      if (response !== "yes") {
        return replyHtml(ctx, HOTEL_CONTINUE_PROMPT);
      }

      const rateLimit = checkRateLimit(ctx.from.id);
      if (rateLimit.limited) {
        const minutes = Math.ceil(rateLimit.retryAfterMs / 60000);
        resetHotelSession(ctx.chat.id);
        return safeReply(ctx, 
          `You're sending too many requests. Please try again in ${minutes} minute(s).`
        );
      }

      const ticketId = nextTicketId();
      tickets.set(ticketId, {
        chatId: ctx.chat.id,
        category: "Hotels",
        answers: session.answers,
        status: "open",
        adminMessages: [],
        botKey: "hotel",
        service: "Hotels",
      });
      createTicketRecord(ticketId, {
        service: "Hotels",
        category: "Hotels",
        chatId: ctx.chat.id,
        botKey: "hotel",
        answers: session.answers,
      });
      addOpenTicketForChat(ctx.chat.id, ticketId);

      const summary = formatHotelSummary(session);
      const userTag = ctx.from.username
        ? `@${ctx.from.username}`
        : `ID ${ctx.from.id}`;
      const adminMessage =
        `New hotel ticket #${ticketId}\n${summary}\nCustomer: ${userTag}\n\n` +
        "Reply to this message to chat with the customer.";

      adminChatIds.forEach((chatId) => {
        hotelBot.telegram
          .sendMessage(chatId, adminMessage, adminTicketKeyboard(ticketId))
          .then((message) => {
            adminTicketMessages.set(
              adminMessageKey(chatId, message.message_id),
              { ticketId, botKey: "hotel" }
            );
            const ticket = tickets.get(ticketId);
            if (ticket) {
              ticket.adminMessages.push({
                chatId,
                messageId: message.message_id,
              });
              tickets.set(ticketId, ticket);
            }
          })
          .catch((err) =>
            logError(`hotel admin notify #${ticketId}`, err)
          );
      });
      refreshWorkerLists(ctx.telegram).catch((err) =>
        logError("hotel refresh worker list", err)
      );

      resetHotelSession(ctx.chat.id);
      safeReply(ctx, "⏳ Connecting you to a concierge…\n⏱ Typical wait: 1–5 min");
      return safeReply(ctx, 
        `✅ Request opened — Ticket #${ticketId}\nA concierge will reply here shortly.`
      );
    }

    return replyHtml(ctx, HOTEL_START_PROMPT);
  });

  hotelBot.on("message", (ctx) => {
    if (isAdminChat(ctx)) {
      return;
    }
    if (ctx.message?.text) {
      return;
    }
    const openTicket = getOpenTicketByChat(ctx.chat.id);
    if (openTicket && !hotelSessions.has(ctx.chat.id)) {
      return forwardCustomerMessage(
        ctx.telegram,
        ctx,
        openTicket.ticketId,
        openTicket.ticket
      );
    }
    return replyHtml(
      ctx,
      "✍️ <b>Please send text details</b> or use /start."
    );
  });

  botEntries.push({ bot: hotelBot, name: "hotel" });
}

const withStartupTimeout = (promise, ms, label) =>
  Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms)
    ),
  ]);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const launchAttempts = new Map();

const attachTextGuards = (botInstance, name) => {
  const originalSendMessage = botInstance.telegram.sendMessage.bind(
    botInstance.telegram
  );
  botInstance.telegram.sendMessage = (chatId, text, extra) =>
    originalSendMessage(chatId, ensureText(text), extra);

  botInstance.use((ctx, next) => {
    if (ctx.reply) {
      const originalReply = ctx.reply.bind(ctx);
      ctx.reply = (text, extra) => {
        const safeText = ensureText(text);
        if (safeText !== text) {
          console.warn(`[${name}] empty reply prevented chat:${ctx.chat?.id}`);
        }
        return withTimeout(
          `${name}:ctx.reply`,
          originalReply(safeText, extra)
        );
      };
    }
    if (ctx.replyWithHTML) {
      const originalReplyHtml = ctx.replyWithHTML.bind(ctx);
      ctx.replyWithHTML = (html, extra) =>
        withTimeout(
          `${name}:ctx.replyWithHTML`,
          originalReplyHtml(ensureText(html), extra)
        );
    }
    if (ctx.replyWithPhoto) {
      const originalReplyPhoto = ctx.replyWithPhoto.bind(ctx);
      ctx.replyWithPhoto = (photo, extra) =>
        withTimeout(
          `${name}:ctx.replyWithPhoto`,
          originalReplyPhoto(photo, extra)
        );
    }
    if (ctx.editMessageText) {
      const originalEdit = ctx.editMessageText.bind(ctx);
      ctx.editMessageText = (text, extra) =>
        withTimeout(
          `${name}:ctx.editMessageText`,
          originalEdit(ensureText(text), extra)
        );
    }
    return next();
  });
};

const getMeWithRetry = async (botInstance, name) => {
  const retries = Number.isFinite(startupGetMeRetries)
    ? startupGetMeRetries
    : 2;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      await withStartupTimeout(
        botInstance.telegram.getMe(),
        startupTimeoutMs,
        `${name} getMe`
      );
      return;
    } catch (err) {
      if (attempt >= retries) {
        throw err;
      }
      await sleep(startupRetryBaseMs * (attempt + 1));
    }
  }
};

const launchBot = async (botInstance, name) => {
  try {
    try {
      await withStartupTimeout(
        botInstance.telegram.deleteWebhook({ drop_pending_updates: true }),
        startupTimeoutMs,
        `${name} deleteWebhook`
      );
    } catch (_) {}
    if (!startupSkipGetMe) {
      try {
        await getMeWithRetry(botInstance, name);
      } catch (err) {
        const message = err?.stack || err?.message || String(err);
        console.warn(`[${name}] getMe failed, continuing: ${message}`);
        if (startupRequireGetMe) {
          throw err;
        }
      }
    }
    await botInstance.launch({
      dropPendingUpdates: true,
      allowedUpdates: ["message", "callback_query"],
    });
    console.log(`[${name}] bot launched`);
    launchAttempts.delete(name);
  } catch (err) {
    const message = err?.stack || err?.message || String(err);
    console.error(`[${name}] bot launch failed: ${message}`);
    try {
      botInstance.stop("restart");
    } catch (_) {}
    const attempt = (launchAttempts.get(name) || 0) + 1;
    launchAttempts.set(name, attempt);
    const backoff = Math.min(
      startupRetryMaxMs,
      startupRetryBaseMs * 2 ** Math.min(attempt, 6)
    );
    const jitter = Math.floor(Math.random() * startupRetryJitterMs);
    setTimeout(() => launchBot(botInstance, name), backoff + jitter);
  }
};

const registerErrorHandler = (botInstance, name) => {
  botInstance.catch((err, ctx) => {
    const message = err?.stack || err?.message || String(err);
    const chatId = ctx?.chat?.id ? ` chat:${ctx.chat.id}` : "";
    console.error(`[${name}] handler error${chatId}: ${message}`);
  });
};

botEntries.forEach(({ bot: botInstance, name }, index) => {
  attachTextGuards(botInstance, name);
  registerErrorHandler(botInstance, name);
  setTimeout(() => launchBot(botInstance, name), startupStaggerMs * index);
});

process.once("SIGINT", () =>
  botEntries.forEach(({ bot: botInstance }) => botInstance.stop("SIGINT"))
);
process.once("SIGTERM", () =>
  botEntries.forEach(({ bot: botInstance }) => botInstance.stop("SIGTERM"))
);

process.on("unhandledRejection", (reason) => {
  console.error("[unhandledRejection]", reason);
});

process.on("uncaughtException", (err) => {
  console.error("[uncaughtException]", err);
});
