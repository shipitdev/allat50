const fs = require("fs");
const path = require("path");
try {
  const dns = require("dns");
  if (typeof dns.setDefaultResultOrder === "function") {
    dns.setDefaultResultOrder("ipv4first");
  }
} catch (_) {}
const { Telegraf, Markup } = require("telegraf");
require("dotenv").config();

const configPath = path.join(__dirname, "..", "config.json");
let config = {};
try {
  config = JSON.parse(fs.readFileSync(configPath, "utf8"));
} catch (err) {
  console.error("Missing or invalid config.json.");
  process.exit(1);
}

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

const saveTicketRecords = () => {
  try {
    fs.writeFileSync(
      ticketRecordsPath,
      JSON.stringify(Object.fromEntries(ticketRecords.entries()), null, 2)
    );
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

const normalizeUsername = (value) =>
  value ? String(value).replace(/^@/, "") : "";

const escapeHtml = (value) =>
  String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

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

const bot = new Telegraf(token);
const sessions = new Map();
const tickets = new Map();
const adminTicketMessages = new Map();
const duffRequestMessages = new Map();
const customerTickets = new Map();
const ticketHistory = new Map();
const adminPrompts = new Map();
const workerListMessages = new Map();
let ticketCounter = Number.isFinite(Number(config.ticketCounter))
  ? Number(config.ticketCounter)
  : 60;

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
      "📞 <b>Phone number?</b> (will be used to receive updates about your order)",
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
  "👇 <b>Tap to start</b>" +
  quickLinksSection();

const HOTEL_HOME = () =>
  "🏨 <b>Hotel Concierge</b>\n" +
  "<i>Verified stays • Premium deals</i>\n\n" +
  "🧭 <b>How it works:</b> Share your trip details → connect with an agent\n" +
  "⏱️ <b>Response:</b> up to 24h\n\n" +
  "👇 <b>Tap to start</b>" +
  quickLinksSection();

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
    id: "restaurants",
    label: "🍽️ Restaurants",
  },
  {
    id: "dine_in",
    label: "🍴 Dine-In",
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
  const rows = [
    [Markup.button.callback(FOOD_CATEGORIES[0].label, `food:${FOOD_CATEGORIES[0].id}`)],
    [Markup.button.callback(FOOD_CATEGORIES[1].label, `food:${FOOD_CATEGORIES[1].id}`)],
    [Markup.button.callback(FOOD_CATEGORIES[2].label, `food:${FOOD_CATEGORIES[2].id}`)],
    [
      Markup.button.callback(FOOD_CATEGORIES[3].label, `food:${FOOD_CATEGORIES[3].id}`),
      Markup.button.callback(FOOD_CATEGORIES[4].label, `food:${FOOD_CATEGORIES[4].id}`),
    ],
    [
      Markup.button.callback(FOOD_CATEGORIES[5].label, `food:${FOOD_CATEGORIES[5].id}`),
      Markup.button.callback(FOOD_CATEGORIES[6].label, `food:${FOOD_CATEGORIES[6].id}`),
    ],
    [
      Markup.button.callback(FOOD_CATEGORIES[7].label, `food:${FOOD_CATEGORIES[7].id}`),
      Markup.button.callback(FOOD_CATEGORIES[8].label, `food:${FOOD_CATEGORIES[8].id}`),
    ],
    [
      Markup.button.callback(FOOD_CATEGORIES[9].label, `food:${FOOD_CATEGORIES[9].id}`),
      Markup.button.callback(FOOD_CATEGORIES[10].label, `food:${FOOD_CATEGORIES[10].id}`),
    ],
    [
      Markup.button.callback(FOOD_CATEGORIES[11].label, `food:${FOOD_CATEGORIES[11].id}`),
      Markup.button.callback(FOOD_CATEGORIES[12].label, `food:${FOOD_CATEGORIES[12].id}`),
    ],
    [
      Markup.button.callback(FOOD_CATEGORIES[13].label, `food:${FOOD_CATEGORIES[13].id}`),
      Markup.button.callback(FOOD_CATEGORIES[14].label, `food:${FOOD_CATEGORIES[14].id}`),
    ],
    [
      Markup.button.callback(FOOD_CATEGORIES[15].label, `food:${FOOD_CATEGORIES[15].id}`),
      Markup.button.callback(FOOD_CATEGORIES[16].label, `food:${FOOD_CATEGORIES[16].id}`),
    ],
    [Markup.button.callback(FOOD_CATEGORIES[17].label, `food:${FOOD_CATEGORIES[17].id}`)],
    [Markup.button.callback(FOOD_CATEGORIES[18].label, `food:${FOOD_CATEGORIES[18].id}`)],
    [
      Markup.button.callback(FOOD_CATEGORIES[19].label, `food:${FOOD_CATEGORIES[19].id}`),
      Markup.button.callback(FOOD_CATEGORIES[20].label, `food:${FOOD_CATEGORIES[20].id}`),
    ],
  ];

  if (includeBack) {
    rows.push([Markup.button.callback("⬅️ Back to main menu", "menu:main")]);
  }

  return Markup.inlineKeyboard(rows);
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
  const couponState = ticket.couponRequested ? "Coupon requested" : "No coupon";
  return `#${ticketId} ${serviceLabel} · ${assigned} · ${couponState}`;
};

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
    Markup.button.callback("🛑 Close ticket", `close:${ticketId}`),
    Markup.button.callback("🚫 Ban customer", `ban:${ticketId}`),
  ]);
  rows.push([Markup.button.callback("🎟️ Request coupon", `coupon:${ticketId}`)]);
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

const getOpenTicketByChat = (chatId) => {
  const ticketId = customerTickets.get(chatId);
  if (!ticketId) {
    return null;
  }
  const ticket = tickets.get(ticketId);
  if (!ticket || ticket.status !== "open") {
    customerTickets.delete(chatId);
    return null;
  }
  return { ticketId, ticket };
};

const closeTicketsForChat = (chatId, closeType, closedBy) => {
  for (const [ticketId, ticket] of tickets.entries()) {
    if (ticket.chatId === chatId && ticket.status === "open") {
      ticket.status = "closed";
      tickets.set(ticketId, ticket);
      closeTicketRecord(ticketId, { closeType, closedBy });
    }
  }
  customerTickets.delete(chatId);
};

const forwardCustomerMessage = async (telegram, ctx, ticketId, ticket) => {
  const customerLabel = ctx.from.first_name || "Customer";
  const header = `Customer (${customerLabel}) on ticket #${ticketId}:`;
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
        const message = await telegram.sendMessage(
          target.chatId,
          `${header} ${ctx.message.text}`,
          replyId ? { reply_to_message_id: replyId } : undefined
        );
        adminTicketMessages.set(
          adminMessageKey(target.chatId, message.message_id),
          { ticketId, botKey: ticket.botKey }
        );
      } catch (_) {
        if (replyId) {
          try {
            const message = await telegram.sendMessage(
              target.chatId,
              `${header} ${ctx.message.text}`
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
      const headerMessage = await telegram.sendMessage(
        target.chatId,
        header,
        replyId ? { reply_to_message_id: replyId } : undefined
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
          const headerMessage = await telegram.sendMessage(
            target.chatId,
            header
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
  ctx.reply(text, { parse_mode: "HTML", ...extra });

const sendHome = async (ctx, caption, keyboard) => {
  if (LOGO_BYTES) {
    try {
      return await ctx.replyWithPhoto(
        { source: LOGO_BYTES },
        { caption, parse_mode: "HTML", ...keyboard }
      );
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
  resetSession(ctx.chat.id);
  return sendHome(
    ctx,
    "👋 <b>Foodbot Concierge</b>\n<i>Food Orders • Flight Bookings • Hotel Bookings</i>\n\n" +
      "🟢 <b>Status:</b> Up\n" +
      "🎯 <b>How it works:</b> Pick a service → answer a few questions → connect with an agent\n" +
      "🔥 <b>Promos:</b> Food 50% off · Flights 40% off\n\n" +
      "👇 <b>Tap a food category to begin</b>" +
      quickLinksSection(),
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
    return ctx.reply("Ticket already closed.");
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
    return ctx.reply("Ticket not found.");
  }

  const record = ticketRecords.get(ticketId);
  const chatId = ticket?.chatId || record?.chatId;
  if (ticket) {
    ticket.status = "closed";
    tickets.set(ticketId, ticket);
  }
  if (chatId) {
    customerTickets.delete(chatId);
    ctx.telegram.sendMessage(chatId, `Ticket #${ticketId} has been closed.`).catch(() => {});
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
  return ctx.reply(
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
    return ctx.reply('Usage: /close <ticket_id> <profit> "remarks"');
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
    return ctx.reply("Usage: /ban <chat_id>");
  }
  bannedChatIds.add(chatId);
  closeTicketsForChat(chatId, "banned", adminAlias(ctx));
  saveConfig();
  return ctx.reply(`Chat ${chatId} banned.`);
});

bot.command("unban", (ctx) => {
  if (!isAdminChat(ctx)) {
    return;
  }
  const parts = ctx.message.text.trim().split(/\s+/);
  const chatId = Number(parts[1]);
  if (!Number.isFinite(chatId)) {
    return ctx.reply("Usage: /unban <chat_id>");
  }
  if (!bannedChatIds.has(chatId)) {
    return ctx.reply("Chat is not banned.");
  }
  bannedChatIds.delete(chatId);
  saveConfig();
  return ctx.reply(`Chat ${chatId} unbanned.`);
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
      await ctx.reply("Ticket is closed or no longer exists.");
      return;
    }

    const adminLabel = adminAlias(ctx);

    if (ctx.message.text) {
      await ctx.telegram.sendMessage(
        ticket.chatId,
        `${adminLabel}: ${ctx.message.text}`
      );
      return;
    }

    await ctx.telegram.sendMessage(ticket.chatId, `${adminLabel} sent:`);
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
      await ctx.reply("Ticket is closed or no longer exists.");
      return;
    }

    const adminId = ticket.assignedAdminId;
    if (!adminId) {
      await ctx.reply("No worker assigned yet. Ask them to /accept first.");
      return;
    }

    const couponText = ctx.message?.text;
    if (!couponText) {
      await ctx.reply("Please send coupon text.");
      return;
    }

    const duffName = ctx.from?.first_name || "Duff";
    ticket.couponProvidedBy = duffName;
    ticket.couponProvidedAt = new Date().toISOString();
    ticket.couponText = couponText;
    tickets.set(entry.ticketId, ticket);
    updateTicketRecord(entry.ticketId, {
      couponProvidedBy: duffName,
      couponProvidedAt: ticket.couponProvidedAt,
    });

    await ctx.telegram.sendMessage(
      adminId,
      `🎟️ Duff coupon for ticket #${entry.ticketId}:\n${couponText}`
    );
    await ctx.reply("Coupon sent to the worker.");
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
    await ctx.reply(text, { parse_mode: "HTML", reply_markup: workerPanelKeyboard().reply_markup });
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
    const sent = await ctx.reply(text, {
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
  await ctx.reply('Send: <code>&lt;ticket_id&gt; &lt;profit&gt; "remarks"</code>', {
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
  await ctx.reply("Send your new alias.");
};

const adminPromptHandler = async (ctx, next) => {
  if (!isAdminChat(ctx)) {
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
      await ctx.reply("Alias cannot be empty.");
      return;
    }
    adminAliases[ctx.chat.id] = alias;
    saveConfig();
    adminPrompts.delete(ctx.chat.id);
    await ctx.reply(`Alias set to: ${alias}`);
    return;
  }
  if (prompt.action === "close") {
    const parsed = parseCloseInput(ctx.message.text, false);
    if (!parsed) {
      await ctx.reply('Usage: <ticket_id> <profit> "remarks"');
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
    return ctx.reply("Usage: /setname <alias>");
  }
  adminAliases[ctx.chat.id] = alias;
  saveConfig();
  return ctx.reply(`Alias set to: ${alias}`);
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
    return ctx.reply("Usage: /accept <ticket_id>");
  }
  const alias = adminAlias(ctx);
  const result = assignTicket(ticketId, ctx.chat.id, alias);
  if (!result.ok) {
    return ctx.reply(
      result.reason === "assigned"
        ? "Ticket already accepted by another worker."
        : "Ticket not found or already closed."
    );
  }
  updateAdminTicketMessages(ticketId, ctx.chat.id, ctx.telegram);
  refreshWorkerLists(ctx.telegram);
  return ctx.reply(`Ticket #${ticketId} accepted as ${alias}.`);
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
    "• <code>/coupon &lt;ticket_id&gt; &lt;coupon text&gt;</code>",
    "• Reply to a coupon request to send the coupon",
    "",
  ];

  if (live.length) {
    lines.push("🟢 <b>Live coupon requests</b>");
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

const couponCommand = (ctx) => {
  if (!isDuffChat(ctx)) {
    return;
  }
  const match = ctx.message.text.trim().match(/^\/coupon\s+(\d+)\s+([\s\S]+)$/i);
  if (!match) {
    return ctx.reply("Usage: /coupon <ticket_id> <coupon text>");
  }
  const ticketId = Number(match[1]);
  const couponText = match[2].trim();
  const ticket = tickets.get(ticketId);
  if (!ticket || ticket.status !== "open") {
    return ctx.reply("Ticket not found or closed.");
  }
  if (!ticket.assignedAdminId) {
    return ctx.reply("No worker assigned yet. Ask them to /accept first.");
  }
  const duffName = ctx.from?.first_name || "Duff";
  ticket.couponProvidedBy = duffName;
  ticket.couponProvidedAt = new Date().toISOString();
  ticket.couponText = couponText;
  tickets.set(ticketId, ticket);
  updateTicketRecord(ticketId, {
    couponProvidedBy: duffName,
    couponProvidedAt: ticket.couponProvidedAt,
  });
  ctx.telegram
    .sendMessage(
      ticket.assignedAdminId,
      `🎟️ Duff coupon for ticket #${ticketId}:\n${couponText}`
    )
    .catch(() => {});
  return ctx.reply("Coupon sent to the worker.");
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
    await ctx.answerCbQuery("Only the assigned worker can request coupons.");
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
  let text = `🎟️ Coupon request for ticket #${ticketId}\nService: ${
    ticket.service || ticket.category || "Order"
  }`;
  if (ticket.category) {
    text += `\nCategory: ${ticket.category}`;
  }
  text += `\nWorker: ${alias}\n\n${summary}\n\n`;
  text += "Reply to this message with the coupon details, or use /coupon <ticket_id> <code>.";

  duffChatIds.forEach((chatId) => {
    ctx.telegram
      .sendMessage(chatId, text)
      .then((message) => {
        duffRequestMessages.set(
          duffMessageKey(chatId, message.message_id),
          { ticketId, botKey: ticket.botKey }
        );
      })
      .catch(() => {});
  });

  await ctx.answerCbQuery("Coupon request sent to Duff.");
  await ctx.reply("🎟️ Coupon request sent to Duff manager.");
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
  customerTickets.delete(ticket.chatId);
  closeTicketRecord(ticketId, {
    closeType: "manual_close",
    closedBy: adminAlias(ctx),
  });

  await ctx.answerCbQuery("Ticket closed.");
  await ctx.reply(`Ticket #${ticketId} closed.`);
  await refreshWorkerLists(ctx.telegram);
  return ctx.telegram.sendMessage(
    ticket.chatId,
    `Ticket #${ticketId} has been closed by an admin.`
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
  customerTickets.delete(ticket.chatId);
  closeTicketRecord(ticketId, {
    closeType: "banned",
    closedBy: adminAlias(ctx),
  });

  await ctx.answerCbQuery("Customer banned.");
  await ctx.reply(`Ticket #${ticketId} closed and customer banned.`);
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
bot.command("coupon", couponCommand);

registerAdminReplyHandler(bot, "food");
registerDuffReplyHandler(bot);

bot.on("text", adminPromptHandler);

bot.action("menu:main", async (ctx) => {
  resetSession(ctx.chat.id);
  await ctx.answerCbQuery();
  return replyHtml(ctx, "🍔 <b>Food menu</b>", mainMenu());
});

bot.action("menu:food", async (ctx) => {
  resetSession(ctx.chat.id);
  await ctx.answerCbQuery();
  return replyHtml(ctx, "🍔 <b>Choose a food category</b>", foodMenu(false));
});

bot.action("worker:panel", workerPanelAction);
bot.action("worker:view", workerViewAction);
bot.action("worker:close", workerCloseAction);
bot.action("worker:alias", workerAliasAction);

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

  setSession(ctx.chat.id, {
    service: "food",
    stage: "food_questions",
    stepIndex: 0,
    answers: {},
    foodCategory: category.label,
  });

  await ctx.answerCbQuery();
  await replyHtml(ctx, FOOD_PROMO);
  return replyHtml(ctx, FOOD_QUESTIONS[0].prompt);
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
  await ctx.reply(
    "Thanks! Your request is queued. We'll follow up shortly."
  );
  resetSession(ctx.chat.id);
});

bot.on("text", (ctx) => {
  if (isAdminChat(ctx)) {
    return;
  }
  const session = sessions.get(ctx.chat.id);
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
    return replyHtml(ctx, START_PROMPT);
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
        return ctx.reply(
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
      });
      customerTickets.set(ctx.chat.id, ticketId);

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
      ctx.reply(
        "🕘 You're being connected over to our workers! This could take a few moments..."
      );
      return ctx.reply(
        `✅ Your ticket has been created, and you're now connected with our workers! This is ticket #${ticketId}`
      );
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
        return ctx.reply(
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
      });
      customerTickets.set(ctx.chat.id, ticketId);

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
      ctx.reply(
        "🕘 You're being connected over to our workers! This could take a few moments..."
      );
      return ctx.reply(
        `✅ Your ticket has been created, and you're now connected with our workers! This is ticket #${ticketId}`
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
        return ctx.reply(
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
      });
      customerTickets.set(ctx.chat.id, ticketId);

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
      ctx.reply(
        "🕘 You're being connected over to our workers! This could take a few moments..."
      );
      return ctx.reply(
        `✅ Your ticket has been created, and you're now connected with our workers! This is ticket #${ticketId}`
      );
    }

    return replyHtml(ctx, START_PROMPT);
  }

  if (session.stage === "confirm") {
    return replyHtml(
      ctx,
      "⚠️ <b>Use the buttons</b> to submit or edit your request."
    );
  }

  if (session.stage !== "collecting") {
    return replyHtml(ctx, START_PROMPT);
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
  return ctx.reply(
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
  const {
    sessions: flightSessions,
    setSession: setFlightSession,
    resetSession: resetFlightSession,
  } = createSessionStore(flightBot.telegram);

  flightBot.use(banGuard);
  registerAdminReplyHandler(flightBot, "flight");
  registerDuffReplyHandler(flightBot);

  flightBot.start((ctx) => {
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
  flightBot.command("coupon", couponCommand);

  flightBot.action("flight:start", async (ctx) => {
    setFlightSession(ctx.chat.id, {
      service: "flight",
      stage: "flight_questions",
      stepIndex: 0,
      answers: {},
    });
    await ctx.answerCbQuery();
    await replyHtml(ctx, FLIGHT_PROMO);
    return replyHtml(ctx, FLIGHT_QUESTIONS[0].prompt);
  });

  flightBot.action("worker:panel", workerPanelAction);
  flightBot.action("worker:view", workerViewAction);
  flightBot.action("worker:close", workerCloseAction);
  flightBot.action("worker:alias", workerAliasAction);

  flightBot.action(/^accept:(\d+)/, handleAcceptAction);
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

  flightBot.on("text", (ctx) => {
    if (isAdminChat(ctx)) {
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
        return ctx.reply(
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
      });
      customerTickets.set(ctx.chat.id, ticketId);

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
      ctx.reply(
        "🕘 You're being connected over to our workers! This could take a few moments..."
      );
      return ctx.reply(
        `✅ Your ticket has been created, and you're now connected with our workers! This is ticket #${ticketId}`
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
  const {
    sessions: hotelSessions,
    setSession: setHotelSession,
    resetSession: resetHotelSession,
  } = createSessionStore(hotelBot.telegram);

  hotelBot.use(banGuard);
  registerAdminReplyHandler(hotelBot, "hotel");
  registerDuffReplyHandler(hotelBot);

  hotelBot.start((ctx) => {
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
  hotelBot.command("coupon", couponCommand);

  hotelBot.action("hotel:start", async (ctx) => {
    setHotelSession(ctx.chat.id, {
      service: "hotel",
      stage: "hotel_questions",
      stepIndex: 0,
      answers: {},
    });
    await ctx.answerCbQuery();
    await replyHtml(ctx, HOTEL_PROMO);
    return replyHtml(ctx, HOTEL_QUESTIONS[0].prompt);
  });

  hotelBot.action("worker:panel", workerPanelAction);
  hotelBot.action("worker:view", workerViewAction);
  hotelBot.action("worker:close", workerCloseAction);
  hotelBot.action("worker:alias", workerAliasAction);

  hotelBot.action(/^accept:(\d+)/, handleAcceptAction);
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

  hotelBot.on("text", (ctx) => {
    if (isAdminChat(ctx)) {
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
        return ctx.reply(
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
      });
      customerTickets.set(ctx.chat.id, ticketId);

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
      ctx.reply(
        "🕘 You're being connected over to our workers! This could take a few moments..."
      );
      return ctx.reply(
        `✅ Your ticket has been created, and you're now connected with our workers! This is ticket #${ticketId}`
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

const withTimeout = (promise, ms, label) =>
  Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms)
    ),
  ]);

const launchBot = async (botInstance, name) => {
  try {
    try {
      await withTimeout(
        botInstance.telegram.deleteWebhook({ drop_pending_updates: true }),
        15000,
        `${name} deleteWebhook`
      );
    } catch (_) {}
    await withTimeout(botInstance.telegram.getMe(), 15000, `${name} getMe`);
    await botInstance.launch({
      dropPendingUpdates: true,
      allowedUpdates: ["message", "callback_query"],
    });
    console.log(`[${name}] bot launched`);
  } catch (err) {
    const message = err?.stack || err?.message || String(err);
    console.error(`[${name}] bot launch failed: ${message}`);
    try {
      botInstance.stop("restart");
    } catch (_) {}
    setTimeout(() => launchBot(botInstance, name), 5000);
  }
};

const registerErrorHandler = (botInstance, name) => {
  botInstance.catch((err, ctx) => {
    const message = err?.stack || err?.message || String(err);
    const chatId = ctx?.chat?.id ? ` chat:${ctx.chat.id}` : "";
    console.error(`[${name}] handler error${chatId}: ${message}`);
  });
};

botEntries.forEach(({ bot: botInstance, name }) => {
  registerErrorHandler(botInstance, name);
  launchBot(botInstance, name);
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
