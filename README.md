# Foodbot (Telegram)

Minimal Telegram concierge bot for:
- Food Orders
- Flight Bookings
- Hotel Bookings

## Setup
1) Create a bot with @BotFather and copy the token.
2) Install dependencies:
```bash
npm install
```
3) Add your bot token and admin destination:
```bash
cp config.example.json config.json
```
Set `botToken` to your Food bot token. If you want separate Flight/Hotel bots, set `flightBotToken` and `hotelBotToken` too. Optionally add `foodBotUsername`, `flightBotUsername`, and `hotelBotUsername` to show clickable links in the homepage messages. Add `adminChatIds` as your worker chat IDs (array) and `duffChatIds` as your coupon manager chat IDs (array). Configure `sessionTimeoutMinutes` to auto-close inactive sessions (set to 0 to disable) and `rateLimit` to throttle ticket creation (set values to 0 to disable). Use `adminAliases` to store worker pseudonyms, and `bannedChatIds` to pre-block known chat IDs. `ticketCounter` starts at 60 so the next ticket is #61, and `ticketRecords` is maintained automatically for reports.
4) Optional `.env` token fallback:
```bash
cp .env.example .env
```
5) Run the bot:
```bash
npm start
```

## Commands
- `/start` show the service menu
- `/help` quick usage
- `/cancel` reset the current request
- `/report` (admin chat only) show ticket totals
- `/work` (admin chat only) worker panel with tickets
- `/setname <alias>` (admin chat only) set worker alias
- `/accept <ticket_id>` (admin chat only) accept a ticket
- `/close <ticket_id> <profit> "remarks"` (admin chat only) close a ticket with profit + remarks
- `/ban <chat_id>` (admin chat only) ban a customer chat ID
- `/unban <chat_id>` (admin chat only) unban a customer chat ID
- `/duff` (duff chat only) coupon manager panel
- `/coupon <ticket_id> <coupon text>` (duff chat only) send coupon to worker
