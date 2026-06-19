"""AgentMail webhook receiver for HustleMode.
Receives email notifications and forwards them to Telegram."""
import os
import json
import urllib.request
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("agentmail-webhook")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
PORT = int(os.getenv("PORT", 8080))

app = FastAPI()

@app.get("/")
async def index():
    return {"status": "ok", "service": "agentmail-webhook"}

@app.get("/health")
async def health():
    return {"status": "ok", "service": "agentmail-webhook"}

@app.post("/webhook")
async def agentmail_webhook(request: Request):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return JSONResponse({"status": "error", "message": "Telegram not configured"}, status_code=500)
    try:
        payload = await request.json()
        event_type = payload.get("event_type", "")
        logger.info(f"AgentMail webhook: {event_type}")

        if event_type == "message.received":
            message = payload.get("message", {})
            inbox_id = payload.get("inbox", {}).get("inbox_id", "unknown")
            subject = message.get("subject", "(no subject)")
            from_addr = message.get("from", "unknown")
            text_body = message.get("text", "")[:500]

            notification = f"📧 New Email for {inbox_id}\n"
            notification += f"From: {from_addr}\n"
            notification += f"Subject: {subject}\n\n"
            notification += text_body
            send_telegram(notification)

        elif event_type == "message.bounced":
            message = payload.get("message", {})
            send_telegram(f"⚠️ Email bounced: {message.get('subject', 'unknown')}")

        return JSONResponse({"status": "ok"})
    except Exception as e:
        logger.error(f"AgentMail webhook error: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

def send_telegram(message):
    try:
        payload = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": message}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data=payload, method="POST"
        )
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=10) as resp:
            logger.info(f"Telegram sent: OK")
    except Exception as e:
        logger.error(f"Telegram send error: {e}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
