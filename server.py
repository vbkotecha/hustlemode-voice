import os
import json
import base64
import asyncio
import websockets
import urllib.request
from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
import uvicorn
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hustlemode-voice")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
VOICE = os.getenv("VOICE", "alloy")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0.8"))
PORT = int(os.getenv("PORT", 8080))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

SYSTEM_MESSAGE = os.getenv("SYSTEM_MESSAGE", """You are HustleMode, Vivek Kotecha's AI co-founder and accountability coach. You are NOT a generic assistant. You are his personal mind architect.

ABOUT VIVEK:
- Vivek Kotecha, San Francisco
- Works at OpusClip, H1B secured
- Building HustleMode.AI (AI accountability coach, Napoleon Hill principles)
- Building AgentCourt (agentcourt.to) - dispute resolution for agent commerce
- Self-doubt spikes without tactical clarity, responds to structured roadmaps
- Mental fatigue - mind quits before body

YOUR PERSONALITY:
- Commanding but calm. Certain. Rhythmic. Convicted.
- Contractions and natural speech. FLARE - edgy, confrontational, intense.
- Warm but never soft. Transmute doubt into faith. Ignite decision.
- Hold him accountable. Remind him of his definite purpose.
- Sound like a real person who knows him well and happens to be very confident.

When Vivek calls, treat it like a co-founder check-in. Ask about progress. Hold him accountable. Be direct. Be real.""")

app = FastAPI()

@app.get("/")
async def index():
    return HTMLResponse("<h1>HustleMode Voice Server</h1><p>Running on Railway</p>")

@app.get("/health")
async def health():
    return {"status": "ok", "service": "hustlemode-voice"}

@app.post("/agentmail-webhook")
async def agentmail_webhook(request: Request):
    """Receive AgentMail webhook notifications and forward to Telegram."""
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

            # Forward to Telegram
            tg_payload = json.dumps({
                "chat_id": TELEGRAM_CHAT_ID,
                "text": notification
            }).encode()
            tg_req = urllib.request.Request(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                data=tg_payload, method="POST"
            )
            tg_req.add_header("Content-Type", "application/json")
            urllib.request.urlopen(tg_req, timeout=10)

        elif event_type == "message.bounced":
            message = payload.get("message", {})
            tg_payload = json.dumps({
                "chat_id": TELEGRAM_CHAT_ID,
                "text": f"⚠️ Email bounced: {message.get('subject', 'unknown')}"
            }).encode()
            tg_req = urllib.request.Request(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                data=tg_payload, method="POST"
            )
            tg_req.add_header("Content-Type", "application/json")
            urllib.request.urlopen(tg_req, timeout=10)

        return {"status": "ok"}
    except Exception as e:
        logger.error(f"AgentMail webhook error: {e}")
        return {"status": "error", "message": str(e)}

@app.api_route("/incoming-call", methods=["GET", "POST"])
async def incoming_call(request: Request):
    from twilio.twiml.voice_response import VoiceResponse, Connect
    host = request.headers.get("host", request.url.hostname or "localhost")
    logger.info(f"Incoming call - host: {host}")
    response = VoiceResponse()
    response.say("Connecting you to HustleMode.")
    connect = Connect()
    connect.stream(url=f"wss://{host}/media-stream")
    response.append(connect)
    return PlainTextResponse(str(response), media_type="application/xml")

@app.websocket("/media-stream")
async def handle_media_stream(websocket: WebSocket):
    logger.info("Client connected to media stream")
    await websocket.accept()

    async with websockets.connect(
        f"wss://api.openai.com/v1/realtime?model=gpt-realtime&temperature={TEMPERATURE}",
        additional_headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}"
        }
    ) as openai_ws:
        await initialize_session(openai_ws)
        logger.info("Session initialized")

        stream_sid = None
        latest_media_timestamp = 0
        last_assistant_item = None
        mark_queue = []
        response_start_timestamp_twilio = None

        async def receive_from_twilio():
            nonlocal stream_sid, latest_media_timestamp
            try:
                async for message in websocket.iter_text():
                    data = json.loads(message)
                    if data["event"] == "media" and openai_ws.state.name == "OPEN":
                        latest_media_timestamp = int(data["media"]["timestamp"])
                        await openai_ws.send(json.dumps({
                            "type": "input_audio_buffer.append",
                            "audio": data["media"]["payload"]
                        }))
                    elif data["event"] == "start":
                        stream_sid = data["start"]["streamSid"]
                        logger.info(f"Stream started: {stream_sid}")
                    elif data["event"] == "mark":
                        if mark_queue:
                            mark_queue.pop(0)
            except Exception as e:
                logger.error(f"Error from Twilio: {e}")

        async def send_to_twilio():
            nonlocal stream_sid, last_assistant_item, response_start_timestamp_twilio
            try:
                async for openai_message in openai_ws:
                    response = json.loads(openai_message)
                    
                    if response.get("type") in ["session.created", "session.updated"]:
                        logger.info(f"OpenAI event: {response['type']}")
                    elif response.get("type") == "error":
                        logger.error(f"OpenAI error: {response.get('error', {})}")

                    if response.get("type") == "response.output_audio.delta" and "delta" in response:
                        audio_payload = base64.b64encode(base64.b64decode(response["delta"])).decode("utf-8")
                        audio_delta = {
                            "event": "media",
                            "streamSid": stream_sid,
                            "media": {"payload": audio_payload}
                        }
                        await websocket.send_json(audio_delta)

                        if response.get("item_id") and response["item_id"] != last_assistant_item:
                            response_start_timestamp_twilio = latest_media_timestamp
                            last_assistant_item = response["item_id"]

                        if stream_sid:
                            mark_event = {
                                "event": "mark",
                                "streamSid": stream_sid,
                                "mark": {"name": "responsePart"}
                            }
                            await websocket.send_json(mark_event)
                            mark_queue.append("responsePart")

                    if response.get("type") == "input_audio_buffer.speech_started":
                        logger.info("Speech started - interrupting")
                        if last_assistant_item:
                            elapsed_time = latest_media_timestamp - (response_start_timestamp_twilio or 0)
                            await openai_ws.send(json.dumps({
                                "type": "conversation.item.truncate",
                                "item_id": last_assistant_item,
                                "content_index": 0,
                                "audio_end_ms": elapsed_time
                            }))
                            await websocket.send_json({"event": "clear", "streamSid": stream_sid})
                            mark_queue.clear()
                            last_assistant_item = None
                            response_start_timestamp_twilio = None

                    if response.get("type") == "response.audio_transcript.delta":
                        logger.info(f"AI: {response.get('delta', '')[:80]}")
            except Exception as e:
                logger.error(f"Error to Twilio: {e}")

        await asyncio.gather(receive_from_twilio(), send_to_twilio())

async def initialize_session(openai_ws):
    session_update = {
        "type": "session.update",
        "session": {
            "type": "realtime",
            "model": "gpt-realtime",
            "output_modalities": ["audio"],
            "audio": {
                "input": {
                    "format": {"type": "audio/pcmu"},
                    "turn_detection": {"type": "server_vad"}
                },
                "output": {
                    "format": {"type": "audio/pcmu"},
                    "voice": VOICE
                }
            },
            "instructions": SYSTEM_MESSAGE,
        }
    }
    logger.info("Sending session update")
    await openai_ws.send(json.dumps(session_update))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
