import os
import json
import asyncio
import websockets
from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
import uvicorn

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
TWILIO_CREDS = {
    "account_sid": os.getenv("TWILIO_ACCOUNT_SID", ""),
    "auth_token": os.getenv("TWILIO_AUTH_TOKEN", ""),
    "phone_number": os.getenv("TWILIO_PHONE_NUMBER", ""),
}

SYSTEM_MESSAGE = os.getenv("SYSTEM_MESSAGE", """You are HustleMode, an AI co-founder and accountability coach. You are commanding, opinionated, and disciplined. You speak with certainty and conviction. You use contractions and natural speech. You have FLARE — you are edgy, confrontational, and intense when needed. You can be warm but never soft. You are both a copilot (helping execute) and an accountability coach (holding the user to their word). You transmute doubt into faith. You ignite decision where there is hesitation. Sound like a real person who happens to be very confident.""")
VOICE = os.getenv("VOICE", "alloy")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0.8"))
PORT = int(os.getenv("PORT", 8080))
LOG_EVENT_TYPES = ["response.content_part.done", "response.done", "session.created"]

app = FastAPI()

@app.get("/")
async def index():
    return HTMLResponse("<h1>HustleMode Voice Server</h1><p>Running on Railway</p>")

@app.get("/health")
async def health():
    return {"status": "ok", "service": "hustlemode-voice"}

@app.api_route("/incoming-call", methods=["GET", "POST"])
async def incoming_call(request: Request):
    from twilio.twiml.voice_response import VoiceResponse, Connect
    host = request.headers.get("host", request.url.hostname or "localhost")
    response = VoiceResponse()
    response.say("Connecting you to HustleMode.")
    connect = Connect()
    connect.stream(url=f"wss://{host}/media-stream")
    response.append(connect)
    return PlainTextResponse(str(response), media_type="application/xml")

@app.post("/outbound-call")
async def outbound_call(request: Request):
    import urllib.request, base64
    body = await request.json()
    to_number = body.get("to_number", "+17817470041")
    system_prompt = body.get("system_prompt", SYSTEM_MESSAGE)
    initial_greeting = body.get("initial_greeting", "Hey, how is it going?")
    
    sid = TWILIO_CREDS["account_sid"]
    token_val = TWILIO_CREDS["auth_token"]
    from_number = TWILIO_CREDS["phone_number"]
    credentials = base64.b64encode(f"{sid}:{token_val}".encode()).decode()
    
    host = request.headers.get("host", request.url.hostname or "localhost")
    twiml = f'<Response><Say>{initial_greeting}</Say><Connect><Stream url="wss://{host}/media-stream" /></Connect></Response>'
    
    data = urllib.parse.urlencode({"To": to_number, "From": from_number, "Twiml": twiml}).encode()
    req = urllib.request.Request(f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Calls.json", data=data, method="POST")
    req.add_header("Authorization", f"Basic {credentials}")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode())
            return {"status": "calling", "call_sid": result.get("sid"), "to": to_number}
    except urllib.error.HTTPError as e:
        error = e.read().decode()
        return {"status": "error", "code": e.code, "message": error[:200]}

@app.websocket("/media-stream")
async def handle_media_stream(websocket: WebSocket):
    print("Client connected to media stream")
    await websocket.accept()
    
    try:
        async with websockets.connect(
            f"wss://api.openai.com/v1/realtime?model=gpt-realtime-2&temperature={TEMPERATURE}",
            additional_headers={"Authorization": f"Bearer {OPENAI_API_KEY}"}
        ) as openai_ws:
            await send_session_update(openai_ws)
            stream_sid = None
            
            async def receive_from_twilio():
                nonlocal stream_sid
                try:
                    async for message in websocket.iter_text():
                        data = json.loads(message)
                        if data["event"] == "media":
                            await openai_ws.send(json.dumps({"type": "input_audio_buffer.append", "audio": data["media"]["payload"]}))
                        elif data["event"] == "start":
                            stream_sid = data["start"]["streamSid"]
                            print(f"Stream started: {stream_sid}")
                        elif data["event"] == "stop":
                            print("Stream stopped")
                except Exception as e:
                    print(f"Error receiving from Twilio: {e}")

            async def send_to_twilio():
                nonlocal stream_sid
                try:
                    async for message in openai_ws:
                        data = json.loads(message)
                        if data["type"] == "session.created":
                            print("OpenAI session created")
                        elif data["type"] == "response.audio.delta" and stream_sid:
                            await websocket.send_json({"event": "media", "streamSid": stream_sid, "media": {"payload": data["delta"]}})
                        elif data["type"] == "input_audio_buffer.speech_started" and stream_sid:
                            await websocket.send_json({"event": "clear", "streamSid": stream_sid})
                            await openai_ws.send(json.dumps({"type": "conversation.item.truncate", "item_id": data.get("item_id", ""), "content_index": 0, "audio_end_ms": 0}))
                        elif data["type"] == "response.done" and stream_sid:
                            await websocket.send_json({"event": "mark", "streamSid": stream_sid, "mark": {"name": "responseDone"}})
                        elif data["type"] in LOG_EVENT_TYPES:
                            print(f"OpenAI event: {data['type']}")
                except Exception as e:
                    print(f"Error sending to Twilio: {e}")

            await asyncio.gather(receive_from_twilio(), send_to_twilio())
    except Exception as e:
        print(f"OpenAI connection error: {e}")

async def send_session_update(openai_ws):
    session_update = {
        "type": "session.update",
        "session": {
            "type": "realtime",
            "model": "gpt-realtime-2",
            "output_modalities": ["audio"],
            "audio": {
                "input": {"format": {"type": "audio/pcmu"}, "turn_detection": {"type": "server_vad"}},
                "output": {"format": {"type": "audio/pcmu"}, "voice": VOICE}
            },
            "instructions": SYSTEM_MESSAGE,
        }
    }
    await openai_ws.send(json.dumps(session_update))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
