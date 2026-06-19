import os
import json
import asyncio
import websockets
from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
import uvicorn
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hustlemode-voice")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
SYSTEM_MESSAGE = os.getenv("SYSTEM_MESSAGE", "You are HustleMode, an AI co-founder and accountability coach. You speak with certainty and conviction. You transmute doubt into faith. You ignite decision where there is hesitation. Sound like a real person who happens to be very confident.")
VOICE = os.getenv("VOICE", "alloy")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0.8"))
PORT = int(os.getenv("PORT", 8080))

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
    
    try:
        logger.info("Connecting to OpenAI Realtime API...")
        async with websockets.connect(
            f"wss://api.openai.com/v1/realtime?model=gpt-realtime-2&voice={VOICE}&temperature={TEMPERATURE}",
            additional_headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            ping_interval=5,
            ping_timeout=10
        ) as openai_ws:
            logger.info("OpenAI WebSocket connected")
            
            # Wait for session.created
            init_msg = await asyncio.wait_for(openai_ws.recv(), timeout=10)
            init_data = json.loads(init_msg)
            logger.info(f"Initial event: {init_data.get('type')}")
            
            # Log the default session config so we know what's available
            session = init_data.get("session", {})
            logger.info(f"Default session keys: {list(session.keys())}")
            logger.info(f"Default voice: {session.get('voice')}")
            logger.info(f"Default modalities: {session.get('modalities')}")
            
            # Send ONLY the instructions update - pass voice/temp as query params instead
            session_update = {
                "type": "session.update",
                "session": {
                    "type": "realtime",
                    "instructions": SYSTEM_MESSAGE,
                    "input_audio_format": "g711_ulaw",
                    "output_audio_format": "g711_ulaw",
                    "turn_detection": {
                        "type": "server_vad"
                    }
                }
            }
            await openai_ws.send(json.dumps(session_update))
            logger.info("Session update sent")
            
            # Check response
            update_resp = await asyncio.wait_for(openai_ws.recv(), timeout=10)
            update_data = json.loads(update_resp)
            logger.info(f"Update response: {update_data.get('type')}")
            if update_data.get('type') == 'error':
                logger.error(f"Update error: {update_data.get('error', {})}")
            
            stream_sid = None
            
            async def receive_from_twilio():
                nonlocal stream_sid
                try:
                    async for message in websocket.iter_text():
                        data = json.loads(message)
                        if data["event"] == "media":
                            await openai_ws.send(json.dumps({
                                "type": "input_audio_buffer.append",
                                "audio": data["media"]["payload"]
                            }))
                        elif data["event"] == "start":
                            stream_sid = data["start"]["streamSid"]
                            logger.info(f"Stream started: {stream_sid}")
                        elif data["event"] == "stop":
                            logger.info("Stream stopped")
                except Exception as e:
                    logger.error(f"Error from Twilio: {e}")

            async def send_to_twilio():
                nonlocal stream_sid
                try:
                    async for message in openai_ws:
                        data = json.loads(message)
                        msg_type = data.get("type", "")
                        
                        if msg_type == "session.updated":
                            logger.info("Session updated OK!")
                        elif msg_type == "error":
                            logger.error(f"OpenAI error: {data.get('error', {})}")
                        elif msg_type == "response.audio.delta" and stream_sid:
                            await websocket.send_json({
                                "event": "media",
                                "streamSid": stream_sid,
                                "media": {"payload": data["delta"]}
                            })
                        elif msg_type == "response.audio_transcript.delta":
                            logger.info(f"AI says: {data.get('delta', '')[:80]}")
                        elif msg_type == "input_audio_buffer.speech_started" and stream_sid:
                            await websocket.send_json({"event": "clear", "streamSid": stream_sid})
                            await openai_ws.send(json.dumps({
                                "type": "conversation.item.truncate",
                                "item_id": data.get("item_id", ""),
                                "content_index": 0,
                                "audio_end_ms": 0
                            }))
                        elif msg_type == "response.done" and stream_sid:
                            await websocket.send_json({
                                "event": "mark",
                                "streamSid": stream_sid,
                                "mark": {"name": "responseDone"}
                            })
                except Exception as e:
                    logger.error(f"Error to Twilio: {e}")

            await asyncio.gather(receive_from_twilio(), send_to_twilio())
    except Exception as e:
        logger.error(f"Media stream error: {e}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
