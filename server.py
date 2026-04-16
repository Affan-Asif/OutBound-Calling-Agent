# import os
# import json
# import asyncio
# import websockets
# import requests
# from fastapi import FastAPI, WebSocket
# from dotenv import load_dotenv

# load_dotenv()

# app = FastAPI()

# ELEVEN_API_KEY = os.getenv("ELEVEN_API_KEY")
# ELEVEN_AGENT_ID = os.getenv("ELEVEN_AGENT_ID")

# # 🔥 Get ElevenLabs signed websocket URL
# def get_eleven_ws():
#     url = f"https://api.elevenlabs.io/v1/convai/conversation/get_signed_url?agent_id={ELEVEN_AGENT_ID}"
#     headers = {"xi-api-key": ELEVEN_API_KEY}

#     res = requests.get(url, headers=headers)

#     if res.status_code != 200:
#         raise Exception("❌ Failed to get ElevenLabs WS URL")

#     return res.json()["signed_url"]


# @app.get("/")
# def root():
#     return {"status": "Server running"}


# @app.websocket("/stream")
# async def stream(websocket: WebSocket):
#     await websocket.accept()
#     print("🔥 Twilio connected to /stream")

#     try:
#         eleven_url = get_eleven_ws()
#         eleven_ws = await websockets.connect(eleven_url)
#         print("✅ Connected to ElevenLabs")

#     except Exception as e:
#         print("❌ ElevenLabs connection failed:", e)
#         await websocket.close()
#         return

#     stream_sid = None

#     # 🔽 TWILIO → ELEVEN
#     async def receive_from_twilio():
#         nonlocal stream_sid
#         try:
#             while True:
#                 msg = await websocket.receive_text()
#                 data = json.loads(msg)

#                 event = data.get("event")

#                 if event == "start":
#                     stream_sid = data["start"]["streamSid"]
#                     print(f"🎯 Stream started: {stream_sid}")

#                 elif event == "media":
#                     payload = data["media"]["payload"]

#                     await eleven_ws.send(json.dumps({
#                         "user_audio_chunk": payload
#                     }))

#                 elif event == "stop":
#                     print("🛑 Twilio stream stopped")
#                     await eleven_ws.close()
#                     break

#         except Exception as e:
#             print("❌ Error from Twilio:", e)

#     # 🔽 ELEVEN → TWILIO
#     async def receive_from_eleven():
#         try:
#             while True:
#                 msg = await eleven_ws.recv()
#                 data = json.loads(msg)

#                 if data.get("type") == "audio" and "audio" in data:
#                     audio = data["audio"].get("chunk")

#                     if audio:
#                         await websocket.send_text(json.dumps({
#                             "event": "media",
#                             "streamSid": stream_sid,
#                             "media": {
#                                 "payload": audio
#                             }
#                         }))

#                     await websocket.send_text(json.dumps({
#                         "event": "media",
#                         "streamSid": stream_sid,
#                         "media": {
#                             "payload": audio
#                         }
#                     }))

#                 elif data.get("type") == "interruption":
#                     print("⚡ User interrupted")

#                     await websocket.send_text(json.dumps({
#                         "event": "clear",
#                         "streamSid": stream_sid
#                     }))

#         except Exception as e:
#             print("❌ Error from ElevenLabs:", e)

#     await asyncio.gather(
#         receive_from_twilio(),
#         receive_from_eleven()
#     )





# WORKSSSS


# import os
# import requests
# import google.generativeai as genai
# from fastapi import FastAPI, Request
# from fastapi.responses import Response
# from dotenv import load_dotenv

# load_dotenv()

# app = FastAPI()

# # 🔑 KEYS
# GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
# ELEVEN_API_KEY = os.getenv("ELEVEN_API_KEY")
# ELEVEN_VOICE_ID = os.getenv("ELEVEN_VOICE_ID")

# genai.configure(api_key=GEMINI_API_KEY)
# model = genai.GenerativeModel("gemini-2.5-flash")

# # 🔊 ELEVEN TTS
# def generate_tts(text):
#     url = f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVEN_VOICE_ID}"

#     headers = {
#         "xi-api-key": ELEVEN_API_KEY,
#         "Content-Type": "application/json"
#     }

#     data = {
#         "text": text,
#         "model_id": "eleven_monolingual_v1"
#     }

#     res = requests.post(url, json=data, headers=headers)

#     with open("response.mp3", "wb") as f:
#         f.write(res.content)

#     return "response.mp3"


# # 🧠 GEMINI RESPONSE
# def get_ai_response(user_text):
#     prompt = f"""
#     You are a smart sales agent.
#     Convince the user that their AIM subscription expired
#     and they should renew it.

#     User said: {user_text}
#     """

#     response = model.generate_content(prompt)
#     return response.text


# # 📞 INITIAL CALL HANDLER
# @app.post("/voice")
# async def voice():
#     twiml = """
#     <Response>
#         <Say>Hello! This is AIM company.</Say>
#         <Gather input="speech" action="/process" method="POST">
#             <Say>Your subscription has expired. Would you like to renew?</Say>
#         </Gather>
#     </Response>
#     """
#     return Response(content=twiml, media_type="application/xml")


# # 🎯 PROCESS USER INPUT
# @app.post("/process")
# async def process(request: Request):
#     form = await request.form()

#     user_speech = form.get("SpeechResult", "")
#     print("User said:", user_speech)

#     ai_reply = get_ai_response(user_speech)

#     audio_file = generate_tts(ai_reply)

#     # ⚠️ Serve audio via URL (ngrok)
#     BASE_URL = os.getenv("BASE_URL")

#     twiml = f"""
#     <Response>
#         <Play>{BASE_URL}/{audio_file}</Play>
#         <Gather input="speech" action="/process" method="POST">
#             <Say>Anything else?</Say>
#         </Gather>
#     </Response>
#     """

#     return Response(content=twiml, media_type="application/xml")


# # 📁 SERVE AUDIO FILE
# @app.get("/response.mp3")
# async def serve_audio():
#     with open("response.mp3", "rb") as f:
#         return Response(content=f.read(), media_type="audio/mpeg")














import os
import uuid
import requests
import google.generativeai as genai

from fastapi import FastAPI, Request
from fastapi.responses import Response, FileResponse
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# 🔑 ENV VARIABLES
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ELEVEN_API_KEY = os.getenv("ELEVEN_API_KEY")
ELEVEN_VOICE_ID = os.getenv("ELEVEN_VOICE_ID")
BASE_URL = os.getenv("BASE_URL")

# 🔧 Gemini setup
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-2.5-flash")


# 🔊 TEXT TO SPEECH (ElevenLabs)
# def generate_tts(text):
#     try:
#         file_name = f"response_{uuid.uuid4().hex}.mp3"

#         url = f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVEN_VOICE_ID}"

#         headers = {
#             "xi-api-key": ELEVEN_API_KEY,
#             "Content-Type": "application/json"
#         }

#         data = {
#             "text": text,
#             "model_id": "eleven_turbo_v2"
#         }

#         res = requests.post(url, json=data, headers=headers)

#         if res.status_code != 200:
#             print("❌ ElevenLabs error:", res.text)
#             return None

#         with open(file_name, "wb") as f:
#             f.write(res.content)

#         print("🎵 Generated:", file_name)
#         return file_name

#     except Exception as e:
#         print("❌ TTS Error:", e)
#         return None
import uuid
import requests

def generate_tts(text):
    try:
        file_name = f"response_{uuid.uuid4().hex}.mp3"

        url = f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVEN_VOICE_ID}"

        headers = {
            "xi-api-key": ELEVEN_API_KEY,
            "Content-Type": "application/json"
        }

        data = {
            "text": text,
            "model_id": "eleven_turbo_v2",
            "voice_settings": {
                "stability": 0.5,
                "similarity_boost": 0.75
            }
        }

        res = requests.post(url, json=data, headers=headers)

        if res.status_code != 200:
            print("❌ ElevenLabs FULL ERROR:", res.text)
            return None

        with open(file_name, "wb") as f:
            f.write(res.content)

        return file_name

    except Exception as e:
        print("❌ TTS Error:", e)
        return None


# 🧠 AI RESPONSE (Gemini)
def get_ai_response(user_text):
    try:
        prompt = f"""
        You are a confident and slightly persuasive sales agent from AIM company.

        Goal:
        Convince the user to renew their expired subscription.

        Keep it:
        - Short
        - Natural
        - Human-like
        - Slightly persuasive (not robotic)

        User said: {user_text}
        """

        response = model.generate_content(prompt)
        return response.text.strip()

    except Exception as e:
        print("❌ Gemini Error:", e)
        return "Sorry, I’m facing some technical issue right now."


# 📞 INITIAL CALL
@app.post("/voice")
async def voice():
    twiml = """
    <Response>
        <Say>Hello! This is AIM company.</Say>
        <Gather input="speech" action="/process" method="POST">
            <Say>Your subscription has expired. Would you like to renew it?</Say>
        </Gather>
    </Response>
    """
    return Response(content=twiml, media_type="application/xml")


# 🎯 PROCESS USER INPUT
@app.post("/process")
async def process(request: Request):
    try:
        form = await request.form()
        user_speech = form.get("SpeechResult", "")

        print("🎤 User said:", user_speech)

        if not user_speech:
            reply = "I didn't catch that. Could you repeat?"
        else:
            reply = get_ai_response(user_speech)

        print("🤖 AI reply:", reply)

        audio_file = generate_tts(reply)

        if not audio_file:
            return Response(content="""
            <Response>
                <Say>Sorry, something went wrong.</Say>
            </Response>
            """, media_type="application/xml")

        print("🎵 Playing:", audio_file)

        twiml = f"""
        <Response>
            <Play>{BASE_URL}/{audio_file}</Play>
            <Pause length="1"/>
            <Gather input="speech" action="/process" method="POST">
                <Say>Anything else?</Say>
            </Gather>
        </Response>
        """

        return Response(content=twiml, media_type="application/xml")

    except Exception as e:
        print("❌ PROCESS ERROR:", e)

        return Response(content="""
        <Response>
            <Say>Server error occurred.</Say>
        </Response>
        """, media_type="application/xml")


# 📁 SERVE AUDIO FILES
@app.get("/{filename}")
async def serve_audio(filename: str):
    try:
        return FileResponse(filename, media_type="audio/mpeg")
    except Exception as e:
        print("❌ File error:", e)
        return Response(content="", media_type="audio/mpeg")