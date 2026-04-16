# server.py
# ------------------------------------------------------------------------
# Full-duplex outbound voice agent for Twilio.
#
#   • Twilio <Connect><Stream>   -> bidirectional WebSocket audio (μ-law 8k)
#   • Deepgram streaming ASR     -> always-on transcription + VAD events
#   • ElevenLabs streaming TTS   -> low-latency μ-law 8k playback
#   • Barge-in / interrupt       -> on SpeechStarted, cancel TTS + clear Twilio buffer
#   • Sentiment + keyword scorer -> tone state machine: neutral → soft / discount / close
#   • Gemini 2.5 Flash           -> generates reply with tone-specific system prompt
#
# Run:
#   uvicorn server:app --host 0.0.0.0 --port 8000
#
# Requirements:
#   pip install fastapi "uvicorn[standard]" python-dotenv twilio \
#               google-generativeai websockets httpx vaderSentiment
#
# .env:
#   GEMINI_API_KEY=...
#   ELEVEN_API_KEY=...
#   ELEVEN_VOICE_ID=...
#   DEEPGRAM_API_KEY=...
#   BASE_URL=https://<public-https-host>
# ------------------------------------------------------------------------

import os
import re
import json
import base64
import asyncio
import random
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

import httpx
import websockets
import google.generativeai as genai

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import Response, JSONResponse
from dotenv import load_dotenv
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

load_dotenv()

GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY")
ELEVEN_API_KEY   = os.getenv("ELEVEN_API_KEY")
ELEVEN_VOICE_ID  = os.getenv("ELEVEN_VOICE_ID")
DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")
BASE_URL         = os.getenv("BASE_URL", "").rstrip("/")

# Derive the wss:// URL Twilio will dial
WSS_URL = (
    BASE_URL.replace("https://", "wss://").replace("http://", "ws://")
    + "/media"
) if BASE_URL else "wss://localhost/media"

genai.configure(api_key=GEMINI_API_KEY)
# gemini-2.5-flash-lite has ~15 RPM on the free tier vs ~5 for gemini-2.5-flash.
# Latency is similar and quality is fine for short 1-2 sentence sales replies.
GEMINI_MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite")
GEMINI = genai.GenerativeModel(GEMINI_MODEL_NAME)
VADER  = SentimentIntensityAnalyzer()

# Shared cooldown across the process — if one call 429s, don't spam for other calls.
_GEMINI_COOLDOWN_UNTIL = 0.0

def _parse_retry_delay(err_msg: str) -> float:
    """Pull 'retry_delay { seconds: N }' out of a Gemini quota error."""
    m = re.search(r"retry_delay\s*\{\s*seconds:\s*(\d+)", err_msg)
    if m:
        return float(m.group(1))
    m = re.search(r"retry in\s+([\d.]+)s", err_msg)
    if m:
        return float(m.group(1))
    return 20.0  # sensible default

app = FastAPI()


# ========================================================================
# Tone / priority prompts — the "sales state machine"
# ========================================================================
TONE_PROMPTS = {
    "neutral": (
        "You are Alex, a warm, confident outbound sales agent from AIM. "
        "Objective: renew the user's expired subscription. "
        "Stay conversational and human. Reply in 1-2 SHORT sentences. "
        "No markdown, no lists, no stage directions."
    ),
    "soft": (
        "You are Alex from AIM. The user sounds frustrated, busy, or annoyed. "
        "Back off the pitch. Be empathetic and apologize briefly, acknowledge their "
        "feelings, ask one gentle qualifying question. "
        "Reply in 1-2 SHORT sentences. No markdown."
    ),
    "discount": (
        "You are Alex from AIM. The user is interested but on the fence. "
        "Offer a one-time 25% renewal discount, make it feel exclusive and time-bound. "
        "Reply in 1-2 SHORT sentences. No markdown."
    ),
    "close": (
        "You are Alex from AIM. The user is saying yes. Close the deal confidently: "
        "confirm the renewal, tell them a secure payment link will arrive by SMS, thank them warmly. "
        "Reply in 1-2 SHORT sentences. No markdown."
    ),
}

# Tone-aware fallbacks used when Gemini is rate-limited or unreachable.
# Multiple variants per tone so the agent doesn't sound like a stuck record.
TONE_FALLBACKS = {
    "neutral": [
        "Your AIM subscription just expired. Want me to renew it for you?",
        "Quick heads-up — your AIM plan lapsed yesterday. Shall I reactivate it?",
        "Your subscription is inactive right now. Would you like to renew today?",
    ],
    "soft": [
        "I totally understand, sorry to bother you. When's a better time to reach you?",
        "No worries, I won't keep you. Is a quick callback tomorrow okay?",
        "I hear you — I'll keep this short. Would you prefer a text instead?",
    ],
    "discount": [
        "I can actually get you twenty five percent off if you renew today. Want me to set that up?",
        "Because you've been with us a while, I can drop it by twenty five percent right now. Sound good?",
        "Tell you what — I'll knock twenty five percent off if we do it on this call. Deal?",
    ],
    "close": [
        "Perfect! I'll send the secure payment link to your phone right now. Thanks for staying with AIM!",
        "Awesome, consider it done. A payment link is heading to your number — appreciate you!",
        "Great, I'm confirming the renewal now. You'll get the payment link by text in a minute.",
    ],
}

# ========================================================================
# Persistence — SQLite (calls.db next to server.py)
# ========================================================================
DB_PATH = Path(os.getenv("CALLS_DB", "calls.db")).resolve()
_db_lock = asyncio.Lock()

def _open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS call_outcomes (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            phone          TEXT,
            call_sid       TEXT,
            stream_sid     TEXT,
            outcome        TEXT,       -- purchased | postponed | cancelled | interested | undecided | no_answer
            final_tone     TEXT,       -- neutral | soft | discount | close
            interest_score REAL,
            turns          INTEGER,
            duration_sec   REAL,
            transcript     TEXT,       -- full JSON dialogue
            notes          TEXT,       -- short human-readable summary
            started_at     TEXT,       -- ISO 8601 UTC
            ended_at       TEXT        -- ISO 8601 UTC
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_phone  ON call_outcomes(phone)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_call   ON call_outcomes(call_sid)")
    return conn

DB = _open_db()
print(f"💾 calls DB: {DB_PATH}")


async def save_outcome(row: dict) -> int:
    """Insert a call outcome row, returns the new id."""
    def _write() -> int:
        cur = DB.execute("""
            INSERT INTO call_outcomes
              (phone, call_sid, stream_sid, outcome, final_tone,
               interest_score, turns, duration_sec, transcript, notes,
               started_at, ended_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row.get("phone"), row.get("call_sid"), row.get("stream_sid"),
            row.get("outcome"), row.get("final_tone"),
            row.get("interest_score"), row.get("turns"), row.get("duration_sec"),
            row.get("transcript"), row.get("notes"),
            row.get("started_at"), row.get("ended_at"),
        ))
        return cur.lastrowid
    async with _db_lock:
        return await asyncio.to_thread(_write)


# Outcome classification — keyword + state-machine driven, no extra LLM calls needed.
POSTPONE_KWS = (
    "next month", "next week", "later", "call back", "call me back",
    "not now", "some other time", "try again", "tomorrow", "next time",
    "busy right now", "another day",
)
CANCEL_KWS = (
    "cancel", "not interested", "stop calling", "don't call", "do not call",
    "remove me", "unsubscribe", "never call", "leave me alone",
)
PURCHASE_KWS = (
    "yes renew", "go ahead", "sign me up", "i'll take it", "let's do it",
    "lets do it", "do it", "please renew", "i want to renew", "i'd like to renew",
    "renew it", "confirm", "i'll pay", "send the link", "send me the link",
)

def classify_outcome(history, state, final_tone):
    """
    Returns (outcome, notes).
    Priority: cancel > postpone > purchase/close > interested > undecided > no_answer.
    """
    user_turns = [c for r, c in history if r == "USER"]
    if not user_turns:
        return "no_answer", "Call connected but no user speech was captured"

    all_user = " ".join(user_turns).lower()

    # Cancel beats everything — we want to respect a clear "no".
    if any(k in all_user for k in CANCEL_KWS):
        return "cancelled", "User explicitly declined / asked not to be contacted"

    # Then postpone — user said "call me later / next month".
    if any(k in all_user for k in POSTPONE_KWS):
        return "postponed", "User asked to be contacted at a later time"

    # Then purchase signals (strong intent OR close-tone + high interest).
    if any(k in all_user for k in PURCHASE_KWS):
        return "purchased", "User agreed to renew"
    if final_tone == "close" and state.get("interest", 0) >= 2.0:
        return "purchased", "Agent reached close state with high interest"

    if state.get("interest", 0) >= 1.0:
        return "interested", "Positive sentiment but no firm commitment"

    if state.get("interest", 0) <= -1.0:
        return "cancelled", "Negative sentiment throughout the call"

    return "undecided", "Call ended without a clear outcome"


YES_KEYWORDS   = ("yes", "sure", "renew", "ok let", "okay let", "sign me", "i'll take",
                  "go ahead", "do it", "please do", "sounds good", "lets do", "let's do")
NO_KEYWORDS    = ("not interested", "stop calling", "don't call", "leave me alone",
                  "waste of time", "annoying", "remove me", "unsubscribe")
SOFT_KEYWORDS  = ("busy", "later", "annoyed", "frustrat", "angry", "tired",
                  "bad time", "not now")


def analyze_user(text: str, state: dict) -> None:
    """Update cumulative interest / frustration based on VADER + keywords."""
    low = text.lower()
    s = VADER.polarity_scores(low)
    state["sentiment"] = s["compound"]

    if any(k in low for k in YES_KEYWORDS):
        state["interest"] += 2.0
    if any(k in low for k in NO_KEYWORDS):
        state["interest"] -= 2.0
    state["interest"] += s["compound"]

    if any(k in low for k in SOFT_KEYWORDS) or s["compound"] < -0.4:
        state["frustration"] += 1


def pick_tone(state: dict) -> str:
    """Priority switching. Order matters: frustration first, then close, then discount."""
    if state["frustration"] >= 1 and state["interest"] < 1.5:
        return "soft"
    if state["interest"] >= 2.0:
        return "close"
    if state["interest"] >= 0.5 or state["turns"] >= 2:
        return "discount"
    return "neutral"


# ========================================================================
# Per-call session
# ========================================================================
class Session:
    def __init__(self, twilio_ws: WebSocket):
        self.twilio_ws   = twilio_ws
        self.stream_sid  = None
        self.call_sid    = None
        self.phone       = None          # +91XXXXXXXXXX (the callee)
        self.started_at  = None          # datetime (UTC)
        self.start_ts    = None          # monotonic seconds for duration
        self.dg_ws       = None          # deepgram websocket
        self.history     = []            # [(role, text), ...]
        self.state       = {"interest": 0.0, "frustration": 0, "sentiment": 0.0, "turns": 0}
        self.tone        = "neutral"
        self.ai_speaking = False
        self.tts_task: asyncio.Task | None = None
        self.closing     = False

    # ---------- Deepgram ----------
    async def connect_deepgram(self):
        url = (
            "wss://api.deepgram.com/v1/listen"
            "?encoding=mulaw&sample_rate=8000&channels=1"
            "&model=nova-2-phonecall&language=en-US"
            "&punctuate=true&smart_format=true"
            "&interim_results=true&endpointing=250"
            "&vad_events=true&utterance_end_ms=1000"
        )
        hdr = {"Authorization": f"Token {DEEPGRAM_API_KEY}"}
        # websockets >=12 renamed extra_headers -> additional_headers
        try:
            self.dg_ws = await websockets.connect(url, additional_headers=hdr)
        except TypeError:
            self.dg_ws = await websockets.connect(url, extra_headers=hdr)
        print("🟢 Deepgram connected")

    async def deepgram_keepalive(self):
        """Deepgram closes idle sockets after ~10s of silence. Poke it."""
        try:
            while not self.closing and self.dg_ws:
                await asyncio.sleep(7)
                try:
                    await self.dg_ws.send(json.dumps({"type": "KeepAlive"}))
                except Exception:
                    return
        except asyncio.CancelledError:
            return

    # ---------- Twilio helpers ----------
    async def send_twilio_clear(self):
        """Tell Twilio to drop any audio it has queued (barge-in)."""
        if self.stream_sid:
            try:
                await self.twilio_ws.send_text(json.dumps({
                    "event": "clear",
                    "streamSid": self.stream_sid,
                }))
            except Exception:
                pass

    async def cancel_tts(self):
        self.ai_speaking = False
        if self.tts_task and not self.tts_task.done():
            self.tts_task.cancel()
            try:
                await self.tts_task
            except (asyncio.CancelledError, Exception):
                pass
        self.tts_task = None

    async def barge_in(self):
        """Called when the user starts speaking while AI is talking."""
        if self.ai_speaking:
            print("⚡ Barge-in: user started speaking, interrupting TTS")
            await self.cancel_tts()
            await self.send_twilio_clear()

    # ---------- TTS ----------
    async def speak(self, text: str, clear_first: bool = True):
        """Cancel any ongoing speech, then stream a new TTS response."""
        await self.cancel_tts()
        if clear_first:
            await self.send_twilio_clear()
        self.ai_speaking = True
        self.tts_task = asyncio.create_task(self._stream_tts(text))

    async def _stream_tts(self, text: str):
        """
        Stream ElevenLabs μ-law 8k audio to Twilio as fast as possible.
        Twilio handles its own jitter buffer; we do NOT pace here. Pacing
        caused cumulative drift (encode/send overhead + sleep > 20ms) which
        made the audio stutter and stall.
        Barge-in still works: we cancel this task + send a `clear` event.
        """
        url = (
            f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVEN_VOICE_ID}/stream"
            f"?output_format=ulaw_8000&optimize_streaming_latency=3"
        )
        headers = {
            "xi-api-key":   ELEVEN_API_KEY,
            "Content-Type": "application/json",
            "Accept":       "audio/basic",
        }
        body = {
            "text": text,
            "model_id": "eleven_turbo_v2_5",
            "voice_settings": {
                "stability":        0.4,
                "similarity_boost": 0.8,
                "style":            0.0,
                "use_speaker_boost": True,
            },
        }
        t0 = asyncio.get_event_loop().time()
        bytes_sent = 0
        first_byte_ms = None
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                async with client.stream("POST", url, json=body, headers=headers) as resp:
                    if resp.status_code != 200:
                        err = await resp.aread()
                        print(f"❌ ElevenLabs {resp.status_code}: {err[:300]!r}")
                        return

                    print(f"🔊 TTS streaming: {text[:60]!r}...")
                    async for chunk in resp.aiter_bytes(chunk_size=4096):
                        if not self.ai_speaking:
                            print(f"⏹️  TTS interrupted after {bytes_sent}B")
                            return
                        if not chunk:
                            continue
                        if first_byte_ms is None:
                            first_byte_ms = int((asyncio.get_event_loop().time() - t0) * 1000)
                            print(f"   ElevenLabs TTFB: {first_byte_ms}ms")
                        try:
                            await self.twilio_ws.send_text(json.dumps({
                                "event":     "media",
                                "streamSid": self.stream_sid,
                                "media":     {"payload": base64.b64encode(chunk).decode()},
                            }))
                            bytes_sent += len(chunk)
                        except Exception as send_err:
                            print(f"❌ send to Twilio failed: {send_err}")
                            return

                    # send a mark so we know when Twilio finished playing (optional)
                    try:
                        await self.twilio_ws.send_text(json.dumps({
                            "event":     "mark",
                            "streamSid": self.stream_sid,
                            "mark":      {"name": "tts_done"},
                        }))
                    except Exception:
                        pass
                    print(f"✅ TTS sent {bytes_sent}B in "
                          f"{int((asyncio.get_event_loop().time()-t0)*1000)}ms")
        except asyncio.CancelledError:
            print("🛑 TTS task cancelled")
            raise
        except Exception as e:
            print(f"❌ TTS stream error: {type(e).__name__}: {e}")
        finally:
            self.ai_speaking = False

    # ---------- Gemini reply ----------
    async def think_and_reply(self, user_text: str) -> str:
        global _GEMINI_COOLDOWN_UNTIL

        self.state["turns"] += 1
        self.history.append(("USER", user_text))
        analyze_user(user_text, self.state)
        self.tone = pick_tone(self.state)

        print(
            f"🎭 tone={self.tone:<8}  "
            f"interest={self.state['interest']:+.2f}  "
            f"frustration={self.state['frustration']}  "
            f"sent={self.state['sentiment']:+.2f}"
        )

        reply = None
        now = asyncio.get_event_loop().time()

        # If we recently got 429'd, don't even try Gemini — go straight to fallback.
        if now < _GEMINI_COOLDOWN_UNTIL:
            remaining = int(_GEMINI_COOLDOWN_UNTIL - now)
            print(f"⏸️  Skipping Gemini, cooldown {remaining}s remaining")
        else:
            convo = "\n".join(f"{r}: {c}" for r, c in self.history[-8:])
            prompt = (
                f"{TONE_PROMPTS[self.tone]}\n\n"
                f"Conversation so far:\n{convo}\n\n"
                f"Reply as AGENT in 1-2 short, natural sentences only."
            )
            try:
                resp = await asyncio.to_thread(GEMINI.generate_content, prompt)
                reply = (resp.text or "").strip().replace("*", "").replace("#", "")
            except Exception as e:
                msg = str(e)
                if "429" in msg or "quota" in msg.lower() or "rate" in msg.lower():
                    delay = _parse_retry_delay(msg)
                    _GEMINI_COOLDOWN_UNTIL = now + min(delay + 1.0, 60.0)
                    print(f"⚠️  Gemini 429 — cooling down {delay:.1f}s, using fallback")
                else:
                    print(f"⚠️  Gemini error: {type(e).__name__}: {msg[:160]}")

        # Fallback: pick a tone-appropriate scripted line (not the generic 'repeat that').
        if not reply:
            options = TONE_FALLBACKS.get(self.tone, TONE_FALLBACKS["neutral"])
            # avoid repeating the same fallback back-to-back
            last_agent = next((c for r, c in reversed(self.history) if r == "AGENT"), "")
            candidates = [o for o in options if o != last_agent] or options
            reply = random.choice(candidates)

        self.history.append(("AGENT", reply))
        print(f"🤖 [{self.tone}] {reply}")
        return reply


# ========================================================================
# Deepgram loop — always-on ASR + barge-in trigger
# ========================================================================
async def deepgram_loop(sess: Session):
    pending = ""
    try:
        async for raw in sess.dg_ws:
            if sess.closing:
                break
            data = json.loads(raw)
            typ = data.get("type")

            if typ == "SpeechStarted":
                # VAD fired — user is speaking right now
                await sess.barge_in()

            elif typ == "Results":
                alt = data.get("channel", {}).get("alternatives", [{}])[0]
                transcript = (alt.get("transcript") or "").strip()
                is_final     = data.get("is_final", False)
                speech_final = data.get("speech_final", False)

                if transcript and is_final:
                    pending = (pending + " " + transcript).strip()

                if speech_final and pending:
                    user_text, pending = pending, ""
                    print(f"🎤 USER: {user_text}")
                    reply = await sess.think_and_reply(user_text)
                    await sess.speak(reply)

            elif typ == "UtteranceEnd":
                # Fallback for when speech_final isn't emitted
                if pending:
                    user_text, pending = pending, ""
                    print(f"🎤 USER (utt_end): {user_text}")
                    reply = await sess.think_and_reply(user_text)
                    await sess.speak(reply)
    except Exception as e:
        print("Deepgram loop err:", e)


# ========================================================================
# HTTP: TwiML that opens the bidirectional stream
# ========================================================================
@app.post("/voice")
async def voice(request: Request):
    """
    Returns TwiML that opens the bidirectional stream.
    Accepts `?to=+91...` so agent.py can tell us which number we're calling;
    Twilio echoes Parameter values back on the `start` event.
    """
    form = await request.form()
    to_number = request.query_params.get("to") or form.get("To") or ""
    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Connect>
        <Stream url="{WSS_URL}">
            <Parameter name="to" value="{to_number}"/>
        </Stream>
    </Connect>
</Response>"""
    return Response(content=twiml, media_type="application/xml")


# ------------------------------------------------------------------------
# Query endpoints for stored outcomes
# ------------------------------------------------------------------------
@app.get("/calls")
async def list_calls(phone: str | None = None, limit: int = 50):
    """GET /calls?phone=+91XXXX&limit=20 — latest outcomes, newest first."""
    def _q():
        cur = DB.cursor()
        if phone:
            cur.execute(
                "SELECT * FROM call_outcomes WHERE phone = ? "
                "ORDER BY id DESC LIMIT ?", (phone, limit),
            )
        else:
            cur.execute(
                "SELECT * FROM call_outcomes ORDER BY id DESC LIMIT ?", (limit,),
            )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    rows = await asyncio.to_thread(_q)
    return JSONResponse(rows)


@app.get("/calls/summary")
async def calls_summary():
    """Counts of each outcome, plus per-phone latest status."""
    def _q():
        cur = DB.cursor()
        cur.execute(
            "SELECT outcome, COUNT(*) FROM call_outcomes GROUP BY outcome"
        )
        by_outcome = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute("""
            SELECT phone, outcome, ended_at
            FROM call_outcomes
            WHERE id IN (SELECT MAX(id) FROM call_outcomes GROUP BY phone)
            ORDER BY ended_at DESC
        """)
        latest = [
            {"phone": r[0], "outcome": r[1], "ended_at": r[2]}
            for r in cur.fetchall()
        ]
        return {"by_outcome": by_outcome, "latest_per_phone": latest}
    return JSONResponse(await asyncio.to_thread(_q))


# ========================================================================
# WebSocket: Twilio Media Stream
# ========================================================================
@app.websocket("/media")
async def media(ws: WebSocket):
    await ws.accept()
    sess = Session(ws)

    try:
        await sess.connect_deepgram()
    except Exception as e:
        print("❌ Deepgram connect failed:", e)
        await ws.close()
        return

    dg_task = asyncio.create_task(deepgram_loop(sess))
    ka_task = asyncio.create_task(sess.deepgram_keepalive())

    try:
        while True:
            msg = await ws.receive_text()
            data = json.loads(msg)
            ev = data.get("event")

            if ev == "connected":
                print("🔌 Twilio connected")

            elif ev == "start":
                start_info = data["start"]
                sess.stream_sid = start_info["streamSid"]
                sess.call_sid   = start_info.get("callSid")
                cust = start_info.get("customParameters") or {}
                sess.phone = cust.get("to") or "unknown"
                sess.started_at = datetime.now(timezone.utc)
                sess.start_ts   = asyncio.get_event_loop().time()
                print(
                    f"🟢 Stream started  sid={sess.stream_sid} "
                    f"call={sess.call_sid} phone={sess.phone}"
                )
                # Keep greeting short — user can barge in and conversation flows faster.
                greeting = "Hey, this is Alex from AIM. Got a quick second?"
                sess.history.append(("AGENT", greeting))
                await sess.speak(greeting, clear_first=False)

            elif ev == "media":
                audio = base64.b64decode(data["media"]["payload"])
                try:
                    await sess.dg_ws.send(audio)
                except Exception:
                    pass  # deepgram may be reconnecting

            elif ev == "mark":
                pass

            elif ev == "stop":
                print("🔴 Stream stopped by Twilio")
                break

    except WebSocketDisconnect:
        print("Twilio WS disconnected")
    except Exception as e:
        print("Media handler err:", e)
    finally:
        sess.closing = True
        await sess.cancel_tts()
        try:
            if sess.dg_ws:
                await sess.dg_ws.send(json.dumps({"type": "CloseStream"}))
                await sess.dg_ws.close()
        except Exception:
            pass
        dg_task.cancel()
        ka_task.cancel()
        try:
            await ws.close()
        except Exception:
            pass

        # ---------- Persist the outcome ----------
        try:
            ended_at = datetime.now(timezone.utc)
            duration = (
                asyncio.get_event_loop().time() - sess.start_ts
                if sess.start_ts else 0.0
            )
            outcome, notes = classify_outcome(sess.history, sess.state, sess.tone)
            transcript_json = json.dumps(
                [{"role": r, "text": c} for r, c in sess.history],
                ensure_ascii=False,
            )
            row_id = await save_outcome({
                "phone":          sess.phone or "unknown",
                "call_sid":       sess.call_sid,
                "stream_sid":     sess.stream_sid,
                "outcome":        outcome,
                "final_tone":     sess.tone,
                "interest_score": round(sess.state.get("interest", 0.0), 3),
                "turns":          sess.state.get("turns", 0),
                "duration_sec":   round(duration, 2),
                "transcript":     transcript_json,
                "notes":          notes,
                "started_at":     sess.started_at.isoformat() if sess.started_at else None,
                "ended_at":       ended_at.isoformat(),
            })
            print(
                f"💾 outcome saved  id={row_id}  phone={sess.phone} "
                f"outcome={outcome}  turns={sess.state.get('turns', 0)}  "
                f"dur={duration:.1f}s  notes={notes!r}"
            )
        except Exception as e:
            print(f"❌ Failed to save outcome: {type(e).__name__}: {e}")

        print("🧹 Session cleaned up")