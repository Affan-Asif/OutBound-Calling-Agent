# agent.py
# Places an outbound Twilio call that connects to the full-duplex voice agent
# running in server.py.
#
# Usage:
#   python agent.py +91XXXXXXXXXX
#
# .env must contain:
#   TWILIO_SID=...
#   TWILIO_AUTH=...
#   TWILIO_PHONE=+1XXXXXXXXXX   (your Twilio number)
#   BASE_URL=https://<public-https-host>   (ngrok / cloud tunnel pointing to server.py)

import os
import sys
from twilio.rest import Client
from dotenv import load_dotenv

load_dotenv()

TWILIO_SID   = os.getenv("TWILIO_SID")
TWILIO_AUTH  = os.getenv("TWILIO_AUTH")
TWILIO_PHONE = os.getenv("TWILIO_PHONE")
BASE_URL     = os.getenv("BASE_URL", "").rstrip("/")


def main():
    if not all([TWILIO_SID, TWILIO_AUTH, TWILIO_PHONE, BASE_URL]):
        print("❌ Missing env vars. Need TWILIO_SID, TWILIO_AUTH, TWILIO_PHONE, BASE_URL")
        sys.exit(1)

    if len(sys.argv) < 2:
        print("❌ Usage: python agent.py +91XXXXXXXXXX")
        sys.exit(1)

    to_number = sys.argv[1].strip()

    client = Client(TWILIO_SID, TWILIO_AUTH)
    call = client.calls.create(
        to=to_number,
        from_=TWILIO_PHONE,
        url=f"{BASE_URL}/voice",
        method="POST",
        # Let the callee hear the stream start fast:
        record=False,
    )
    print(f"✅ Call started: SID={call.sid}  to={to_number}")


if __name__ == "__main__":
    main()