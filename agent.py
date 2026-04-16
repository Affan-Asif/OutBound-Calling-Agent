# import os
# import sys
# from twilio.rest import Client
# from dotenv import load_dotenv

# load_dotenv()

# TWILIO_SID = os.getenv("TWILIO_SID")
# TWILIO_AUTH = os.getenv("TWILIO_AUTH")
# TWILIO_PHONE = os.getenv("TWILIO_PHONE")
# BASE_URL = os.getenv("BASE_URL")  # ngrok HTTPS URL

# client = Client(TWILIO_SID, TWILIO_AUTH)

# if len(sys.argv) < 2:
#     print("❌ Please provide phone number")
#     print("Usage: python agent.py +91XXXXXXXXXX")
#     exit()

# to_number = sys.argv[1]

# # IMPORTANT: Twilio requires WSS (secure websocket)
# stream_url = BASE_URL.replace("https://", "wss://")

# twiml = f"""
# <Response>
#     <Say>Connecting your AI assistant. Please press any key.</Say>
#     <Pause length="2"/>
#     <Connect>
#         <Stream url="{stream_url}/stream" />
#     </Connect>
# </Response>
# """

# try:
#     call = client.calls.create(
#         to=to_number,
#         from_=TWILIO_PHONE,
#         twiml=twiml
#     )

#     print("✅ Call started successfully!")
#     print("Call SID:", call.sid)

# except Exception as e:
#     print("❌ Error creating call:", str(e))









# WORKSSS



# import os
# import sys
# from twilio.rest import Client
# from dotenv import load_dotenv

# load_dotenv()

# client = Client(
#     os.getenv("TWILIO_SID"),
#     os.getenv("TWILIO_AUTH")
# )

# to_number = sys.argv[1]

# BASE_URL = os.getenv("BASE_URL")

# call = client.calls.create(
#     to=to_number,
#     from_=os.getenv("TWILIO_PHONE"),
#     url=f"{BASE_URL}/voice"
# )

# print("✅ Call started:", call.sid)







import os
import sys
from twilio.rest import Client
from dotenv import load_dotenv

load_dotenv()

TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_AUTH = os.getenv("TWILIO_AUTH")
TWILIO_PHONE = os.getenv("TWILIO_PHONE")
BASE_URL = os.getenv("BASE_URL")

client = Client(TWILIO_SID, TWILIO_AUTH)

if len(sys.argv) < 2:
    print("❌ Usage: python agent.py +91XXXXXXXXXX")
    exit()

to_number = sys.argv[1]

call = client.calls.create(
    to=to_number,
    from_=TWILIO_PHONE,
    url=f"{BASE_URL}/voice"
)

print("✅ Call started:", call.sid)