

https://github.com/user-attachments/assets/3e754f61-befa-47cd-9ec7-e522da84db95

Open Anaconda Prompt
conda activate MajorProject

Terminal 1 -
uvicorn server:app --host 0.0.0.0 --port 8000

Terminal 2 - 
python agent.py +919121795950

Terminal 3 - 
ngrok http 8000

Last Step - 
Copy the ngrok url in .env

To Check the DB
sqlite3 calls.db "SELECT phone, outcome, notes, ended_at FROM call_outcomes ORDER BY id DESC;"


view FULL table
sqlite3 -header -column calls.db "SELECT * FROM call_outcomes ORDER BY id DESC;"

Skip transcript
sqlite3 -header -column calls.db "
SELECT id, phone, outcome, final_tone, interest_score, turns, duration_sec, notes, ended_at 
FROM call_outcomes 
ORDER BY id DESC;"
