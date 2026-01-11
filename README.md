# Streamerphile

**Warning: This is a product of vibe coding with minimal code review. Use at your own risk!** 

Flask + SQLite web app that lets you track Twitch streams for followed games.

## Setup

- Copy `config.json.example` to `config.json` and fill in your Twitch client ID/secret.
- Create a venv and install deps:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python main.py
```

Then open `http://127.0.0.1:5000`.

## Notes

- Streams and streamer follower counts are cached in SQLite with a 7-day TTL.
- The backend fetches streams on an interval and pushes updates to the frontend via SSE.

