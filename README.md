# musicdiscordbot

A Discord music bot that streams audio from [UNDISCLOSED]

## Features
* Streams audio from [UNDISCLOSED] directly to Discord voice channels.
* Generates persistent playback history and configuration state per server.
* Hidden owner-exclusive queue locks and configurable toggles (`u!tl`, `u!toe`).

## Requirements
* Python 3.13+
* **FFmpeg** (must be installed on your system and available in your system PATH)

## Setup
1. Clone the repository or download the files.
2. Install the required Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to a new file named `.env` and fill in your details, including your `DISCORD_TOKEN` and `OWNER_USER_ID`.
4. Run the bot:
   ```bash
   python bot3.py
   ```

## Usage
* The default prefix for text commands is `u!`.
* The bot also supports slash commands for standard playback interactions.

## idk man
* By using this bot, you are aknowledging that you're responsible for any and all actions done using it.
