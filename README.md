# musicdiscordbot

A Discord music bot that streams audio from tidal, using monochrome

## Features
* Streams audio from tidal directly to Discord voice channels.
* Accepts Spotify track/album/playlist/artist links by matching Spotify metadata to tidal tracks before playback.
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
4. To use Spotify links, also set Spotify app credentials in `.env`:
   ```env
   SPOTIFY_CLIENT_ID=your_spotify_client_id
   SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
   ```
   Optional for certain mix playlists that cap with app-only credentials:
   ```env
   SPOTIFY_USER_ACCESS_TOKEN=your_spotify_user_token
   ```
   Optional alternative for auto-refreshing a web-player token:
   ```env
   SPOTIFY_SP_DC=your_spotify_sp_dc_cookie
   ```
5. Run the bot:
   ```bash
   python bot.py
   ```

## Usage
* The default prefix for text commands is `u!`.
* The bot also supports slash commands for standard playback interactions.
* If `DEBUG_SELFTEST` is set to a truthy value, or if you run the bot in an interactive terminal, pressing Enter will run the built-in self-test suite.

## idk man
* By using this bot, you are aknowledging that you're responsible for any and all actions done using it.
