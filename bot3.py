"""
Tidal Discord Bot — monochrome API (hifi-api v2.0)
 Supports: u!play <query>, u!album <id>, u!playlist <id>, u!skip, u!queue, u!stop
"""

import asyncio
import base64
import json
import logging
import os
import random
import re
import uuid
from collections import deque
from typing import Any, Literal
from urllib.parse import quote

from dotenv import load_dotenv
import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

class MyBot(commands.Bot):
    async def setup_hook(self):
        logger.info("Syncing command tree...")
        await self.tree.sync()
        logger.info("Command tree synced.")

TIDAL_API = "https://ohio-1.monochrome.tf"  # Default fallback, will be updated in on_ready

async def pick_best_api():
    """Fetch the uptime list and pick a random working streaming API instance."""
    global TIDAL_API
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://tidal-uptime.jiffy-puffs-1j.workers.dev/") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    streaming_nodes = data.get("streaming", [])
                    if streaming_nodes:
                        # Pick a random working node that's >= version 2.7 if possible (supports trackManifests)
                        v28_nodes = [n["url"] for n in streaming_nodes if n.get("version", "").startswith("2.8") or n.get("version", "") == "2.7"]
                        if v28_nodes:
                            TIDAL_API = random.choice(v28_nodes).rstrip("/")
                        else:
                            TIDAL_API = random.choice(streaming_nodes)["url"].rstrip("/")
                        logger.info("Automatically selected working Tidal API node: %s", TIDAL_API)
    except Exception as e:
        logger.warning("Failed to fetch API uptime tracker; using default node. %s", e)

QUEUE_DISPLAY_LIMIT = 14
intents = discord.Intents.default()
intents.message_content = True
bot = MyBot(command_prefix='u!', intents=intents, help_command=None)
OWNER_USER_ID = int(os.getenv("OWNER_USER_ID", "771715423666176070"))
link_command_enabled_by_guild: dict[int, bool] = {}
owner_exclusivity_enabled_by_guild: dict[int, bool] = {}
BOT_DATA_DIR = os.path.join(os.path.dirname(__file__), "guild_data")

# Per-guild data: guild_id -> deque/list/dict
queues: dict[int, deque] = {}
histories: dict[int, list] = {}
current_tracks: dict[int, dict] = {}
loop_modes: dict[int, str] = {}
skip_requests: set[int] = set()
voice_locks: dict[int, asyncio.Lock] = {}


def ensure_guild_data_dir(guild_id: int) -> str:
    guild_dir = os.path.join(BOT_DATA_DIR, str(guild_id))
    os.makedirs(guild_dir, exist_ok=True)
    return guild_dir


def get_guild_state_path(guild_id: int) -> str:
    return os.path.join(ensure_guild_data_dir(guild_id), "state.json")


def get_guild_history_path(guild_id: int) -> str:
    return os.path.join(ensure_guild_data_dir(guild_id), "history.json")


def load_guild_state(guild_id: int) -> None:
    state_path = get_guild_state_path(guild_id)
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return
    except Exception as e:
        logger.warning("Failed to load guild state for %s: %s", guild_id, e)
        return

    link_command_enabled_by_guild[guild_id] = bool(data.get("link_command_enabled", False))
    owner_exclusivity_enabled_by_guild[guild_id] = bool(data.get("owner_exclusivity_enabled", False))


def save_guild_state(guild_id: int) -> None:
    ensure_guild_data_dir(guild_id)
    data = {
        "link_command_enabled": is_link_enabled(guild_id),
        "owner_exclusivity_enabled": is_owner_exclusivity_enabled(guild_id),
    }
    state_path = get_guild_state_path(guild_id)
    temp_path = f"{state_path}.tmp"
    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)
        os.replace(temp_path, state_path)
    except Exception as e:
        logger.warning("Failed to save guild state for %s: %s", guild_id, e)
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except OSError:
            pass


def load_guild_history(guild_id: int) -> None:
    history_path = get_guild_history_path(guild_id)
    try:
        with open(history_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        histories[guild_id] = []
        return
    except Exception as e:
        logger.warning("Failed to load guild history for %s: %s", guild_id, e)
        histories[guild_id] = []
        return

    histories[guild_id] = data if isinstance(data, list) else []


def save_guild_history(guild_id: int) -> None:
    ensure_guild_data_dir(guild_id)
    history_path = get_guild_history_path(guild_id)
    temp_path = f"{history_path}.tmp"
    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(get_history(guild_id), f, indent=2, sort_keys=True)
        os.replace(temp_path, history_path)
    except Exception as e:
        logger.warning("Failed to save guild history for %s: %s", guild_id, e)
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except OSError:
            pass


def load_all_guild_data() -> None:
    os.makedirs(BOT_DATA_DIR, exist_ok=True)
    for guild in bot.guilds:
        gid = guild.id
        ensure_guild_data_dir(gid)
        load_guild_state(gid)
        load_guild_history(gid)


def is_owner_user(user_id: int) -> bool:
    return user_id == OWNER_USER_ID


def is_link_enabled(guild_id: int) -> bool:
    return link_command_enabled_by_guild.get(guild_id, False)


def is_owner_exclusivity_enabled(guild_id: int) -> bool:
    return owner_exclusivity_enabled_by_guild.get(guild_id, False)


def is_owner_protected_track(guild_id: int) -> bool:
    if not is_owner_exclusivity_enabled(guild_id):
        return False
    curr = current_tracks.get(guild_id)
    if not curr:
        return False
    return curr.get('requester_id') == OWNER_USER_ID


def has_owner_song_locked(guild_id: int) -> bool:
    """Return True if an owner-requested song is currently playing or queued."""
    if not is_owner_exclusivity_enabled(guild_id):
        return False
    if is_owner_protected_track(guild_id):
        return True
    q = get_queue(guild_id)
    return any(track.get('requester_id') == OWNER_USER_ID for track in q)


async def ensure_owner_playback_control(ctx: commands.Context) -> bool:
    """Block non-owner control actions when the owner requested the current track."""
    if not is_owner_exclusivity_enabled(ctx.guild.id):
        return True
    if is_owner_protected_track(ctx.guild.id) and not is_owner_user(ctx.author.id):
        return False
    return True


@bot.check
async def global_owner_queue_lock(ctx: commands.Context) -> bool:
    """Silently block non-owner commands while owner-requested songs are active/queued."""
    if not ctx.guild:
        return True
    if not is_owner_exclusivity_enabled(ctx.guild.id):
        return True
    if is_owner_user(ctx.author.id):
        return True
    return not has_owner_song_locked(ctx.guild.id)


@bot.tree.interaction_check
async def global_owner_queue_lock_interactions(interaction: discord.Interaction) -> bool:
    """Apply the same owner-song lock for slash command interactions."""
    if not interaction.guild:
        return True
    if not is_owner_exclusivity_enabled(interaction.guild.id):
        return True
    if is_owner_user(interaction.user.id):
        return True
    return not has_owner_song_locked(interaction.guild.id)

async def get_or_connect_voice(ctx: commands.Context) -> discord.VoiceClient | None:
    if ctx.voice_client:
        return ctx.voice_client
        
    gid = ctx.guild.id
    if gid not in voice_locks:
        voice_locks[gid] = asyncio.Lock()
        
    async with voice_locks[gid]:
        if ctx.voice_client:
            return ctx.voice_client
        try:
            return await ctx.author.voice.channel.connect(timeout=20.0, self_deaf=True)
        except asyncio.TimeoutError:
            await ctx.send("❌ Timed out connecting to voice. Please try again.", silent=True)
            return None
        except Exception as e:
            logger.error("Voice connect error: %s", e)
            await ctx.send("❌ Error connecting to voice.", silent=True)
            return None

def get_loop_mode(guild_id: int) -> str:
    return loop_modes.get(guild_id, 'off')


# ── helpers ──────────────────────────────────────────────────────────

def get_queue(guild_id: int) -> deque:
    if guild_id not in queues:
        queues[guild_id] = deque()
    return queues[guild_id]


def get_history(guild_id: int) -> list:
    if guild_id not in histories:
        histories[guild_id] = []
    return histories[guild_id]


async def _check_voice(ctx: commands.Context) -> bool:
    """Return True if the invoking user is in a valid voice channel.

    Sends the appropriate error message and returns False on failure.
    The bot may or may not already be connected; this only validates the user side.
    """
    if not ctx.author.voice:
        await ctx.send("❌ Join a voice channel first!", silent=True)
        return False
    vc = ctx.voice_client
    if vc and vc.channel != ctx.author.voice.channel:
        await ctx.send("❌ You must be in the same voice channel as the bot to use this command.", silent=True)
        return False
    return True


def extract_stream_url(manifest_b64: str, mime_type: str = '') -> str | None:
    """Decode the base64 manifest and return a streamable URL.

    Handles two formats returned by the v2.5 API:
    - application/vnd.tidal.bts  → base64-encoded JSON with a 'urls' list
    - application/dash+xml       → base64-encoded DASH MPD (FFmpeg can stream the raw MPD URL
                                    via the 'uri' field, but we decode and grab the first BaseURL)
    """
    try:
        raw = base64.b64decode(manifest_b64).decode(errors='replace')
        if mime_type == 'application/vnd.tidal.bts' or raw.lstrip().startswith('{'):
            manifest = json.loads(raw)
            return (manifest.get('urls') or [None])[0]
        # DASH MPD XML — extract the first URL-like value from initialization or BaseURL
        import re as _re
        # Try to find initialization URL from SegmentTemplate
        m = _re.search(r'initialization=["\']([^"\']+)["\']', raw)
        if m:
            return m.group(1)
        # Fallback: grab first https:// URL in the manifest
        m = _re.search(r'https://[^\s"<>\']+', raw)
        if m:
            return m.group(0)
        return None
    except Exception as e:
        logger.error("Manifest decode error: %s", e)
        return None


async def api_get(session: aiohttp.ClientSession, url: str, params: dict, retries: int = 4) -> tuple[int, Any]:
    """GET with automatic retry on 429 (the API returns 429 for upstream timeouts too)."""
    for attempt in range(retries):
        async with session.get(url, params=params) as resp:
            if resp.status == 429:
                wait = 3 * (attempt + 1)
                logger.warning("Got 429 (upstream timeout/rate limit), retry %d/%d in %ds...", attempt + 1, retries, wait)
                await asyncio.sleep(wait)
                continue
            if resp.status == 200:
                return resp.status, await resp.json()
            return resp.status, None
    return 429, None


async def get_stream_url(session: aiohttp.ClientSession, track_id: int) -> tuple[str | None, int]:
    """Fetch a streamable URL for a track ID.

    Strategy:
    1. /track/ LOSSLESS — direct audio URL from BTS JSON manifest (most reliable for FFmpeg).
    2. /track/ HIGH    — same, lower quality fallback.
    3. /trackManifests/ DASH MPD — last resort; requires FFmpeg DASH demuxer and can be fragile.

    Returns (url, last_http_status).
    """
    last_status = 0

    # Strategy 1 & 2: /track/ direct audio URL (preferred — works reliably with FFmpegPCMAudio)
    for quality in ('LOSSLESS', 'HIGH'):
        status, data = await api_get(session, f"{TIDAL_API}/track/", {'id': track_id, 'quality': quality})
        last_status = status
        if status == 403:
            logger.warning("/track/ returned 403 for id=%s — upstream Tidal API error.", track_id)
            break
        if status != 200 or not data:
            continue
        track_data = data.get('data', {})
        manifest_b64 = track_data.get('manifest')
        mime_type = track_data.get('manifestMimeType', '')
        if not manifest_b64:
            continue
        url = extract_stream_url(manifest_b64, mime_type)
        if url:
            logger.debug("Streaming track %s via /track/ (%s)", track_id, quality)
            return url, 200

    # Strategy 3: /trackManifests/ DASH MPD last-resort fallback
    tm_params = [
        ('id', str(track_id)),
        ('formats', 'FLAC'),
        ('formats', 'AACLC'),
        ('formats', 'HEAACV1'),
        ('manifestType', 'MPEG_DASH'),
        ('uriScheme', 'HTTPS'),
        ('adaptive', 'true'),
        ('usage', 'PLAYBACK'),
    ]
    try:
        async with session.get(f"{TIDAL_API}/trackManifests/", params=tm_params) as resp:
            if resp.status == 200:
                data = await resp.json()
                uri = data['data']['data']['attributes']['uri']
                if uri:
                    logger.debug("Streaming track %s via /trackManifests/ DASH (fallback)", track_id)
                    return uri, 200
            last_status = resp.status
    except Exception as e:
        logger.warning("trackManifests request failed: %s", e)

    return None, last_status


async def play_next(ctx: commands.Context):
    """Play the next streamable track from the guild queue."""
    vc = ctx.voice_client
    if not vc:
        return

    gid = ctx.guild.id
    q = get_queue(gid)
    stream_url = None
    track = None

    # Iterate through the queue until a track with a valid stream URL is found.
    # This avoids recursive calls that could overflow the stack on a long run of dead streams.
    while q:
        candidate = q.popleft()
        async with aiohttp.ClientSession() as session:
            url, http_status = await get_stream_url(session, candidate['id'])
        if url:
            candidate['stream_url'] = url
            track = candidate
            stream_url = url
            break
        if http_status == 403:
            # Upstream Tidal token expired on the API server — entire stream endpoint is down
            logger.error("Stream API returned 403 for all qualities. Upstream token likely expired.")
            current_tracks.pop(gid, None)
            await ctx.send(
                "❌ **Stream API is down** (HTTP 403 — the monochrome.tf Tidal token has expired).\n"
                "Nothing can be streamed right now. Try again later.",
                silent=True
            )
            await vc.disconnect()
            return
        logger.warning("No stream URL for '%s' (id=%s, HTTP %s), skipping.", candidate['title'], candidate['id'], http_status)
        await ctx.send(f"⚠️ Couldn't stream **{candidate['title']}**, skipping.", silent=True)

    if not track:
        current_tracks.pop(gid, None)
        await ctx.send("📭 Queue empty — disconnecting.", silent=True)
        await vc.disconnect()
        return

    current_tracks[gid] = track
    track.setdefault('started_at', discord.utils.utcnow().isoformat())

    if vc.is_playing():
        vc.stop()

    def after_play(error):
        if error:
            logger.error("Playback error: %s", error)

        # If the voice client is gone or no longer connected, don't try to play next.
        # This happens when vc.stop() is called during a disconnect or handshake teardown.
        current_vc = ctx.voice_client
        if not current_vc or not current_vc.is_connected():
            current_tracks.pop(gid, None)
            return

        # Move current track to history
        finished_track = current_tracks.pop(gid, None)
        mode = get_loop_mode(gid)

        if finished_track:
            finished_track = dict(finished_track)
            finished_track.setdefault('finished_at', discord.utils.utcnow().isoformat())
            get_history(gid).append(finished_track)
            save_guild_history(gid)

            if gid not in skip_requests:
                if mode == 'track':
                    q.appendleft(finished_track)
                elif mode == 'queue':
                    q.append(finished_track)

        skip_requests.discard(gid)

        asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop)

    ffmpeg_options = {
        'before_options': '-reconnect 1 -reconnect_at_eof 1 -reconnect_streamed 1 -reconnect_delay_max 5',
        'options': '-vn'
    }
    source = discord.FFmpegPCMAudio(stream_url, **ffmpeg_options)
    vc.play(source, after=after_play)

    embed = discord.Embed(
        title="🎵 Now Playing",
        description=f"**{track['title']}** — {track['artist']}",
        color=0x1db954,
    )
    if q:
        embed.set_footer(text=f"{len(q)} more in queue")
    await ctx.send(embed=embed, silent=True)


def parse_tidal_url(text: str) -> tuple[str | None, str | None]:
    """Try to extract type and id from a tidal URL or listen.tidal.com link."""
    m = re.search(r'tidal\.com/(?:browse/)?(album|playlist|track|artist)/([a-zA-Z0-9\-]+)', text)
    if m:
        return m.group(1), m.group(2)
    return None, None


# ── events ───────────────────────────────────────────────────────────

@bot.event
async def on_ready():
    await pick_best_api()
    load_all_guild_data()
    # Let's verify the API version we latched onto
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(TIDAL_API) as r:
                data = await r.json()
                api_version = data.get("version", "unknown")
    except:
        api_version = "unknown"
    logger.info("Logged in as %s | %s v%s", bot.user, TIDAL_API, api_version)


@bot.event
async def on_guild_join(guild):
    ensure_guild_data_dir(guild.id)
    load_guild_state(guild.id)
    load_guild_history(guild.id)


@bot.event
async def on_voice_state_update(member, before, after):
    """Leave the voice channel if the bot is alone."""
    # We only care if someone leaves a channel
    if before.channel is None or (after.channel == before.channel):
        return

    vc = member.guild.voice_client
    if not vc:
        return

    # Check the channel the bot is in
    if vc.channel == before.channel:
        # Count non-bot members
        members = [m for m in before.channel.members if not m.bot]
        if not members:
            gid = member.guild.id
            queues.pop(gid, None)
            current_tracks.pop(gid, None)
            loop_modes.pop(gid, None)
            skip_requests.discard(gid)
            save_guild_state(gid)
            
            if vc.is_playing() or vc.is_paused():
                vc.stop()
            await vc.disconnect()
            logger.info("Leaving empty channel in guild %s", member.guild.name)


USAGE_HINTS = {
    'track': '`u!track <add|play> <search query or Tidal URL>`',
    'album': '`u!album <add|play> <search query or Tidal URL>`',
    'playlist': '`u!playlist <add|play> <search query or Tidal URL>`',
    'artist': '`u!artist <add|play> <search query or Tidal URL>`',
    'skip': '`u!skip [amount]`',
}

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingRequiredArgument):
        hint = USAGE_HINTS.get(ctx.command.name, '')
        await ctx.send(f"❌ Missing argument! Usage: {hint}" if hint else f"❌ Missing required argument: `{error.param.name}`", silent=True)
    elif isinstance(error, commands.CommandNotFound):
        pass  # silently ignore unknown commands
    elif isinstance(error, commands.CheckFailure):
        pass  # silently ignore blocked commands
    else:
        raise error


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CheckFailure):
        return
    raise error


# ── commands ─────────────────────────────────────────────────────────

@bot.hybrid_command(name="help")
async def help_cmd(ctx: commands.Context):
    """Show all available commands."""
    embed = discord.Embed(
        title="🎵 Tidal Bot — Commands",
        description="All commands work as slash commands (`/command`) or with the `u!` prefix.",
        color=0x1db954,
    )
    embed.add_field(
        name="🔍 Music",
        value=(
            "`u!track <add|play> <query or URL>` — Play or queue a track\n"
            "`u!album <add|play> <query or URL>` — Play or queue a full album\n"
            "`u!playlist <add|play> <query or URL>` — Play or queue a playlist\n"
            "`u!artist <add|play> <query or URL>` — Play or queue an artist's top tracks\n"
            "*Accepts search queries or direct Tidal URLs.*"
        ),
        inline=False,
    )
    embed.add_field(
        name="⏯️ Playback",
        value="\n".join(
            line for line in [
                "`u!pause` — Pause the current track",
                "`u!resume` — Resume playback",
                "`u!skip [amount]` — Skip one or more tracks",
                "`u!previous` — Go back to the previous track",
                "`u!link [query or song URL]` — Get a download link for the playing track or the specified song" if is_link_enabled(ctx.guild.id) else None,
                "`u!stop` — Stop playback, clear the queue, and disconnect",
            ]
            if line is not None
        ),
        inline=False,
    )
    embed.add_field(
        name="📋 Queue",
        value=(
            "`u!queue` — Show the current queue (up to 14 tracks)\n"
            "`u!shuffle` — Shuffle the queue\n"
            "`u!loop <off|track|queue>` — Set loop mode (omit to see current mode)"
        ),
        inline=False,
    )
    await ctx.send(embed=embed, silent=True)

async def _handle_single_track(ctx: commands.Context, track_entry: dict, is_add: bool = False):
    """Handle enqueuing or playing a single track based on invoked command."""
    track_entry.setdefault('requester_id', ctx.author.id)
    vc = ctx.voice_client
    gid = ctx.guild.id
    q = get_queue(gid)
    
    if is_add:
        q.append(track_entry)
        if vc.is_playing() or vc.is_paused():
            await ctx.send(f"✅ Queued **{track_entry['title']}** — {track_entry['artist']}", silent=True)
        else:
            await play_next(ctx)
    else:
        curr = current_tracks.pop(gid, None)
        if curr:
            q.appendleft(curr)
        q.appendleft(track_entry)
        
        msg = f"▶️ Playing immediately: **{track_entry['title']}** — {track_entry['artist']} (pushed current track to queue)" if curr else f"▶️ Playing immediately: **{track_entry['title']}** — {track_entry['artist']}"
        
        await ctx.send(msg, silent=True)
        if vc.is_playing() or vc.is_paused():
            vc.stop()
        else:
            await play_next(ctx)


@bot.hybrid_command(name="track")
async def track(ctx: commands.Context, action: Literal["add", "play"], *, query: str):
    """Play or add a track by search query or Tidal URL."""
    await ctx.defer()
    
    is_add = action == "add"

    if not await _check_voice(ctx):
        return

    vc = await get_or_connect_voice(ctx)
    if not vc:
        return
    q = get_queue(ctx.guild.id)

    # Check if query is a Tidal URL
    link_type, link_id = parse_tidal_url(query)
    if link_type == 'album':
        tracks_msg, tracks = await _fetch_album(ctx, link_id)
        if tracks:
            await _handle_multiple_tracks(ctx, tracks, tracks_msg, is_add)
        return
    elif link_type == 'playlist':
        tracks_msg, tracks = await _fetch_playlist(ctx, link_id)
        if tracks:
            await _handle_multiple_tracks(ctx, tracks, tracks_msg, is_add)
        return
    elif link_type == 'artist':
        tracks_msg, tracks = await _fetch_artist_top_tracks(ctx, link_id)
        if tracks:
            await _handle_multiple_tracks(ctx, tracks, tracks_msg, is_add)
        return
    elif link_type == 'track':
        # Single track link — search won't work, use /info/ to get metadata
        if not link_id:
            await ctx.send("❌ Invalid track link.", silent=True)
            return
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{TIDAL_API}/info/", params={'id': int(link_id)}) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    info = data.get('data', {})
                    track_entry = {
                        'id': info['id'],
                        'title': info.get('title', 'Unknown'),
                        'artist': info.get('artist', {}).get('name', 'Unknown'),
                    }
                    await _handle_single_track(ctx, track_entry, is_add)
                    return
                else:
                    await ctx.send(f"❌ Track not found (HTTP {resp.status})", silent=True)
                    return

    # Regular search query
    async with aiohttp.ClientSession() as session:
        status, data = await api_get(session, f"{TIDAL_API}/search/", {'s': query})
        if status == 429:
            await ctx.send("⏳ Rate limited by API, try again in a few seconds.", silent=True)
            return
        if status != 200 or not data:
            await ctx.send(f"❌ Search failed (HTTP {status})", silent=True)
            return
        items = data.get('data', {}).get('items', [])
        if not items:
            await ctx.send("❌ No results found.", silent=True)
            return
        track = items[0]
        track_entry = {
            'id': track['id'],
            'title': track.get('title', 'Unknown'),
            'artist': track.get('artist', {}).get('name', 'Unknown'),
        }

    await _handle_single_track(ctx, track_entry, is_add)


async def _resolve_search(query: str, search_type: str) -> str | None:
    """Resolve a search query to a Tidal ID (album, playlist, or artist)."""
    async with aiohttp.ClientSession() as session:
        params = {search_type: query}
        status, data = await api_get(session, f"{TIDAL_API}/search/", params)
        if status != 200 or not data:
            return None
            
        key_map = {'al': 'albums', 'p': 'playlists', 'a': 'artists'}
        res_key = key_map.get(search_type)
        items = data.get('data', {}).get(res_key, {}).get('items', [])
        
        if not items:
            return None
            
        first_item = items[0]
        if search_type == 'p':
            return first_item.get('uuid')
        return str(first_item.get('id'))


@bot.hybrid_command(name="album")
async def album(ctx: commands.Context, action: Literal["add", "play"], *, query: str):
    """Play or add all tracks from a Tidal album (by search query or URL)."""
    await ctx.defer()
    
    is_add = action == "add"

    if not await _check_voice(ctx):
        return

    vc = await get_or_connect_voice(ctx)
    if not vc:
        return

    # Accept URL or raw ID/Query
    _, parsed_id = parse_tidal_url(query)
    album_id = parsed_id or query

    if not album_id.isdigit():
        resolved_id = await _resolve_search(album_id, 'al')
        if not resolved_id:
            await ctx.send("❌ No album found for your query.", silent=True)
            return
        album_id = resolved_id

    tracks_msg, tracks = await _fetch_album(ctx, album_id)
    if tracks:
        await _handle_multiple_tracks(ctx, tracks, tracks_msg, is_add)


@bot.hybrid_command(name="playlist")
async def playlist(ctx: commands.Context, action: Literal["add", "play"], *, query: str):
    """Play or add all tracks from a Tidal playlist (by search query or URL)."""
    await ctx.defer()
    
    is_add = action == "add"

    if not await _check_voice(ctx):
        return

    vc = await get_or_connect_voice(ctx)
    if not vc:
        return

    _, parsed_id = parse_tidal_url(query)
    playlist_id = parsed_id or query

    try:
        uuid.UUID(playlist_id)
        is_valid_uuid = True
    except ValueError:
        is_valid_uuid = False

    if not is_valid_uuid:
        resolved_id = await _resolve_search(playlist_id, 'p')
        if not resolved_id:
            await ctx.send("❌ No playlist found for your query.", silent=True)
            return
        playlist_id = resolved_id

    tracks_msg, tracks = await _fetch_playlist(ctx, playlist_id)
    if tracks:
        await _handle_multiple_tracks(ctx, tracks, tracks_msg, is_add)


@bot.hybrid_command(name="artist")
async def artist(ctx: commands.Context, action: Literal["add", "play"], *, query: str):
    """Play or add top tracks from a Tidal artist (by search query or URL)."""
    await ctx.defer()

    is_add = action == "add"

    if not await _check_voice(ctx):
        return

    vc = await get_or_connect_voice(ctx)
    if not vc:
        return

    _, parsed_id = parse_tidal_url(query)
    artist_id = parsed_id or query

    if not artist_id.isdigit():
        resolved_id = await _resolve_search(artist_id, 'a')
        if not resolved_id:
            await ctx.send("❌ No artist found for your query.", silent=True)
            return
        artist_id = resolved_id

    tracks_msg, tracks = await _fetch_artist_top_tracks(ctx, artist_id)
    if tracks:
        await _handle_multiple_tracks(ctx, tracks, tracks_msg, is_add)


async def _fetch_album(ctx: commands.Context, album_id: str | None) -> tuple[str, list[dict[str, Any]]]:
    """Fetch album tracks and return them."""
    tracks: list[dict[str, Any]] = []
    if not album_id:
        return "", tracks
    try:
        album_id_int = int(album_id)
    except ValueError:
        await ctx.send("❌ Invalid album ID.", silent=True)
        return "", tracks
        
    async with aiohttp.ClientSession() as session:
        status, data = await api_get(session, f"{TIDAL_API}/album/", {'id': album_id_int})
        if status == 429:
            await ctx.send("⏳ Rate limited by API, try again in a few seconds.", silent=True)
            return "", tracks
        if status != 200 or not data:
            await ctx.send(f"❌ Album not found (HTTP {status})", silent=True)
            return "", tracks
        album_data: dict[str, Any] = data.get('data', {})
        album_title = album_data.get('title', 'Unknown Album')
        items = album_data.get('items', [])
        for entry in items:
            item = entry.get('item', {})
            if entry.get('type') == 'track' and item.get('id'):
                tracks.append({
                    'id': item['id'],
                    'title': item.get('title', 'Unknown'),
                    'artist': item.get('artist', {}).get('name', 'Unknown'),
                })
        return f"💿 **{len(tracks)}** tracks from **{album_title}**", tracks


async def _fetch_playlist(ctx: commands.Context, playlist_id: str | None) -> tuple[str, list[dict[str, Any]]]:
    """Fetch playlist tracks and return them."""
    tracks: list[dict[str, Any]] = []
    if not playlist_id:
        return "", tracks
    async with aiohttp.ClientSession() as session:
        status, data = await api_get(session, f"{TIDAL_API}/playlist/", {'id': playlist_id})
        if status == 429:
            await ctx.send("⏳ Rate limited by API, try again in a few seconds.", silent=True)
            return "", tracks
        if status != 200 or not data:
            await ctx.send(f"❌ Playlist not found (HTTP {status})", silent=True)
            return "", tracks
        playlist_data: dict[str, Any] = data.get('playlist', {})
        playlist_title = playlist_data.get('title', 'Unknown Playlist')
        items = data.get('items', [])
        for entry in items:
            item = entry.get('item', {})
            if entry.get('type') == 'track' and item.get('id'):
                tracks.append({
                    'id': item['id'],
                    'title': item.get('title', 'Unknown'),
                    'artist': item.get('artist', {}).get('name', 'Unknown'),
                })
        return f"📋 **{len(tracks)}** tracks from **{playlist_title}**", tracks

async def _fetch_artist_top_tracks(ctx: commands.Context, artist_id: str | None) -> tuple[str, list[dict[str, Any]]]:
    """Fetch artist top tracks via search query for reliability."""
    tracks: list[dict[str, Any]] = []
    if not artist_id:
        return "", tracks
    try:
        artist_id_int = int(artist_id)
    except ValueError:
        await ctx.send("❌ Invalid artist ID.", silent=True)
        return "", tracks
        
    async with aiohttp.ClientSession() as session:
        # Step 1: Get the artist's name
        status, data = await api_get(session, f"{TIDAL_API}/artist/", {'id': artist_id_int})
        if status == 429:
            await ctx.send("⏳ Rate limited by API, try again in a few seconds.", silent=True)
            return "", tracks
        if status != 200 or not data:
            await ctx.send(f"❌ Artist not found (HTTP {status})", silent=True)
            return "", tracks
            
        artist_data: dict[str, Any] = data.get('artist', {})
        artist_name = artist_data.get('name', 'Unknown Artist')
        
        # Step 2: Search for tracks by that artist name
        s_status, s_data = await api_get(session, f"{TIDAL_API}/search/", {'a': artist_name})
        if s_status != 200 or not s_data:
            return f"🎤 Found artist **{artist_name}** but could not fetch tracks.", tracks

        tracks_list = s_data.get('data', {}).get('tracks', {}).get('items', [])
        
        for item in tracks_list:
            if item.get('id'):
                tracks.append({
                    'id': item['id'],
                    'title': item.get('title', 'Unknown'),
                    'artist': item.get('artists', [{}])[0].get('name', artist_name),
                })
        return f"🎤 **{len(tracks)}** top tracks from **{artist_name}**", tracks


async def _handle_multiple_tracks(ctx: commands.Context, tracks: list[dict], source_msg: str, is_add: bool = False):
    """Handle enqueuing or playing a list of tracks based on invoked command."""
    for track in tracks:
        track.setdefault('requester_id', ctx.author.id)

    vc = ctx.voice_client
    gid = ctx.guild.id
    q = get_queue(gid)
    
    if not tracks:
        return
        
    if is_add:
        q.extend(tracks)
        if vc.is_playing() or vc.is_paused():
            await ctx.send(f"✅ Added {source_msg} to the queue.", silent=True)
        else:
            await ctx.send(f"✅ Playing {source_msg}.", silent=True)
            await play_next(ctx)
    else:
        # Prepend logic: add tracks to front of the queue
        curr = current_tracks.pop(gid, None)
        if curr:
            q.appendleft(curr)
        
        # Add tracks in reverse order to prepend properly
        for track in reversed(tracks):
            q.appendleft(track)
            
        msg = f"▶️ Interrupted to play {source_msg}. (Pushed current track to queue)" if curr else f"▶️ Playing {source_msg} immediately."
        
        await ctx.send(msg, silent=True)
        if vc.is_playing() or vc.is_paused():
            vc.stop()
        else:
            await play_next(ctx)


@bot.hybrid_command(name="skip")
async def skip(ctx: commands.Context, amount: int = 1):
    """Skip the current track, or multiple tracks."""
    if not await _check_voice(ctx):
        return

    if not await ensure_owner_playback_control(ctx):
        return

    vc = ctx.voice_client
    if vc and (vc.is_playing() or vc.is_paused()):
        gid = ctx.guild.id
        q = get_queue(gid)
        
        # Drop extra tracks if amount > 1
        if amount > 1:
            to_drop = min(amount - 1, len(q))
            for _ in range(to_drop):
                q.popleft()
                
        skip_requests.add(gid)
        vc.stop()  # triggers after_play -> move current to history -> play_next
        msg = f"⏭️ Skipped {amount} track(s)!" if amount > 1 else "⏭️ Skipped!"
        await ctx.send(msg, silent=True)
    else:
        await ctx.send("❌ Nothing is playing.", silent=True)


@bot.hybrid_command(name="previous")
async def previous(ctx: commands.Context):
    """Play the previous track."""
    if not ctx.author.voice:
        await ctx.send("❌ Join a voice channel first!", silent=True)
        return
        
    vc = ctx.voice_client
    if not vc:
        await ctx.send("❌ Not in a voice channel.", silent=True)
        return
        
    if vc.channel != ctx.author.voice.channel:
        await ctx.send("❌ You must be in the same voice channel as the bot to use this command.", silent=True)
        return

    gid = ctx.guild.id
    h = get_history(gid)
    if not h:
        await ctx.send("❌ No playback history.", silent=True)
        return

    prev_track = h.pop()
    save_guild_history(gid)
    q = get_queue(gid)
    
    # If something is currently playing, put it back in queue
    curr = current_tracks.get(gid)
    if curr:
        q.appendleft(curr)
        # Clear current_tracks[gid] so after_play doesn't push it to history again
        current_tracks.pop(gid, None)

    q.appendleft(prev_track)
    
    if vc.is_playing() or vc.is_paused():
        vc.stop() # triggers after_play -> play_next
    else:
        await play_next(ctx)

    await ctx.send("⏮️ Back to previous!", silent=True)


@bot.hybrid_command(name="queue")
async def queue_cmd(ctx: commands.Context):
    """Show the current queue."""
    gid = ctx.guild.id
    q = get_queue(gid)
    curr = current_tracks.get(gid)
    
    if not q and not curr:
        await ctx.send("📭 Queue is empty.", silent=True)
        return

    lines = []
    if curr:
        lines.append(f"▶️ **{curr['title']}** — {curr['artist']}")
    
    for i, track in enumerate(q):
        lines.append(f"`{i+1}.` **{track['title']}** — {track['artist']}")
        if i >= QUEUE_DISPLAY_LIMIT - 1:
            lines.append(f"… and {len(q) - QUEUE_DISPLAY_LIMIT} more")
            break

    embed = discord.Embed(title="🎶 Queue", description="\n".join(lines), color=0x1db954)
    await ctx.send(embed=embed, silent=True)


@bot.hybrid_command(name="link")
async def link_cmd(ctx: commands.Context, *, query: str | None = None):
    if not is_link_enabled(ctx.guild.id):
        return

    gid = ctx.guild.id
    track_title = None
    stream_url = None

    if query is None:
        curr = current_tracks.get(gid)
        if not curr or 'stream_url' not in curr:
            await ctx.send("❌ No track currently playing or stream link unavailable.", silent=True)
            return
        track_title = curr['title']
        stream_url = curr['stream_url']
    else:
        link_type, link_id = parse_tidal_url(query)
        if link_type in {'album', 'playlist', 'artist'}:
            await ctx.send("❌ Download links only work for songs, not albums, playlists, or artists.", silent=True)
            return

        if link_type == 'track':
            if not link_id:
                await ctx.send("❌ Invalid song link.", silent=True)
                return
            try:
                track_id = int(link_id)
            except ValueError:
                await ctx.send("❌ Invalid song link.", silent=True)
                return

            async with aiohttp.ClientSession() as session:
                async with session.get(f"{TIDAL_API}/info/", params={'id': track_id}) as resp:
                    if resp.status != 200:
                        await ctx.send(f"❌ Track not found (HTTP {resp.status})", silent=True)
                        return
                    data = await resp.json()
                    info = data.get('data', {})
                    track_title = info.get('title', 'Unknown')

            async with aiohttp.ClientSession() as stream_session:
                stream_url, http_status = await get_stream_url(stream_session, track_id)
            if not stream_url:
                await ctx.send(f"❌ Could not generate a download link (HTTP {http_status}).", silent=True)
                return
        else:
            async with aiohttp.ClientSession() as session:
                status, data = await api_get(session, f"{TIDAL_API}/search/", {'s': query})
                if status == 429:
                    await ctx.send("⏳ Rate limited by API, try again in a few seconds.", silent=True)
                    return
                if status != 200 or not data:
                    await ctx.send(f"❌ Search failed (HTTP {status})", silent=True)
                    return
                items = data.get('data', {}).get('items', [])
                if not items:
                    await ctx.send("❌ No results found.", silent=True)
                    return
                track = items[0]
                track_title = track.get('title', 'Unknown')
                track_id = track.get('id')
                if not track_id:
                    await ctx.send("❌ The first search result is not a valid song.", silent=True)
                    return

            async with aiohttp.ClientSession() as stream_session:
                stream_url, http_status = await get_stream_url(stream_session, int(track_id))
            if not stream_url:
                await ctx.send(f"❌ Could not generate a download link (HTTP {http_status}).", silent=True)
                return

    encoded_src = quote(stream_url, safe=':/?&=%')
    wrapped_url = f"https://mp.astrasphe.re?src=%22{encoded_src}%22&dl=1"
    view = discord.ui.View()
    view.add_item(discord.ui.Button(label="Download Link", url=wrapped_url))
    await ctx.send(f"🔗 **Download Link for {track_title}**", view=view, silent=True, delete_after=30)


@bot.hybrid_command(name="stop")
async def stop(ctx: commands.Context):
    """Stop playback, clear queue, and disconnect."""
    if not ctx.author.voice:
        await ctx.send("❌ Join a voice channel first!", silent=True)
        return

    vc = ctx.voice_client
    if not vc:
        await ctx.send("❌ Not in a voice channel.", silent=True)
        return
        
    if vc.channel != ctx.author.voice.channel:
        await ctx.send("❌ You must be in the same voice channel as the bot to use this command.", silent=True)
        return

    if not await ensure_owner_playback_control(ctx):
        return
        
    gid = ctx.guild.id
    queues.pop(gid, None)
    current_tracks.pop(gid, None)
    loop_modes.pop(gid, None)
    vc.stop()
    save_guild_state(gid)
    await vc.disconnect()
    await ctx.send("⏹️ Stopped and disconnected.", silent=True)





@bot.hybrid_command(name="pause")
async def pause_cmd(ctx: commands.Context):
    """Pause the current track."""
    if not await _check_voice(ctx):
        return

    if not await ensure_owner_playback_control(ctx):
        return

    vc = ctx.voice_client
    if vc and vc.is_playing():
        vc.pause()
        await ctx.send("⏸️ Paused playback.", silent=True)
    else:
        await ctx.send("❌ Nothing is playing to pause.", silent=True)

@bot.hybrid_command(name="resume")
async def resume_cmd(ctx: commands.Context):
    """Resume the paused track."""
    if not await _check_voice(ctx):
        return

    vc = ctx.voice_client
    if vc and vc.is_paused():
        vc.resume()
        await ctx.send("▶️ Resumed playback.", silent=True)
    else:
        await ctx.send("❌ Nothing is paused.", silent=True)


@bot.command(name="tl")
async def toggle_link_cmd(ctx: commands.Context):
    if not is_owner_user(ctx.author.id):
        return

    gid = ctx.guild.id
    link_command_enabled_by_guild[gid] = not is_link_enabled(gid)
    state = "enabled" if link_command_enabled_by_guild[gid] else "disabled"
    save_guild_state(gid)
    await ctx.send(f"✅ `u!link` is now {state}.", silent=True)


@bot.command(name="toe")
async def toggle_owner_exclusive_cmd(ctx: commands.Context):
    if not is_owner_user(ctx.author.id):
        return

    gid = ctx.guild.id
    owner_exclusivity_enabled_by_guild[gid] = not is_owner_exclusivity_enabled(gid)
    state = "enabled" if owner_exclusivity_enabled_by_guild[gid] else "disabled"
    save_guild_state(gid)
    await ctx.send(f"✅ Owner exclusivity is now {state}.", silent=True)

@bot.hybrid_command(name="loop")
async def loop(ctx: commands.Context, mode: Literal["off", "track", "queue"] | None = None):
    """Set loop mode: off, track, queue."""
    if not await _check_voice(ctx):
        return

    gid = ctx.guild.id
    if mode is None:
        curr = get_loop_mode(gid)
        await ctx.send(f"🔁 Current loop mode: **{curr}**", silent=True)
        return
        
    mode = mode.lower()
    if mode not in ['off', 'track', 'queue']:
        await ctx.send("❌ Invalid mode. Use `off`, `track`, or `queue`.", silent=True)
        return
        
    loop_modes[gid] = mode
    save_guild_state(gid)
    await ctx.send(f"🔁 Loop mode set to: **{mode}**", silent=True)

@bot.hybrid_command(name="shuffle")
async def shuffle(ctx: commands.Context):
    """Shuffle the current queue."""
    if not await _check_voice(ctx):
        return

    gid = ctx.guild.id
    q = get_queue(gid)
    if len(q) < 2:
        await ctx.send("❌ Not enough tracks in queue to shuffle.", silent=True)
        return
    
    q_list = list(q)
    random.shuffle(q_list)
    q.clear()
    q.extend(q_list)
    await ctx.send("🔀 Queue shuffled!", silent=True)

if __name__ == "__main__":
    load_dotenv()
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_TOKEN not set. Copy .env.example to .env and fill it in.")
    bot.run(token)
