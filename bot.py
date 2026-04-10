"""
Tidal Discord Bot — monochrome API (hifi-api v2.0)
 Supports: u!play <query>, u!album <id>, u!playlist <id>, u!skip, u!queue, u!stop
"""

import asyncio
import base64
import io
import json
import logging
import os
import random
import re
import sys
import time
import uuid
from collections import deque
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Awaitable, Callable, Literal
from urllib.parse import quote
import tempfile

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

# Load .env early so module-level config reads the intended values.
load_dotenv()

class MyBot(commands.Bot):
    async def setup_hook(self):
        logger.info("Syncing command tree...")
        await self.tree.sync()
        logger.info("Command tree synced.")
        global _debug_selftest_listener_started
        if is_debug_selftest_enabled() and not _debug_selftest_listener_started:
            _debug_selftest_listener_started = True
            asyncio.create_task(_debug_selftest_listener())

TIDAL_API = "https://ohio-1.monochrome.tf"  # Default fallback, will be updated in on_ready
SPOTIFY_ACCOUNTS_API = "https://accounts.spotify.com/api"
SPOTIFY_WEB_API = "https://api.spotify.com/v1"
SPOTIFY_WEB_PLAYER_TOKEN_URL = "https://open.spotify.com/get_access_token"
_spotify_access_token: str | None = None
_spotify_access_token_expiry = 0.0
_spotify_web_player_access_token: str | None = None
_spotify_web_player_access_token_expiry = 0.0
_debug_selftest_listener_started = False
DEBUG_SELFTEST_REPORT_PATH = Path(__file__).with_name("debug-selftest-results.txt")


def is_debug_selftest_enabled() -> bool:
    """Enable Enter-key self-tests in interactive terminals by default."""
    value = os.getenv("DEBUG_SELFTEST")
    if value is not None:
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return sys.stdin.isatty()

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

QUEUE_COLUMNS = 3
QUEUE_ROWS_PER_COLUMN = 8
QUEUE_PAGE_SIZE = QUEUE_COLUMNS * QUEUE_ROWS_PER_COLUMN
SPECTROGRAM_FIXED_SIZE = "854x480"
TIMESTAMPED_LYRICS_LINE_RE = re.compile(r"\[(\d{1,2}):(\d{2})(?:[.:](\d{1,3}))?\]")
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
metadata_message_refs_by_guild: dict[int, list[dict[str, int]]] = {}


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


def _debug_make_voice_client(channel: Any, playing: bool = False, paused: bool = False):
    class _VoiceClient:
        def __init__(self) -> None:
            self.channel = channel
            self._playing = playing
            self._paused = paused
            self.stopped = 0
            self.disconnected = 0
            self.played_sources: list[Any] = []
            self.connected = True

        def is_playing(self) -> bool:
            return self._playing

        def is_paused(self) -> bool:
            return self._paused

        def is_connected(self) -> bool:
            return self.connected

        def play(self, source: Any, after=None) -> None:
            self.played_sources.append(source)
            self._playing = True
            self._paused = False
            self.after = after

        def stop(self) -> None:
            self.stopped += 1
            self._playing = False
            self._paused = False

        def pause(self) -> None:
            self._paused = True
            self._playing = False

        def resume(self) -> None:
            self._paused = False
            self._playing = True

        async def disconnect(self) -> None:
            self.disconnected += 1
            self.connected = False
            self._playing = False
            self._paused = False

    return _VoiceClient()


def _debug_make_voice_channel(voice_client: Any):
    class _VoiceChannel:
        def __init__(self) -> None:
            self.name = "Debug Voice"
            self.members = []

        async def connect(self, timeout: float = 20.0, self_deaf: bool = True):
            return voice_client

    return _VoiceChannel()


def _debug_make_ctx(
    guild_id: int,
    user_id: int,
    voice_client: Any | None = None,
    voice_enabled: bool = True,
    author_is_bot: bool = False,
    match_voice_channel: bool = True,
):
    sent_messages: list[dict[str, Any]] = []
    deferred = 0
    if voice_enabled:
        if match_voice_channel and voice_client is not None and getattr(voice_client, "channel", None) is not None:
            voice_channel = voice_client.channel
        else:
            voice_channel = _debug_make_voice_channel(voice_client)
    else:
        voice_channel = None
    author_voice = SimpleNamespace(channel=voice_channel) if voice_channel else None
    author = SimpleNamespace(id=user_id, bot=author_is_bot, voice=author_voice)
    guild = SimpleNamespace(id=guild_id, name=f"Debug Guild {guild_id}", voice_client=voice_client)
    ctx = SimpleNamespace(
        guild=guild,
        author=author,
        voice_client=voice_client,
        sent_messages=sent_messages,
        deferred=0,
    )

    async def send(content: str | None = None, **kwargs: Any):
        sent_messages.append({"content": content, "kwargs": kwargs})
        return SimpleNamespace(content=content, kwargs=kwargs)

    async def defer():
        nonlocal deferred
        deferred += 1
        ctx.deferred = deferred

    ctx.send = send
    ctx.defer = defer
    return ctx


def _debug_write_report(results: list[dict[str, str]]) -> None:
    pass_count = sum(1 for result in results if result["status"] == "pass")
    skip_count = sum(1 for result in results if result["status"] == "skip")
    fail_count = sum(1 for result in results if result["status"] == "fail")
    lines = [
        "Debug Self-Test Report",
        f"Generated: {discord.utils.utcnow().isoformat()}",
        f"Passed: {pass_count}",
        f"Skipped: {skip_count}",
        f"Failed: {fail_count}",
        "",
    ]
    for result in results:
        lines.append(f"[{result['status'].upper()}] {result['name']}: {result['detail']}")
    lines.append("")
    DEBUG_SELFTEST_REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def _debug_record(results: list[dict[str, str]], name: str, status: str, detail: str) -> None:
    results.append({"name": name, "status": status, "detail": detail})
    if status == "pass":
        logger.info("[self-test] PASS: %s - %s", name, detail)
    elif status == "skip":
        logger.warning("[self-test] SKIP: %s - %s", name, detail)
    else:
        logger.error("[self-test] FAIL: %s - %s", name, detail)


async def run_debug_self_tests() -> list[dict[str, str]]:
    """Run a best-effort self-test pass across parsers, persistence, commands, and remote lookup paths."""
    results: list[dict[str, str]] = []
    debug_guild_id = 987654321012345678
    debug_user_id = OWNER_USER_ID
    non_owner_user_id = OWNER_USER_ID + 1

    original_api_get = api_get
    original_spotify_api_get = spotify_api_get
    original_resolve_spotify_track_to_tidal = resolve_spotify_track_to_tidal
    original_play_next = play_next
    original_handle_single_track = _handle_single_track
    original_handle_multiple_tracks = _handle_multiple_tracks
    original_enqueue_spotify_album_progressive = _enqueue_spotify_album_progressive
    original_enqueue_spotify_artist_progressive = _enqueue_spotify_artist_progressive
    original_enqueue_spotify_playlist_progressive = _enqueue_spotify_playlist_progressive
    original_bot_data_dir = BOT_DATA_DIR

    def reset_debug_state() -> None:
        queues.pop(debug_guild_id, None)
        histories.pop(debug_guild_id, None)
        current_tracks.pop(debug_guild_id, None)
        loop_modes.pop(debug_guild_id, None)
        skip_requests.discard(debug_guild_id)
        link_command_enabled_by_guild.pop(debug_guild_id, None)
        owner_exclusivity_enabled_by_guild.pop(debug_guild_id, None)

    async def fake_api_get(_session: aiohttp.ClientSession, _url: str, _params: dict, retries: int = 4) -> tuple[int, Any]:
        return 200, {
            "data": {
                "items": [
                    {"id": 111, "title": "Never Gonna Give You Up", "artist": {"name": "Rick Astley"}, "duration": 214},
                    {"id": 222, "title": "Different Song", "artist": {"name": "Different Artist"}, "duration": 180},
                ]
            }
        }

    async def fake_resolve_spotify_track_to_tidal(_session: aiohttp.ClientSession, spotify_track: dict[str, Any]) -> dict[str, Any] | None:
        if spotify_track.get("title") == "Never Gonna Give You Up":
            return {"id": 111, "title": "Never Gonna Give You Up", "artist": "Rick Astley"}
        return None

    # Pure utility checks.
    tidal_link = parse_tidal_url("https://tidal.com/browse/track/123456")
    spotify_link = parse_spotify_url("https://open.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC?si=test")
    normalized = _normalize_text("Never Gonna Give You Up (Remastered)")
    tidal_manifest_json = base64.b64encode(json.dumps({"urls": ["https://example.com/audio.flac"]}).encode()).decode()
    tidal_manifest_dash = base64.b64encode(b'<MPD><BaseURL>https://example.com/dash.mpd</BaseURL></MPD>').decode()
    spotify_track_payload = _spotify_track_from_payload(
        {"id": "abc123", "name": "Never Gonna Give You Up", "duration_ms": 213000, "artists": [{"name": "Rick Astley"}]}
    )
    _debug_record(results, "Tidal URL parser", "pass" if tidal_link == ("track", "123456") else "fail", f"got {tidal_link}")
    _debug_record(results, "Spotify URL parser", "pass" if spotify_link == ("track", "4uLU6hMCjMI75M1A2tKUQC") else "fail", f"got {spotify_link}")
    _debug_record(results, "Text normalizer", "pass" if normalized == "never gonna give you up" else "fail", f"got {normalized!r}")
    _debug_record(results, "Tidal manifest JSON decode", "pass" if extract_stream_url(tidal_manifest_json, "application/vnd.tidal.bts") == "https://example.com/audio.flac" else "fail", "JSON manifest decode")
    _debug_record(results, "Tidal manifest DASH decode", "pass" if extract_stream_url(tidal_manifest_dash, "application/dash+xml") == "https://example.com/dash.mpd" else "fail", "DASH manifest decode")
    _debug_record(results, "Spotify payload parser", "pass" if spotify_track_payload and spotify_track_payload["artist"] == "Rick Astley" else "fail", f"got {spotify_track_payload}")

    # Persistence checks.
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            globals()["BOT_DATA_DIR"] = tmpdir
            reset_debug_state()
            link_command_enabled_by_guild[debug_guild_id] = True
            owner_exclusivity_enabled_by_guild[debug_guild_id] = True
            histories[debug_guild_id] = [{"id": 1, "title": "Track A", "artist": "Artist A"}]

            save_guild_state(debug_guild_id)
            save_guild_history(debug_guild_id)

            reset_debug_state()
            load_guild_state(debug_guild_id)
            load_guild_history(debug_guild_id)

            state_ok = is_link_enabled(debug_guild_id) and is_owner_exclusivity_enabled(debug_guild_id)
            history_ok = get_history(debug_guild_id) == [{"id": 1, "title": "Track A", "artist": "Artist A"}]
            _debug_record(results, "Guild state persistence", "pass" if state_ok else "fail", f"state={state_ok}")
            _debug_record(results, "Guild history persistence", "pass" if history_ok else "fail", f"history={get_history(debug_guild_id)}")
    finally:
        globals()["BOT_DATA_DIR"] = original_bot_data_dir
        reset_debug_state()

    # Command and lock helpers.
    owner_vc_channel = _debug_make_voice_channel(None)
    owner_vc = _debug_make_voice_client(owner_vc_channel, playing=True)
    owner_vc.channel = owner_vc_channel
    owner_ctx = _debug_make_ctx(debug_guild_id, debug_user_id, owner_vc)
    non_owner_ctx = _debug_make_ctx(debug_guild_id, non_owner_user_id, owner_vc)
    voiceless_ctx = _debug_make_ctx(debug_guild_id, debug_user_id, owner_vc, voice_enabled=False)
    mismatch_vc_channel = _debug_make_voice_channel(None)
    mismatch_ctx = _debug_make_ctx(debug_guild_id, debug_user_id, _debug_make_voice_client(mismatch_vc_channel, playing=True), match_voice_channel=False)

    _debug_record(results, "Voice check missing voice", "pass" if not await _check_voice(voiceless_ctx) else "fail", "expected rejection")
    _debug_record(results, "Voice check channel mismatch", "pass" if not await _check_voice(mismatch_ctx) else "fail", "expected rejection")

    connect_target_vc = _debug_make_voice_client(None)
    connect_ctx = _debug_make_ctx(debug_guild_id, debug_user_id, None)
    connect_ctx.author.voice.channel = _debug_make_voice_channel(connect_target_vc)
    connected_vc = await get_or_connect_voice(connect_ctx)
    _debug_record(results, "Voice connection helper", "pass" if connected_vc is not None else "fail", f"connected={connected_vc is not None}")

    reset_debug_state()
    current_tracks[debug_guild_id] = {"id": 9, "title": "Locked Song", "artist": "Locked Artist", "requester_id": OWNER_USER_ID}
    queues[debug_guild_id] = deque([{"id": 10, "title": "Queued Song", "artist": "Queued Artist", "requester_id": OWNER_USER_ID}])
    owner_exclusivity_enabled_by_guild[debug_guild_id] = True
    owner_lock_ctx = _debug_make_ctx(debug_guild_id, non_owner_user_id, owner_vc)
    _debug_record(results, "Owner playback control lock", "pass" if not await ensure_owner_playback_control(owner_lock_ctx) else "fail", "non-owner should be blocked")
    _debug_record(results, "Global owner queue lock", "pass" if not await global_owner_queue_lock(owner_lock_ctx) else "fail", "non-owner should be blocked")
    reset_debug_state()

    # Queue/playback helpers.
    play_next_calls: list[str] = []

    async def fake_play_next(_ctx: commands.Context):
        play_next_calls.append("called")

    globals()["play_next"] = fake_play_next
    try:
        playing_vc = _debug_make_voice_client(_debug_make_voice_channel(None), playing=True)
        playing_ctx = _debug_make_ctx(debug_guild_id, debug_user_id, playing_vc)
        await _handle_single_track(playing_ctx, {"id": 1, "title": "Song 1", "artist": "Artist 1"}, is_add=True)
        _debug_record(results, "Single-track add branch", "pass" if len(get_queue(debug_guild_id)) == 1 and not play_next_calls else "fail", f"queue={list(get_queue(debug_guild_id))}")

        reset_debug_state()
        play_next_calls.clear()
        idle_vc = _debug_make_voice_client(_debug_make_voice_channel(None), playing=False)
        idle_ctx = _debug_make_ctx(debug_guild_id, debug_user_id, idle_vc)
        await _handle_single_track(idle_ctx, {"id": 2, "title": "Song 2", "artist": "Artist 2"}, is_add=False)
        _debug_record(results, "Single-track play branch", "pass" if play_next_calls else "fail", f"play_next_calls={len(play_next_calls)}")

        reset_debug_state()
        multi_vc = _debug_make_voice_client(_debug_make_voice_channel(None), playing=True)
        multi_ctx = _debug_make_ctx(debug_guild_id, debug_user_id, multi_vc)
        current_tracks[debug_guild_id] = {"id": 3, "title": "Current", "artist": "Artist"}
        await _handle_multiple_tracks(multi_ctx, [{"id": 4, "title": "A", "artist": "A"}, {"id": 5, "title": "B", "artist": "B"}], "2 tracks", is_add=False)
        queue_ids = [track["id"] for track in get_queue(debug_guild_id)]
        _debug_record(results, "Multiple-track prepend branch", "pass" if queue_ids[:3] == [4, 5, 3] else "fail", f"queue={queue_ids}")
    finally:
        globals()["play_next"] = original_play_next

    # Spotify helper and routing tests.
    async def fake_spotify_api_get(session: aiohttp.ClientSession, endpoint: str, params: dict[str, Any] | None = None, retries: int = 3) -> tuple[int, Any]:
        if endpoint == "/tracks/track123":
            return 200, {"id": "track123", "name": "Never Gonna Give You Up", "duration_ms": 213000, "artists": [{"name": "Rick Astley"}]}
        if endpoint == "/albums/album123":
            return 200, {"name": "Debug Album", "tracks": {"items": [{"id": "a1", "name": "Never Gonna Give You Up", "duration_ms": 213000, "artists": [{"name": "Rick Astley"}]}, {"id": "a2", "name": "Different", "duration_ms": 200000, "artists": [{"name": "Other"}]}], "next": None}}
        if endpoint == "/playlists/playlist123":
            return 200, {"name": "Debug Playlist"}
        if endpoint == "/playlists/playlist123/tracks":
            return 200, {"items": [{"track": {"id": "p1", "name": "Never Gonna Give You Up", "duration_ms": 213000, "artists": [{"name": "Rick Astley"}]}}, {"track": {"id": "p2", "name": "Other", "duration_ms": 200000, "artists": [{"name": "Other"}]}}], "next": None}
        if endpoint == "/artists/artist123":
            return 200, {"name": "Rick Astley"}
        if endpoint == "/artists/artist123/top-tracks":
            return 200, {"tracks": [{"id": "t1", "name": "Never Gonna Give You Up", "duration_ms": 213000, "artists": [{"name": "Rick Astley"}]}, {"id": "t2", "name": "Other", "duration_ms": 200000, "artists": [{"name": "Other"}]}]}
        if endpoint == "/search":
            return 200, {"tracks": {"items": [{"id": "track123", "name": "Never Gonna Give You Up", "duration_ms": 213000, "artists": [{"name": "Rick Astley"}] } ]}}
        return 404, None

    async def fake_resolve(session: aiohttp.ClientSession, spotify_track: dict[str, Any]) -> dict[str, Any] | None:
        if spotify_track.get("title") == "Never Gonna Give You Up":
            return {"id": 111, "title": "Never Gonna Give You Up", "artist": "Rick Astley"}
        return None

    globals()["spotify_api_get"] = fake_spotify_api_get
    globals()["resolve_spotify_track_to_tidal"] = fake_resolve
    try:
        async with aiohttp.ClientSession() as session:
            album_msg, album_tracks = await _fetch_spotify_album_as_tidal(_debug_make_ctx(debug_guild_id, debug_user_id, owner_vc), "album123")
            playlist_msg, playlist_tracks = await _fetch_spotify_playlist_as_tidal(_debug_make_ctx(debug_guild_id, debug_user_id, owner_vc), "playlist123")
            artist_msg, artist_tracks = await _fetch_spotify_artist_top_tracks_as_tidal(_debug_make_ctx(debug_guild_id, debug_user_id, owner_vc), "artist123")
            _debug_record(results, "Spotify album matcher", "pass" if len(album_tracks) == 1 else "fail", album_msg)
            _debug_record(results, "Spotify playlist matcher", "pass" if len(playlist_tracks) == 1 else "fail", playlist_msg)
            _debug_record(results, "Spotify artist matcher", "pass" if len(artist_tracks) == 1 else "fail", artist_msg)

        route_calls: list[dict[str, Any]] = []

        async def fake_single(ctx: commands.Context, track_entry: dict, is_add: bool = False):
            route_calls.append({"kind": "single", "track": track_entry, "is_add": is_add})

        async def fake_multi(ctx: commands.Context, tracks: list[dict], source_msg: str, is_add: bool = False):
            route_calls.append({"kind": "multi", "count": len(tracks), "source_msg": source_msg, "is_add": is_add})

        async def fake_progressive(ctx: commands.Context, playlist_id: str, is_add: bool = False):
            route_calls.append({"kind": "playlist_progressive", "playlist_id": playlist_id, "is_add": is_add})

        async def fake_album_progressive(ctx: commands.Context, album_id: str, is_add: bool = False):
            route_calls.append({"kind": "album_progressive", "album_id": album_id, "is_add": is_add})

        async def fake_artist_progressive(ctx: commands.Context, artist_id: str, is_add: bool = False):
            route_calls.append({"kind": "artist_progressive", "artist_id": artist_id, "is_add": is_add})

        globals()["_handle_single_track"] = fake_single
        globals()["_handle_multiple_tracks"] = fake_multi
        globals()["_enqueue_spotify_album_progressive"] = fake_album_progressive
        globals()["_enqueue_spotify_artist_progressive"] = fake_artist_progressive
        globals()["_enqueue_spotify_playlist_progressive"] = fake_progressive

        track_ctx = _debug_make_ctx(debug_guild_id, debug_user_id, owner_vc)
        await track(track_ctx, "play", query="https://open.spotify.com/track/track123")
        await album(_debug_make_ctx(debug_guild_id, debug_user_id, owner_vc), "add", query="https://open.spotify.com/album/album123")
        await playlist(_debug_make_ctx(debug_guild_id, debug_user_id, owner_vc), "play", query="https://open.spotify.com/playlist/playlist123")
        await artist(_debug_make_ctx(debug_guild_id, debug_user_id, owner_vc), "play", query="https://open.spotify.com/artist/artist123")
        route_kinds = [entry["kind"] for entry in route_calls]
        _debug_record(results, "Track command Spotify route", "pass" if route_kinds and route_kinds[0] == "single" else "fail", f"routes={route_kinds}")
        _debug_record(results, "Album command Spotify route", "pass" if any(entry["kind"] == "album_progressive" or (entry["kind"] == "multi" and entry.get("source_msg", "").startswith("💿")) for entry in route_calls) else "fail", f"routes={route_kinds}")
        _debug_record(results, "Playlist command Spotify route", "pass" if any(entry["kind"] == "playlist_progressive" or (entry["kind"] == "multi" and entry.get("source_msg", "").startswith("📋")) for entry in route_calls) else "fail", f"routes={route_kinds}")
        _debug_record(results, "Artist command Spotify route", "pass" if any(entry["kind"] == "artist_progressive" or (entry["kind"] == "multi" and entry.get("source_msg", "").startswith("🎤")) for entry in route_calls) else "fail", f"routes={route_kinds}")
    finally:
        globals()["spotify_api_get"] = original_spotify_api_get
        globals()["resolve_spotify_track_to_tidal"] = original_resolve_spotify_track_to_tidal
        globals()["_handle_single_track"] = original_handle_single_track
        globals()["_handle_multiple_tracks"] = original_handle_multiple_tracks
        globals()["_enqueue_spotify_album_progressive"] = original_enqueue_spotify_album_progressive
        globals()["_enqueue_spotify_artist_progressive"] = original_enqueue_spotify_artist_progressive
        globals()["_enqueue_spotify_playlist_progressive"] = original_enqueue_spotify_playlist_progressive

    # Playback and control command checks.
    reset_debug_state()
    current_tracks[debug_guild_id] = {"id": 6, "title": "Playing Track", "artist": "Artist", "stream_url": "https://example.com/stream"}
    queues[debug_guild_id] = deque([
        {"id": 7, "title": "Queued 1", "artist": "Artist"},
        {"id": 8, "title": "Queued 2", "artist": "Artist"},
        {"id": 9, "title": "Queued 3", "artist": "Artist"},
    ])
    history_entry = {"id": 5, "title": "Previous Track", "artist": "Artist"}
    histories[debug_guild_id] = [history_entry]
    loop_modes[debug_guild_id] = "off"
    link_command_enabled_by_guild[debug_guild_id] = True

    control_vc = _debug_make_voice_client(_debug_make_voice_channel(None), playing=True)
    control_ctx = _debug_make_ctx(debug_guild_id, debug_user_id, control_vc)
    skip_ctx = _debug_make_ctx(debug_guild_id, debug_user_id, control_vc)
    prev_ctx = _debug_make_ctx(debug_guild_id, debug_user_id, control_vc)
    queue_ctx = _debug_make_ctx(debug_guild_id, debug_user_id, control_vc)
    link_ctx = _debug_make_ctx(debug_guild_id, debug_user_id, control_vc)
    stop_ctx = _debug_make_ctx(debug_guild_id, debug_user_id, control_vc)
    loop_ctx = _debug_make_ctx(debug_guild_id, debug_user_id, control_vc)
    shuffle_ctx = _debug_make_ctx(debug_guild_id, debug_user_id, control_vc)

    await skip(skip_ctx, 2)
    _debug_record(results, "Skip command", "pass" if len(get_queue(debug_guild_id)) == 2 and control_vc.stopped >= 1 else "fail", f"queue_len={len(get_queue(debug_guild_id))}")

    control_vc._playing = True
    await pause_cmd(control_ctx)
    pause_ok = control_vc.is_paused() and any("Paused playback" in (entry["content"] or "") for entry in control_ctx.sent_messages)
    await resume_cmd(control_ctx)
    resume_ok = control_vc.is_playing() and any("Resumed playback" in (entry["content"] or "") for entry in control_ctx.sent_messages)
    _debug_record(results, "Pause command", "pass" if pause_ok else "fail", f"paused={control_vc.is_paused()}")
    _debug_record(results, "Resume command", "pass" if resume_ok else "fail", f"playing={control_vc.is_playing()}")

    await previous(prev_ctx)
    _debug_record(results, "Previous command", "pass" if get_queue(debug_guild_id) and get_history(debug_guild_id) == [] else "fail", f"queue={list(get_queue(debug_guild_id))}")

    await queue_cmd(queue_ctx)
    queue_message = queue_ctx.sent_messages[-1]["kwargs"].get("embed") if queue_ctx.sent_messages else None
    queue_ok = bool(queue_message and "Queue" in getattr(queue_message, "title", "") and "Now Playing" in getattr(queue_message, "title", "") or queue_message)
    _debug_record(results, "Queue command", "pass" if queue_message is not None else "fail", getattr(queue_message, "description", ""))

    await link_cmd(link_ctx, query="https://tidal.com/browse/track/74237")
    link_sent = link_ctx.sent_messages[-1]["content"] if link_ctx.sent_messages else ""
    _debug_record(results, "Link command", "pass" if "Download Link" in link_sent else "fail", link_sent or "no response")

    await loop(loop_ctx, "track")
    _debug_record(results, "Loop command", "pass" if get_loop_mode(debug_guild_id) == "track" else "fail", f"mode={get_loop_mode(debug_guild_id)}")

    before_shuffle = [track["id"] for track in list(get_queue(debug_guild_id))]
    await shuffle(shuffle_ctx)
    after_shuffle = sorted(track["id"] for track in list(get_queue(debug_guild_id)))
    _debug_record(results, "Shuffle command", "pass" if after_shuffle == sorted(before_shuffle) else "fail", f"before={before_shuffle}, after={list(track['id'] for track in get_queue(debug_guild_id))}")

    await stop(stop_ctx)
    stopped_ok = not get_queue(debug_guild_id) and debug_guild_id not in current_tracks and control_vc.disconnected >= 1
    _debug_record(results, "Stop command", "pass" if stopped_ok else "fail", f"queue_empty={not get_queue(debug_guild_id)}, disconnected={control_vc.disconnected}")

    # Remote lookups and full pipeline checks.
    async with aiohttp.ClientSession() as session:
        status, data = await api_get(session, f"{TIDAL_API}/search/", {"s": "Never Gonna Give You Up"})
        if status == 200 and data:
            items = data.get("data", {}).get("items", [])
            if items and items[0].get("id"):
                track_id = items[0]["id"]
                stream_url, stream_status = await get_stream_url(session, track_id)
                _debug_record(results, "Tidal search and stream lookup", "pass" if stream_url else "fail", f"track_id={track_id}, http={stream_status}")
            else:
                _debug_record(results, "Tidal search and stream lookup", "skip", "no usable search results")
        else:
            _debug_record(results, "Tidal search and stream lookup", "skip", f"HTTP {status}")

    client_id, client_secret = _spotify_credential_values()
    if not client_id or not client_secret:
        _debug_record(results, "Spotify pipeline", "skip", "Spotify credentials not set")
    else:
        async with aiohttp.ClientSession() as session:
            token = await get_spotify_access_token(session)
            if not token:
                _debug_record(results, "Spotify token fetch", "skip", "could not retrieve access token")
            else:
                status, data = await spotify_api_get(
                    session,
                    "/search",
                    {"q": "Never Gonna Give You Up", "type": "track", "limit": 1},
                )
                items = data.get("tracks", {}).get("items", []) if status == 200 and data else []
                if items:
                    spotify_track = _spotify_track_from_payload(items[0])
                    if spotify_track:
                        matched = await resolve_spotify_track_to_tidal(session, spotify_track)
                        if matched:
                            stream_url, stream_status = await get_stream_url(session, matched["id"])
                            _debug_record(results, "Spotify -> Tidal -> stream pipeline", "pass" if stream_url else "fail", f"matched_id={matched['id']}, http={stream_status}")
                        else:
                            _debug_record(results, "Spotify -> Tidal -> stream pipeline", "fail", "no Tidal match")
                    else:
                        _debug_record(results, "Spotify -> Tidal -> stream pipeline", "fail", "invalid Spotify track payload")
                else:
                    _debug_record(results, "Spotify track search", "skip", f"HTTP {status}")

    required_commands = [
        "help",
        "track",
        "album",
        "playlist",
        "artist",
        "skip",
        "previous",
        "queue",
        "link",
        "spectrogram",
        "trackinfo",
        "cover",
        "lyrics",
        "audiostats",
        "stop",
        "pause",
        "resume",
        "loop",
        "shuffle",
    ]
    command_ok = all(bot.get_command(name) is not None for name in required_commands)
    _debug_record(results, "Core command registration", "pass" if command_ok else "fail", ", ".join(required_commands))

    _debug_write_report(results)
    pass_count = sum(1 for result in results if result["status"] == "pass")
    skip_count = sum(1 for result in results if result["status"] == "skip")
    fail_count = sum(1 for result in results if result["status"] == "fail")
    logger.info("[self-test] Wrote report to %s", DEBUG_SELFTEST_REPORT_PATH)
    logger.info("[self-test] Summary: %s passed, %s skipped, %s failed", pass_count, skip_count, fail_count)
    return results


async def _debug_selftest_listener() -> None:
    """Wait for Enter in an interactive terminal and run the self-test suite."""
    logger.info("Debug self-test mode enabled. Press Enter in the terminal to run self-tests.")
    while not bot.is_closed():
        try:
            line = await asyncio.to_thread(sys.stdin.readline)
        except Exception as exc:
            logger.warning("Debug self-test listener stopped: %s", exc)
            return

        if not line:
            await asyncio.sleep(0.25)
            continue

        if line.strip():
            continue

        logger.info("[self-test] Enter pressed; starting feature checks...")
        results = await run_debug_self_tests()
        fail_count = sum(1 for result in results if result["status"] == "fail")
        if fail_count:
            logger.error("[self-test] Completed with %s failure(s).", fail_count)
        else:
            logger.info("[self-test] Completed successfully.")


# ── helpers ──────────────────────────────────────────────────────────

def get_queue(guild_id: int) -> deque:
    if guild_id not in queues:
        queues[guild_id] = deque()
    return queues[guild_id]


def get_history(guild_id: int) -> list:
    if guild_id not in histories:
        histories[guild_id] = []
    return histories[guild_id]


def _track_entry_id(track_entry: dict[str, Any] | None) -> int | None:
    if not isinstance(track_entry, dict):
        return None
    raw_track_id = track_entry.get("id")
    try:
        return int(raw_track_id)
    except (TypeError, ValueError):
        return None


def _is_current_track_id(guild_id: int, track_id: int) -> bool:
    return _track_entry_id(current_tracks.get(guild_id)) == track_id


def _current_track_elapsed_seconds(guild_id: int, track_id: int) -> float:
    if not _is_current_track_id(guild_id, track_id):
        return 0.0

    curr = current_tracks.get(guild_id) or {}
    started_at_raw = curr.get("started_at")
    if not isinstance(started_at_raw, str):
        return 0.0

    started_at = discord.utils.parse_time(started_at_raw)
    if started_at is None:
        return 0.0

    now = discord.utils.utcnow()
    if started_at.tzinfo is None and now.tzinfo is not None:
        started_at = started_at.replace(tzinfo=now.tzinfo)

    return max(0.0, (now - started_at).total_seconds())


def _register_metadata_message_for_current_track(
    ctx: commands.Context,
    track_id: int | None,
    sent_message: Any,
) -> None:
    """Track metadata messages so they can be removed when the current song ends."""
    if not ctx.guild or not isinstance(track_id, int):
        return
    if not isinstance(sent_message, discord.Message):
        return

    if not _is_current_track_id(ctx.guild.id, track_id):
        return

    refs = metadata_message_refs_by_guild.setdefault(ctx.guild.id, [])
    refs.append(
        {
            "track_id": track_id,
            "channel_id": sent_message.channel.id,
            "message_id": sent_message.id,
        }
    )
    if len(refs) > 200:
        del refs[:-200]


async def _send_trackable_metadata_message(ctx: commands.Context, *args: Any, **kwargs: Any) -> Any:
    """Send metadata responses and return a message object when possible."""
    if getattr(ctx, "interaction", None) is not None:
        kwargs.setdefault("wait", True)
    return await ctx.send(*args, **kwargs)


async def _delete_metadata_messages_for_track(guild_id: int, track_id: int) -> None:
    """Delete tracked metadata messages associated with the finished track."""
    refs = metadata_message_refs_by_guild.get(guild_id, [])
    if not refs:
        return

    remaining_refs: list[dict[str, int]] = []
    for ref in refs:
        if ref.get("track_id") != track_id:
            remaining_refs.append(ref)
            continue

        channel_id = ref.get("channel_id")
        message_id = ref.get("message_id")
        if not isinstance(channel_id, int) or not isinstance(message_id, int):
            continue

        channel = bot.get_channel(channel_id)
        if channel is None:
            guild = bot.get_guild(guild_id)
            if guild is not None:
                channel = guild.get_channel(channel_id) or guild.get_thread(channel_id)
        if channel is None:
            continue

        try:
            if hasattr(channel, "get_partial_message"):
                await channel.get_partial_message(message_id).delete()
            else:
                message = await channel.fetch_message(message_id)
                await message.delete()
        except (discord.NotFound, discord.Forbidden):
            pass
        except Exception as exc:
            logger.debug("Failed deleting metadata message %s in guild %s: %s", message_id, guild_id, exc)

    if remaining_refs:
        metadata_message_refs_by_guild[guild_id] = remaining_refs
    else:
        metadata_message_refs_by_guild.pop(guild_id, None)


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
    async with aiohttp.ClientSession() as session:
        while q:
            candidate = q.popleft()
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
            disconnected_track = current_tracks.pop(gid, None)
            disconnected_track_id = _track_entry_id(disconnected_track)
            if disconnected_track_id is not None:
                asyncio.run_coroutine_threadsafe(
                    _delete_metadata_messages_for_track(gid, disconnected_track_id),
                    bot.loop,
                )
            return

        # Move current track to history
        finished_track = current_tracks.pop(gid, None)
        finished_track_id = _track_entry_id(finished_track)
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

        if finished_track_id is not None:
            asyncio.run_coroutine_threadsafe(
                _delete_metadata_messages_for_track(gid, finished_track_id),
                bot.loop,
            )

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


def parse_spotify_url(text: str) -> tuple[str | None, str | None]:
    """Extract resource type and ID from Spotify URL/URI."""
    m = re.search(r'spotify\.com/(?:intl-[a-z]{2}/)?(track|album|playlist|artist)/([A-Za-z0-9]+)', text)
    if m:
        return m.group(1), m.group(2)
    m = re.search(r'spotify:(track|album|playlist|artist):([A-Za-z0-9]+)', text)
    if m:
        return m.group(1), m.group(2)
    return None, None


def _normalize_text(value: str) -> str:
    value = value.lower()
    value = re.sub(r'\([^)]*\)', ' ', value)
    value = re.sub(r'\[[^\]]*\]', ' ', value)
    value = re.sub(r'[^a-z0-9\s]', ' ', value)
    value = re.sub(r'\s+', ' ', value).strip()
    return value


def _spotify_credential_values() -> tuple[str | None, str | None]:
    client_id = os.getenv("SPOTIFY_CLIENT_ID") or os.getenv("SPOTIFY_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET") or os.getenv("SPOTIFY_SECRET")
    return client_id, client_secret


def _spotify_user_access_token_value() -> str | None:
    token = os.getenv("SPOTIFY_USER_ACCESS_TOKEN") or os.getenv("SPOTIFY_ACCESS_TOKEN")
    if not token:
        return None
    token = token.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    return token or None


def _spotify_sp_dc_cookie_value() -> str | None:
    cookie = os.getenv("SPOTIFY_SP_DC") or os.getenv("SP_DC")
    if not cookie:
        return None
    cookie = cookie.strip()
    return cookie or None


async def get_spotify_access_token(session: aiohttp.ClientSession) -> str | None:
    """Get or refresh Spotify app token using Client Credentials flow."""
    global _spotify_access_token, _spotify_access_token_expiry

    now = time.time()
    if _spotify_access_token and now < (_spotify_access_token_expiry - 30):
        return _spotify_access_token

    client_id, client_secret = _spotify_credential_values()
    if not client_id or not client_secret:
        return None

    auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}"}
    payload = {"grant_type": "client_credentials"}

    async with session.post(f"{SPOTIFY_ACCOUNTS_API}/token", data=payload, headers=headers) as resp:
        if resp.status != 200:
            body = await resp.text()
            logger.warning("Spotify token request failed (HTTP %s): %s", resp.status, body[:250])
            return None

        data = await resp.json()
        token = data.get("access_token")
        expires_in = int(data.get("expires_in", 3600))
        if not token:
            return None

        _spotify_access_token = token
        _spotify_access_token_expiry = now + expires_in
        return token


async def spotify_api_get(
    session: aiohttp.ClientSession,
    endpoint: str,
    params: dict[str, Any] | None = None,
    retries: int = 3,
) -> tuple[int, Any]:
    """Spotify GET helper with auto token retrieval and lightweight retry policy."""
    for attempt in range(retries):
        token = await get_spotify_access_token(session)
        if not token:
            return 401, None

        headers = {"Authorization": f"Bearer {token}"}
        async with session.get(f"{SPOTIFY_WEB_API}{endpoint}", params=params, headers=headers) as resp:
            if resp.status == 429:
                retry_after = int(resp.headers.get("Retry-After", "2"))
                await asyncio.sleep(max(1, retry_after))
                continue
            if resp.status == 401 and attempt < retries - 1:
                # Force refresh token then retry once.
                global _spotify_access_token, _spotify_access_token_expiry
                _spotify_access_token = None
                _spotify_access_token_expiry = 0
                continue
            if resp.status != 200:
                return resp.status, None
            return 200, await resp.json()
    return 429, None


async def get_spotify_web_player_access_token(session: aiohttp.ClientSession) -> str | None:
    """Fetch anonymous Spotify Web Player token for public playlist fallback paths."""
    global _spotify_web_player_access_token, _spotify_web_player_access_token_expiry

    now = time.time()
    if _spotify_web_player_access_token and now < (_spotify_web_player_access_token_expiry - 30):
        return _spotify_web_player_access_token

    sp_dc = _spotify_sp_dc_cookie_value()
    params = {"reason": "transport", "productType": "web_player"}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://open.spotify.com/",
        "Origin": "https://open.spotify.com",
    }
    if sp_dc:
        headers["Cookie"] = f"sp_dc={sp_dc}"
    async with session.get(SPOTIFY_WEB_PLAYER_TOKEN_URL, params=params, headers=headers) as resp:
        if resp.status != 200:
            return None
        data = await resp.json(content_type=None)
        token = data.get("accessToken")
        expires_ms = data.get("accessTokenExpirationTimestampMs")
        if not token:
            return None
        if isinstance(expires_ms, (int, float)) and expires_ms > 0:
            _spotify_web_player_access_token_expiry = float(expires_ms) / 1000.0
        else:
            _spotify_web_player_access_token_expiry = now + 1800
        _spotify_web_player_access_token = token
        return token


async def _spotify_get_json_by_full_url_with_app_token(
    session: aiohttp.ClientSession,
    url: str,
    retries: int = 3,
) -> tuple[int, Any]:
    """Request a full Spotify URL using the app token with retry on 401/429."""
    for attempt in range(retries):
        token = await get_spotify_access_token(session)
        if not token:
            return 401, None

        headers = {"Authorization": f"Bearer {token}"}
        async with session.get(url, headers=headers) as resp:
            if resp.status == 429:
                retry_after = int(resp.headers.get("Retry-After", "2"))
                await asyncio.sleep(max(1, retry_after))
                continue
            if resp.status == 401 and attempt < retries - 1:
                global _spotify_access_token, _spotify_access_token_expiry
                _spotify_access_token = None
                _spotify_access_token_expiry = 0
                continue
            if resp.status != 200:
                return resp.status, None
            return 200, await resp.json()
    return 429, None


async def _spotify_get_json_by_full_url_with_web_token(
    session: aiohttp.ClientSession,
    url: str,
    retries: int = 3,
) -> tuple[int, Any]:
    """Request a full Spotify URL using the anonymous web player token."""
    for attempt in range(retries):
        token = await get_spotify_web_player_access_token(session)
        if not token:
            return 401, None

        headers = {"Authorization": f"Bearer {token}", "User-Agent": "Mozilla/5.0"}
        async with session.get(url, headers=headers) as resp:
            if resp.status == 429:
                retry_after = int(resp.headers.get("Retry-After", "2"))
                await asyncio.sleep(max(1, retry_after))
                continue
            if resp.status == 401 and attempt < retries - 1:
                global _spotify_web_player_access_token, _spotify_web_player_access_token_expiry
                _spotify_web_player_access_token = None
                _spotify_web_player_access_token_expiry = 0
                continue
            if resp.status != 200:
                return resp.status, None
            return 200, await resp.json()
    return 429, None


async def _spotify_get_json_by_full_url_with_user_token(
    session: aiohttp.ClientSession,
    url: str,
    retries: int = 3,
) -> tuple[int, Any]:
    """Request a full Spotify URL using an optional user-provided access token."""
    token = _spotify_user_access_token_value()
    if not token:
        return 401, None

    headers = {"Authorization": f"Bearer {token}", "User-Agent": "Mozilla/5.0"}
    for _ in range(retries):
        async with session.get(url, headers=headers) as resp:
            if resp.status == 429:
                # User-token route is optional; do not block playback on long Spotify cooldowns.
                retry_after = int(resp.headers.get("Retry-After", "2"))
                if retry_after > 5:
                    return 429, None
                await asyncio.sleep(max(1, retry_after))
                continue
            if resp.status != 200:
                return resp.status, None
            return 200, await resp.json()
    return 429, None


def _spotify_track_from_payload(track: dict[str, Any]) -> dict[str, Any] | None:
    if not track or not track.get("id"):
        return None

    artists = track.get("artists") or []
    artist_name = artists[0].get("name") if artists else "Unknown"
    return {
        "spotify_id": track.get("id"),
        "title": track.get("name", "Unknown"),
        "artist": artist_name,
        "duration_ms": track.get("duration_ms"),
    }


def _spotify_row_track_id(row: dict[str, Any]) -> str | None:
    """Return a stable Spotify track ID from a playlist row payload."""
    if not isinstance(row, dict):
        return None

    raw_track = row.get("track")
    if not isinstance(raw_track, dict):
        raw_track = row

    track_id = raw_track.get("id")
    if isinstance(track_id, str) and track_id:
        return track_id

    uri = raw_track.get("uri")
    if isinstance(uri, str) and ":" in uri:
        return uri.rsplit(":", 1)[-1]

    return None


async def find_tidal_track_match(
    session: aiohttp.ClientSession,
    title: str,
    artist: str,
    duration_ms: int | None = None,
) -> dict[str, Any] | None:
    """Find the best Tidal track candidate for a Spotify track descriptor."""
    query = f"{title} {artist}".strip()
    status, data = await api_get(session, f"{TIDAL_API}/search/", {"s": query})
    if status == 403:
        logger.warning("Tidal search returned 403 for query %r; reselecting API node and retrying once.", query)
        await pick_best_api()
        status, data = await api_get(session, f"{TIDAL_API}/search/", {"s": query})
    if status != 200 or not data:
        return None

    items = data.get("data", {}).get("items", [])
    if not items:
        return None

    norm_title = _normalize_text(title)
    norm_artist = _normalize_text(artist)

    best_item = None
    best_score = -10_000
    for item in items[:10]:
        item_title = item.get("title", "")
        item_artist = (item.get("artist") or {}).get("name", "")
        item_title_norm = _normalize_text(item_title)
        item_artist_norm = _normalize_text(item_artist)

        score = 0
        if item_title_norm == norm_title:
            score += 50
        elif norm_title and (norm_title in item_title_norm or item_title_norm in norm_title):
            score += 25

        if item_artist_norm == norm_artist:
            score += 40
        elif norm_artist and (norm_artist in item_artist_norm or item_artist_norm in norm_artist):
            score += 20

        item_duration = item.get("duration")
        if duration_ms is not None and isinstance(item_duration, int):
            delta = abs(duration_ms - (item_duration * 1000))
            if delta <= 2000:
                score += 15
            elif delta <= 5000:
                score += 8
            elif delta > 20000:
                score -= 12

        if score > best_score:
            best_score = score
            best_item = item

    if not best_item or not best_item.get("id"):
        return None

    return {
        "id": best_item["id"],
        "title": best_item.get("title", title),
        "artist": (best_item.get("artist") or {}).get("name", artist),
    }


async def resolve_spotify_track_to_tidal(
    session: aiohttp.ClientSession,
    spotify_track: dict[str, Any],
) -> dict[str, Any] | None:
    return await find_tidal_track_match(
        session=session,
        title=spotify_track.get("title", ""),
        artist=spotify_track.get("artist", ""),
        duration_ms=spotify_track.get("duration_ms"),
    )


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
            finished_track = current_tracks.pop(gid, None)
            finished_track_id = _track_entry_id(finished_track)
            loop_modes.pop(gid, None)
            skip_requests.discard(gid)
            save_guild_state(gid)

            if finished_track_id is not None:
                await _delete_metadata_messages_for_track(gid, finished_track_id)
            
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
        name="🧾 Metadata",
        value=(
            "`u!spectrogram [query or song URL]` — Send a spectrogram image (`u!spectogram`, `u!spec`)\n"
            "`u!trackinfo [query or song URL]` — Show artist, album, year, and track metadata\n"
            "`u!cover [query or song URL]` — Show album art\n"
            "`u!lyrics [query or song URL]` — Show lyrics (with TXT attachment if long)\n"
            "`u!audiostats [query or song URL]` — Probe codec, bitrate, sample rate, and channels"
        ),
        inline=False,
    )
    embed.add_field(
        name="📋 Queue",
        value=(
            "`u!queue [page]` — Show the current queue in columns with pagination\n"
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
    spotify_type, spotify_id = parse_spotify_url(query)

    if spotify_type == 'track':
        async with aiohttp.ClientSession() as session:
            s_status, s_data = await spotify_api_get(session, f"/tracks/{spotify_id}")
            if s_status == 401:
                await ctx.send("❌ Spotify API credentials missing or invalid. Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET.", silent=True)
                return
            if s_status != 200 or not s_data:
                await ctx.send(f"❌ Spotify track lookup failed (HTTP {s_status})", silent=True)
                return

            spotify_track = _spotify_track_from_payload(s_data)
            if not spotify_track:
                await ctx.send("❌ Invalid Spotify track payload.", silent=True)
                return

            track_entry = await resolve_spotify_track_to_tidal(session, spotify_track)
            if not track_entry:
                await ctx.send("❌ Could not find a matching Tidal track for that Spotify song.", silent=True)
                return

        await _handle_single_track(ctx, track_entry, is_add)
        return
    elif spotify_type == 'album':
        await _enqueue_spotify_album_progressive(ctx, spotify_id, is_add)
        return
    elif spotify_type == 'playlist':
        await _enqueue_spotify_playlist_progressive(ctx, spotify_id, is_add)
        return
    elif spotify_type == 'artist':
        await _enqueue_spotify_artist_progressive(ctx, spotify_id, is_add)
        return

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

    # Accept Spotify URL directly.
    spotify_type, spotify_id = parse_spotify_url(query)
    if spotify_type == 'album':
        await _enqueue_spotify_album_progressive(ctx, spotify_id, is_add)
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

    spotify_type, spotify_id = parse_spotify_url(query)
    if spotify_type == 'playlist':
        await _enqueue_spotify_playlist_progressive(ctx, spotify_id, is_add)
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

    spotify_type, spotify_id = parse_spotify_url(query)
    if spotify_type == 'artist':
        await _enqueue_spotify_artist_progressive(ctx, spotify_id, is_add)
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


async def _spotify_paged_items(
    session: aiohttp.ClientSession,
    endpoint: str,
    items_key: str = "items",
    params: dict[str, Any] | None = None,
    max_items: int = 200,
) -> tuple[int, list[dict[str, Any]], Any]:
    """Collect Spotify paginated item arrays from standard paging responses."""
    collected: list[dict[str, Any]] = []
    status, data = await spotify_api_get(session, endpoint, params=params)
    if status != 200 or not data:
        return status, collected, data

    paging = data
    if items_key != "items":
        paging = data.get(items_key, {})

    items = paging.get("items", [])
    collected.extend(items)
    next_url = paging.get("next")

    while next_url and len(collected) < max_items:
        page_status, page = await _spotify_get_json_by_full_url_with_app_token(session, next_url)
        if page_status != 200 or not page:
            break
        page_items = page.get("items", [])
        collected.extend(page_items)
        next_url = page.get("next")

    return 200, collected[:max_items], data


async def _fetch_spotify_playlist_tracks(
    session: aiohttp.ClientSession,
    playlist_id: str,
    on_rows: Callable[[list[dict[str, Any]]], Awaitable[None]] | None = None,
) -> tuple[int, str, list[dict[str, Any]], bool]:
    """Fetch playlist rows by merging available Spotify sources and deduping by track ID."""
    playlist_name = "Spotify Playlist"
    merged_items: list[dict[str, Any]] = []
    seen_track_ids: set[str] = set()
    status_codes: list[int] = []
    app_complete = False
    playlist_total_hint = 0
    playlist_next_offset_hint = 0
    spotify_autocomplete_used = False

    async def _merge_rows(rows: list[dict[str, Any]]) -> None:
        newly_added: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            track_id = _spotify_row_track_id(row)
            if not track_id or track_id in seen_track_ids:
                continue
            seen_track_ids.add(track_id)
            merged_items.append(row)
            newly_added.append(row)
        if newly_added and on_rows is not None:
            await on_rows(newly_added)

    # 1) Standard app-token playlist tracks paging.
    status, items, first_page = await _spotify_paged_items(
        session,
        f"/playlists/{playlist_id}/tracks",
        params={"limit": 100, "offset": 0, "additional_types": "track"},
        max_items=1000,
    )
    status_codes.append(status)
    if status == 200:
        await _merge_rows(items)
        if isinstance(first_page, dict):
            playlist_name = first_page.get("name", playlist_name)
            declared_total = first_page.get("total")
            if isinstance(declared_total, int) and declared_total > 0:
                playlist_total_hint = max(playlist_total_hint, declared_total)
            if isinstance(declared_total, int) and declared_total > len(items):
                offset = len(items)
                while offset < declared_total and len(merged_items) < 1000:
                    page_status, page_data = await spotify_api_get(
                        session,
                        f"/playlists/{playlist_id}/tracks",
                        params={"limit": 100, "offset": offset, "additional_types": "track"},
                    )
                    status_codes.append(page_status)
                    if page_status != 200 or not isinstance(page_data, dict):
                        break
                    page_items = page_data.get("items", [])
                    if not page_items:
                        break
                    await _merge_rows(page_items)
                    offset += len(page_items)
                    if len(page_items) < 100:
                        break
            # Probe additional offsets even when Spotify reports a low total for mix-like playlists.
            if len(items) > 0 and len(merged_items) < 1000:
                offset = len(items)
                stagnant_pages = 0
                while offset < 1000 and stagnant_pages < 2:
                    page_status, page_data = await spotify_api_get(
                        session,
                        f"/playlists/{playlist_id}/tracks",
                        params={"limit": 100, "offset": offset, "additional_types": "track"},
                    )
                    status_codes.append(page_status)
                    if page_status != 200 or not isinstance(page_data, dict):
                        break
                    page_items = page_data.get("items", [])
                    if not page_items:
                        break
                    before_len = len(merged_items)
                    await _merge_rows(page_items)
                    if len(merged_items) == before_len:
                        stagnant_pages += 1
                    else:
                        stagnant_pages = 0
                    offset += len(page_items)
                    if len(page_items) < 100 and stagnant_pages > 0:
                        break
            if isinstance(declared_total, int) and declared_total > 0 and len(merged_items) >= declared_total:
                app_complete = True

    if app_complete:
        return 200, playlist_name, merged_items[:1000], spotify_autocomplete_used

    # 2) Playlist metadata + nested tracks paging.
    status_meta, meta = await spotify_api_get(session, f"/playlists/{playlist_id}")
    status_codes.append(status_meta)
    if status_meta == 200 and isinstance(meta, dict):
        playlist_name = meta.get("name", playlist_name)
        tracks_block = meta.get("tracks", {})
        if isinstance(tracks_block, dict):
            declared_meta_total = tracks_block.get("total")
            if isinstance(declared_meta_total, int) and declared_meta_total > 0:
                playlist_total_hint = max(playlist_total_hint, declared_meta_total)
            await _merge_rows(tracks_block.get("items", []))
            next_url = tracks_block.get("next")
            while next_url and len(merged_items) < 1000:
                next_status, next_data = await _spotify_get_json_by_full_url_with_app_token(session, next_url)
                status_codes.append(next_status)
                if next_status != 200 or not isinstance(next_data, dict):
                    break
                await _merge_rows(next_data.get("items", []))
                next_url = next_data.get("next")

    # 3) Optional user token source for mixes that public endpoints may cap.
    user_token = _spotify_user_access_token_value()
    if user_token and len(merged_items) < 1000:
        user_meta_url = f"{SPOTIFY_WEB_API}/playlists/{playlist_id}?fields=name"
        user_meta_status, user_meta = await _spotify_get_json_by_full_url_with_user_token(session, user_meta_url)
        status_codes.append(user_meta_status)
        if user_meta_status == 200 and isinstance(user_meta, dict):
            playlist_name = user_meta.get("name", playlist_name)

        offset = 0
        stagnant_pages = 0
        while offset < 1000 and stagnant_pages < 2:
            user_page_url = f"{SPOTIFY_WEB_API}/playlists/{playlist_id}/tracks?limit=100&offset={offset}&additional_types=track"
            page_status, page_data = await _spotify_get_json_by_full_url_with_user_token(session, user_page_url)
            status_codes.append(page_status)
            if page_status != 200 or not isinstance(page_data, dict):
                break
            page_items = page_data.get("items", [])
            if not page_items:
                break
            before_len = len(merged_items)
            await _merge_rows(page_items)
            if len(merged_items) == before_len:
                stagnant_pages += 1
            else:
                stagnant_pages = 0
            offset += len(page_items)
            if len(page_items) < 100 and stagnant_pages > 0:
                break

    # 4) Web-player token fallback for mixes/radios.
    if len(merged_items) < 1000:
        first_page_url = f"{SPOTIFY_WEB_API}/playlists/{playlist_id}/tracks?limit=100&offset=0&additional_types=track&market=from_token"
        web_status, web_page = await _spotify_get_json_by_full_url_with_web_token(session, first_page_url)
        status_codes.append(web_status)
        if web_status == 200 and isinstance(web_page, dict):
            first_web_items = web_page.get("items", [])
            await _merge_rows(first_web_items)
            next_url = web_page.get("next")
            while next_url and len(merged_items) < 1000:
                next_status, next_data = await _spotify_get_json_by_full_url_with_web_token(session, next_url)
                status_codes.append(next_status)
                if next_status != 200 or not isinstance(next_data, dict):
                    break
                await _merge_rows(next_data.get("items", []))
                next_url = next_data.get("next")

            # Some mix endpoints can omit `next`; try manual offsets when total suggests more rows.
            declared_web_total = web_page.get("total")
            if isinstance(declared_web_total, int) and declared_web_total > 0:
                playlist_total_hint = max(playlist_total_hint, declared_web_total)
            if isinstance(declared_web_total, int) and declared_web_total > len(first_web_items) and len(merged_items) < 1000:
                offset = len(first_web_items)
                while offset < declared_web_total and len(merged_items) < 1000:
                    page_url = f"{SPOTIFY_WEB_API}/playlists/{playlist_id}/tracks?limit=100&offset={offset}&additional_types=track&market=from_token"
                    page_status, page_data = await _spotify_get_json_by_full_url_with_web_token(session, page_url)
                    status_codes.append(page_status)
                    if page_status != 200 or not isinstance(page_data, dict):
                        break
                    page_items = page_data.get("items", [])
                    if not page_items:
                        break
                    await _merge_rows(page_items)
                    offset += len(page_items)
                    if len(page_items) < 100:
                        break

            # Probe offsets with web token as a final attempt when totals/next are unreliable.
            if len(first_web_items) > 0 and len(merged_items) < 1000:
                offset = len(first_web_items)
                stagnant_pages = 0
                while offset < 1000 and stagnant_pages < 2:
                    page_url = f"{SPOTIFY_WEB_API}/playlists/{playlist_id}/tracks?limit=100&offset={offset}&additional_types=track&market=from_token"
                    page_status, page_data = await _spotify_get_json_by_full_url_with_web_token(session, page_url)
                    status_codes.append(page_status)
                    if page_status != 200 or not isinstance(page_data, dict):
                        break
                    page_items = page_data.get("items", [])
                    if not page_items:
                        break
                    before_len = len(merged_items)
                    await _merge_rows(page_items)
                    if len(merged_items) == before_len:
                        stagnant_pages += 1
                    else:
                        stagnant_pages = 0
                    offset += len(page_items)
                    if len(page_items) < 100 and stagnant_pages > 0:
                        break

            if playlist_name == "Spotify Playlist":
                meta_url = f"{SPOTIFY_WEB_API}/playlists/{playlist_id}?fields=name&market=from_token"
                meta_status_web, meta_web = await _spotify_get_json_by_full_url_with_web_token(session, meta_url)
                status_codes.append(meta_status_web)
                if meta_status_web == 200 and isinstance(meta_web, dict):
                    playlist_name = meta_web.get("name", playlist_name)

    # 5) HTML scrape fallback for any rows not seen yet.
    if len(merged_items) < 1000:
        try:
            async with session.get(f"https://open.spotify.com/playlist/{playlist_id}", headers={"User-Agent": "Mozilla/5.0"}) as resp:
                status_codes.append(resp.status)
                if resp.status == 200:
                    html = await resp.text()
                    import re, base64, json
                    match = re.search(r'<script[^>]*id="initialState"[^>]*>(.*?)</script>', html, re.IGNORECASE)
                    if match:
                        data_str = base64.b64decode(match.group(1).strip()).decode("utf-8")
                        j = json.loads(data_str)
                        p_item = j.get("entities", {}).get("items", {}).get(f"spotify:playlist:{playlist_id}")
                        if p_item:
                            playlist_name = p_item.get("name", playlist_name)
                            p_content = p_item.get("content", {}) if isinstance(p_item.get("content"), dict) else {}
                            p_total_count = p_content.get("totalCount")
                            if isinstance(p_total_count, int) and p_total_count > 0:
                                playlist_total_hint = max(playlist_total_hint, p_total_count)
                            p_paging = p_content.get("pagingInfo") if isinstance(p_content.get("pagingInfo"), dict) else {}
                            p_next_offset = p_paging.get("nextOffset")
                            if isinstance(p_next_offset, int) and p_next_offset > 0:
                                playlist_next_offset_hint = max(playlist_next_offset_hint, p_next_offset)
                            p_tracks = p_content.get("items", [])
                            scraped_items: list[dict[str, Any]] = []
                            for track_wrapper in p_tracks:
                                item_data = track_wrapper.get("itemV2", {}).get("data", {})
                                if not item_data or item_data.get("__typename") != "Track":
                                    continue
                                track_uri = item_data.get("uri", "")
                                if not track_uri:
                                    continue
                                artists = item_data.get("artists", {}).get("items", [])
                                artist_name = artists[0].get("profile", {}).get("name", "Unknown") if artists else "Unknown"
                                album_uri = (item_data.get("albumOfTrack") or {}).get("uri", "") if isinstance(item_data.get("albumOfTrack"), dict) else ""
                                album_id = album_uri.rsplit(":", 1)[-1] if isinstance(album_uri, str) and album_uri.startswith("spotify:album:") else None
                                track_payload: dict[str, Any] = {
                                    "id": track_uri.split(":")[-1],
                                    "name": item_data.get("name", "Unknown"),
                                    "artists": [{"name": artist_name}],
                                    "duration_ms": item_data.get("duration", {}).get("totalMilliseconds", 0),
                                }
                                if album_id:
                                    track_payload["album"] = {"id": album_id}
                                scraped_items.append({
                                    "track": track_payload
                                })
                            await _merge_rows(scraped_items)
        except Exception as e:
            logger.error(f"Spotify HTML fallback scraper failed: {e}")

    should_expand_from_album_pages = bool(
        merged_items
        and len(merged_items) <= 30
        and (playlist_total_hint > len(merged_items) or playlist_next_offset_hint > 0)
    )

    if should_expand_from_album_pages:
        expanded_rows = await _expand_spotify_playlist_rows_from_album_pages(
            session,
            merged_items,
            target_count=min(250, max(120, min(playlist_total_hint, 250))),
        )
        if expanded_rows:
            spotify_autocomplete_used = True
            logger.info(
                "Spotify playlist %s expanded from %d to %d rows using album-page fallback (total_hint=%d, next_offset_hint=%d).",
                playlist_id,
                len(merged_items),
                len(merged_items) + len(expanded_rows),
                playlist_total_hint,
                playlist_next_offset_hint,
            )
            await _merge_rows(expanded_rows)

    if merged_items:
        if len(merged_items) <= 30 and not _spotify_user_access_token_value():
            logger.warning(
                "Spotify playlist %s appears limited to %d rows from available sources; set SPOTIFY_USER_ACCESS_TOKEN for full personalized access.",
                playlist_id,
                len(merged_items),
            )
        return 200, playlist_name, merged_items[:1000], spotify_autocomplete_used
    if 401 in status_codes:
        return 401, "", [], spotify_autocomplete_used
    fallback_status = next((code for code in status_codes if isinstance(code, int) and code > 0), 404)
    return fallback_status, playlist_name, [], spotify_autocomplete_used


async def _expand_spotify_playlist_rows_from_album_pages(
    session: aiohttp.ClientSession,
    base_rows: list[dict[str, Any]],
    target_count: int,
) -> list[dict[str, Any]]:
    """Expand truncated playlist snapshots by scraping albums of visible tracks."""
    if not base_rows or target_count <= len(base_rows):
        return []

    seen_track_ids: set[str] = set()
    album_ids: list[str] = []
    seen_album_ids: set[str] = set()

    for row in base_rows:
        track_id = _spotify_row_track_id(row)
        if track_id:
            seen_track_ids.add(track_id)

        raw_track = row.get("track") if isinstance(row, dict) else None
        if not isinstance(raw_track, dict):
            raw_track = row if isinstance(row, dict) else {}

        album = raw_track.get("album") if isinstance(raw_track.get("album"), dict) else {}
        album_id = album.get("id")
        if not album_id:
            album_uri = album.get("uri") if isinstance(album.get("uri"), str) else ""
            if album_uri.startswith("spotify:album:"):
                album_id = album_uri.rsplit(":", 1)[-1]

        if isinstance(album_id, str) and album_id and album_id not in seen_album_ids:
            seen_album_ids.add(album_id)
            album_ids.append(album_id)

    expanded_rows: list[dict[str, Any]] = []
    for album_id in album_ids[:12]:
        album_tracks = await _fetch_spotify_album_tracks_from_html(session, album_id)
        for raw_track in album_tracks:
            track_id = raw_track.get("id") if isinstance(raw_track, dict) else None
            if not isinstance(track_id, str) or not track_id or track_id in seen_track_ids:
                continue
            seen_track_ids.add(track_id)
            expanded_rows.append({"track": raw_track})
            if len(base_rows) + len(expanded_rows) >= target_count:
                return expanded_rows

    return expanded_rows


async def _fetch_spotify_album_as_tidal(ctx: commands.Context, album_id: str | None) -> tuple[str, list[dict[str, Any]]]:
    tracks: list[dict[str, Any]] = []
    if not album_id:
        return "", tracks

    async with aiohttp.ClientSession() as session:
        status, items, data = await _spotify_paged_items(session, f"/albums/{album_id}", items_key="tracks")
        if status == 401:
            await ctx.send("❌ Spotify API credentials missing or invalid. Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET.", silent=True)
            return "", tracks
        if status != 200 or not data:
            await ctx.send(f"❌ Spotify album lookup failed (HTTP {status})", silent=True)
            return "", tracks

        album_name = data.get("name", "Unknown Album")
        for raw in items:
            spotify_track = _spotify_track_from_payload(raw)
            if not spotify_track:
                continue
            matched = await resolve_spotify_track_to_tidal(session, spotify_track)
            if matched:
                tracks.append(matched)

        if not tracks:
            await ctx.send("❌ No Spotify album tracks could be matched on Tidal.", silent=True)
            return "", tracks
        return f"💿 **{len(tracks)}** matched tracks from Spotify album **{album_name}**", tracks


async def _fetch_spotify_playlist_as_tidal(ctx: commands.Context, playlist_id: str | None) -> tuple[str, list[dict[str, Any]]]:
    tracks: list[dict[str, Any]] = []
    if not playlist_id:
        return "", tracks

    async with aiohttp.ClientSession() as session:
        status, playlist_name, items, used_spotify_autocomplete = await _fetch_spotify_playlist_tracks(session, playlist_id)
        if status == 401:
            await ctx.send("❌ Spotify API credentials missing or invalid. Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET.", silent=True)
            return "", tracks
        if status != 200 or not items:
            await ctx.send(f"❌ Spotify playlist tracks fetch failed (HTTP {status})", silent=True)
            return "", tracks

        matched_count = 0
        unmatched_count = 0
        for row in items:
            raw_track = row.get("track") if isinstance(row, dict) else None
            spotify_track = _spotify_track_from_payload(raw_track or {})
            if not spotify_track:
                unmatched_count += 1
                continue
            matched = await resolve_spotify_track_to_tidal(session, spotify_track)
            if matched:
                tracks.append(matched)
                matched_count += 1
            else:
                unmatched_count += 1

        if not tracks:
            await ctx.send("❌ No Spotify playlist tracks could be matched on Tidal.", silent=True)
            return "", tracks
        summary = f"📋 **{matched_count}** matched tracks from Spotify playlist **{playlist_name}**"
        if unmatched_count:
            summary += f" ({unmatched_count} skipped)"
        if used_spotify_autocomplete:
            summary += " | ⚠️ Spotify autocomplete fallback was used to expand this playlist."
        return summary, tracks


async def _enqueue_spotify_playlist_progressive(
    ctx: commands.Context,
    playlist_id: str | None,
    is_add: bool = False,
) -> None:
    """Resolve a Spotify playlist progressively so playback can start quickly."""
    if not playlist_id:
        return

    gid = ctx.guild.id
    vc = ctx.voice_client
    q = get_queue(gid)

    matched_count = 0
    unmatched_count = 0
    progress_announced = False
    prepared_play_mode = False
    previous_current_track: dict[str, Any] | None = None
    previous_queue = deque()

    async def _consume_rows(rows: list[dict[str, Any]]) -> None:
        nonlocal matched_count, unmatched_count, progress_announced, prepared_play_mode
        nonlocal previous_current_track, previous_queue

        for row in rows:
            raw_track = row.get("track") if isinstance(row, dict) else None
            spotify_track = _spotify_track_from_payload(raw_track or {})
            if not spotify_track:
                unmatched_count += 1
                continue

            matched = await resolve_spotify_track_to_tidal(session, spotify_track)
            if not matched:
                unmatched_count += 1
                continue

            matched["requester_id"] = ctx.author.id

            if not is_add and not prepared_play_mode:
                previous_current_track = current_tracks.pop(gid, None)
                previous_queue = deque(q)
                q.clear()
                prepared_play_mode = True

            q.append(matched)
            matched_count += 1

            if not progress_announced:
                if not is_add:
                    if vc and (vc.is_playing() or vc.is_paused()):
                        vc.stop()
                    else:
                        await play_next(ctx)
                else:
                    if not (vc and (vc.is_playing() or vc.is_paused())):
                        await play_next(ctx)

                await ctx.send("📥 Parsing Spotify playlist and adding tracks one by one...", silent=True)
                progress_announced = True

    async with aiohttp.ClientSession() as session:
        status, playlist_name, items, used_spotify_autocomplete = await _fetch_spotify_playlist_tracks(session, playlist_id, on_rows=_consume_rows)
        if status == 401:
            await ctx.send("❌ Spotify API credentials missing or invalid. Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET.", silent=True)
            return
        if status != 200 or not items:
            await ctx.send(f"❌ Spotify playlist tracks fetch failed (HTTP {status})", silent=True)
            return

        if not is_add and prepared_play_mode:
            if previous_current_track:
                q.append(previous_current_track)
            if previous_queue:
                q.extend(previous_queue)

        if matched_count == 0:
            await ctx.send("❌ No Spotify playlist tracks could be matched on Tidal.", silent=True)
            return

        parsed_rows = matched_count + unmatched_count
        summary = f"✅ Added **{matched_count}** matched tracks from Spotify playlist **{playlist_name}**"
        summary += f" (parsed {parsed_rows} Spotify rows)"
        if unmatched_count:
            summary += f" ({unmatched_count} skipped)"
        if used_spotify_autocomplete:
            summary += " | ⚠️ Spotify autocomplete fallback was used to expand this playlist."
        if len(items) >= 1000:
            summary += " (capped at first 1000 Spotify tracks)"
        await ctx.send(summary, silent=True)


async def _enqueue_spotify_album_progressive(
    ctx: commands.Context,
    album_id: str | None,
    is_add: bool = False,
) -> None:
    """Resolve a Spotify album progressively so playback can start quickly."""
    if not album_id:
        return

    gid = ctx.guild.id
    vc = ctx.voice_client
    q = get_queue(gid)

    matched_count = 0
    unmatched_count = 0
    progress_announced = False
    prepared_play_mode = False
    previous_current_track: dict[str, Any] | None = None
    previous_queue = deque()

    async with aiohttp.ClientSession() as session:
        status, items, data = await _spotify_paged_items(
            session,
            f"/albums/{album_id}",
            items_key="tracks",
            params={"limit": 50, "offset": 0},
            max_items=1000,
        )
        if status == 401:
            await ctx.send("❌ Spotify API credentials missing or invalid. Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET.", silent=True)
            return
        if status != 200 or not data:
            await ctx.send(f"❌ Spotify album lookup failed (HTTP {status})", silent=True)
            return

        album_name = data.get("name", "Unknown Album")

        for raw_track in items:
            spotify_track = _spotify_track_from_payload(raw_track or {})
            if not spotify_track:
                unmatched_count += 1
                continue

            matched = await resolve_spotify_track_to_tidal(session, spotify_track)
            if not matched:
                unmatched_count += 1
                continue

            matched["requester_id"] = ctx.author.id

            if not is_add and not prepared_play_mode:
                previous_current_track = current_tracks.pop(gid, None)
                previous_queue = deque(q)
                q.clear()
                prepared_play_mode = True

            q.append(matched)
            matched_count += 1

            if not progress_announced:
                if not is_add:
                    if vc and (vc.is_playing() or vc.is_paused()):
                        vc.stop()
                    else:
                        await play_next(ctx)
                else:
                    if not (vc and (vc.is_playing() or vc.is_paused())):
                        await play_next(ctx)

                await ctx.send(f"📥 Parsing Spotify album **{album_name}** and adding tracks one by one...", silent=True)
                progress_announced = True

        if not is_add and prepared_play_mode:
            if previous_current_track:
                q.append(previous_current_track)
            if previous_queue:
                q.extend(previous_queue)

        if matched_count == 0:
            await ctx.send("❌ No Spotify album tracks could be matched on Tidal.", silent=True)
            return

        parsed_rows = matched_count + unmatched_count
        summary = f"✅ Added **{matched_count}** matched tracks from Spotify album **{album_name}**"
        summary += f" (parsed {parsed_rows} Spotify rows)"
        if unmatched_count:
            summary += f" ({unmatched_count} skipped)"
        if len(items) >= 1000:
            summary += " (capped at first 1000 Spotify tracks)"
        await ctx.send(summary, silent=True)


async def _enqueue_spotify_artist_progressive(
    ctx: commands.Context,
    artist_id: str | None,
    is_add: bool = False,
) -> None:
    """Resolve Spotify artist tracks progressively so playback can start quickly."""
    if not artist_id:
        return

    gid = ctx.guild.id
    vc = ctx.voice_client
    q = get_queue(gid)

    matched_count = 0
    unmatched_count = 0
    progress_announced = False
    prepared_play_mode = False
    previous_current_track: dict[str, Any] | None = None
    previous_queue = deque()

    async with aiohttp.ClientSession() as session:
        max_tracks = 25
        source_label = "Spotify top tracks"
        artist_name = "Unknown Artist"

        status, data = await spotify_api_get(session, f"/artists/{artist_id}")
        if status == 401:
            await ctx.send("❌ Spotify API credentials missing or invalid. Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET.", silent=True)
            return
        if status == 200 and data:
            artist_name = data.get("name", artist_name)

        raw_spotify_tracks: list[dict[str, Any]] = []
        top_status, top_data = await spotify_api_get(session, f"/artists/{artist_id}/top-tracks", {"market": "US"})
        if top_status == 200 and top_data:
            raw_spotify_tracks.extend(top_data.get("tracks", [])[:max_tracks])

        user_top_status = 0
        if not raw_spotify_tracks and _spotify_user_access_token_value():
            user_top_status, user_top_data = await _spotify_get_json_by_full_url_with_user_token(
                session,
                f"{SPOTIFY_WEB_API}/artists/{artist_id}/top-tracks?market=from_token",
            )
            if user_top_status == 200 and isinstance(user_top_data, dict):
                raw_spotify_tracks.extend((user_top_data.get("tracks") or [])[:max_tracks])

        web_top_status = 0
        if not raw_spotify_tracks:
            web_top_status, web_top_data = await _spotify_get_json_by_full_url_with_web_token(
                session,
                f"{SPOTIFY_WEB_API}/artists/{artist_id}/top-tracks?market=from_token",
            )
            if web_top_status == 200 and isinstance(web_top_data, dict):
                raw_spotify_tracks.extend((web_top_data.get("tracks") or [])[:max_tracks])

        if not raw_spotify_tracks:
            artist_name, this_is_name, this_is_playlist_id, artist_page_tracks, artist_page_album_ids = await _fetch_spotify_artist_page_data(
                session,
                artist_id,
                artist_name_hint=artist_name,
            )
            if this_is_playlist_id:
                playlist_status, _, playlist_rows, _ = await _fetch_spotify_playlist_tracks(session, this_is_playlist_id)
                if playlist_status == 200 and playlist_rows:
                    source_label = f"playlist **{this_is_name or 'This Is'}**"
                    for row in playlist_rows:
                        raw_track = row.get("track") if isinstance(row, dict) else None
                        if isinstance(raw_track, dict):
                            raw_spotify_tracks.append(raw_track)
                        if len(raw_spotify_tracks) >= max_tracks:
                            break
            if not raw_spotify_tracks and artist_page_tracks:
                source_label = "artist page tracks"
                raw_spotify_tracks.extend(artist_page_tracks[:max_tracks])

            if len(raw_spotify_tracks) < max_tracks and artist_page_album_ids:
                source_label = "artist page albums"
                seen_ids = {track.get("id") for track in raw_spotify_tracks if isinstance(track, dict)}
                for album_id in artist_page_album_ids[:10]:
                    album_tracks = await _fetch_spotify_album_tracks_from_html(session, album_id)
                    for album_track in album_tracks:
                        track_id = album_track.get("id") if isinstance(album_track, dict) else None
                        if not track_id or track_id in seen_ids:
                            continue
                        seen_ids.add(track_id)
                        raw_spotify_tracks.append(album_track)
                        if len(raw_spotify_tracks) >= max_tracks:
                            break
                    if len(raw_spotify_tracks) >= max_tracks:
                        break

        if not raw_spotify_tracks:
            logger.warning(
                "Spotify artist lookup exhausted all sources for %s (artist=%s, top=%s, user_top=%s, web_top=%s)",
                artist_id,
                status,
                top_status,
                user_top_status,
                web_top_status,
            )
            await ctx.send("❌ Could not fetch Spotify artist tracks from available sources right now.", silent=True)
            return

        for raw_track in raw_spotify_tracks[:max_tracks]:
            spotify_track = _spotify_track_from_payload(raw_track or {})
            if not spotify_track:
                unmatched_count += 1
                continue

            matched = await resolve_spotify_track_to_tidal(session, spotify_track)
            if not matched:
                unmatched_count += 1
                continue

            matched["requester_id"] = ctx.author.id

            if not is_add and not prepared_play_mode:
                previous_current_track = current_tracks.pop(gid, None)
                previous_queue = deque(q)
                q.clear()
                prepared_play_mode = True

            q.append(matched)
            matched_count += 1

            if not progress_announced:
                if not is_add:
                    if vc and (vc.is_playing() or vc.is_paused()):
                        vc.stop()
                    else:
                        await play_next(ctx)
                else:
                    if not (vc and (vc.is_playing() or vc.is_paused())):
                        await play_next(ctx)

                await ctx.send(f"📥 Parsing Spotify artist **{artist_name}** and adding tracks one by one...", silent=True)
                progress_announced = True

        if not is_add and prepared_play_mode:
            if previous_current_track:
                q.append(previous_current_track)
            if previous_queue:
                q.extend(previous_queue)

        if matched_count == 0:
            await ctx.send("❌ No Spotify artist tracks could be matched on Tidal.", silent=True)
            return

        parsed_rows = matched_count + unmatched_count
        summary = f"✅ Added **{matched_count}** matched tracks from Spotify artist **{artist_name}**"
        if source_label != "Spotify top tracks":
            summary += f" via {source_label}"
            summary += " | ⚠️ Spotify autocomplete fallback was used."
        summary += f" (parsed {parsed_rows} Spotify rows)"
        if unmatched_count:
            summary += f" ({unmatched_count} skipped)"
        await ctx.send(summary, silent=True)


async def _fetch_spotify_artist_page_data(
    session: aiohttp.ClientSession,
    artist_id: str,
    artist_name_hint: str | None = None,
) -> tuple[str | None, str | None, str | None, list[dict[str, Any]], list[str]]:
    """Extract artist-page metadata including This Is playlist and fallback track payloads."""

    def _walk_json(obj: Any):
        if isinstance(obj, dict):
            yield obj
            for value in obj.values():
                yield from _walk_json(value)
        elif isinstance(obj, list):
            for item in obj:
                yield from _walk_json(item)

    artist_name = artist_name_hint
    playlist_name: str | None = None
    playlist_id: str | None = None
    artist_tracks: list[dict[str, Any]] = []
    seen_track_ids: set[str] = set()
    artist_album_ids: list[str] = []
    seen_album_ids: set[str] = set()

    try:
        async with session.get(f"https://open.spotify.com/artist/{artist_id}", headers={"User-Agent": "Mozilla/5.0"}) as resp:
            if resp.status != 200:
                return artist_name, playlist_name, playlist_id, artist_tracks, artist_album_ids
            html = await resp.text()
    except Exception:
        return artist_name, playlist_name, playlist_id, artist_tracks, artist_album_ids

    match = re.search(r'<script[^>]*id="initialState"[^>]*>(.*?)</script>', html, re.IGNORECASE)
    if not match:
        return artist_name, playlist_name, playlist_id, artist_tracks, artist_album_ids

    try:
        decoded = base64.b64decode(match.group(1).strip()).decode("utf-8")
        data = json.loads(decoded)
    except Exception:
        return artist_name, playlist_name, playlist_id, artist_tracks, artist_album_ids

    items_block = ((data.get("entities") or {}).get("items") or {}) if isinstance(data, dict) else {}
    artist_entity = items_block.get(f"spotify:artist:{artist_id}") if isinstance(items_block, dict) else None
    if isinstance(artist_entity, dict):
        artist_name = artist_entity.get("name", artist_name)

    norm_artist = _normalize_text(artist_name or "")
    best_score = -1

    for node in _walk_json(data):
        uri = node.get("uri")
        name = node.get("name")

        if isinstance(uri, str) and uri.startswith("spotify:track:") and isinstance(name, str) and name:
            track_id = uri.rsplit(":", 1)[-1]
            if track_id and track_id not in seen_track_ids:
                seen_track_ids.add(track_id)
                artists_block = node.get("artists") if isinstance(node.get("artists"), dict) else {}
                artists_items = artists_block.get("items") if isinstance(artists_block.get("items"), list) else []
                parsed_artists: list[dict[str, str]] = []
                for artist_item in artists_items:
                    if not isinstance(artist_item, dict):
                        continue
                    profile = artist_item.get("profile") if isinstance(artist_item.get("profile"), dict) else {}
                    parsed_name = profile.get("name") or artist_item.get("name")
                    if isinstance(parsed_name, str) and parsed_name:
                        parsed_artists.append({"name": parsed_name})
                if not parsed_artists and artist_name:
                    parsed_artists.append({"name": artist_name})

                track_payload: dict[str, Any] = {
                    "id": track_id,
                    "name": name,
                    "artists": parsed_artists,
                }

                duration = node.get("duration") if isinstance(node.get("duration"), dict) else {}
                total_ms = duration.get("totalMilliseconds")
                if isinstance(total_ms, int) and total_ms > 0:
                    track_payload["duration_ms"] = total_ms

                artist_tracks.append(track_payload)

        if isinstance(uri, str) and uri.startswith("spotify:album:"):
            album_id = uri.rsplit(":", 1)[-1]
            if album_id and album_id not in seen_album_ids:
                seen_album_ids.add(album_id)
                artist_album_ids.append(album_id)

        if not isinstance(uri, str) or not uri.startswith("spotify:playlist:"):
            continue
        if not isinstance(name, str) or not name.lower().startswith("this is "):
            continue

        candidate_id = uri.rsplit(":", 1)[-1]
        candidate_artist = _normalize_text(re.sub(r"^this\s+is\s+", "", name, flags=re.IGNORECASE))

        score = 10
        if norm_artist and candidate_artist == norm_artist:
            score += 100
        elif norm_artist and (norm_artist in candidate_artist or candidate_artist in norm_artist):
            score += 60

        owner_name = None
        owner_v2 = node.get("ownerV2")
        if isinstance(owner_v2, dict):
            owner_data = owner_v2.get("data") if isinstance(owner_v2.get("data"), dict) else {}
            owner_profile = owner_data.get("profile") if isinstance(owner_data.get("profile"), dict) else {}
            owner_name = owner_data.get("name") or owner_profile.get("name")
        if isinstance(owner_name, str) and "spotify" in owner_name.lower():
            score += 15

        if score > best_score:
            best_score = score
            playlist_name = name
            playlist_id = candidate_id

    return artist_name, playlist_name, playlist_id, artist_tracks, artist_album_ids


async def _fetch_spotify_album_tracks_from_html(
    session: aiohttp.ClientSession,
    album_id: str,
) -> list[dict[str, Any]]:
    """Best-effort extraction of track payloads from a public Spotify album page."""

    def _walk_json(obj: Any):
        if isinstance(obj, dict):
            yield obj
            for value in obj.values():
                yield from _walk_json(value)
        elif isinstance(obj, list):
            for item in obj:
                yield from _walk_json(item)

    tracks: list[dict[str, Any]] = []
    seen_track_ids: set[str] = set()

    try:
        async with session.get(f"https://open.spotify.com/album/{album_id}", headers={"User-Agent": "Mozilla/5.0"}) as resp:
            if resp.status != 200:
                return tracks
            html = await resp.text()
    except Exception:
        return tracks

    match = re.search(r'<script[^>]*id="initialState"[^>]*>(.*?)</script>', html, re.IGNORECASE)
    if not match:
        return tracks

    try:
        decoded = base64.b64decode(match.group(1).strip()).decode("utf-8")
        data = json.loads(decoded)
    except Exception:
        return tracks

    for node in _walk_json(data):
        uri = node.get("uri")
        name = node.get("name")
        if not isinstance(uri, str) or not uri.startswith("spotify:track:"):
            continue
        if not isinstance(name, str) or not name:
            continue

        track_id = uri.rsplit(":", 1)[-1]
        if not track_id or track_id in seen_track_ids:
            continue
        seen_track_ids.add(track_id)

        artists_block = node.get("artists") if isinstance(node.get("artists"), dict) else {}
        artists_items = artists_block.get("items") if isinstance(artists_block.get("items"), list) else []
        parsed_artists: list[dict[str, str]] = []
        for artist_item in artists_items:
            if not isinstance(artist_item, dict):
                continue
            profile = artist_item.get("profile") if isinstance(artist_item.get("profile"), dict) else {}
            parsed_name = profile.get("name") or artist_item.get("name")
            if isinstance(parsed_name, str) and parsed_name:
                parsed_artists.append({"name": parsed_name})

        track_payload: dict[str, Any] = {
            "id": track_id,
            "name": name,
            "artists": parsed_artists,
        }

        duration = node.get("duration") if isinstance(node.get("duration"), dict) else {}
        total_ms = duration.get("totalMilliseconds")
        if isinstance(total_ms, int) and total_ms > 0:
            track_payload["duration_ms"] = total_ms

        tracks.append(track_payload)

    return tracks


async def _fetch_spotify_artist_this_is_playlist(
    session: aiohttp.ClientSession,
    artist_id: str,
    artist_name_hint: str | None = None,
) -> tuple[str | None, str | None, str | None]:
    """Best-effort lookup of the artist's "This Is ..." playlist from the artist page payload."""
    artist_name, playlist_name, playlist_id, _, _ = await _fetch_spotify_artist_page_data(
        session,
        artist_id,
        artist_name_hint,
    )
    return artist_name, playlist_name, playlist_id


async def _fetch_spotify_artist_top_tracks_as_tidal(ctx: commands.Context, artist_id: str | None) -> tuple[str, list[dict[str, Any]]]:
    tracks: list[dict[str, Any]] = []
    if not artist_id:
        return "", tracks

    async with aiohttp.ClientSession() as session:
        max_tracks = 25
        source_label = "Spotify top tracks"
        artist_name = "Unknown Artist"

        status, data = await spotify_api_get(session, f"/artists/{artist_id}")
        if status == 401:
            await ctx.send("❌ Spotify API credentials missing or invalid. Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET.", silent=True)
            return "", tracks

        if status == 200 and data:
            artist_name = data.get("name", artist_name)

        raw_spotify_tracks: list[dict[str, Any]] = []
        top_status, top_data = await spotify_api_get(session, f"/artists/{artist_id}/top-tracks", {"market": "US"})
        if top_status == 200 and top_data:
            raw_spotify_tracks.extend(top_data.get("tracks", [])[:max_tracks])

        user_top_status = 0
        if not raw_spotify_tracks and _spotify_user_access_token_value():
            user_top_status, user_top_data = await _spotify_get_json_by_full_url_with_user_token(
                session,
                f"{SPOTIFY_WEB_API}/artists/{artist_id}/top-tracks?market=from_token",
            )
            if user_top_status == 200 and isinstance(user_top_data, dict):
                raw_spotify_tracks.extend((user_top_data.get("tracks") or [])[:max_tracks])

        web_top_status = 0
        if not raw_spotify_tracks:
            web_top_status, web_top_data = await _spotify_get_json_by_full_url_with_web_token(
                session,
                f"{SPOTIFY_WEB_API}/artists/{artist_id}/top-tracks?market=from_token",
            )
            if web_top_status == 200 and isinstance(web_top_data, dict):
                raw_spotify_tracks.extend((web_top_data.get("tracks") or [])[:max_tracks])

        if not raw_spotify_tracks:
            artist_name, this_is_name, this_is_playlist_id, artist_page_tracks, artist_page_album_ids = await _fetch_spotify_artist_page_data(
                session,
                artist_id,
                artist_name_hint=artist_name,
            )
            if this_is_playlist_id:
                playlist_status, _, playlist_rows, _ = await _fetch_spotify_playlist_tracks(session, this_is_playlist_id)
                if playlist_status == 200 and playlist_rows:
                    source_label = f"playlist **{this_is_name or 'This Is'}**"
                    for row in playlist_rows:
                        raw_track = row.get("track") if isinstance(row, dict) else None
                        if isinstance(raw_track, dict):
                            raw_spotify_tracks.append(raw_track)
                        if len(raw_spotify_tracks) >= max_tracks:
                            break
            if not raw_spotify_tracks and artist_page_tracks:
                source_label = "artist page tracks"
                raw_spotify_tracks.extend(artist_page_tracks[:max_tracks])

            if len(raw_spotify_tracks) < max_tracks and artist_page_album_ids:
                source_label = "artist page albums"
                seen_ids = {track.get("id") for track in raw_spotify_tracks if isinstance(track, dict)}
                for album_id in artist_page_album_ids[:10]:
                    album_tracks = await _fetch_spotify_album_tracks_from_html(session, album_id)
                    for album_track in album_tracks:
                        track_id = album_track.get("id") if isinstance(album_track, dict) else None
                        if not track_id or track_id in seen_ids:
                            continue
                        seen_ids.add(track_id)
                        raw_spotify_tracks.append(album_track)
                        if len(raw_spotify_tracks) >= max_tracks:
                            break
                    if len(raw_spotify_tracks) >= max_tracks:
                        break

        if not raw_spotify_tracks:
            logger.warning(
                "Spotify artist lookup exhausted all sources for %s (artist=%s, top=%s, user_top=%s, web_top=%s)",
                artist_id,
                status,
                top_status,
                user_top_status,
                web_top_status,
            )
            await ctx.send("❌ Could not fetch Spotify artist tracks from available sources right now.", silent=True)
            return "", tracks

        unmatched_count = 0
        for raw in raw_spotify_tracks[:max_tracks]:
            spotify_track = _spotify_track_from_payload(raw)
            if not spotify_track:
                unmatched_count += 1
                continue
            matched = await resolve_spotify_track_to_tidal(session, spotify_track)
            if matched:
                tracks.append(matched)
            else:
                unmatched_count += 1

        if not tracks:
            await ctx.send("❌ No Spotify artist tracks could be matched on Tidal.", silent=True)
            return "", tracks

        summary = f"🎤 **{len(tracks)}** matched tracks from Spotify artist **{artist_name}**"
        if source_label != "Spotify top tracks":
            summary += f" via {source_label}"
            summary += " | ⚠️ Spotify autocomplete fallback was used."
        if unmatched_count:
            summary += f" ({unmatched_count} skipped)"
        return summary, tracks


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
async def queue_cmd(ctx: commands.Context, page: int = 1):
    """Show the current queue in a paginated, multi-column layout."""
    gid = ctx.guild.id
    q = list(get_queue(gid))
    curr = current_tracks.get(gid)

    def _clip(text: str, max_len: int) -> str:
        if len(text) <= max_len:
            return text
        return text[: max_len - 1] + "…"

    if not q and not curr:
        await ctx.send("📭 Queue is empty.", silent=True)
        return

    total_pages = max(1, (len(q) + QUEUE_PAGE_SIZE - 1) // QUEUE_PAGE_SIZE)
    if page < 1:
        page = 1
    if page > total_pages:
        await ctx.send(f"❌ Page {page} doesn't exist. Queue has {total_pages} page(s).", silent=True)
        return

    start = (page - 1) * QUEUE_PAGE_SIZE
    end = min(start + QUEUE_PAGE_SIZE, len(q))
    page_tracks = q[start:end]

    embed = discord.Embed(title=f"🎶 Queue • Page {page}/{total_pages}", color=0x1db954)

    if curr:
        now_title = _clip(str(curr.get('title', 'Unknown')), 80)
        now_artist = _clip(str(curr.get('artist', 'Unknown')), 60)
        embed.add_field(name="▶️ Now Playing", value=f"**{now_title}** — {now_artist}", inline=False)

    for col_idx in range(QUEUE_COLUMNS):
        col_start = col_idx * QUEUE_ROWS_PER_COLUMN
        col_tracks = page_tracks[col_start:col_start + QUEUE_ROWS_PER_COLUMN]
        if not col_tracks:
            continue

        lines: list[str] = []
        for row_idx, track in enumerate(col_tracks):
            absolute_index = start + col_start + row_idx + 1
            title = _clip(str(track.get('title', 'Unknown')), 45)
            artist = _clip(str(track.get('artist', 'Unknown')), 32)
            lines.append(f"`{absolute_index}.` **{title}** — {artist}")

        field_start = start + col_start + 1
        field_end = start + col_start + len(col_tracks)
        embed.add_field(
            name=f"Tracks {field_start}-{field_end}",
            value="\n".join(lines),
            inline=True,
        )

    footer = f"Queued tracks: {len(q)}"
    if total_pages > 1:
        footer += " | Use u!queue <page>"
    embed.set_footer(text=footer)
    await ctx.send(embed=embed, silent=True)


def _tidal_cover_url(cover_id: str | None, size: int = 1280) -> str | None:
    if not cover_id:
        return None
    return f"https://resources.tidal.com/images/{cover_id.replace('-', '/')}/{size}x{size}.jpg"


def _format_duration_seconds(duration_seconds: Any) -> str:
    try:
        total_seconds = int(float(duration_seconds))
    except (TypeError, ValueError):
        return "Unknown"

    if total_seconds <= 0:
        return "Unknown"

    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes}:{seconds:02d}"


def _format_bitrate(bits_per_second: Any) -> str:
    try:
        bps = int(float(bits_per_second))
    except (TypeError, ValueError):
        return "Unknown"

    if bps <= 0:
        return "Unknown"
    if bps >= 1_000_000:
        return f"{bps / 1_000_000:.2f} Mbps"
    if bps >= 1_000:
        return f"{bps / 1_000:.0f} kbps"
    return f"{bps} bps"


def _safe_filename_stem(value: str, fallback: str = "track") -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", value).strip("._") or fallback


async def _resolve_track_context(
    ctx: commands.Context,
    query: str | None,
    non_song_error: str,
    *,
    need_stream: bool = False,
    generation_target: str = "a stream URL",
    include_album_release: bool = False,
) -> dict[str, Any] | None:
    """Resolve a song query/current track to a rich context payload."""
    gid = ctx.guild.id
    track_id: int | None = None
    track_title: str | None = None
    track_artist: str | None = None
    stream_url: str | None = None
    search_query: str | None = None

    if query is None:
        curr = current_tracks.get(gid)
        if not curr:
            await ctx.send("❌ No track currently playing.", silent=True)
            return None

        track_title = curr.get('title')
        track_artist = curr.get('artist')
        stream_url = curr.get('stream_url')

        raw_track_id = curr.get('id')
        if raw_track_id is not None:
            try:
                track_id = int(raw_track_id)
            except (TypeError, ValueError):
                track_id = None

        if track_id is None:
            search_query = f"{track_title or ''} {track_artist or ''}".strip()
            if not search_query:
                await ctx.send("❌ Could not determine the currently playing track.", silent=True)
                return None
    else:
        link_type, link_id = parse_tidal_url(query)
        if link_type in {'album', 'playlist', 'artist'}:
            await ctx.send(non_song_error, silent=True)
            return None

        if link_type == 'track':
            if not link_id:
                await ctx.send("❌ Invalid song link.", silent=True)
                return None
            try:
                track_id = int(link_id)
            except ValueError:
                await ctx.send("❌ Invalid song link.", silent=True)
                return None
        else:
            search_query = query

    async with aiohttp.ClientSession() as session:
        if search_query:
            status, data = await api_get(session, f"{TIDAL_API}/search/", {'s': search_query})
            if status == 429:
                await ctx.send("⏳ Rate limited by API, try again in a few seconds.", silent=True)
                return None
            if status != 200 or not data:
                await ctx.send(f"❌ Search failed (HTTP {status})", silent=True)
                return None

            items = data.get('data', {}).get('items', [])
            if not items:
                await ctx.send("❌ No results found.", silent=True)
                return None

            track_entry = items[0]
            track_title = track_entry.get('title', track_title or 'Unknown')
            track_artist = (track_entry.get('artist') or {}).get('name', track_artist or 'Unknown')
            raw_track_id = track_entry.get('id')
            if raw_track_id is None:
                await ctx.send("❌ The first search result is not a valid song.", silent=True)
                return None
            try:
                track_id = int(raw_track_id)
            except (TypeError, ValueError):
                await ctx.send("❌ The first search result is not a valid song.", silent=True)
                return None

        if track_id is None:
            await ctx.send("❌ Could not resolve a valid song ID.", silent=True)
            return None

        async with session.get(f"{TIDAL_API}/info/", params={'id': track_id}) as resp:
            if resp.status != 200:
                await ctx.send(f"❌ Track not found (HTTP {resp.status})", silent=True)
                return None
            payload = await resp.json()

        info = payload.get('data', {}) if isinstance(payload, dict) else {}
        track_title = info.get('title', track_title or 'Unknown')
        track_artist = (info.get('artist') or {}).get('name', track_artist or 'Unknown')
        album_data = info.get('album') if isinstance(info.get('album'), dict) else {}
        album_id = album_data.get('id')
        cover_id = album_data.get('cover')
        release_date: str | None = None

        if include_album_release and album_id is not None:
            try:
                album_id_int = int(album_id)
            except (TypeError, ValueError):
                album_id_int = None

            if album_id_int is not None:
                album_status, album_payload = await api_get(session, f"{TIDAL_API}/album/", {'id': album_id_int})
                if album_status == 200 and album_payload:
                    album_info = album_payload.get('data', {})
                    if isinstance(album_info, dict):
                        release_date = album_info.get('releaseDate') or album_info.get('streamStartDate')

        if need_stream and not stream_url:
            stream_url, http_status = await get_stream_url(session, track_id)
            if not stream_url:
                await ctx.send(f"❌ Could not generate {generation_target} (HTTP {http_status}).", silent=True)
                return None

    return {
        'id': track_id,
        'title': track_title or 'Unknown',
        'artist': track_artist or 'Unknown',
        'duration': info.get('duration'),
        'audio_quality': info.get('audioQuality'),
        'audio_modes': info.get('audioModes') or [],
        'isrc': info.get('isrc'),
        'bpm': info.get('bpm'),
        'key': info.get('key'),
        'key_scale': info.get('keyScale'),
        'explicit': info.get('explicit'),
        'copyright': info.get('copyright'),
        'track_url': info.get('url'),
        'album_id': album_id,
        'album_title': album_data.get('title'),
        'cover_id': cover_id,
        'cover_url': _tidal_cover_url(cover_id),
        'release_date': release_date,
        'stream_url': stream_url,
    }


async def _resolve_track_title_and_stream(
    ctx: commands.Context,
    query: str | None,
    non_song_error: str,
    generation_target: str,
) -> tuple[str | None, str | None]:
    """Resolve a query (or current track) into track title + stream URL."""
    track_context = await _resolve_track_context(
        ctx,
        query,
        non_song_error,
        need_stream=True,
        generation_target=generation_target,
    )
    if not track_context:
        return None, None

    return track_context.get('title'), track_context.get('stream_url')


async def _fetch_track_lyrics(track_id: int) -> tuple[str | None, str | None, str | None]:
    """Fetch lyrics and subtitle payloads for a track ID."""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{TIDAL_API}/lyrics/", params={'id': track_id}) as resp:
            if resp.status != 200:
                if resp.status == 404:
                    return None, None, "❌ Lyrics are not available for this track."
                return None, None, f"❌ Lyrics lookup failed (HTTP {resp.status})."
            payload = await resp.json()

    lyrics_block = payload.get('lyrics', {}) if isinstance(payload, dict) else {}
    lyrics_text = lyrics_block.get('lyrics')
    subtitles_text = lyrics_block.get('subtitles')

    if isinstance(lyrics_text, str):
        lyrics_text = lyrics_text.strip()
    else:
        lyrics_text = None

    if isinstance(subtitles_text, str):
        subtitles_text = subtitles_text.strip()
    else:
        subtitles_text = None

    if not lyrics_text and not subtitles_text:
        return None, None, "❌ Lyrics are not available for this track."
    return lyrics_text, subtitles_text, None


def _parse_timestamped_lyrics(subtitles_text: str | None) -> list[tuple[float, str]]:
    if not isinstance(subtitles_text, str) or not subtitles_text.strip():
        return []

    entries: list[tuple[float, str]] = []
    for raw_line in subtitles_text.splitlines():
        timestamp_matches = list(TIMESTAMPED_LYRICS_LINE_RE.finditer(raw_line))
        if not timestamp_matches:
            continue

        line_text = TIMESTAMPED_LYRICS_LINE_RE.sub("", raw_line).strip()
        if not line_text:
            continue

        for match in timestamp_matches:
            minutes = int(match.group(1))
            seconds = int(match.group(2))
            fraction_raw = match.group(3)
            fraction = int(fraction_raw) / (10 ** len(fraction_raw)) if fraction_raw else 0.0
            entries.append((minutes * 60 + seconds + fraction, line_text))

    entries.sort(key=lambda item: item[0])
    deduped: list[tuple[float, str]] = []
    for timestamp, line_text in entries:
        if deduped and abs(deduped[-1][0] - timestamp) < 1e-9 and deduped[-1][1] == line_text:
            continue
        deduped.append((timestamp, line_text))
    return deduped


def _timestamped_lyrics_index_for_elapsed(
    timed_lines: list[tuple[float, str]],
    elapsed_seconds: float,
) -> int:
    if not timed_lines:
        return 0

    index = 0
    for idx, (timestamp, _) in enumerate(timed_lines):
        if timestamp <= elapsed_seconds:
            index = idx
        else:
            break
    return index


def _flatten_timestamped_lyrics(timed_lines: list[tuple[float, str]]) -> str:
    return "\n".join(line_text for _, line_text in timed_lines)


def _build_synced_lyrics_embed(
    title: str,
    artist: str,
    timed_lines: list[tuple[float, str]],
    index: int,
) -> discord.Embed:
    prev_line = timed_lines[index - 1][1] if index > 0 else "..."
    curr_line = timed_lines[index][1]
    next_line = timed_lines[index + 1][1] if index + 1 < len(timed_lines) else "..."

    embed = discord.Embed(
        title=f"📝 Lyrics — {title}",
        description=f"⬆️ {prev_line}\n\n🎤 **{curr_line}**\n\n⬇️ {next_line}",
        color=0x1db954,
    )
    embed.set_footer(text=f"{artist} • line {index + 1}/{len(timed_lines)}")
    return embed


async def _run_synced_lyrics_updates(
    sent_message: discord.Message,
    *,
    title: str,
    artist: str,
    timed_lines: list[tuple[float, str]],
    start_index: int,
    start_elapsed: float,
    guild_id: int | None,
    track_id: int,
    enforce_current_track: bool,
) -> None:
    if not timed_lines:
        return

    anchor_monotonic = time.monotonic() - max(0.0, start_elapsed)

    for next_index in range(start_index + 1, len(timed_lines)):
        next_timestamp = timed_lines[next_index][0]
        target_time = anchor_monotonic + next_timestamp

        while True:
            if enforce_current_track and isinstance(guild_id, int) and not _is_current_track_id(guild_id, track_id):
                return

            remaining = target_time - time.monotonic()
            if remaining <= 0:
                break
            await asyncio.sleep(min(remaining, 1.0))

        if enforce_current_track and isinstance(guild_id, int) and not _is_current_track_id(guild_id, track_id):
            return

        embed = _build_synced_lyrics_embed(title, artist, timed_lines, next_index)
        try:
            await sent_message.edit(embed=embed)
        except (discord.NotFound, discord.Forbidden):
            return
        except Exception as exc:
            logger.debug("Failed to update synced lyrics message %s: %s", sent_message.id, exc)
            return


def _start_synced_lyrics_updates(task_coro: Awaitable[None]) -> None:
    task = asyncio.create_task(task_coro)

    def _handle_task_done(done_task: asyncio.Task) -> None:
        try:
            done_task.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.debug("Synced lyrics updater task failed: %s", exc)

    task.add_done_callback(_handle_task_done)


async def _probe_audio_stream(stream_url: str) -> tuple[dict[str, Any] | None, str | None]:
    """Probe stream codec and transport stats using ffprobe."""
    ffprobe_cmd = [
        "ffprobe",
        "-v",
        "error",
        "-reconnect",
        "1",
        "-reconnect_streamed",
        "1",
        "-reconnect_delay_max",
        "5",
        "-show_streams",
        "-show_format",
        "-of",
        "json",
        stream_url,
    ]

    try:
        process = await asyncio.create_subprocess_exec(
            *ffprobe_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        return None, "❌ ffprobe is not installed or not available in PATH."
    except Exception as e:
        logger.error("Failed to launch ffprobe: %s", e)
        return None, "❌ Could not start ffprobe for audio stats."

    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=45)
    except asyncio.TimeoutError:
        process.kill()
        await process.communicate()
        return None, "❌ Timed out while probing audio stats."

    if process.returncode != 0 or not stdout:
        probe_error = (stderr or b"").decode(errors="replace").strip()
        if probe_error:
            logger.warning("ffprobe audio stats failed: %s", probe_error[:400])
        return None, "❌ Could not read audio stats for this track."

    try:
        payload = json.loads(stdout.decode(errors="replace"))
    except json.JSONDecodeError:
        return None, "❌ Received invalid ffprobe output while reading audio stats."

    streams = payload.get('streams') if isinstance(payload, dict) else None
    if not isinstance(streams, list):
        return None, "❌ Could not find an audio stream in ffprobe output."

    audio_stream = next((stream for stream in streams if stream.get('codec_type') == 'audio'), None)
    if not audio_stream:
        return None, "❌ Could not find an audio stream in ffprobe output."

    format_block = payload.get('format') if isinstance(payload.get('format'), dict) else {}
    return {
        'codec': audio_stream.get('codec_name'),
        'sample_rate': audio_stream.get('sample_rate'),
        'channels': audio_stream.get('channels'),
        'channel_layout': audio_stream.get('channel_layout'),
        'stream_bitrate': audio_stream.get('bit_rate'),
        'container_bitrate': format_block.get('bit_rate'),
        'duration': format_block.get('duration') or audio_stream.get('duration'),
    }, None


async def _generate_spectrogram_image(stream_url: str, image_size: str) -> tuple[bytes | None, str | None]:
    """Render a spectrogram PNG from an audio stream using FFmpeg."""
    ffmpeg_cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-reconnect",
        "1",
        "-reconnect_at_eof",
        "1",
        "-reconnect_streamed",
        "1",
        "-reconnect_delay_max",
        "5",
        "-i",
        stream_url,
        "-t",
        "30",
        "-lavfi",
        f"showspectrumpic=s={image_size}:legend=1:orientation=vertical:scale=log",
        "-frames:v",
        "1",
        "-f",
        "image2pipe",
        "-vcodec",
        "png",
        "pipe:1",
    ]

    try:
        process = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        return None, "❌ FFmpeg is not installed or not available in PATH."
    except Exception as e:
        logger.error("Failed to launch FFmpeg for spectrogram generation: %s", e)
        return None, "❌ Could not start FFmpeg to generate spectrogram image."

    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=75)
    except asyncio.TimeoutError:
        process.kill()
        await process.communicate()
        return None, "❌ Timed out while generating spectrogram image."

    if process.returncode != 0 or not stdout:
        ffmpeg_error = (stderr or b"").decode(errors="replace").strip()
        if ffmpeg_error:
            logger.warning("FFmpeg spectrogram generation failed: %s", ffmpeg_error[:400])
        return None, "❌ Could not generate spectrogram image for this track."

    return stdout, None


async def _generate_best_spectrogram_image(
    _ctx: commands.Context,
    stream_url: str,
) -> tuple[bytes | None, str | None, str | None]:
    """Render a fixed 480p spectrogram to minimize CPU and RAM usage."""
    image_bytes, error_message = await _generate_spectrogram_image(stream_url, SPECTROGRAM_FIXED_SIZE)
    if image_bytes:
        return image_bytes, None, SPECTROGRAM_FIXED_SIZE
    return None, error_message or "❌ Could not generate spectrogram image for this track.", None


@bot.hybrid_command(name="link")
async def link_cmd(ctx: commands.Context, *, query: str | None = None):
    if not is_link_enabled(ctx.guild.id):
        return

    track_title, stream_url = await _resolve_track_title_and_stream(
        ctx,
        query,
        non_song_error="❌ Download links only work for songs, not albums, playlists, or artists.",
        generation_target="a download link",
    )
    if track_title is None or stream_url is None:
        return

    encoded_src = quote(stream_url, safe=':/?&=%')
    wrapped_url = f"https://mp.astrasphe.re?src=%22{encoded_src}%22&dl=1"
    view = discord.ui.View()
    view.add_item(discord.ui.Button(label="Download Link", url=wrapped_url))
    await ctx.send(f"🔗 **Download Link for {track_title}**", view=view, silent=True, delete_after=30)


@bot.hybrid_command(name="spectrogram", aliases=["spectogram", "spec"])
async def spectrogram_cmd(ctx: commands.Context, *, query: str | None = None):
    await ctx.defer()
    track_context = await _resolve_track_context(
        ctx,
        query,
        non_song_error="❌ Spectrogram images only work for songs, not albums, playlists, or artists.",
        need_stream=True,
        generation_target="a spectrogram image",
    )
    if not track_context:
        return

    track_id = _track_entry_id(track_context)
    track_title = track_context.get('title', 'Unknown')
    stream_url = track_context.get('stream_url')
    if not stream_url:
        await ctx.send("❌ Could not resolve a stream for this track.", silent=True)
        return

    image_bytes, error_message, selected_size = await _generate_best_spectrogram_image(ctx, stream_url)
    if not image_bytes:
        await ctx.send(error_message or "❌ Could not generate spectrogram image.", silent=True)
        return

    safe_track_name = _safe_filename_stem(track_title, fallback="track")
    image_file = discord.File(io.BytesIO(image_bytes), filename=f"{safe_track_name}_spectrogram.png")
    resolution_label = selected_size or "auto"
    sent_message = await _send_trackable_metadata_message(ctx, f"🖼️ **Spectrogram for {track_title}** ({resolution_label})", file=image_file, silent=True)
    _register_metadata_message_for_current_track(ctx, track_id, sent_message)


@bot.hybrid_command(name="trackinfo", aliases=["meta", "songinfo"])
async def trackinfo_cmd(ctx: commands.Context, *, query: str | None = None):
    """Show detailed track metadata."""
    await ctx.defer()

    track_context = await _resolve_track_context(
        ctx,
        query,
        non_song_error="❌ Track info only works for songs, not albums, playlists, or artists.",
        include_album_release=True,
    )
    if not track_context:
        return

    track_id = _track_entry_id(track_context)

    title = track_context.get('title', 'Unknown')
    artist = track_context.get('artist', 'Unknown')
    release_date = track_context.get('release_date')
    release_year = release_date[:4] if isinstance(release_date, str) and len(release_date) >= 4 else "Unknown"
    key_root = track_context.get('key')
    key_scale = track_context.get('key_scale')
    musical_key = f"{key_root} {key_scale}" if key_root and key_scale else key_root or "Unknown"

    embed = discord.Embed(
        title="🎼 Track Info",
        description=f"**{title}** — {artist}",
        color=0x1db954,
    )
    embed.add_field(name="Album", value=track_context.get('album_title') or "Unknown", inline=True)
    embed.add_field(name="Release Year", value=release_year, inline=True)
    embed.add_field(name="Duration", value=_format_duration_seconds(track_context.get('duration')), inline=True)
    embed.add_field(name="Audio Quality", value=track_context.get('audio_quality') or "Unknown", inline=True)
    embed.add_field(name="ISRC", value=track_context.get('isrc') or "Unknown", inline=True)
    embed.add_field(name="BPM / Key", value=f"{track_context.get('bpm') or 'Unknown'} / {musical_key}", inline=True)

    cover_url = track_context.get('cover_url')
    if cover_url:
        embed.set_thumbnail(url=cover_url)

    track_url = track_context.get('track_url')
    if track_url:
        embed.add_field(name="Track URL", value=track_url, inline=False)

    sent_message = await _send_trackable_metadata_message(ctx, embed=embed, silent=True)
    _register_metadata_message_for_current_track(ctx, track_id, sent_message)


@bot.hybrid_command(name="cover", aliases=["art", "albumart"])
async def cover_cmd(ctx: commands.Context, *, query: str | None = None):
    """Show album art for a song."""
    await ctx.defer()

    track_context = await _resolve_track_context(
        ctx,
        query,
        non_song_error="❌ Cover art only works for songs, not albums, playlists, or artists.",
        include_album_release=True,
    )
    if not track_context:
        return

    track_id = _track_entry_id(track_context)

    cover_url = track_context.get('cover_url')
    if not cover_url:
        await ctx.send("❌ No album art found for this track.", silent=True)
        return

    title = track_context.get('title', 'Unknown')
    artist = track_context.get('artist', 'Unknown')
    album_title = track_context.get('album_title') or "Unknown Album"

    embed = discord.Embed(
        title="🖼️ Album Art",
        description=f"**{title}** — {artist}\nAlbum: **{album_title}**",
        color=0x1db954,
    )
    embed.set_image(url=cover_url)
    release_date = track_context.get('release_date')
    if isinstance(release_date, str) and release_date:
        embed.set_footer(text=f"Release: {release_date[:10]}")

    sent_message = await _send_trackable_metadata_message(ctx, embed=embed, silent=True)
    _register_metadata_message_for_current_track(ctx, track_id, sent_message)


@bot.hybrid_command(name="lyrics")
async def lyrics_cmd(ctx: commands.Context, *, query: str | None = None):
    """Show track lyrics, with file fallback for very long lyrics."""
    await ctx.defer()

    track_context = await _resolve_track_context(
        ctx,
        query,
        non_song_error="❌ Lyrics only work for songs, not albums, playlists, or artists.",
    )
    if not track_context:
        return

    track_id = track_context.get('id')
    if not isinstance(track_id, int):
        await ctx.send("❌ Could not resolve a valid track for lyrics.", silent=True)
        return

    lyrics_text, subtitles_text, lyrics_error = await _fetch_track_lyrics(track_id)
    if not lyrics_text and not subtitles_text:
        await ctx.send(lyrics_error or "❌ Lyrics are unavailable for this track.", silent=True)
        return

    title = track_context.get('title', 'Unknown')
    artist = track_context.get('artist', 'Unknown')
    timed_lines = _parse_timestamped_lyrics(subtitles_text)
    is_current_track = bool(ctx.guild and _is_current_track_id(ctx.guild.id, track_id))

    if timed_lines:
        start_elapsed = _current_track_elapsed_seconds(ctx.guild.id, track_id) if is_current_track and ctx.guild else 0.0
        current_index = _timestamped_lyrics_index_for_elapsed(timed_lines, start_elapsed)

        embed = _build_synced_lyrics_embed(title, artist, timed_lines, current_index)
        sent_message = await _send_trackable_metadata_message(ctx, embed=embed, silent=True)
        _register_metadata_message_for_current_track(ctx, track_id, sent_message)

        if current_index < len(timed_lines) - 1:
            _start_synced_lyrics_updates(
                _run_synced_lyrics_updates(
                    sent_message,
                    title=title,
                    artist=artist,
                    timed_lines=timed_lines,
                    start_index=current_index,
                    start_elapsed=start_elapsed,
                    guild_id=ctx.guild.id if ctx.guild else None,
                    track_id=track_id,
                    enforce_current_track=is_current_track,
                )
            )
        return

    lyrics_body = lyrics_text or _flatten_timestamped_lyrics(timed_lines) or subtitles_text
    if not isinstance(lyrics_body, str) or not lyrics_body:
        await ctx.send("❌ Lyrics are unavailable for this track.", silent=True)
        return

    max_embed_chars = 3400

    if len(lyrics_body) <= max_embed_chars:
        embed = discord.Embed(
            title=f"📝 Lyrics — {title}",
            description=lyrics_body,
            color=0x1db954,
        )
        embed.set_footer(text=artist)
        sent_message = await _send_trackable_metadata_message(ctx, embed=embed, silent=True)
        _register_metadata_message_for_current_track(ctx, track_id, sent_message)
        return

    preview = lyrics_body[:max_embed_chars].rstrip() + "\n\n... (truncated; full lyrics attached)"
    safe_name = _safe_filename_stem(f"{artist}_{title}", fallback="lyrics")
    lyrics_file = discord.File(io.BytesIO(lyrics_body.encode("utf-8")), filename=f"{safe_name}_lyrics.txt")

    embed = discord.Embed(
        title=f"📝 Lyrics — {title}",
        description=preview,
        color=0x1db954,
    )
    embed.set_footer(text=artist)
    sent_message = await _send_trackable_metadata_message(ctx, embed=embed, file=lyrics_file, silent=True)
    _register_metadata_message_for_current_track(ctx, track_id, sent_message)


@bot.hybrid_command(name="audiostats", aliases=["stats"])
async def audiostats_cmd(ctx: commands.Context, *, query: str | None = None):
    """Probe codec and stream transport metadata."""
    await ctx.defer()

    track_context = await _resolve_track_context(
        ctx,
        query,
        non_song_error="❌ Audio stats only work for songs, not albums, playlists, or artists.",
        need_stream=True,
        generation_target="audio stats",
    )
    if not track_context:
        return

    track_id = _track_entry_id(track_context)

    stream_url = track_context.get('stream_url')
    if not stream_url:
        await ctx.send("❌ Could not resolve a stream for this track.", silent=True)
        return

    stats, probe_error = await _probe_audio_stream(stream_url)
    if not stats:
        await ctx.send(probe_error or "❌ Could not read audio stats for this track.", silent=True)
        return

    title = track_context.get('title', 'Unknown')
    artist = track_context.get('artist', 'Unknown')
    sample_rate = stats.get('sample_rate')
    try:
        sample_rate_hz = int(sample_rate)
        sample_rate_label = f"{sample_rate_hz / 1000:.1f} kHz"
    except (TypeError, ValueError):
        sample_rate_label = "Unknown"

    channels = stats.get('channels')
    channel_layout = stats.get('channel_layout')
    if channels and channel_layout:
        channel_label = f"{channels} ({channel_layout})"
    elif channels:
        channel_label = str(channels)
    else:
        channel_label = "Unknown"

    duration_value = stats.get('duration')
    duration_label = _format_duration_seconds(duration_value)

    embed = discord.Embed(
        title="📊 Audio Stats",
        description=f"**{title}** — {artist}",
        color=0x1db954,
    )
    embed.add_field(name="Codec", value=stats.get('codec') or "Unknown", inline=True)
    embed.add_field(name="Sample Rate", value=sample_rate_label, inline=True)
    embed.add_field(name="Channels", value=channel_label, inline=True)
    embed.add_field(name="Stream Bitrate", value=_format_bitrate(stats.get('stream_bitrate')), inline=True)
    embed.add_field(name="Container Bitrate", value=_format_bitrate(stats.get('container_bitrate')), inline=True)
    embed.add_field(name="Duration", value=duration_label, inline=True)
    embed.add_field(name="Tidal Quality", value=track_context.get('audio_quality') or "Unknown", inline=True)
    audio_modes = track_context.get('audio_modes')
    if isinstance(audio_modes, list) and audio_modes:
        embed.add_field(name="Audio Modes", value=", ".join(str(mode) for mode in audio_modes), inline=True)

    sent_message = await _send_trackable_metadata_message(ctx, embed=embed, silent=True)
    _register_metadata_message_for_current_track(ctx, track_id, sent_message)


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
    finished_track = current_tracks.pop(gid, None)
    finished_track_id = _track_entry_id(finished_track)
    loop_modes.pop(gid, None)
    if finished_track_id is not None:
        await _delete_metadata_messages_for_track(gid, finished_track_id)
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
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_TOKEN not set. Copy .env.example to .env and fill it in.")
    bot.run(token)
