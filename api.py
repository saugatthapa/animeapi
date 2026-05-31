import asyncio, base64, json, gzip, httpx, os, re, time
import hashlib
from copy import deepcopy
from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Any, Callable
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
from curl_cffi import requests as curl_requests
from curl_cffi.requests.exceptions import Timeout as CurlTimeout, RequestException
from urllib.parse import parse_qs, quote, urljoin, urlparse
import functools, random
from providers.anizone_provider import AnizoneProvider, encode_anizone_url, normalize_anizone_url
from providers.resolver import PRIMARY_STREAM_PROVIDER, STREAM_PROVIDER_ORDER, ProviderResolver


def _safe_log_value(value: str) -> str:
    return str(value or "").encode("ascii", errors="backslashreplace").decode("ascii")

load_dotenv()

app = FastAPI(title="Miruro API", version="2.8")

# ─── Async Redis Cache ───────────────────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL")
_redis_pool = None

async def _get_redis():
    global _redis_pool
    if _redis_pool is None:
        if not REDIS_URL:
            print("[Cache] REDIS_URL not set — running without Redis")
            return None
        try:
            import redis.asyncio as aio_redis
            _redis_pool = aio_redis.from_url(
                REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=3,
                socket_timeout=3,
                retry_on_timeout=False,
            )
            await _redis_pool.ping()
            print(f"[Cache] Redis connected")
        except Exception as e:
            print(f"[Cache] Redis connection failed: {e}")
            _redis_pool = None
    return _redis_pool


def _redis_key(prefix: str, *parts: str) -> str:
    """Build a normalized cache key: prefix:part1:part2:..."""
    safe = []
    for p in parts:
        s = str(p).strip().lower().replace(" ", "_")[:200]
        safe.append(s)
    return f"{prefix}:{':'.join(safe)}"


async def aget_cache(prefix: str, *parts: str) -> Optional[Any]:
    """Async get from Redis. Returns parsed JSON or None."""
    r = await _get_redis()
    if not r:
        return None
    key = _redis_key(prefix, *parts)
    try:
        data = await r.get(key)
        if data is not None:
            return json.loads(data)
    except Exception:
        pass
    return None


async def aset_cache(prefix: str, value: Any, ttl_seconds: int, *parts: str):
    """Async set in Redis with TTL."""
    r = await _get_redis()
    if not r:
        return
    key = _redis_key(prefix, *parts)
    try:
        await r.setex(key, ttl_seconds, json.dumps(value, default=str))
    except Exception:
        pass


async def adelete_cache(prefix: str, *parts: str):
    """Async delete from Redis."""
    r = await _get_redis()
    if not r:
        return
    key = _redis_key(prefix, *parts)
    try:
        await r.delete(key)
    except Exception:
        pass


async def aget_or_set_cache(prefix: str, ttl_seconds: int, fetch_fn: Callable, *parts: str) -> dict:
    """Get from cache or fetch + store. Includes stale fallback and stampede prevention."""
    r = await _get_redis()
    key = _redis_key(prefix, *parts)
    lock_key = f"lock:{key}"
    stale_key = f"stale:{key}"

    # 1. Try live cache
    if r:
        try:
            data = await r.get(key)
            if data is not None:
                return json.loads(data)
        except Exception:
            pass

    # 2. Try stale cache (while we refresh)
    if r:
        try:
            stale = await r.get(stale_key)
            if stale is not None:
                stale_data = json.loads(stale)
        except Exception:
            stale_data = None
    else:
        stale_data = None

    # 3. Cache stampede prevention: try to acquire lock
    acquired_lock = False
    if r:
        try:
            acquired_lock = await r.setnx(lock_key, "1")
            if acquired_lock:
                await r.expire(lock_key, 15)  # lock expires after 15s
            else:
                # Another worker is scraping — wait briefly and retry cache
                for _ in range(5):
                    await asyncio.sleep(0.3)
                    try:
                        data = await r.get(key)
                        if data is not None:
                            return json.loads(data)
                    except Exception:
                        pass
                    # Also check stale
                    if stale_data is not None:
                        return {**stale_data, "cached": True, "stale": True}
        except Exception:
            pass

    # 4. Fetch fresh data
    try:
        fresh = await fetch_fn()
        if r:
            try:
                await r.setex(key, ttl_seconds, json.dumps(fresh, default=str))
                # Also store as stale backup with longer TTL
                await r.setex(stale_key, ttl_seconds * 2, json.dumps(fresh, default=str))
            except Exception:
                pass
        return fresh
    except Exception as e:
        if stale_data is not None:
            return {**stale_data, "cached": True, "stale": True}
        raise
    finally:
        if acquired_lock and r:
            try:
                await r.delete(lock_key)
            except Exception:
                pass

# --- Security Configuration ---
ALLOWED_ORIGINS = os.getenv(
    "ALLOWED_ORIGINS",
    "http://localhost:3000,https://animio.qzz.io,https://ani-vanta.vercel.app,https://anizen.saugatthapa43.workers.dev",
).split(",")
API_KEY_NAME = "x-api-key"
VALID_API_KEY = os.getenv("API_KEY")
ALLOW_API_KEY_ANY_ORIGIN = os.getenv("ALLOW_API_KEY_ANY_ORIGIN", "1").strip().lower() not in {"0", "false", "no"}

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_origin_regex=(
        "https://.*\\.vercel\\.app|http://localhost:.*|http://127\\.0\\.0\\.1:.*|"
        "https://ani-vanta\\.vercel\\.app|https://anizen\\.saugatthapa43\\.workers\\.dev|"
        "https://animio\\.qzz\\.io|https://.*\\.qzz\\.io|"
        "https?://.*"
    ),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



def _has_valid_api_key(request: Request) -> bool:
    api_key = request.headers.get(API_KEY_NAME)
    return bool(VALID_API_KEY and api_key == VALID_API_KEY)

@app.middleware("http")
async def secure_api(request: Request, call_next):
    PUBLIC_PATHS = {"/", "/docs", "/redoc", "/openapi.json"}
    if request.url.path in PUBLIC_PATHS or request.url.path.startswith("/health") or request.url.path == "/anizone/health":
        return await call_next(request)
        return await call_next(request)

    # Allow browser preflight OPTIONS requests without restrictions
    if request.method == "OPTIONS":
        return await call_next(request)

    # 1. Check API Key
    if _has_valid_api_key(request):
        return await call_next(request)

    # 2. Check Origin or Referer
    origin = request.headers.get("origin")
    referer = request.headers.get("referer")

    is_allowed = False
    for allowed in ALLOWED_ORIGINS:
        if (origin and origin.startswith(allowed)) or (referer and referer.startswith(allowed)):
            is_allowed = True
            break
            
    # Wildcard/Developer resilient checks for Vercel/localhost subdomains
    if not is_allowed:
        for val in [origin, referer]:
            if val:
                val_lower = val.lower()
                if val_lower.startswith("http://localhost:") or val_lower.startswith("http://127.0.0.1:"):
                    is_allowed = True
                    break
                if (
                    ".vercel.app" in val_lower
                    or "ani-vanta" in val_lower
                    or "anivanta" in val_lower
                    or "anizen.saugatthapa43.workers.dev" in val_lower
                    or "animio.qzz.io" in val_lower
                    or ".qzz.io" in val_lower
                ):
                    is_allowed = True
                    break
             
    if not is_allowed:
        print(f"[AUTH WARN] Blocked request | Origin: {origin} | Referer: {referer} | Allowed: {ALLOWED_ORIGINS}")
        return JSONResponse(
            status_code=403,
            content={"detail": "Access forbidden: Invalid Origin, Referer, or API Key."}
        )

    return await call_next(request)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Referer": "https://www.miruro.tv/"}
ANILIST_URL = "https://graphql.anilist.co"
JIKAN_URL = "https://api.jikan.moe/v4"
MANGADEX_URL = "https://api.mangadex.org"
MANGADEX_COVERS_URL = "https://uploads.mangadex.org/covers"
MIRURO_PIPE_URL = "https://www.miruro.tv/api/secure/pipe"
ANIZIP_URL = "https://api.ani.zip/mappings"
STREAM_PROXY_URL = os.getenv("STREAM_PROXY_URL", "http://panel.thapasir.qzz.io:16261").rstrip("/")
STREAM_PROXY_ORDER = ["animekai", "animanga", "anikuro", "lunaranime", "miruro"]
ENABLE_STREAM_PROXY = os.getenv("ENABLE_STREAM_PROXY", "0").strip().lower() in {"1", "true", "yes"}
ENABLE_SUBTITLE_FALLBACKS = os.getenv("ENABLE_SUBTITLE_FALLBACKS", "0").strip().lower() in {"1", "true", "yes"}
STREAM_PROXY_TIMEOUT_SECONDS = float(os.getenv("STREAM_PROXY_TIMEOUT_SECONDS", "2.5"))
SUBTITLE_FALLBACK_TIMEOUT_SECONDS = float(os.getenv("SUBTITLE_FALLBACK_TIMEOUT_SECONDS", "4"))
DISABLED_STREAM_PROVIDERS = {
    item.strip().lower()
    for item in os.getenv("DISABLED_STREAM_PROVIDERS", "kiwi").split(",")
    if item.strip()
}
ANIZONE_CACHE_SEARCH_TTL = int(os.getenv("ANIZONE_CACHE_SEARCH_TTL", "1800"))
ANIZONE_CACHE_MATCH_TTL = int(os.getenv("ANIZONE_CACHE_MATCH_TTL", "86400"))
ANIZONE_CACHE_EPISODES_TTL = int(os.getenv("ANIZONE_CACHE_EPISODES_TTL", "21600"))
ANIZONE_CACHE_SOURCES_TTL = int(os.getenv("ANIZONE_CACHE_SOURCES_TTL", "600"))
ANIZONE_CACHE_HEALTH_TTL = int(os.getenv("ANIZONE_CACHE_HEALTH_TTL", "60"))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MAPPINGS_PATH = os.path.join(BASE_DIR, "mappings.json")
MANGA_MAPPINGS_PATH = os.path.join(BASE_DIR, "manga_mappings.json")
# ─── Local Mappings Database ───────────────────────────────────────────────────

_LOCAL_MAPPINGS = {}
_LOCAL_MANGA_MAPPINGS = {"mal": {}, "anilist": {}, "title": {}}

def _load_local_mappings():
    """Load local MAL-to-AniList mappings from mappings.json."""
    global _LOCAL_MAPPINGS
    try:
        with open(MAPPINGS_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Remove comment field
            _LOCAL_MAPPINGS = {str(k): int(v) for k, v in data.items() if k != "_comment"}
            print(f"[Mappings] Loaded {len(_LOCAL_MAPPINGS)} local MAL-to-AniList mappings")
    except FileNotFoundError:
        print("[Mappings] mappings.json not found, starting with empty local mappings")
        _LOCAL_MAPPINGS = {}
    except Exception as e:
        print(f"[Mappings] Error loading mappings: {str(e)}")
        _LOCAL_MAPPINGS = {}

# Manga mappings are loaded at startup below.
def _load_local_manga_mappings():
    """Load local manga ID mappings for offline-friendly MangaDex resolution."""
    global _LOCAL_MANGA_MAPPINGS
    try:
        with open(MANGA_MAPPINGS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        _LOCAL_MANGA_MAPPINGS = {
            "mal": {str(k): str(v) for k, v in data.get("mal", {}).items()},
            "anilist": {str(k): str(v) for k, v in data.get("anilist", {}).items()},
            "title": {str(k).lower(): str(v) for k, v in data.get("title", {}).items()},
        }
        total = sum(len(values) for values in _LOCAL_MANGA_MAPPINGS.values())
        print(f"[Manga Mappings] Loaded {total} local MangaDex mappings")
    except FileNotFoundError:
        print("[Manga Mappings] manga_mappings.json not found, starting empty")
        _LOCAL_MANGA_MAPPINGS = {"mal": {}, "anilist": {}, "title": {}}
    except Exception as e:
        print(f"[Manga Mappings] Error loading manga_mappings.json: {str(e)}")
        _LOCAL_MANGA_MAPPINGS = {"mal": {}, "anilist": {}, "title": {}}


# Load mappings on startup
_load_local_mappings()
_load_local_manga_mappings()
_provider_resolver = ProviderResolver()
_anizone_provider = _provider_resolver.anizone

# ─── Synchronous In-Memory Cache (for internal functions) ──────────────
class _CacheEntry:
    __slots__ = ("data", "expires_at")
    def __init__(self, data, ttl_hours):
        self.data = data
        self.expires_at = datetime.now() + timedelta(hours=ttl_hours)
    def is_expired(self):
        return datetime.now() > self.expires_at

_memory_cache: dict[str, dict[str, _CacheEntry]] = {}

def _get_cache(cache_type: str, key: str):
    """Synchronous in-memory cache for internal functions."""
    bucket = _memory_cache.get(cache_type)
    if not bucket:
        return None
    entry = bucket.get(key)
    if not entry:
        return None
    if entry.is_expired():
        del bucket[key]
        return None
    return entry.data

def _set_cache(cache_type: str, key: str, data, ttl_hours: int):
    """Synchronous in-memory cache for internal functions."""
    bucket = _memory_cache.setdefault(cache_type, {})
    bucket[key] = _CacheEntry(data, ttl_hours)


# ─── Timing / Profiling ─────────────────────────────────────────────────
def _log_timing(name: str, start: float):
    elapsed = time.time() - start
    if elapsed > 1:
        print(f"[TIMING] ⚠ {name} took {elapsed:.2f}s")
    elif elapsed > 0.2:
        print(f"[TIMING] {name} took {elapsed:.2f}s")


# ─── API Response Caching Helper ───────────────────────────────────────
async def _cached_response(prefix: str, ttl_seconds: int, fetch_fn, *key_parts):
    """Wrap an async endpoint with Redis caching + stale fallback + timing.
    Falls back to in-memory cache when Redis is unavailable."""
    t0 = time.time()
    r = await _get_redis()
    key = _redis_key(prefix, *key_parts)
    lock_key = f"lock:{key}"
    stale_key = f"stale:{key}"

    # In-memory fallback when Redis is unavailable
    mem_cache_key = f"_cached:{key}"

    # 1. Check live cache
    if r:
        try:
            data = await r.get(key)
            if data is not None:
                result = json.loads(data)
                elapsed = (time.time() - t0) * 1000
                print(f"[CACHE] HIT {key} ({elapsed:.0f}ms)")
                return {**result, "cached": True, "response_time_ms": round(elapsed)}
        except Exception:
            pass
    else:
        # In-memory fallback
        cached = _get_cache("_cached_response", mem_cache_key)
        if cached is not None:
            elapsed = (time.time() - t0) * 1000
            print(f"[CACHE] MEM HIT {key} ({elapsed:.0f}ms)")
            return {**cached, "cached": True, "response_time_ms": round(elapsed)}

    # 2. Try stale fallback
    stale_data = None
    if r:
        try:
            stale_raw = await r.get(stale_key)
            if stale_raw is not None:
                stale_data = json.loads(stale_raw)
        except Exception:
            pass

    # 3. Cache stampede prevention
    acquired_lock = False
    if r:
        try:
            acquired_lock = await r.setnx(lock_key, "1")
            if acquired_lock:
                await r.expire(lock_key, 15)
            else:
                for _ in range(5):
                    await asyncio.sleep(0.3)
                    try:
                        data = await r.get(key)
                        if data is not None:
                            result = json.loads(data)
                            elapsed = (time.time() - t0) * 1000
                            return {**result, "cached": True, "response_time_ms": round(elapsed)}
                    except Exception:
                        pass
                    if stale_data is not None:
                        return {**stale_data, "cached": True, "stale": True, "response_time_ms": round((time.time() - t0) * 1000)}
        except Exception:
            pass

    # 4. Fetch fresh data
    try:
        fresh = await fetch_fn()
        elapsed = (time.time() - t0) * 1000
        result = {**fresh, "cached": False, "response_time_ms": round(elapsed)}
        if r:
            try:
                await r.setex(key, ttl_seconds, json.dumps(fresh, default=str))
                await r.setex(stale_key, ttl_seconds * 2, json.dumps(fresh, default=str))
            except Exception:
                pass
        else:
            # In-memory fallback: cache with TTL converted to hours (minimum 0.25)
            _set_cache("_cached_response", mem_cache_key, fresh, ttl_hours=max(0.25, ttl_seconds / 3600))
        print(f"[CACHE] MISS {key} ({elapsed:.0f}ms)")
        return result
    except Exception as e:
        if stale_data is not None:
            print(f"[CACHE] STALE {key} (fetch failed: {e})")
            return {**stale_data, "cached": True, "stale": True, "response_time_ms": round((time.time() - t0) * 1000)}
        raise
    finally:
        if acquired_lock and r:
            try:
                await r.delete(lock_key)
            except Exception:
                pass


def _save_local_manga_mappings():
    """Persist manga mappings learned from MangaDex, Jikan, and AniList metadata."""
    data = {
        "_comment": "Local manga mapping database. Values are MangaDex UUIDs.",
        "mal": dict(sorted(_LOCAL_MANGA_MAPPINGS.get("mal", {}).items())),
        "anilist": dict(sorted(_LOCAL_MANGA_MAPPINGS.get("anilist", {}).items())),
        "title": dict(sorted(_LOCAL_MANGA_MAPPINGS.get("title", {}).items())),
    }
    tmp_path = f"{MANGA_MAPPINGS_PATH}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            f.write("\n")
        os.replace(tmp_path, MANGA_MAPPINGS_PATH)
    except Exception as e:
        print(f"[Manga Mappings] Could not persist manga_mappings.json: {str(e)}")


def _store_manga_mapping(kind: str, source_id, mangadex_id: str, persist: bool = True) -> bool:
    if kind not in _LOCAL_MANGA_MAPPINGS or not source_id or not mangadex_id:
        return False
    key = str(source_id).lower() if kind == "title" else str(source_id)
    changed = _LOCAL_MANGA_MAPPINGS[kind].get(key) != mangadex_id
    _LOCAL_MANGA_MAPPINGS[kind][key] = mangadex_id
    _set_cache(
        "manga_resolve",
        f"{kind}:{key}",
        {"resolved": True, "mangadexId": mangadex_id, "source": f"local_{kind}_mapping"},
        ttl_hours=24,
    )
    if changed and persist:
        _save_local_manga_mappings()
    return changed


def _manga_mapping_snapshot() -> str:
    return json.dumps(_LOCAL_MANGA_MAPPINGS, sort_keys=True)


def _save_manga_mappings_if_changed(snapshot: str) -> bool:
    if snapshot != _manga_mapping_snapshot():
        _save_local_manga_mappings()
        return True
    return False


def _mapping_sort_key(item):
    key = str(item[0])
    return (0, int(key)) if key.isdigit() else (1, key)


def _save_local_mappings():
    """Persist MAL-to-AniList mappings so fallback streaming works when AniList is down."""
    data = {
        "_comment": "Local MAL-to-AniList mapping database. Add mappings as malId: anilistId pairs. These work even when AniList is down."
    }
    for mal_id, anilist_id in sorted(_LOCAL_MAPPINGS.items(), key=_mapping_sort_key):
        data[str(mal_id)] = int(anilist_id)

    tmp_path = f"{MAPPINGS_PATH}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            f.write("\n")
        os.replace(tmp_path, MAPPINGS_PATH)
    except Exception as e:
        print(f"[Mappings] Could not persist mappings.json: {str(e)}")


def _store_mal_mapping(mal_id, anilist_id, source: str = "anilist", persist: bool = True) -> bool:
    try:
        mal_id = int(mal_id)
        anilist_id = int(anilist_id)
    except (TypeError, ValueError):
        return False

    cache_key = str(mal_id)
    changed = _LOCAL_MAPPINGS.get(cache_key) != anilist_id
    _LOCAL_MAPPINGS[cache_key] = anilist_id
    _set_cache(
        "mal_to_anilist",
        cache_key,
        {
            "anilistId": anilist_id,
            "malId": mal_id,
            "source": source,
            "streamable": True,
            "resolved": True,
        },
        ttl_hours=24,
    )
    if changed and persist:
        _save_local_mappings()
    return changed


def _learn_mappings_from_anilist_payload(obj) -> bool:
    """Learn idMal -> id pairs from any AniList payload shape."""
    changed = False
    if isinstance(obj, dict):
        if obj.get("id") is not None and obj.get("idMal") is not None:
            changed = _store_mal_mapping(obj.get("idMal"), obj.get("id"), persist=False) or changed
        for value in obj.values():
            changed = _learn_mappings_from_anilist_payload(value) or changed
    elif isinstance(obj, list):
        for item in obj:
            changed = _learn_mappings_from_anilist_payload(item) or changed
    return changed


def _proxy_img(url: str) -> str:
    # Proxy removed — return original image URL
    return url


def _proxy_deep_images(obj):
    # Proxy removed — return data unchanged
    return obj


def _is_disabled_stream_provider(provider_name: str) -> bool:
    return (provider_name or "").strip().lower() in DISABLED_STREAM_PROVIDERS


def _remove_disabled_stream_providers(data: dict) -> dict:
    if not isinstance(data, dict) or not DISABLED_STREAM_PROVIDERS:
        return data

    providers = data.get("providers")
    if isinstance(providers, dict):
        for provider_name in list(providers.keys()):
            if _is_disabled_stream_provider(provider_name):
                providers.pop(provider_name, None)

    return data


def _order_stream_providers(data: dict) -> dict:
    if not isinstance(data, dict):
        return data
    providers = data.get("providers")
    if not isinstance(providers, dict):
        return data
    ordered_names = _provider_resolver.order(providers.keys())
    data["providers"] = {name: providers[name] for name in ordered_names if name in providers}
    return data


def _normalize_title_for_match(value: str) -> str:
    value = re.sub(r"\([^)]*\)", " ", value or "")
    value = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return re.sub(r"\s+", " ", value).strip()


def _anizone_title_hash(titles: list[str]) -> str:
    joined = "|".join(_normalize_title_for_match(title) for title in titles if title)
    return hashlib.sha1(joined.encode()).hexdigest()[:16]


def _decode_anizone_episode_id(value: str) -> str:
    return normalize_anizone_url(value)


def _anizone_watch_id(anilist_id: int, category: str, episode_url: str) -> str:
    return f"watch/anizone/{anilist_id}/{category}/{encode_anizone_url(episode_url)}"


def _anizone_episode_response(anilist_id: int, episodes: list[dict]) -> dict:
    items = []
    for index, episode in enumerate(episodes or [], start=1):
        number = episode.get("number") or str(index)
        try:
            response_number = int(float(number))
        except (TypeError, ValueError):
            response_number = number
        source_id = episode.get("sourceId") or episode.get("id")
        if not source_id:
            continue
        items.append(
            {
                "id": _anizone_watch_id(anilist_id, "sub", source_id),
                "provider": "anizone",
                "number": response_number,
                "title": episode.get("title") or f"Episode {number}",
                "image": None,
                "airDate": None,
                "duration": None,
                "description": None,
                "filler": False,
                "sourceId": source_id,
            }
        )
    return {"episodes": {"sub": items, "dub": items}}


def _episode_slug_prefix(provider_name: str, episode_id, episode_number) -> str:
    """Extract a stable provider slug without reusing already-wrapped watch routes."""
    if _is_animekai_provider(provider_name):
        return "animekai"

    value = str(episode_id or "").strip().strip("/")
    if not value:
        return provider_name

    if ":" in value:
        value = value.split(":", 1)[0]
    elif "/" in value:
        value = value.split("/")[-1]

    number = str(episode_number)
    value = re.sub(rf"(?:-{re.escape(number)})+$", "", value)
    return value or provider_name


def _inject_source_slugs(data: dict, anilist_id: int):
    """Transform episode IDs into simplified path-based slugs: watch/PROV/ALID/CAT/PREFIX-NUMBER"""
    data = _remove_disabled_stream_providers(data)
    providers = data.get("providers", {})
    for provider_name, provider_data in providers.items():
        if not isinstance(provider_data, dict):
            continue
        episodes = provider_data.get("episodes", {})
        if not isinstance(episodes, dict):
            # Some providers return a flat list — wrap it
            if isinstance(episodes, list):
                provider_data["episodes"] = {"sub": episodes}
                episodes = provider_data["episodes"]
            else:
                continue
        for category, ep_list in episodes.items():
            if not isinstance(ep_list, list):
                continue
            for ep in ep_list:
                if not isinstance(ep, dict):
                    continue
                if "id" in ep and "number" in ep:
                    orig_id = ep["id"]
                    if isinstance(orig_id, str) and orig_id.startswith(("watch/", "watch-by-mal/")):
                        continue
                    prefix = _episode_slug_prefix(provider_name, orig_id, ep["number"])
                    ep["id"] = f"watch/{provider_name}/{anilist_id}/{category}/{prefix}-{ep['number']}"
    return data


def _inject_mal_source_slugs(data: dict, mal_id: int):
    """Transform episode IDs into MAL-backup slugs: watch-by-mal/MALID/PROV/CAT/PREFIX-NUMBER"""
    data = _remove_disabled_stream_providers(data)
    providers = data.get("providers", {})
    for provider_name, provider_data in providers.items():
        if not isinstance(provider_data, dict):
            continue
        episodes = provider_data.get("episodes", {})
        if not isinstance(episodes, dict):
            if isinstance(episodes, list):
                provider_data["episodes"] = {"sub": episodes}
                episodes = provider_data["episodes"]
            else:
                continue
        for category, ep_list in episodes.items():
            if not isinstance(ep_list, list):
                continue
            for ep in ep_list:
                if not isinstance(ep, dict):
                    continue
                if "id" in ep and "number" in ep:
                    orig_id = ep["id"]
                    if isinstance(orig_id, str) and orig_id.startswith(("watch/", "watch-by-mal/")):
                        continue
                    prefix = _episode_slug_prefix(provider_name, orig_id, ep["number"])
                    ep["id"] = f"watch-by-mal/{mal_id}/{provider_name}/{category}/{prefix}-{ep['number']}"
    return data


def _get_local_anilist_id_for_mal(mal_id: int):
    """Return a local MAL-to-AniList mapping without calling AniList."""
    mapped = _LOCAL_MAPPINGS.get(str(mal_id))
    if mapped is None:
        return None
    try:
        return int(mapped)
    except (TypeError, ValueError):
        return None


def _get_local_mal_id_for_anilist(anilist_id: int):
    """Best-effort reverse lookup from local MAL-to-AniList mappings."""
    target = str(anilist_id)
    for mal_id, mapped in _LOCAL_MAPPINGS.items():
        if str(mapped) == target:
            try:
                return int(mal_id)
            except (TypeError, ValueError):
                return None
    return None


async def _resolve_anilist_to_mal_with_anizip(anilist_id: int):
    """Resolve AniList to MAL using ani.zip so /info/{anilist_id} can survive AniList outages."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            res = await client.get(ANIZIP_URL, params={"anilist_id": anilist_id})
            if res.status_code != 200:
                return None
            mappings = res.json().get("mappings", {})
            mal_id = mappings.get("mal_id")
            if not mal_id:
                return None
            _store_mal_mapping(mal_id, anilist_id, source="ani_zip_reverse")
            return int(mal_id)
    except Exception as e:
        print(f"[AniZip Reverse Resolve Exception]: {str(e)}")
    return None


def _mal_mapping_required_response(mal_id: Optional[int] = None, include_empty_episodes: bool = False):
    content = {
        "streamable": False,
        "needsMapping": True,
        "detail": "No MAL-to-AniList mapping found",
    }
    if mal_id is not None:
        content["malId"] = mal_id
        content["resolved"] = False
    if include_empty_episodes:
        content["providers"] = {}
        content["episodes"] = []
    return JSONResponse(
        status_code=404,
        content=content,
    )


def _has_usable_episode_data(data: dict) -> bool:
    providers = data.get("providers") if isinstance(data, dict) else None
    if not isinstance(providers, dict) or not providers:
        return False

    for provider_data in providers.values():
        if not isinstance(provider_data, dict):
            continue
        episodes = provider_data.get("episodes", {})
        if isinstance(episodes, list) and episodes:
            return True
        if isinstance(episodes, dict):
            for ep_list in episodes.values():
                if isinstance(ep_list, list) and ep_list:
                    return True
    return False


def _has_usable_sources(data: dict) -> bool:
    if not isinstance(data, dict):
        return False
    streams = data.get("streams")
    if isinstance(streams, list) and len(streams) > 0:
        return True
    for key in ("ssub", "sub", "sdub", "dub"):
        branch = data.get(key)
        if not isinstance(branch, dict):
            continue
        branch_streams = branch.get("streams")
        if isinstance(branch_streams, list) and len(branch_streams) > 0:
            return True
    return False


def _get_provider_episode_list(data: dict, provider: str, category: str):
    prov_data = data.get("providers", {}).get(provider, {}) if isinstance(data, dict) else {}
    episodes = prov_data.get("episodes", {}) if isinstance(prov_data, dict) else {}
    if isinstance(episodes, dict):
        return episodes.get(category, [])
    if isinstance(episodes, list) and category == "sub":
        return episodes
    return []


def _resolve_slug_to_episode_id(data: dict, provider: str, category: str, slug: str):
    ep_list = _get_provider_episode_list(data, provider, category)
    for ep in ep_list:
        if not isinstance(ep, dict):
            continue
        orig_id = ep.get("id", "")
        prefix = orig_id.split(":")[0] if ":" in orig_id else orig_id
        generated = f"{prefix}-{ep.get('number')}"
        if generated == slug:
            return orig_id
    return None


def _raise_pipe_lookup_error(error, fallback_detail: str):
    if isinstance(error, HTTPException):
        raise error
    raise HTTPException(status_code=502, detail=fallback_detail)


async def _fetch_pipe(path: str, query: dict, translate_ids: bool = True) -> dict:
    """Internal helper to fetch raw, decoded data from Miruro pipe."""
    payload = {
        "path": path,
        "method": "GET",
        "query": query,
        "body": None,
        "version": "0.1.0",
    }
    encoded_req = _encode_pipe_request(payload)
    async with httpx.AsyncClient(timeout=15.0) as client:
        res = await client.get(f"{MIRURO_PIPE_URL}?e={encoded_req}", headers=HEADERS)
        if res.status_code != 200:
            raise HTTPException(status_code=res.status_code, detail="Pipe request failed")
        data = _decode_pipe_response(res.text.strip())
        if translate_ids:
            _deep_translate(data)
        return data


async def _fetch_raw_episodes_by_query(query: dict) -> dict:
    return await _fetch_pipe("episodes", query)


async def _fetch_raw_episodes(anilist_id: int) -> dict:
    """Internal helper to fetch raw, decoded episode data from Miruro pipe."""
    cache_key = str(anilist_id)
    cached = _get_cache("miruro_episodes", cache_key)
    if cached is not None:
        return cached
    data = await _fetch_raw_episodes_by_query({"anilistId": anilist_id})
    _set_cache("miruro_episodes", cache_key, data, ttl_hours=1)
    return data


async def _fetch_raw_sources(episode_id: str, provider: str, category: str, anime_query: dict) -> dict:
    cache_key = json.dumps(
        {
            "episodeId": episode_id,
            "provider": provider,
            "category": category,
            "animeQuery": anime_query,
        },
        sort_keys=True,
        default=str,
    )
    cached = _get_cache("miruro_sources", cache_key)
    if cached is not None:
        return deepcopy(cached)

    enc_id = base64.urlsafe_b64encode(episode_id.encode()).decode().rstrip('=')
    query = {
        "episodeId": enc_id,
        "provider": provider,
        "category": category,
    }
    query.update(anime_query)
    data = await _fetch_pipe("sources", query, translate_ids=False)
    _set_cache("miruro_sources", cache_key, deepcopy(data), ttl_hours=0.25)
    return data


async def _try_episode_fetch(query: dict):
    t0 = time.time()
    try:
        data = await _fetch_raw_episodes_by_query(query)
        _log_timing(f"_fetch_pipe(episodes, {query.get('anilistId', '?')})", t0)
        if _has_usable_episode_data(data):
            return data, None
        return None, HTTPException(status_code=404, detail="No streaming episodes found")
    except HTTPException as exc:
        return None, exc
    except Exception as exc:
        return None, exc


async def _try_source_fetch(episode_id: str, provider: str, category: str, anime_query: dict):
    try:
        data = await _fetch_raw_sources(episode_id, provider, category, anime_query)
        if _has_usable_sources(data):
            return data, None
        return None, HTTPException(status_code=404, detail="No streaming sources found")
    except HTTPException as exc:
        return None, exc
    except Exception as exc:
        return None, exc


async def _fetch_mal_backup_episode_data(mal_id: int):
    """Resolve MAL to numeric AniList first, then use the existing pipe flow."""
    resolution = await _resolve_mal_to_anilist(mal_id)
    if not resolution:
        return {"response": _mal_mapping_required_response(mal_id, include_empty_episodes=True)}

    mapped_anilist_id = resolution["anilistId"]
    data, error = await _try_episode_fetch({"anilistId": mapped_anilist_id})
    if data is not None:
        return {
            "data": data,
            "malId": mal_id,
            "anilistId": mapped_anilist_id,
            "source": resolution.get("source", "mapping"),
        }

    return {
        "response": JSONResponse(
            status_code=404,
            content={
                "streamable": False,
                "detail": "MAL backup AniList mapping failed",
                "malId": mal_id,
                "anilistId": mapped_anilist_id,
            },
        ),
        "error": error,
    }

# ─── Shared GraphQL Fragments ────────────────────────────────────────────────

async def _fetch_stream_proxy_candidates(stream_url: str, referer: str):
    """Generate external proxy fallback URLs for a stream URL + referer pair."""
    if not ENABLE_STREAM_PROXY or not STREAM_PROXY_URL or not stream_url or not referer:
        return None

    cache_key = f"{stream_url}|{referer}"
    cached = _get_cache("stream_proxy", cache_key)
    if cached is not None:
        return cached

    try:
        async with httpx.AsyncClient(timeout=STREAM_PROXY_TIMEOUT_SECONDS) as client:
            res = await client.get(
                f"{STREAM_PROXY_URL}/proxy",
                params={"data": f"{stream_url}|{referer}"},
            )
            if res.status_code != 200:
                _set_cache("stream_proxy", cache_key, None, ttl_hours=1)
                return None
            payload = res.json()
            providers = payload.get("proxifiedSource")
            if not isinstance(providers, dict) or not providers:
                _set_cache("stream_proxy", cache_key, None, ttl_hours=1)
                return None
            if not providers.get("animekai") and providers.get("animanga"):
                providers["animekai"] = providers["animanga"]

            result = {
                "service": STREAM_PROXY_URL,
                "providers": providers,
                "recommendedOrder": [name for name in STREAM_PROXY_ORDER if providers.get(name)],
            }
            _set_cache("stream_proxy", cache_key, result, ttl_hours=12)
            return result
    except Exception as e:
        print(f"[Stream Proxy Exception]: {str(e)}")

    _set_cache("stream_proxy", cache_key, None, ttl_hours=1)
    return None


async def _attach_stream_proxy_candidates_to_streams(streams):
    if not isinstance(streams, list) or not streams:
        return

    async def attach_one(stream):
        if not isinstance(stream, dict):
            return
        stream_url = stream.get("url")
        referer = stream.get("referer")
        if not stream_url or not referer:
            return
        proxy_data = await _fetch_stream_proxy_candidates(stream_url, referer)
        if proxy_data:
            stream["proxy"] = proxy_data

    await asyncio.gather(*(attach_one(stream) for stream in streams))


def _normalize_payload_subtitles_recursive(payload):
    if not isinstance(payload, dict):
        return

    subtitles = _extract_direct_subtitles_from_payload(payload)
    if subtitles:
        payload["subtitles"] = subtitles

    for key in ("ssub", "sub", "sdub", "dub"):
        branch = payload.get(key)
        if isinstance(branch, dict):
            _normalize_payload_subtitles_recursive(branch)


async def _attach_stream_proxy_candidates(payload: dict):
    """Enrich source payloads with proxy fallback URLs and normalize subtitles for playback."""
    if not isinstance(payload, dict):
        return payload

    await _attach_stream_proxy_candidates_to_streams(payload.get("streams"))

    for key in ("ssub", "sub", "sdub", "dub"):
        branch = payload.get(key)
        if isinstance(branch, dict):
            await _attach_stream_proxy_candidates_to_streams(branch.get("streams"))

    _normalize_payload_subtitles_recursive(payload)
    if isinstance(payload, dict):
        payload["subtitles"] = _extract_subtitles_from_payload(payload)
    return payload


def _normalize_subtitle_entry(entry, fallback_index: int = 0) -> Optional[dict]:
    if isinstance(entry, str):
        url = entry.strip()
        if not url:
            return None
        return {
            "label": f"Subtitle {fallback_index}" if fallback_index else "Subtitle",
            "lang": None,
            "url": url,
            "type": "vtt" if ".vtt" in url.lower() else None,
        }

    if not isinstance(entry, dict):
        return None

    url = (
        entry.get("url")
        or entry.get("src")
        or entry.get("file")
        or entry.get("track")
        or entry.get("link")
    )
    if not url:
        return None

    label = (
        entry.get("label")
        or entry.get("name")
        or entry.get("title")
        or entry.get("language")
        or entry.get("lang")
        or f"Subtitle {fallback_index}"
    )
    lang = entry.get("lang") or entry.get("srclang") or entry.get("language")
    sub_type = (
        entry.get("type")
        or entry.get("format")
        or ("vtt" if ".vtt" in str(url).lower() else None)
        or ("srt" if ".srt" in str(url).lower() else None)
    )

    return {
        "label": label,
        "lang": lang,
        "url": url,
        "type": sub_type,
    }


def _extract_direct_subtitles_from_payload(payload: dict) -> list[dict]:
    if not isinstance(payload, dict):
        return []

    raw_candidates = []
    for key in ("subtitles", "subtitle", "tracks", "captions"):
        value = payload.get(key)
        if isinstance(value, list):
            raw_candidates.extend(value)
        elif isinstance(value, dict):
            for nested in value.values():
                if isinstance(nested, list):
                    raw_candidates.extend(nested)
                elif nested:
                    raw_candidates.append(nested)

    normalized = []
    seen = set()
    for index, entry in enumerate(raw_candidates, start=1):
        item = _normalize_subtitle_entry(entry, index)
        if not item:
            continue
        key = (item.get("url"), item.get("lang"), item.get("label"))
        if key in seen:
            continue
        seen.add(key)
        normalized.append(item)
    return normalized


def _extract_subtitles_from_payload(payload: dict) -> list[dict]:
    if not isinstance(payload, dict):
        return []

    normalized = []
    seen = set()

    def add_items(items):
        for item in items:
            key = (item.get("url"), item.get("lang"), item.get("label"))
            if key in seen:
                continue
            seen.add(key)
            normalized.append(item)

    add_items(_extract_direct_subtitles_from_payload(payload))
    for key in ("ssub", "sub", "sdub", "dub"):
        branch = payload.get(key)
        if isinstance(branch, dict):
            add_items(_extract_subtitles_from_payload(branch))

    return normalized


def _extract_animekai_subtitles_from_url(url: str) -> list[dict]:
    if not url:
        return []

    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=False)
    subtitles = []
    seen = set()

    def add_sub(sub_url: str, label: Optional[str] = None, lang: Optional[str] = None):
        if not sub_url:
            return
        normalized = _normalize_subtitle_entry(
            {
                "url": sub_url,
                "label": label or "English",
                "lang": lang or "en",
                "type": "vtt" if ".vtt" in sub_url.lower() else None,
            }
        )
        if not normalized:
            return
        key = (normalized.get("url"), normalized.get("lang"), normalized.get("label"))
        if key in seen:
            return
        seen.add(key)
        subtitles.append(normalized)

    for sub_url in params.get("sub", []):
        add_sub(sub_url)

    caption_indexes = set()
    label_indexes = set()
    file_indexes = set()
    for key in params:
        match = re.match(r"caption_(\d+)$", key)
        if match:
            caption_indexes.add(match.group(1))
        match = re.match(r"sub_(\d+)$", key)
        if match:
            label_indexes.add(match.group(1))
        match = re.match(r"c(\d+)_file$", key)
        if match:
            file_indexes.add(match.group(1))

    for idx in sorted(caption_indexes):
        urls = params.get(f"caption_{idx}", [])
        labels = params.get(f"sub_{idx}", [])
        label = labels[0] if labels else None
        for sub_url in urls:
            add_sub(sub_url, label=label)

    for idx in sorted(file_indexes):
        urls = params.get(f"c{idx}_file", [])
        labels = params.get(f"c{idx}_label", [])
        label = labels[0] if labels else None
        for sub_url in urls:
            add_sub(sub_url, label=label)

    return subtitles


def _apply_subtitle_mode_hints(payload: dict, category: str) -> dict:
    if not isinstance(payload, dict):
        return payload

    normalized = []
    seen = set()
    for index, subtitle in enumerate(payload.get("subtitles", []) if isinstance(payload.get("subtitles"), list) else [], start=1):
        item = _normalize_subtitle_entry(subtitle, index)
        if not item:
            continue
        key = (item.get("url"), item.get("lang"), item.get("label"))
        if key in seen:
            continue
        seen.add(key)
        normalized.append(item)

    payload["subtitles"] = normalized

    if category.lower() == "sub" and normalized:
        payload["subtitleMode"] = "auto"
        payload["defaultSubtitle"] = 0
    else:
        payload["subtitleMode"] = "manual"
        payload["defaultSubtitle"] = -1

    # Ensure branch structure for sub/dub routing
    branch_key = {"sub": "ssub", "dub": "sdub"}.get(category.lower())
    if branch_key:
        top_streams = payload.get("streams")
        existing_branch = payload.get(branch_key)
        if isinstance(top_streams, list) and len(top_streams) > 0:
            if not isinstance(existing_branch, dict):
                payload[branch_key] = {"streams": top_streams, "subtitles": payload.get("subtitles", [])}
            elif not isinstance(existing_branch.get("streams"), list) or len(existing_branch.get("streams", [])) == 0:
                existing_branch["streams"] = top_streams

    return payload


async def _attach_provider_subtitle_fallbacks(
    payload: dict,
    *,
    provider: str,
    category: str,
    episode_id: str,
    anilist_id: int,
    anime_query: dict,
) -> dict:
    if not isinstance(payload, dict):
        return payload

    category_key = (category or "").lower()
    if category_key not in {"sub", "dub"}:
        payload.pop("subtitleFallbackProvider", None)
        payload.pop("subtitleFallbackProviders", None)
        return _apply_subtitle_mode_hints(payload, category)

    existing = payload.get("subtitles", [])
    merged = []
    seen = set()
    for subtitle in existing if isinstance(existing, list) else []:
        item = _normalize_subtitle_entry(subtitle)
        if not item:
            continue
        key = (item.get("url"), item.get("lang"))
        if key in seen:
            continue
        seen.add(key)
        merged.append(item)

    if category_key == "sub" and merged:
        payload["subtitles"] = merged
        payload.pop("subtitleFallbackProvider", None)
        payload.pop("subtitleFallbackProviders", None)
        return _apply_subtitle_mode_hints(payload, category)

    data, _ = await _try_episode_fetch(anime_query)
    if data is None:
        payload["subtitles"] = merged
        return _apply_subtitle_mode_hints(payload, category)

    data = await _inject_animekai_provider(data, anilist_id)
    fallback_order = (
        ["animekai", "hop", "dune", "bee", "ally", "kiwi"]
        if category_key == "sub"
        else ["animekai", "bee", "hop", "ally", "kiwi", "dune"]
    )
    target_number = None

    if _is_animekai_provider(provider):
        try:
            _, parsed_episode = _animekai_parse_episode_id(episode_id)
            target_number = str(parsed_episode)
        except HTTPException:
            normalized = _normalize_animekai_watch_slug(episode_id)
            match = re.search(r"animekai-(\d+)", normalized)
            if match:
                target_number = match.group(1)
    else:
        for ep in _get_provider_episode_list(data, provider, category):
            if not isinstance(ep, dict):
                continue
            if ep.get("id") == episode_id:
                number = ep.get("number")
                target_number = str(number) if number is not None else None
                break

    if not target_number:
        match = re.search(r"(\d+)", str(episode_id))
        if match:
            target_number = match.group(1)

    fallback_providers_used = []

    for fallback_provider in fallback_order:
        fallback_episode_id = None
        if _is_animekai_provider(fallback_provider):
            if not target_number:
                continue
            anime_slug = await _animekai_lookup_slug(anilist_id)
            if not anime_slug:
                continue
            fallback_episode_id = f"animekai:{anime_slug}:{target_number}"
        else:
            for ep in _get_provider_episode_list(data, fallback_provider, category):
                if not isinstance(ep, dict):
                    continue
                number = ep.get("number")
                if number is None or str(number) != str(target_number):
                    continue
                fallback_episode_id = ep.get("id")
                break

        if not fallback_episode_id:
            continue
        if fallback_provider == provider and merged:
            # We already have current-provider subtitles; no need to refetch only for dedupe.
            continue

        try:
            if _is_animekai_provider(fallback_provider):
                animekai_fallback_category = "sub" if category.lower() == "dub" else category
                fallback_payload = await _animekai_sources_from_episode_id(fallback_episode_id, animekai_fallback_category)
            else:
                fallback_payload = await _fetch_raw_sources(
                    episode_id=fallback_episode_id,
                    provider=fallback_provider,
                    category=category,
                    anime_query=anime_query,
                )
            fallback_payload = await _attach_stream_proxy_candidates(fallback_payload)
            fallback_subtitles = fallback_payload.get("subtitles", [])
            if fallback_subtitles:
                added_any = False
                for subtitle in fallback_subtitles:
                    item = _normalize_subtitle_entry(subtitle)
                    if not item:
                        continue
                    key = (item.get("url"), item.get("lang"))
                    if key in seen:
                        continue
                    seen.add(key)
                    if category_key == "dub" and fallback_provider != provider:
                        item["label"] = f"{item.get('label') or 'Subtitle'} [Fallback: {fallback_provider}]"
                    merged.append(item)
                    added_any = True
                if added_any and fallback_provider != provider and fallback_provider not in fallback_providers_used:
                    fallback_providers_used.append(fallback_provider)
        except Exception:
            continue

    payload["subtitles"] = merged
    if category_key == "dub" and fallback_providers_used:
        payload["subtitleFallbackProvider"] = fallback_providers_used[0]
        payload["subtitleFallbackProviders"] = fallback_providers_used
    elif "subtitleFallbackProvider" in payload:
        payload.pop("subtitleFallbackProvider", None)
        payload.pop("subtitleFallbackProviders", None)

    return _apply_subtitle_mode_hints(payload, category)


async def _prepare_source_payload(
    payload: dict,
    *,
    provider: str,
    category: str,
    episode_id: str,
    anilist_id: int,
    anime_query: dict,
) -> dict:
    """Keep playback fast by making optional enrichments non-blocking by default."""
    payload = await _attach_stream_proxy_candidates(payload)

    if not ENABLE_SUBTITLE_FALLBACKS:
        return _apply_subtitle_mode_hints(payload, category)

    try:
        return await asyncio.wait_for(
            _attach_provider_subtitle_fallbacks(
                payload,
                provider=provider,
                category=category,
                episode_id=episode_id,
                anilist_id=anilist_id,
                anime_query=anime_query,
            ),
            timeout=SUBTITLE_FALLBACK_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        print(f"[SUBTITLE FALLBACK TIMEOUT] {provider}/{episode_id}")
    except Exception as exc:
        print(f"[SUBTITLE FALLBACK WARN] {provider}/{episode_id}: {exc}")
    return _apply_subtitle_mode_hints(payload, category)


def _build_lunaranime_proxy_url(url: str, referer: str) -> Optional[str]:
    """Build a browser-safe proxy URL for assets that require a fixed Referer."""
    if not url or not referer:
        return None
    from urllib.parse import quote
    return (
        "https://cluster.lunaranime.ru/api/proxy/hls/custom"
        f"?url={quote(url, safe=':/')}"
        f"&referer={quote(referer, safe=':/')}"
    )


MEDIA_LIST_FIELDS = """
    id
    idMal
    title { romaji english native }
    coverImage { large extraLarge }
    bannerImage
    format
    season
    seasonYear
    episodes
    duration
    status
    averageScore
    meanScore
    popularity
    favourites
    genres
    source
    countryOfOrigin
    isAdult
    studios(isMain: true) { nodes { name isAnimationStudio } }
    nextAiringEpisode { episode airingAt timeUntilAiring }
    startDate { year month day }
    endDate { year month day }
"""

MEDIA_FULL_FIELDS = """
    id
    idMal
    title { romaji english native }
    description(asHtml: false)
    coverImage { large extraLarge color }
    bannerImage
    format
    season
    seasonYear
    episodes
    duration
    status
    averageScore
    meanScore
    popularity
    favourites
    trending
    genres
    tags { name rank isMediaSpoiler }
    source
    countryOfOrigin
    isAdult
    hashtag
    synonyms
    siteUrl
    trailer { id site thumbnail }
    studios { nodes { id name isAnimationStudio siteUrl } }
    nextAiringEpisode { episode airingAt timeUntilAiring }
    startDate { year month day }
    endDate { year month day }
    characters(sort: [ROLE, RELEVANCE], perPage: 25) {
        edges {
            role
            node { id name { full native } image { large } }
            voiceActors(language: JAPANESE) { id name { full native } image { large } languageV2 }
        }
    }
    staff(sort: RELEVANCE, perPage: 25) {
        edges {
            role
            node { id name { full native } image { large } }
        }
    }
    relations {
        edges {
            relationType(version: 2)
            node {
                id
                title { romaji english native }
                coverImage { large }
                format
                type
                status
                episodes
                meanScore
            }
        }
    }
    recommendations(sort: RATING_DESC, perPage: 10) {
        nodes {
            rating
            mediaRecommendation {
                id
                title { romaji english native }
                coverImage { large }
                format
                episodes
                status
                meanScore
                averageScore
            }
        }
    }
    externalLinks { url site type }
    streamingEpisodes { title thumbnail url site }
    stats {
        scoreDistribution { score amount }
        statusDistribution { status amount }
    }
"""

# ─── Utility Functions ───────────────────────────────────────────────────────

def _translate_id(encoded_id: str) -> str:
    """Decode a base64-encoded episode ID back to plain text."""
    try:
        decoded = base64.urlsafe_b64decode(encoded_id + '=' * (4 - len(encoded_id) % 4)).decode()
        if ':' in decoded:
            return decoded
        return encoded_id
    except Exception:
        return encoded_id


def _deep_translate(obj):
    """Recursively walk a JSON structure and decode any base64 'id' fields."""
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == 'id' and isinstance(value, str):
                obj[key] = _translate_id(value)
            elif isinstance(value, (dict, list)):
                _deep_translate(value)
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                _deep_translate(item)


def _decode_pipe_response(encoded_str: str) -> dict:
    """Decode a base64+gzip pipe response into a plain dict."""
    try:
        encoded_str += '=' * (4 - len(encoded_str) % 4)
        compressed = base64.urlsafe_b64decode(encoded_str)
        return json.loads(gzip.decompress(compressed).decode('utf-8'))
    except Exception:
        raise ValueError("Failed to decode pipe response")


def _encode_pipe_request(payload: dict) -> str:
    """Encode a dict into the base64 format expected by the pipe endpoint."""
    return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip('=')


async def _is_anilist_available() -> bool:
    """Check if AniList API is available (cached for 5 minutes)."""
    cached = _get_cache("anilist_available", "status")
    if cached is not None:
        return cached
    
    try:
        body = {"query": "query { Page(page: 1) { pageInfo { total } } }"}
        async with httpx.AsyncClient(timeout=5.0) as client:
            res = await client.post(ANILIST_URL, json=body)
            available = res.status_code == 200
            _set_cache("anilist_available", "status", available, ttl_hours=0.08)  # 5 minutes
            return available
    except Exception:
        _set_cache("anilist_available", "status", False, ttl_hours=0.08)
        return False


def _anilist_cache_key(query: str, variables: Optional[dict] = None) -> str:
    return json.dumps(
        {
            "query": re.sub(r"\s+", " ", query.strip()),
            "variables": variables or {},
        },
        sort_keys=True,
        separators=(",", ":"),
    )


async def _anilist_query(query: str, variables: dict = None):
    """Execute an AniList GraphQL query with caching and rate-limit backoff."""
    cache_key = _anilist_cache_key(query, variables)
    cached = _get_cache("anilist_query", cache_key)
    if cached is not None:
        return cached

    body = {"query": query}
    if variables:
        body["variables"] = variables

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            last_status = None
            last_text = ""
            for attempt in range(3):
                res = await client.post(ANILIST_URL, json=body)
                last_status = res.status_code
                last_text = res.text

                if res.status_code == 200:
                    json_data = res.json()
                    if "errors" in json_data and not json_data.get("data"):
                        print("[AniList Error Response]:", json_data.get("errors"))
                        return {}
                    data = json_data.get("data") or {}
                    _set_cache("anilist_query", cache_key, data, ttl_hours=0.17)  # ~10 minutes
                    if _learn_mappings_from_anilist_payload(data):
                        _save_local_mappings()
                    return data

                if res.status_code == 429 and attempt < 2:
                    retry_after = res.headers.get("Retry-After")
                    try:
                        delay = max(1.0, float(retry_after)) if retry_after else (1.5 * (attempt + 1))
                    except ValueError:
                        delay = 1.5 * (attempt + 1)
                    await asyncio.sleep(delay)
                    continue

                break

            print(f"[AniList HTTP Error] Status: {last_status} | Body: {last_text}")
            return {"__anilist_error__": last_status} if last_status else {}
    except Exception as e:
        print("[AniList Connection Exception]:", str(e))
        return {}


async def _jikan_search(query: str, page: int = 1, per_page: int = 20):
    """Search Jikan (MAL) API for anime."""
    cache_key = f"{query}_{page}_{per_page}"
    cached = _get_cache("jikan_search", cache_key)
    if cached:
        return cached
    
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.get(
                f"{JIKAN_URL}/anime",
                params={"q": query, "page": page, "limit": per_page, "type": "tv"}
            )
            if res.status_code == 200:
                data = res.json().get("data", [])
                _set_cache("jikan_search", cache_key, data, ttl_hours=6)
                return data
    except Exception as e:
        print(f"[Jikan Search Exception]: {str(e)}")
    
    return []


async def _jikan_anime_by_mal(mal_id: int):
    """Fetch MAL metadata from Jikan for graceful /info/mal-* fallback."""
    cache_key = str(mal_id)
    cached = _get_cache("jikan_anime", cache_key)
    if cached:
        return cached

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.get(f"{JIKAN_URL}/anime/{mal_id}")
            if res.status_code == 200:
                data = res.json().get("data")
                if data:
                    _set_cache("jikan_anime", cache_key, data, ttl_hours=12)
                return data
            print(f"[Jikan Anime HTTP Error] MAL {mal_id} | Status: {res.status_code} | Body: {res.text[:200]}")
    except Exception as e:
        print(f"[Jikan Anime Exception]: {str(e)}")
    return None


async def _jikan_manga_by_mal(mal_id: int):
    """Fetch MAL manga metadata from Jikan for MangaDex resolution."""
    cache_key = str(mal_id)
    cached = _get_cache("jikan_manga", cache_key)
    if cached:
        return cached
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.get(f"{JIKAN_URL}/manga/{mal_id}")
            if res.status_code == 200:
                data = res.json().get("data")
                _set_cache("jikan_manga", cache_key, data, ttl_hours=12)
                return data
    except Exception as e:
        print(f"[Jikan Manga Exception]: {str(e)}")
    return None


def _split_csv(value: Optional[str], default=None):
    if value is None:
        return list(default or [])
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [part.strip() for part in str(value).split(",") if part.strip()]


def _localized_value(values: dict, preferred_lang: str = "en"):
    if not isinstance(values, dict) or not values:
        return None
    for key in (preferred_lang, "en", "ja-ro", "ja", "ko", "zh"):
        if values.get(key):
            return values[key]
    return next((value for value in values.values() if value), None)


def _title_key(value: Optional[str]) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def _manga_title_candidates(*items) -> list:
    seen = set()
    titles = []
    for item in items:
        if not item:
            continue
        if isinstance(item, str):
            candidates = [item]
        elif isinstance(item, dict):
            candidates = [
                item.get("title"),
                item.get("title_english"),
                item.get("title_japanese"),
            ]
            for title_obj in item.get("titles", []) or []:
                if isinstance(title_obj, dict):
                    candidates.append(title_obj.get("title"))
        else:
            candidates = []
        for title in candidates:
            if not title:
                continue
            key = _title_key(title)
            if key and key not in seen:
                seen.add(key)
                titles.append(title)
    return titles


def _mangadex_headers():
    return {
        "User-Agent": "AnimeAPI/2.7 (+https://github.com/saugatthapa/animeapi)",
        "Accept": "application/json",
    }


async def _mangadex_get(path: str, params=None, cache_type: Optional[str] = None, cache_key: Optional[str] = None, ttl_hours: float = 1):
    if cache_type and cache_key:
        cached = _get_cache(cache_type, cache_key)
        if cached:
            return cached
    try:
        async with httpx.AsyncClient(timeout=20.0, headers=_mangadex_headers()) as client:
            res = await client.get(f"{MANGADEX_URL}{path}", params=params)
            if res.status_code == 404:
                raise HTTPException(status_code=404, detail="MangaDex item not found")
            if res.status_code == 429:
                raise HTTPException(status_code=429, detail="MangaDex rate limit reached")
            res.raise_for_status()
            payload = res.json()
            if cache_type and cache_key:
                _set_cache(cache_type, cache_key, payload, ttl_hours=ttl_hours)
            return payload
    except HTTPException:
        raise
    except Exception as e:
        print(f"[MangaDex Exception] {path}: {str(e)}")
        raise HTTPException(status_code=502, detail="MangaDex request failed")


def _mangadex_array_params(name: str, values: list):
    return [(f"{name}[]", value) for value in values if value]


def _manga_relation(manga: dict, rel_type: str):
    for rel in manga.get("relationships", []) or []:
        if rel.get("type") == rel_type:
            return rel
    return None


def _manga_relation_names(item: dict, rel_type: str) -> list:
    names = []
    for rel in item.get("relationships", []) or []:
        if rel.get("type") != rel_type:
            continue
        name = (rel.get("attributes") or {}).get("name")
        if name and name not in names:
            names.append(name)
    return names


def _cover_urls(manga_id: str, file_name: Optional[str]):
    if not file_name:
        return {"medium": None, "large": None, "extraLarge": None}
    base = f"{MANGADEX_COVERS_URL}/{manga_id}/{file_name}"
    return {
        "medium": f"{base}.256.jpg",
        "large": f"{base}.512.jpg",
        "extraLarge": base,
    }


def _manga_has_language(manga: dict, language: str) -> bool:
    if not language:
        return False
    languages = None
    if isinstance(manga, dict):
        if "availableTranslatedLanguages" in manga:
            languages = manga.get("availableTranslatedLanguages")
        else:
            attrs = manga.get("attributes", {}) or {}
            languages = attrs.get("availableTranslatedLanguages")
    if not isinstance(languages, list):
        return False
    return language in languages


def _normalize_mangadex_manga(manga: dict, preferred_lang: str = "en") -> dict:
    attrs = manga.get("attributes", {}) or {}
    links = attrs.get("links", {}) or {}
    manga_id = manga.get("id")
    title = attrs.get("title", {}) or {}
    alt_titles = []
    for alt in attrs.get("altTitles", []) or []:
        if isinstance(alt, dict):
            for value in alt.values():
                if value and value not in alt_titles:
                    alt_titles.append(value)

    cover_rel = _manga_relation(manga, "cover_art")
    cover_file = (cover_rel.get("attributes") or {}).get("fileName") if cover_rel else None
    tag_items = []
    for tag in attrs.get("tags", []) or []:
        tag_attrs = tag.get("attributes", {}) or {}
        name = _localized_value(tag_attrs.get("name", {}), preferred_lang)
        if name:
            tag_items.append({"id": tag.get("id"), "name": name, "group": tag_attrs.get("group")})

    mal_id = links.get("mal")
    anilist_id = links.get("al")
    if mal_id:
        _store_manga_mapping("mal", mal_id, manga_id, persist=False)
    if anilist_id:
        _store_manga_mapping("anilist", anilist_id, manga_id, persist=False)

    title_text = _localized_value(title, preferred_lang)
    if title_text:
        _store_manga_mapping("title", title_text, manga_id, persist=False)
    for alt_title in alt_titles[:10]:
        _store_manga_mapping("title", alt_title, manga_id, persist=False)

    return {
        "id": manga_id,
        "source": "mangadex",
        "title": {
            "preferred": title_text,
            "english": title.get("en") or next((alt.get("en") for alt in attrs.get("altTitles", []) if isinstance(alt, dict) and alt.get("en")), None),
            "native": title.get("ja") or title.get("ja-ro") or title.get("ko") or title.get("zh"),
        },
        "titleText": title_text,
        "altTitles": alt_titles,
        "description": _localized_value(attrs.get("description", {}), preferred_lang),
        "coverImage": _cover_urls(manga_id, cover_file),
        "status": attrs.get("status"),
        "year": attrs.get("year"),
        "contentRating": attrs.get("contentRating"),
        "publicationDemographic": attrs.get("publicationDemographic"),
        "originalLanguage": attrs.get("originalLanguage"),
        "availableTranslatedLanguages": attrs.get("availableTranslatedLanguages") or [],
        "hasEnglishChapters": _manga_has_language(manga, "en"),
        "lastVolume": attrs.get("lastVolume"),
        "lastChapter": attrs.get("lastChapter"),
        "latestUploadedChapter": attrs.get("latestUploadedChapter"),
        "genres": [tag["name"] for tag in tag_items if tag.get("group") == "genre"],
        "tags": tag_items,
        "authors": _manga_relation_names(manga, "author"),
        "artists": _manga_relation_names(manga, "artist"),
        "links": links,
        "malId": int(mal_id) if str(mal_id or "").isdigit() else mal_id,
        "anilistId": int(anilist_id) if str(anilist_id or "").isdigit() else anilist_id,
        "siteUrl": f"https://mangadex.org/title/{manga_id}",
        "createdAt": attrs.get("createdAt"),
        "updatedAt": attrs.get("updatedAt"),
    }


def _normalize_mangadex_chapter(chapter: dict) -> dict:
    attrs = chapter.get("attributes", {}) or {}
    manga_id = None
    for rel in chapter.get("relationships", []) or []:
        if rel.get("type") == "manga":
            manga_id = rel.get("id")
            break
    pages = attrs.get("pages") or 0
    external_url = attrs.get("externalUrl")
    readable = bool(pages and not external_url)
    return {
        "id": chapter.get("id"),
        "source": "mangadex",
        "mangaId": manga_id,
        "volume": attrs.get("volume"),
        "chapter": attrs.get("chapter"),
        "title": attrs.get("title"),
        "language": attrs.get("translatedLanguage"),
        "translatedLanguage": attrs.get("translatedLanguage"),
        "pages": pages,
        "readable": readable,
        "readUrl": f"/manga/read/{chapter.get('id')}" if readable else None,
        "externalUrl": external_url,
        "isUnavailable": attrs.get("isUnavailable", False),
        "publishAt": attrs.get("publishAt"),
        "readableAt": attrs.get("readableAt"),
        "scanlationGroups": _manga_relation_names(chapter, "scanlation_group"),
    }


def _remember_manga_mappings_from_results(results: list) -> bool:
    before = _manga_mapping_snapshot()
    for item in results:
        _normalize_mangadex_manga(item)
    return _save_manga_mappings_if_changed(before)


async def _search_mangadex_raw(query: str, page: int = 1, limit: int = 20, preferred_lang: str = "en", content_rating: Optional[str] = None):
    ratings = _split_csv(content_rating, ["safe", "suggestive"])
    offset = (page - 1) * limit
    params = [
        ("title", query),
        ("limit", limit),
        ("offset", offset),
        ("order[relevance]", "desc"),
        ("hasAvailableChapters", "true"),
    ]
    params += _mangadex_array_params("availableTranslatedLanguage", ["en"])
    params += _mangadex_array_params("includes", ["cover_art", "author", "artist"])
    params += _mangadex_array_params("contentRating", ratings)
    cache_key = f"{query.lower()}:{page}:{limit}:{preferred_lang}:{','.join(ratings)}"
    return await _mangadex_get("/manga", params=params, cache_type="manga_search", cache_key=cache_key, ttl_hours=4)


async def _get_mangadex_manga_raw(manga_id: str):
    params = _mangadex_array_params("includes", ["cover_art", "author", "artist"])
    return await _mangadex_get(f"/manga/{manga_id}", params=params, cache_type="manga_info", cache_key=manga_id, ttl_hours=12)


async def _find_mangadex_by_title_candidates(titles: list, mal_id: Optional[int] = None, anilist_id: Optional[int] = None):
    snapshot = _manga_mapping_snapshot()
    wanted_keys = {_title_key(title) for title in titles if title}
    first_exact = None
    first_any = None
    for title in titles[:8]:
        payload = await _search_mangadex_raw(title, page=1, limit=10, content_rating="safe,suggestive,erotica")
        for candidate in payload.get("data", []) or []:
            normalized = _normalize_mangadex_manga(candidate)
            links = normalized.get("links", {})
            if mal_id and str(links.get("mal")) == str(mal_id):
                _save_manga_mappings_if_changed(snapshot)
                return {"mangadexId": normalized["id"], "source": "mangadex_mal_link", "manga": normalized}
            if anilist_id and str(links.get("al")) == str(anilist_id):
                _save_manga_mappings_if_changed(snapshot)
                return {"mangadexId": normalized["id"], "source": "mangadex_anilist_link", "manga": normalized}
            candidate_keys = {_title_key(normalized.get("titleText"))}
            candidate_keys.update(_title_key(alt) for alt in normalized.get("altTitles", []))
            if wanted_keys.intersection(candidate_keys) and not first_exact:
                first_exact = normalized
            if not first_any:
                first_any = normalized
        if first_exact:
            _save_manga_mappings_if_changed(snapshot)
            return {"mangadexId": first_exact["id"], "source": "title_exact_match", "manga": first_exact}
    if first_any and not (mal_id or anilist_id):
        _save_manga_mappings_if_changed(snapshot)
        return {"mangadexId": first_any["id"], "source": "title_search", "manga": first_any}
    _save_manga_mappings_if_changed(snapshot)
    return None


async def _resolve_manga_to_mangadex(mal_id: Optional[int] = None, anilist_id: Optional[int] = None, title: Optional[str] = None):
    if mal_id:
        key = f"mal:{mal_id}"
        cached = _get_cache("manga_resolve", key)
        if cached:
            return cached
        mapped = _LOCAL_MANGA_MAPPINGS.get("mal", {}).get(str(mal_id))
        if mapped:
            result = {"resolved": True, "mangadexId": mapped, "malId": mal_id, "source": "local_mal_mapping"}
            _set_cache("manga_resolve", key, result, ttl_hours=24)
            return result
        jikan_item = await _jikan_manga_by_mal(mal_id)
        titles = _manga_title_candidates(jikan_item)
        match = await _find_mangadex_by_title_candidates(titles, mal_id=mal_id) if titles else None
        if match:
            _store_manga_mapping("mal", mal_id, match["mangadexId"])
            result = {"resolved": True, "mangadexId": match["mangadexId"], "malId": mal_id, "source": match["source"]}
            _set_cache("manga_resolve", key, result, ttl_hours=24)
            return result

    if anilist_id:
        key = f"anilist:{anilist_id}"
        cached = _get_cache("manga_resolve", key)
        if cached:
            return cached
        mapped = _LOCAL_MANGA_MAPPINGS.get("anilist", {}).get(str(anilist_id))
        if mapped:
            result = {"resolved": True, "mangadexId": mapped, "anilistId": anilist_id, "source": "local_anilist_mapping"}
            _set_cache("manga_resolve", key, result, ttl_hours=24)
            return result
        gql = """
        query ($id: Int) {
            Media(id: $id, type: MANGA) {
                id
                idMal
                title { romaji english native }
            }
        }
        """
        data = await _anilist_query(gql, {"id": anilist_id})
        media = data.get("Media") or {}
        if media.get("idMal"):
            result = await _resolve_manga_to_mangadex(mal_id=media["idMal"])
            if result and result.get("resolved"):
                _store_manga_mapping("anilist", anilist_id, result["mangadexId"])
                result["anilistId"] = anilist_id
                return result
        title_obj = media.get("title", {}) or {}
        titles = _manga_title_candidates(title_obj.get("english"), title_obj.get("romaji"), title_obj.get("native"))
        match = await _find_mangadex_by_title_candidates(titles, anilist_id=anilist_id) if titles else None
        if match:
            _store_manga_mapping("anilist", anilist_id, match["mangadexId"])
            result = {"resolved": True, "mangadexId": match["mangadexId"], "anilistId": anilist_id, "source": match["source"]}
            _set_cache("manga_resolve", key, result, ttl_hours=24)
            return result

    if title:
        key = f"title:{title.lower()}"
        cached = _get_cache("manga_resolve", key)
        if cached:
            return cached
        mapped = _LOCAL_MANGA_MAPPINGS.get("title", {}).get(title.lower())
        if mapped:
            result = {"resolved": True, "mangadexId": mapped, "title": title, "source": "local_title_mapping"}
            _set_cache("manga_resolve", key, result, ttl_hours=24)
            return result
        match = await _find_mangadex_by_title_candidates([title])
        if match:
            _store_manga_mapping("title", title, match["mangadexId"])
            result = {"resolved": True, "mangadexId": match["mangadexId"], "title": title, "source": match["source"]}
            _set_cache("manga_resolve", key, result, ttl_hours=24)
            return result

    return None


def _duration_minutes(duration):
    if not isinstance(duration, str):
        return duration
    parts = duration.split()
    for idx, part in enumerate(parts):
        if part.isdigit() and idx + 1 < len(parts) and parts[idx + 1].startswith("min"):
            return int(part)
    return duration


def _jikan_date(prop: dict):
    if not isinstance(prop, dict):
        return {"year": None, "month": None, "day": None}
    return {
        "year": prop.get("year"),
        "month": prop.get("month"),
        "day": prop.get("day"),
    }


def _jikan_next_airing_timestamp(broadcast: dict):
    """Best-effort next airing timestamp from Jikan broadcast day/time/timezone."""
    if not isinstance(broadcast, dict):
        return None

    day = (broadcast.get("day") or "").lower().rstrip("s")
    time_str = broadcast.get("time")
    tz_name = broadcast.get("timezone") or "Asia/Tokyo"
    weekdays = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6,
    }
    if day not in weekdays or not time_str:
        return None

    try:
        hour, minute = [int(part) for part in time_str.split(":", 1)]
        tz = ZoneInfo(tz_name)
        now = datetime.now(tz)
        days_until = (weekdays[day] - now.weekday()) % 7
        candidate = (now + timedelta(days=days_until)).replace(
            hour=hour,
            minute=minute,
            second=0,
            microsecond=0,
        )
        if candidate <= now:
            candidate += timedelta(days=7)
        return int(candidate.timestamp())
    except Exception:
        return None


def _normalize_jikan_schedule_item(jikan_item: dict) -> dict:
    mal_id = jikan_item.get("mal_id")
    anilist_id = _get_local_anilist_id_for_mal(mal_id) if mal_id is not None else None
    airing_at = _jikan_next_airing_timestamp(jikan_item.get("broadcast", {}))
    now_ts = int(datetime.now(timezone.utc).timestamp())
    time_until_airing = max(airing_at - now_ts, 0) if airing_at else None
    images = jikan_item.get("images", {}).get("jpg", {})
    aired = jikan_item.get("aired", {})
    aired_prop = aired.get("prop", {}) if isinstance(aired, dict) else {}
    studios = [
        {"name": studio.get("name"), "isAnimationStudio": True}
        for studio in jikan_item.get("studios", [])
        if isinstance(studio, dict)
    ]

    entry = {
        "id": anilist_id or f"mal-{mal_id}",
        "anilistId": anilist_id,
        "idMal": mal_id,
        "source": "jikan",
        "streamable": anilist_id is not None,
        "needsMapping": anilist_id is None,
        "watchUrl": f"/info/{anilist_id}" if anilist_id else f"/info/mal-{mal_id}",
        "title": {
            "romaji": jikan_item.get("title_japanese") or jikan_item.get("title", ""),
            "english": jikan_item.get("title_english") or jikan_item.get("title", ""),
            "native": jikan_item.get("title_japanese") or jikan_item.get("title", ""),
        },
        "coverImage": {
            "large": images.get("large_image_url") or images.get("image_url"),
            "extraLarge": images.get("large_image_url") or images.get("image_url"),
        },
        "bannerImage": images.get("large_image_url") or images.get("image_url"),
        "format": _normalize_format(jikan_item.get("type", "")),
        "season": (jikan_item.get("season") or "").upper() or None,
        "seasonYear": jikan_item.get("year"),
        "episodes": jikan_item.get("episodes"),
        "duration": _duration_minutes(jikan_item.get("duration")),
        "status": _normalize_status(jikan_item.get("status", "")),
        "averageScore": int((jikan_item.get("score") or 0) * 10) if jikan_item.get("score") else None,
        "meanScore": int((jikan_item.get("score") or 0) * 10) if jikan_item.get("score") else None,
        "popularity": jikan_item.get("members"),
        "favourites": jikan_item.get("favorites"),
        "genres": [g.get("name") for g in jikan_item.get("genres", []) if isinstance(g, dict)],
        "sourceMaterial": jikan_item.get("source"),
        "countryOfOrigin": "JP",
        "isAdult": False,
        "studios": {"nodes": studios},
        "startDate": _jikan_date(aired_prop.get("from", {})),
        "endDate": _jikan_date(aired_prop.get("to", {})),
        "siteUrl": jikan_item.get("url"),
        "broadcast": jikan_item.get("broadcast"),
        "next_episode": None,
        "airingAt": airing_at,
        "timeUntilAiring": time_until_airing,
        "nextAiringEpisode": {
            "episode": None,
            "airingAt": airing_at,
            "timeUntilAiring": time_until_airing,
        } if airing_at else None,
    }
    return entry


async def _jikan_schedule(page: int = 1, per_page: int = 20):
    """Fetch MAL schedule data from Jikan as an AniList schedule fallback."""
    cache_key = f"{page}_{per_page}"
    cached = _get_cache("jikan_schedule", cache_key)
    if cached:
        return cached

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.get(
                f"{JIKAN_URL}/schedules",
                params={"page": page, "limit": per_page, "sfw": "true"},
            )
            if res.status_code == 200:
                payload = res.json()
                pagination = payload.get("pagination", {})
                results = [_normalize_jikan_schedule_item(item) for item in payload.get("data", [])]
                results.sort(key=lambda item: item.get("airingAt") or 9999999999)
                response = {
                    "page": pagination.get("current_page", page),
                    "perPage": pagination.get("items", {}).get("per_page", per_page),
                    "total": pagination.get("items", {}).get("total", len(results)),
                    "hasNextPage": pagination.get("has_next_page", False),
                    "results": results,
                    "_fallback": {
                        "source": "jikan",
                        "reason": "AniList schedule unavailable",
                    },
                }
                _set_cache("jikan_schedule", cache_key, response, ttl_hours=1)
                return response
            print(f"[Jikan Schedule HTTP Error] Status: {res.status_code} | Body: {res.text}")
    except Exception as e:
        print(f"[Jikan Schedule Exception]: {str(e)}")

    return {
        "page": page,
        "perPage": per_page,
        "total": 0,
        "hasNextPage": False,
        "results": [],
        "_fallback": {
            "source": "jikan",
            "reason": "Jikan schedule unavailable",
        },
    }


def _normalize_jikan_to_anilist(jikan_item: dict) -> dict:
    """Convert Jikan anime result to AniList-like format."""
    return {
        "id": f"mal-{jikan_item.get('mal_id')}",
        "idMal": jikan_item.get('mal_id'),
        "source": "jikan",
        "title": {
            "romaji": jikan_item.get("title_japanese") or jikan_item.get("title", ""),
            "english": jikan_item.get("title_english") or jikan_item.get("title", ""),
            "native": jikan_item.get("title_japanese") or jikan_item.get("title", ""),
        },
        "coverImage": {
            "large": jikan_item.get("images", {}).get("jpg", {}).get("large_image_url", ""),
            "extraLarge": jikan_item.get("images", {}).get("jpg", {}).get("large_image_url", ""),
        },
        "bannerImage": jikan_item.get("images", {}).get("jpg", {}).get("large_image_url"),
        "format": _normalize_format(jikan_item.get("type", "")),
        "status": _normalize_status(jikan_item.get("status", "")),
        "episodes": jikan_item.get("episodes"),
        "duration": jikan_item.get("duration"),
        "averageScore": int((jikan_item.get("score") or 0) * 10) if jikan_item.get("score") else None,
        "meanScore": int((jikan_item.get("score") or 0) * 10) if jikan_item.get("score") else None,
        "genres": [g.get("name") for g in jikan_item.get("genres", [])],
        "description": jikan_item.get("synopsis", ""),
        "seasonYear": jikan_item.get("year"),
    }


def _normalize_format(jikan_format: str) -> str:
    """Convert Jikan format to AniList format."""
    mapping = {
        "TV": "TV",
        "Movie": "MOVIE",
        "OVA": "OVA",
        "ONA": "ONA",
        "Special": "SPECIAL",
        "Music": "MUSIC",
    }
    return mapping.get(jikan_format, jikan_format)


def _normalize_status(jikan_status: str) -> str:
    """Convert Jikan status to AniList status."""
    mapping = {
        "Currently Airing": "RELEASING",
        "Finished Airing": "FINISHED",
        "Not yet aired": "NOT_YET_RELEASED",
    }
    return mapping.get(jikan_status, jikan_status)


async def _resolve_mal_with_anizip(mal_id: int):
    """Resolve MAL to AniList using ani.zip when AniList GraphQL is unavailable."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            res = await client.get(ANIZIP_URL, params={"mal_id": mal_id})
            if res.status_code != 200:
                return None
            mappings = res.json().get("mappings", {})
            anilist_id = mappings.get("anilist_id")
            if not anilist_id:
                return None
            _store_mal_mapping(mal_id, anilist_id, source="ani_zip")
            return {
                "anilistId": int(anilist_id),
                "malId": mal_id,
                "source": "ani_zip",
                "streamable": True,
                "resolved": True,
            }
    except Exception as e:
        print(f"[AniZip Resolve Exception]: {str(e)}")
    return None


async def _resolve_mal_to_anilist(mal_id: int):
    """Resolve a MAL ID to AniList ID using: cache → local mappings → AniList."""
    cache_key = str(mal_id)
    
    # 1. Check cache
    cached = _get_cache("mal_to_anilist", cache_key)
    if cached:
        return cached
    
    # 2. Check local mappings (works even when AniList is down)
    if cache_key in _LOCAL_MAPPINGS:
        anilist_id = _LOCAL_MAPPINGS[cache_key]
        result = {
            "anilistId": anilist_id,
            "malId": mal_id,
            "source": "local_mapping",
            "streamable": True,
            "resolved": True,
        }
        _set_cache("mal_to_anilist", cache_key, result, ttl_hours=24)
        print(f"[Resolve] MAL {mal_id} -> AniList {anilist_id} (from local mapping)")
        return result
    
    # 3. Try ani.zip before AniList so fallback still works during AniList outages.
    anizip_result = await _resolve_mal_with_anizip(mal_id)
    if anizip_result:
        return anizip_result

    # 4. Check failed cache after external mapping attempts.
    failed_cached = _get_cache("failed_resolve", cache_key)
    if failed_cached:
        return None
    
    # 5. Try AniList GraphQL only when it is reachable.
    if not await _is_anilist_available():
        failure = {"error": "AniList unavailable", "streamable": False, "resolved": False}
        _set_cache("failed_resolve", cache_key, failure, ttl_hours=1)
        return None

    gql = """
    query ($mal_id: Int) {
        Media(idMal: $mal_id, type: ANIME) {
            id
            idMal
            title { romaji english native }
        }
    }
    """
    
    data = await _anilist_query(gql, {"mal_id": mal_id})
    media = data.get("Media")
    
    if media:
        _store_mal_mapping(mal_id, media.get("id"), source="anilist")
        result = {
            "anilistId": media.get("id"),
            "malId": mal_id,
            "source": "anilist",
            "streamable": True,
            "resolved": True,
        }
        _set_cache("mal_to_anilist", cache_key, result, ttl_hours=24)
        return result
    else:
        # Cache the failure
        failure = {"error": "Could not resolve MAL to AniList", "streamable": False, "resolved": False}
        _set_cache("failed_resolve", cache_key, failure, ttl_hours=1)
        return None


# ─── Homepage ──────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def home():
    return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Miruro API v2.8</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;500;700&display=swap" rel="stylesheet">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; font-family: 'Outfit', sans-serif; transition: all 0.3s ease; }
        body { background: radial-gradient(circle at top, #0f172a, #020617); color: #e2e8f0; min-height: 100vh; padding: 50px 20px; }
        .container { max-width: 960px; margin: 0 auto; background: rgba(30, 41, 59, 0.5); backdrop-filter: blur(10px); padding: 40px; border-radius: 24px; border: 1px solid rgba(255, 255, 255, 0.1); }
        .header { text-align: center; margin-bottom: 50px; }
        .logo { width: 120px; border-radius: 20px; box-shadow: 0 0 30px rgba(56, 189, 248, 0.3); border: 1px solid rgba(255,255,255,0.1); margin-bottom: 25px; object-fit: cover; }
        h1 { font-size: 3em; font-weight: 700; background: linear-gradient(to right, #38bdf8, #818cf8); -webkit-background-clip: text; color: transparent; margin-bottom: 10px; }
        .subtitle { color: #94a3b8; font-size: 1.1em; font-weight: 300; }
        .version { display: inline-block; background: rgba(56, 189, 248, 0.15); color: #38bdf8; padding: 4px 14px; border-radius: 20px; font-size: 0.85em; margin-top: 10px; border: 1px solid rgba(56, 189, 248, 0.3); }
        .section-title { font-size: 1.3em; font-weight: 700; color: #818cf8; margin: 35px 0 15px; border-left: 3px solid #818cf8; padding-left: 12px; }
        .endpoint { background: rgba(15, 23, 42, 0.8); border-left: 4px solid #38bdf8; padding: 25px; margin: 15px 0; border-radius: 0 16px 16px 0; border: 1px solid rgba(255,255,255,0.02); }
        .endpoint:hover { transform: translateX(5px); box-shadow: 0 10px 20px rgba(0,0,0,0.2); border-left-color: #818cf8; background: rgba(30, 41, 59, 0.9); }
        .method { color: #10b981; font-weight: 700; background: rgba(16, 185, 129, 0.1); padding: 4px 10px; border-radius: 6px; font-size: 0.9em; margin-right: 10px; }
        .url { font-family: monospace; color: #cbd5e1; font-size: 1.1em; }
        .params { margin-top: 10px; font-size: 0.85em; color: #64748b; font-family: monospace; line-height: 1.8; }
        .params span { color: #a5b4fc; }
        .example { margin-top: 15px; font-size: 0.95em; color: #64748b; }
        a { color: #38bdf8; text-decoration: none; word-break: break-all; font-weight: 500; }
        a:hover { color: #818cf8; text-shadow: 0 0 10px rgba(129, 140, 248, 0.5); }
        .desc { color: #cbd5e1; font-size: 1em; margin-top: 10px; font-weight: 300; line-height: 1.6; }
        .badge { display: inline-block; font-size: 0.7em; padding: 2px 8px; border-radius: 6px; margin-left: 8px; font-weight: 500; vertical-align: middle; }
        .badge-new { background: rgba(16, 185, 129, 0.15); color: #10b981; border: 1px solid rgba(16, 185, 129, 0.3); }
        .badge-improved { background: rgba(129, 140, 248, 0.15); color: #818cf8; border: 1px solid rgba(129, 140, 248, 0.3); }
        .returns { margin-top: 12px; font-size: 0.85em; color: #94a3b8; line-height: 1.6; }
        .returns b { color: #a5b4fc; font-weight: 500; }
        pre.snippet { background: #020617; padding: 14px; border-radius: 10px; margin-top: 12px; color: #a5b4fc; font-family: monospace; font-size: 0.82em; border: 1px solid rgba(255,255,255,0.05); overflow-x: auto; }
        .step-num { display: inline-block; background: rgba(56, 189, 248, 0.15); color: #38bdf8; width: 26px; height: 26px; text-align: center; line-height: 26px; border-radius: 50%; font-size: 0.9em; margin-right: 8px; }
        .note { background: rgba(250, 204, 21, 0.08); border: 1px solid rgba(250, 204, 21, 0.15); border-radius: 10px; padding: 14px 18px; margin-top: 12px; font-size: 0.88em; color: #fbbf24; line-height: 1.6; }
        .note b { color: #fde68a; }
        table.param-table { width: 100%; margin-top: 12px; border-collapse: collapse; font-size: 0.85em; }
        table.param-table th { text-align: left; color: #818cf8; font-weight: 500; padding: 6px 10px; border-bottom: 1px solid rgba(255,255,255,0.08); }
        table.param-table td { padding: 6px 10px; color: #94a3b8; border-bottom: 1px solid rgba(255,255,255,0.03); }
        table.param-table td:first-child { color: #a5b4fc; font-family: monospace; white-space: nowrap; }
        .footer { text-align: center; margin-top: 50px; color: #475569; font-size: 0.9em; border-top: 1px solid rgba(255,255,255,0.05); padding-top: 20px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <img src="https://www.miruro.to/icon-512x512.png" alt="Logo" class="logo">
            <h1>Miruro Native API</h1>
            <div class="subtitle">Decrypted anime streaming API plus MangaDex manga reader routes</div>
            <div class="version">v2.8 - Miruro + ThapaSIR</div>
        </div>

        <div class="note" style="background: rgba(16, 185, 129, 0.08); border-color: rgba(16, 185, 129, 0.2); color: #10b981;">
            <b>v2.8:</b> Main Miruro endpoints stay intact, and ThapaSIR is now integrated into the normal <code>/episodes</code> and <code>/watch</code> provider flow.
        </div>

        <!-- ───────── SEARCH & DISCOVERY ───────── -->
        <div class="section-title">🔍 Search &amp; Discovery</div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/search</span> <span class="badge badge-improved">IMPROVED</span></div>
            <div class="desc">Search anime by name with <b>Jikan fallback</b>. Try AniList first; if empty/down, returns Jikan results. Returns full metadata per result.</div>
            <div class="params">Params: <span>query</span> (required), <span>page</span>=1, <span>per_page</span>=20</div>
            <div class="returns">Returns: <b>page</b>, <b>perPage</b>, <b>total</b>, <b>hasNextPage</b>, <b>results[]</b> — with <b>source</b> field ("anilist" or "jikan")</div>
            <div class="example">Try: <a target="_blank" href="/search?query=rezero&page=1&per_page=5">/search?query=rezero&page=1&per_page=5</a></div>
        </div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/resolve</span> <span class="badge badge-improved">IMPROVED</span></div>
            <div class="desc">Resolve MAL ID to AniList ID. Uses cache, local mappings, ani.zip, then AniList GraphQL when available. Works offline for known MAL IDs!</div>
            <div class="params">Params: <span>malId</span> (required)</div>
            <div class="returns">Returns: <b>malId</b>, <b>anilistId</b>, <b>resolved</b>, <b>source</b></div>
            <div class="example">Try: <a target="_blank" href="/resolve?malId=59983">/resolve?malId=59983</a> (Wistoria Season 2 - in mappings.json)</div>
        </div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/stream-search</span> <span class="badge badge-improved">IMPROVED</span></div>
            <div class="desc">Find anime with clear streamability status. Better handling when AniList is down.</div>
            <div class="params">Params: <span>query</span> (required), <span>page</span>=1, <span>per_page</span>=10</div>
            <div class="returns">Returns: <b>results[]</b> with <b>id</b>, <b>anilistId</b>, <b>idMal</b>, <b>title</b>, <b>source</b>, <b>streamable</b>, <b>reason</b> (if not streamable)</div>
            <div class="example">Try: <a target="_blank" href="/stream-search?query=naruto">/stream-search?query=naruto</a></div>
        </div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/search-streamable-anilist</span> <span class="badge badge-new">NEW</span></div>
            <div class="desc">Search AniList directly for streamable titles. Returns 503 if AniList is down.</div>
            <div class="params">Params: <span>query</span> (required), <span>page</span>=1, <span>per_page</span>=20</div>
            <div class="returns">Returns: AniList results only. HTTP 503 if AniList unavailable.</div>
            <div class="example">Try: <a target="_blank" href="/search-streamable-anilist?query=jjk">/search-streamable-anilist?query=jjk</a></div>
        </div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/suggestions</span></div>
            <div class="desc">Lightweight search for autocomplete / dropdown. Returns only essentials. Max 8 results.</div>
            <div class="params">Params: <span>query</span> (required)</div>
            <div class="returns">Returns: <b>suggestions[]</b> — id, title, poster, format, status, year, episodes</div>
            <div class="example">Try: <a target="_blank" href="/suggestions?query=aot">/suggestions?query=aot</a></div>
        </div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/spotlight</span></div>
            <div class="desc">Top 10 trending &amp; popular anime. Perfect for hero banners and carousels.</div>
            <div class="example">Try: <a target="_blank" href="/spotlight">/spotlight</a></div>
        </div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/filter</span></div>
            <div class="desc">Advanced filter with genre, tag, year, season, format, status, sort.</div>
            <table class="param-table">
                <tr><th>Param</th><th>Values</th></tr>
                <tr><td>genre</td><td>Action, Romance, Comedy, Drama, Fantasy, Sci-Fi, etc.</td></tr>
                <tr><td>tag</td><td>Isekai, Time Skip, Reincarnation, etc.</td></tr>
                <tr><td>year</td><td>2025, 2024, etc.</td></tr>
                <tr><td>season</td><td>WINTER · SPRING · SUMMER · FALL</td></tr>
                <tr><td>format</td><td>TV · MOVIE · OVA · ONA · SPECIAL</td></tr>
                <tr><td>status</td><td>RELEASING · FINISHED · NOT_YET_RELEASED · CANCELLED</td></tr>
                <tr><td>sort</td><td>SCORE_DESC · POPULARITY_DESC · TRENDING_DESC · START_DATE_DESC</td></tr>
                <tr><td>page / per_page</td><td>Pagination (default 1 / 20)</td></tr>
            </table>
            <div class="example">Try: <a target="_blank" href="/filter?genre=Action&format=TV&sort=SCORE_DESC&per_page=5">/filter?genre=Action&format=TV&sort=SCORE_DESC&per_page=5</a></div>
        </div>

        <!-- ───────── COLLECTIONS ───────── -->
        <div class="section-title">📊 Collections (Paginated)</div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/trending</span></div>
            <div class="desc">Currently trending anime across the community.</div>
            <div class="params">Params: <span>page</span>=1, <span>per_page</span>=20</div>
            <div class="example">Try: <a target="_blank" href="/trending?per_page=5">/trending?per_page=5</a></div>
        </div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/popular</span></div>
            <div class="desc">Most popular anime of all time by total user count.</div>
            <div class="params">Params: <span>page</span>=1, <span>per_page</span>=20</div>
            <div class="example">Try: <a target="_blank" href="/popular">/popular</a></div>
        </div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/upcoming</span></div>
            <div class="desc">Most anticipated anime that haven't aired yet.</div>
            <div class="params">Params: <span>page</span>=1, <span>per_page</span>=20</div>
            <div class="example">Try: <a target="_blank" href="/upcoming">/upcoming</a></div>
        </div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/recent</span></div>
            <div class="desc">Currently airing / this season's anime.</div>
            <div class="params">Params: <span>page</span>=1, <span>per_page</span>=20</div>
            <div class="example">Try: <a target="_blank" href="/recent">/recent</a></div>
        </div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/schedule</span></div>
            <div class="desc">Next episodes airing soon with timestamps. Uses Jikan schedule fallback when AniList is unavailable.</div>
            <div class="params">Params: <span>page</span>=1, <span>per_page</span>=20</div>
            <div class="example">Try: <a target="_blank" href="/schedule">/schedule</a></div>
        </div>

        <!-- ───────── ANIME DETAILS ───────── -->
        <div class="section-title">📖 Anime Details</div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/info/{anilist_id}</span></div>
            <div class="desc">Complete anime page data — everything you need.</div>
            <div class="example">Try: <a target="_blank" href="/info/20">/info/20</a> (Naruto) · <a target="_blank" href="/info/21">/info/21</a> (One Piece)</div>
        </div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/anime/{id}/characters</span></div>
            <div class="desc">Paginated character list with voice actors.</div>
            <div class="params">Params: <span>page</span>=1, <span>per_page</span>=25</div>
            <div class="example">Try: <a target="_blank" href="/anime/20/characters">/anime/20/characters</a></div>
        </div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/anime/{id}/relations</span></div>
            <div class="desc">Related media — sequels, prequels, spin-offs, etc.</div>
            <div class="example">Try: <a target="_blank" href="/anime/20/relations">/anime/20/relations</a></div>
        </div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/anime/{id}/recommendations</span></div>
            <div class="desc">Community recommendations sorted by rating.</div>
            <div class="params">Params: <span>page</span>=1, <span>per_page</span>=10</div>
            <div class="example">Try: <a target="_blank" href="/anime/20/recommendations">/anime/20/recommendations</a></div>
        </div>

        <!-- ───────── STREAMING ───────── -->
        <!-- Manga Reader -->
        <div class="section-title">Manga Reader</div>

        <div class="note">
            Manga routes use MangaDex for readable chapters and page images. MAL/AniList IDs are resolved to MangaDex UUIDs through cache, local mappings, Jikan, AniList, and MangaDex metadata links.
        </div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/manga/search</span> <span class="badge badge-new">NEW</span></div>
            <div class="desc">Search MangaDex manga by title.</div>
            <div class="params">Params: <span>q</span> (required), <span>page</span>=1, <span>per_page</span>=20, <span>lang</span>=en</div>
            <div class="example">Try: <a target="_blank" href="/manga/search?q=witch%20hat%20atelier&per_page=1">/manga/search?q=witch%20hat%20atelier</a></div>
        </div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/manga/resolve</span></div>
            <div class="desc">Resolve MAL manga ID, AniList manga ID, or title to a MangaDex UUID.</div>
            <div class="params">Params: <span>malId</span>, <span>anilistId</span>, or <span>title</span></div>
            <div class="example">Try: <a target="_blank" href="/manga/resolve?malId=100035">/manga/resolve?malId=100035</a></div>
        </div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/manga/{mangadex_id}</span> / <span class="url">/manga/{mangadex_id}/chapters</span></div>
            <div class="desc">Get manga details and language-filtered chapter lists.</div>
            <div class="example">Try: <a target="_blank" href="/manga/67e7453b-9ee5-4ae5-9316-215b03e4a71d/chapters?lang=en">/manga/{id}/chapters?lang=en</a></div>
        </div>

        <div class="endpoint" style="border-left-color: #10b981; background: rgba(16, 185, 129, 0.05);">
            <div><span class="method">GET</span> <span class="url">/manga/read/{chapter_id}</span></div>
            <div class="desc">Return ordered page image URLs for a MangaDex chapter. Use <span>quality=data</span> for full quality or <span>quality=data-saver</span> for compressed pages.</div>
            <div class="example">Try: <a target="_blank" href="/manga/read/a8cfe9d2-57f2-4552-b415-fdbc6806aae3">/manga/read/{chapter_id}</a></div>
        </div>

        <div class="section-title">▶️ Streaming (3-Step Flow)</div>

        <div class="note">
            <b>Important:</b> AniList ID remains the primary streaming ID. MAL ID is backup-only: <span style="font-family: monospace;">/info/mal-{malId}</span>, <span style="font-family: monospace;">/episodes/mal-{malId}</span>, and <span style="font-family: monospace;">/watch-by-mal/...</span> resolve to numeric AniList IDs internally.
        </div>

        <div class="endpoint">
            <div><span class="step-num">1</span><span class="method">GET</span> <span class="url">/episodes/{anilist_id}?malId={malId}</span></div>
            <div class="desc">Get all available episodes. AniList is tried first; if it fails and malId is provided, MAL backup is attempted.</div>
            <div class="example">Try: <a target="_blank" href="/episodes/178005?malId=6594">/episodes/178005?malId=6594</a></div>
        </div>

        <div class="endpoint">
            <div><span class="method">GET</span> <span class="url">/info/mal-{malId}</span> / <span class="url">/episodes/mal-{malId}</span> <span class="badge badge-new">BACKUP</span></div>
            <div class="desc">Explicit MAL fallback routes. They use cache and mappings.json first, then AniList idMal lookup only when AniList is reachable.</div>
            <div class="example">Try: <a target="_blank" href="/episodes/mal-59983">/episodes/mal-59983</a></div>
        </div>

        <div class="endpoint" style="border-left-color: #10b981; background: rgba(16, 185, 129, 0.05);">
            <div><span class="step-num">2</span> <span class="url">/watch/{provider}/{anilistId}/{category}/{slug}</span> or <span class="url">/watch-by-mal/{malId}/{provider}/{category}/{episodeId}</span> <span class="badge badge-new">RECOMMENDED</span></div>
            <div class="desc">Get sources. Just use the <b>id</b> from the episode response directly; MAL backup responses emit watch-by-mal IDs.</div>
            <div class="example">Try: <a target="_blank" href="/watch/kiwi/178005/sub/animepahe-1">/watch/kiwi/178005/sub/animepahe-1</a></div>
        </div>

        <div class="endpoint" style="border-left-color: #818cf8;">
            <div><span class="step-num">3</span> <span class="url" style="color: #818cf8;">Play the stream</span></div>
            <div class="desc">Take streams[0].url and feed into any HLS player (Video.js, hls.js, VLC, mpv, etc.).</div>
        </div>

        <div class="footer">
            All collection endpoints return paginated responses: <span style="color: #a5b4fc; font-family: monospace;">{ page, perPage, total, hasNextPage, results[] }</span>
            <br><br>
            Developed by Walter | <a href="https://github.com/walterwhite-69" target="_blank">github.com/walterwhite-69</a>
        </div>
    </div>
</body>
</html>"""


# ─── Search & Suggestions ───────────────────────────────────────────────────

@app.get("/manga/search")
async def search_manga(
    q: str = Query(..., min_length=1, description="Manga title"),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    lang: str = Query("en", description="Preferred title/description language"),
    contentRating: Optional[str] = Query("safe,suggestive", description="Comma-separated MangaDex content ratings"),
):
    """Search MangaDex for manga titles that have English chapters."""
    payload = await _search_mangadex_raw(q, page=page, limit=per_page, preferred_lang=lang, content_rating=contentRating)
    snapshot = _manga_mapping_snapshot()
    results = [
        _normalize_mangadex_manga(item, preferred_lang=lang)
        for item in payload.get("data", [])
        if _manga_has_language(item, "en")
    ]
    _save_manga_mappings_if_changed(snapshot)
    total = payload.get("total", 0)
    return _proxy_deep_images({
        "page": page,
        "perPage": per_page,
        "total": total,
        "hasNextPage": (page * per_page) < total,
        "results": results,
        "_source": "mangadex",
    })


@app.get("/manga/resolve")
async def resolve_manga(
    malId: Optional[int] = Query(None, description="MAL manga ID"),
    anilistId: Optional[int] = Query(None, description="AniList manga ID"),
    title: Optional[str] = Query(None, description="Manga title fallback"),
):
    """Resolve MAL/AniList/title metadata to a MangaDex UUID for English-readable manga."""
    if not malId and not anilistId and not title:
        raise HTTPException(status_code=400, detail="Provide malId, anilistId, or title")

    result = await _resolve_manga_to_mangadex(mal_id=malId, anilist_id=anilistId, title=title)
    if not result:
        return JSONResponse(
            status_code=404,
            content={
                "resolved": False,
                "malId": malId,
                "anilistId": anilistId,
                "title": title,
                "detail": "No MangaDex mapping found",
            },
        )

    payload = await _get_mangadex_manga_raw(result["mangadexId"])
    manga = payload.get("data")
    if not manga or not _manga_has_language(manga, "en"):
        return JSONResponse(
            status_code=404,
            content={
                "resolved": False,
                "malId": malId,
                "anilistId": anilistId,
                "title": title,
                "detail": "Manga not available in English",
            },
        )
    return result


@app.get("/manga/chapter/{chapter_id}")
async def get_manga_chapter(chapter_id: str):
    """Get metadata for a MangaDex chapter."""
    params = _mangadex_array_params("includes", ["manga", "scanlation_group"])
    payload = await _mangadex_get(f"/chapter/{chapter_id}", params=params, cache_type="manga_chapters", cache_key=f"chapter:{chapter_id}", ttl_hours=1)
    chapter = payload.get("data")
    if not chapter:
        raise HTTPException(status_code=404, detail="Manga chapter not found")
    return _proxy_deep_images(_normalize_mangadex_chapter(chapter))


@app.get("/manga/read/{chapter_id}")
async def read_manga_chapter(
    chapter_id: str,
    quality: str = Query("data-saver", description="data for full quality, data-saver for compressed pages"),
):
    """Return ordered MangaDex page image URLs for a chapter."""
    quality = "data" if quality == "data" else "data-saver"
    cache_key = f"{chapter_id}:{quality}"
    cached = _get_cache("manga_read", cache_key)
    if cached:
        return _proxy_deep_images(cached)

    chapter_meta = await get_manga_chapter(chapter_id)
    if chapter_meta.get("externalUrl") and not chapter_meta.get("pages"):
        return {
            "chapterId": chapter_id,
            "source": "mangadex",
            "readable": False,
            "externalUrl": chapter_meta.get("externalUrl"),
            "detail": "This chapter is hosted externally by the publisher/source.",
        }

    payload = await _mangadex_get(f"/at-home/server/{chapter_id}")
    chapter = payload.get("chapter", {}) or {}
    chapter_hash = chapter.get("hash")
    filenames = chapter.get("data") if quality == "data" else chapter.get("dataSaver")
    folder = "data" if quality == "data" else "data-saver"
    base_url = payload.get("baseUrl")

    if not base_url or not chapter_hash or not filenames:
        response = {
            "chapterId": chapter_id,
            "source": "mangadex",
            "readable": False,
            "externalUrl": chapter_meta.get("externalUrl"),
            "detail": "No MangaDex page images found for this chapter",
        }
        _set_cache("manga_read", cache_key, response, ttl_hours=1)
        return response

    referer = "https://mangadex.org/"
    pages = []
    for idx, filename in enumerate(filenames):
        page_url = f"{base_url}/{folder}/{chapter_hash}/{filename}"
        proxy_url = _build_lunaranime_proxy_url(page_url, referer)
        pages.append(
            {
                "index": idx + 1,
                "filename": filename,
                "url": page_url,
                "proxyUrl": proxy_url,
                "displayUrl": proxy_url or page_url,
            }
        )
    response = {
        "chapterId": chapter_id,
        "source": "mangadex",
        "readable": True,
        "quality": quality,
        "baseUrl": base_url,
        "hash": chapter_hash,
        "total": len(pages),
        "pages": pages,
        "headers": {"Referer": referer},
    }
    _set_cache("manga_read", cache_key, response, ttl_hours=1)
    return _proxy_deep_images(response)


@app.get("/manga/{manga_id}/chapters")
async def get_manga_chapters(
    manga_id: str,
    lang: str = Query("en", description="Translated language code"),
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=500),
    order: str = Query("asc", description="asc or desc"),
):
    """Get MangaDex chapters for a manga in one language."""
    order = "desc" if order.lower() == "desc" else "asc"
    offset = (page - 1) * per_page
    params = [
        ("limit", per_page),
        ("offset", offset),
        ("order[volume]", order),
        ("order[chapter]", order),
    ]
    params += _mangadex_array_params("translatedLanguage", _split_csv(lang, ["en"]))
    params += _mangadex_array_params("includes", ["scanlation_group"])
    cache_key = f"{manga_id}:{lang}:{page}:{per_page}:{order}"
    payload = await _mangadex_get(f"/manga/{manga_id}/feed", params=params, cache_type="manga_chapters", cache_key=cache_key, ttl_hours=0.5)
    chapters = [_normalize_mangadex_chapter(item) for item in payload.get("data", [])]
    total = payload.get("total", 0)
    return _proxy_deep_images({
        "mangaId": manga_id,
        "source": "mangadex",
        "language": lang,
        "page": page,
        "perPage": per_page,
        "total": total,
        "hasNextPage": (page * per_page) < total,
        "chapters": chapters,
    })


@app.get("/manga/{manga_id}")
async def get_manga_info(
    manga_id: str,
    lang: str = Query("en", description="Preferred title/description language"),
):
    """Get MangaDex manga metadata by UUID, restricted to titles with English chapters."""
    payload = await _get_mangadex_manga_raw(manga_id)
    manga = payload.get("data")
    if not manga:
        raise HTTPException(status_code=404, detail="Manga not found")
    if not _manga_has_language(manga, "en"):
        raise HTTPException(status_code=404, detail="Manga not available in English")
    snapshot = _manga_mapping_snapshot()
    result = _normalize_mangadex_manga(manga, preferred_lang=lang)
    _save_manga_mappings_if_changed(snapshot)
    return _proxy_deep_images(result)


@app.get("/search")
async def search_anime(
    query: str,
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=50, description="Results per page"),
):
    """Search for anime by name via AniList GraphQL with Jikan fallback."""
    async def fetch_fn():
        gql = f"""
        query ($search: String, $page: Int, $perPage: Int) {{
            Page(page: $page, perPage: $perPage) {{
                pageInfo {{ total currentPage lastPage hasNextPage perPage }}
                media(search: $search, type: ANIME, sort: SEARCH_MATCH) {{
                    {MEDIA_LIST_FIELDS}
                }}
            }}
        }}
        """
        data = await _anilist_query(gql, {"search": query, "page": page, "perPage": per_page})
        page_data = data.get("Page", {})
        page_info = page_data.get("pageInfo", {})
        results = page_data.get("media", [])

        if not results:
            print(f"[Search Fallback] AniList returned empty for '{query}', trying Jikan...")
            jikan_results = await _jikan_search(query, page=page, per_page=per_page)
            results = [_normalize_jikan_to_anilist(item) for item in jikan_results]
            page_info = {
                "currentPage": page,
                "perPage": per_page,
                "total": len(jikan_results) * 2,
                "hasNextPage": len(jikan_results) >= per_page,
            }

        response = {
            "page": page_info.get("currentPage", page),
            "perPage": page_info.get("perPage", per_page),
            "total": page_info.get("total", 0),
            "hasNextPage": page_info.get("hasNextPage", False),
            "results": results,
        }
        return _proxy_deep_images(response)

    return await _cached_response("search", 900, fetch_fn, query.strip().lower(), str(page), str(per_page))


@app.get("/resolve")
async def resolve_anime(
    malId: int = Query(..., description="MAL anime ID"),
):
    """Resolve MAL ID to AniList ID using cache, local mappings, ani.zip, then AniList."""
    result = await _resolve_mal_to_anilist(malId)
    
    if not result:
        return JSONResponse(
            status_code=404,
            content={
                "malId": malId,
                "resolved": False,
                "streamable": False,
                "needsMapping": True,
                "detail": "No MAL-to-AniList mapping found",
            }
        )
    return {
        "malId": malId,
        "anilistId": result["anilistId"],
        "resolved": True,
        "source": result.get("source", "mapping"),
    }


@app.get("/search-streamable-anilist")
async def search_streamable_anilist(
    query: str,
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=50, description="Results per page"),
):
    """Search AniList directly for streamable titles. Returns 503 if AniList unavailable."""
    gql = f"""
    query ($search: String, $page: Int, $perPage: Int) {{
        Page(page: $page, perPage: $perPage) {{
            pageInfo {{ total currentPage lastPage hasNextPage perPage }}
            media(search: $search, type: ANIME, sort: SEARCH_MATCH) {{
                {MEDIA_LIST_FIELDS}
            }}
        }}
    }}
    """
    data = await _anilist_query(gql, {"search": query, "page": page, "perPage": per_page})
    page_data = data.get("Page", {})
    page_info = page_data.get("pageInfo", {})
    results = page_data.get("media", [])
    
    # If AniList returned nothing, it's likely down
    if not results and not page_info:
        return JSONResponse(
            status_code=503,
            content={
                "detail": "AniList GraphQL unavailable. Cannot resolve streamable AniList IDs.",
                "suggestion": "Try /search for Jikan results, or use /resolve with local mappings if available."
            }
        )
    
    response = {
        "page": page_info.get("currentPage", page),
        "perPage": page_info.get("perPage", per_page),
        "total": page_info.get("total", 0),
        "hasNextPage": page_info.get("hasNextPage", False),
        "results": results,
    }
    return _proxy_deep_images(response)


@app.get("/stream-search")
async def stream_search(
    query: str,
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=50),
):
    """Search for anime with streamability confirmation. Better handling when AniList is down."""
    # First, get search results
    search_result = await search_anime(query=query, page=page, per_page=per_page)
    results = []
    
    anilist_available = await _is_anilist_available()
    
    for item in search_result.get("results", []):
        # Try to get streamable AniList ID
        anilist_id = item.get("id")
        mal_id = item.get("idMal")
        source = item.get("source", "unknown")
        title = item.get("title", {}).get("english") or item.get("title", {}).get("romaji")
        
        streamable = False
        resolved_anilist_id = None
        reason = None
        needs_mapping = False
        watch_url = None
        
        if source == "anilist":
            # Already AniList ID from AniList directly
            streamable = True
            resolved_anilist_id = anilist_id
            watch_url = f"/info/{anilist_id}"
            
        elif source == "jikan" and mal_id:
            # Jikan result - try to resolve
            resolution = await _resolve_mal_to_anilist(mal_id)
            
            if resolution and "anilistId" in resolution:
                # Successfully resolved
                streamable = True
                resolved_anilist_id = resolution["anilistId"]
                watch_url = f"/info/{resolved_anilist_id}"
                anilist_id = resolved_anilist_id
                
            else:
                # Could not resolve
                streamable = False
                resolved_anilist_id = None
                needs_mapping = True
                if anilist_available:
                    reason = "MAL ID not found on AniList"
                else:
                    reason = "AniList unavailable; MAL-to-AniList mapping unavailable"
        
        results.append({
            "id": anilist_id,
            "anilistId": resolved_anilist_id,
            "idMal": mal_id,
            "title": title,
            "source": source,
            "streamable": streamable,
            "watchUrl": watch_url,
            "fallbackSource": "jikan" if (source == "jikan" and not streamable) else None,
            "needsAnilistMapping": needs_mapping,
            "reason": reason,
        })
    
    return {
        "page": search_result.get("page"),
        "perPage": search_result.get("perPage"),
        "total": search_result.get("total"),
        "hasNextPage": search_result.get("hasNextPage"),
        "results": results,
        "_info": {
            "anilistAvailable": anilist_available,
            "notice": "streamable=true means anilistId exists and /info/{anilistId} works. Use watchUrl for direct navigation."
        }
    }


@app.get("/suggestions")
async def search_suggestions(
    query: str = Query(..., min_length=1, description="Search query for autocomplete"),
):
    """Lightweight search for dropdown autocomplete — returns minimal data fast."""
    gql = """
    query ($search: String) {
        Page(page: 1, perPage: 8) {
            media(search: $search, type: ANIME, sort: SEARCH_MATCH) {
                id
                title { romaji english }
                coverImage { large }
                format
                status
                startDate { year }
                episodes
            }
        }
    }
    """
    data = await _anilist_query(gql, {"search": query})
    results = []
    for item in data.get("Page", {}).get("media", []):
        results.append({
            "id": item["id"],
            "title": item["title"].get("english") or item["title"].get("romaji"),
            "title_romaji": item["title"].get("romaji"),
            "poster": item["coverImage"]["large"],
            "format": item.get("format"),
            "status": item.get("status"),
            "year": (item.get("startDate") or {}).get("year"),
            "episodes": item.get("episodes"),
        })
    return _proxy_deep_images({"suggestions": results})


# ─── Advanced Filter ───────────────────────────────────────────────────────

SORT_MAP = {
    "SCORE_DESC": "SCORE_DESC",
    "POPULARITY_DESC": "POPULARITY_DESC",
    "TRENDING_DESC": "TRENDING_DESC",
    "START_DATE_DESC": "START_DATE_DESC",
    "FAVOURITES_DESC": "FAVOURITES_DESC",
    "UPDATED_AT_DESC": "UPDATED_AT_DESC",
}

ANIMEKAI_BASE = "https://www1.anikai.cc"
ANIMEKAI_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
ANIMEKAI_HEADERS = {
    "User-Agent": ANIMEKAI_UA,
    "Referer": f"{ANIMEKAI_BASE}/",
    "Accept": "text/html, */*",
}


def _animekai_absolute_url(value: str) -> str:
    return urljoin(f"{ANIMEKAI_BASE}/", value or "")


def _animekai_extract_image(node) -> str:
    if not node:
        return ""
    return (
        node.get("src")
        or node.get("data-src")
        or node.get("data-lazy-src")
        or node.get("data-original")
        or ""
    )


async def _animekai_fetch_text(path: str, extra_headers: Optional[dict] = None) -> str:
    url = path if path.startswith("http") else _animekai_absolute_url(path)
    headers = dict(ANIMEKAI_HEADERS)
    if extra_headers:
        headers.update(extra_headers)

    def _request():
        return curl_requests.get(
            url,
            headers=headers,
            impersonate="chrome124",
            timeout=8,
            allow_redirects=True,
        )

    try:
        response = await asyncio.to_thread(_request)
    except CurlTimeout as e:
        print(f"[ANIMEKAI TIMEOUT] {url}: {e}")
        return ""
    except RequestException as e:
        print(f"[ANIMEKAI REQUEST ERROR] {url}: {e}")
        return ""
    except Exception as e:
        print(f"[ANIMEKAI UNKNOWN ERROR] {url}: {e}")
        return ""

    if response.status_code >= 400:
        print(f"[ANIMEKAI HTTP {response.status_code}] {url}")
        return ""

    return response.text or ""


async def _animekai_fetch_soup(path: str, extra_headers: Optional[dict] = None) -> BeautifulSoup:
    html = await _animekai_fetch_text(path, extra_headers=extra_headers)
    return BeautifulSoup(html or "", "html.parser")


def _animekai_text(node, selector: str) -> str:
    child = node.select_one(selector) if node else None
    return child.get_text(" ", strip=True) if child else ""


def _animekai_parse_info_spans(node) -> dict:
    info = {"sub_episodes": "", "dub_episodes": "", "type": ""}
    for span in node.select(".info span") if node else []:
        classes = " ".join(span.get("class", []))
        text = span.get_text(" ", strip=True)
        if "sub" in classes:
            info["sub_episodes"] = text
        elif "dub" in classes:
            info["dub_episodes"] = text
        elif text:
            info["type"] = text
    return info


def _animekai_parse_item(node) -> dict:
    anchor = node.select_one("a")
    href = anchor.get("href", "") if anchor else ""
    slug = href.replace("/watch/", "").split("/ep-")[0].strip("/")
    image = _animekai_extract_image(node.select_one("img"))
    title_el = node.select_one(".title")
    title = title_el.get_text(" ", strip=True) if title_el else ""
    japanese_title = title_el.get("data-jp", "") if title_el else ""
    payload = {
        "title": title,
        "japanese_title": japanese_title,
        "slug": slug or None,
        "poster": image or None,
    }
    payload.update(_animekai_parse_info_spans(node))
    return payload


def _animekai_normalize_title(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


async def _animekai_title_candidates(anilist_id: int) -> list[str]:
    gql = """
    query ($id: Int) {
        Media(id: $id, type: ANIME) {
            title { romaji english native }
            synonyms
        }
    }
    """
    data = await _anilist_query(gql, {"id": anilist_id})
    media = data.get("Media") or {}
    titles = []
    title_obj = media.get("title") or {}
    for value in [title_obj.get("english"), title_obj.get("romaji"), title_obj.get("native"), *(media.get("synonyms") or [])]:
        if value and value not in titles:
            titles.append(value)
    return titles


async def _animekai_lookup_slug(anilist_id: int) -> Optional[str]:
    cache_key = str(anilist_id)
    cached = _get_cache("animekai_lookup", cache_key)
    if cached is not None:
        return cached

    titles = await _animekai_title_candidates(anilist_id)
    best_slug = None
    normalized_targets = {_animekai_normalize_title(title) for title in titles if title}

    for title in titles:
        soup = await _animekai_fetch_soup(f"/browser?keyword={quote(title)}")
        results = []
        for item in soup.select("div.aitem"):
            parsed = _animekai_parse_item(item)
            if parsed.get("title") and parsed.get("slug"):
                results.append(parsed)
        for result in results:
            candidates = {
                _animekai_normalize_title(result.get("title", "")),
                _animekai_normalize_title(result.get("japanese_title", "")),
                _animekai_normalize_title((result.get("slug") or "").replace("-", " ")),
            }
            if normalized_targets.intersection({c for c in candidates if c}):
                _set_cache("animekai_lookup", cache_key, result["slug"], ttl_hours=6)
                return result["slug"]
        if not best_slug and results:
            best_slug = results[0]["slug"]

    _set_cache("animekai_lookup", cache_key, best_slug, ttl_hours=1)
    return best_slug


async def _animekai_build_provider(anilist_id: int) -> Optional[dict]:
    cache_key = str(anilist_id)
    cached = _get_cache("animekai_episodes", cache_key)
    if cached is not None:
        return cached

    slug = await _animekai_lookup_slug(anilist_id)
    if not slug:
        _set_cache("animekai_episodes", cache_key, None, ttl_hours=1)
        return None

    soup = await _animekai_fetch_soup(f"/watch/{slug}")
    sub_episodes = []
    dub_episodes = []
    for episode in soup.select(".eplist a, a.eplist, .eplist a[data-num]"):
        number = episode.get("data-num") or episode.get("num") or ""
        if not number:
            continue
        title_span = episode.select_one("span")
        payload = {
            "id": f"animekai:{slug}:{number}",
            "title": title_span.get_text(" ", strip=True) if title_span else f"Episode {number}",
            "number": int(number) if str(number).isdigit() else number,
        }
        if episode.get("data-sub") != "0":
            sub_episodes.append(payload.copy())
        if episode.get("data-dub") == "1":
            dub_episodes.append(payload.copy())

    if not sub_episodes and not dub_episodes:
        _set_cache("animekai_episodes", cache_key, None, ttl_hours=1)
        return None

    provider = {
        "id": "animekai",
        "name": "ThapaSIR",
        "episodes": {
            "sub": sub_episodes,
            "dub": dub_episodes,
        },
    }
    _set_cache("animekai_episodes", cache_key, provider, ttl_hours=1)
    return provider


async def _inject_animekai_provider(data: dict, anilist_id: int) -> dict:
    try:
        provider = await asyncio.wait_for(_animekai_build_provider(anilist_id), timeout=8)
    except Exception as e:
        print(f"[ANIMEKAI WARN] Failed to inject provider for {anilist_id}: {e}")
        return data

    if provider is None:
        return data

    providers = data.setdefault("providers", {})
    providers["animekai"] = deepcopy(provider)
    return data


async def _inject_extra_stream_providers(data: dict, anilist_id: int) -> dict:
    t0 = time.time()
    data = await _inject_animekai_provider(data, anilist_id)
    _log_timing(f"_inject_extra_stream_providers({anilist_id})", t0)
    return data


async def _fetch_anilist_episode_count(anilist_id: int) -> Optional[int]:
    """Get total episode count from AniList for the given anime."""
    gql = """
    query ($id: Int) {
        Media(id: $id, type: ANIME) {
            episodes
            nextAiringEpisode { episode }
        }
    }
    """
    try:
        data = await _anilist_query(gql, {"id": anilist_id})
        media = data.get("Media") or {}
        if media.get("episodes"):
            return int(media["episodes"])
        next_airing = media.get("nextAiringEpisode") or {}
        if next_airing.get("episode"):
            return int(next_airing["episode"]) - 1
    except Exception:
        pass
    return None


async def _animekai_only_episode_payload(anilist_id: int) -> Optional[dict]:
    provider = await _animekai_build_provider(anilist_id)
    if provider is None:
        return None
    return {
        "providers": {"animekai": deepcopy(provider)},
        "episodes": deepcopy(provider.get("episodes", {}).get("sub", [])),
        "animekaiOnly": True,
    }


def _animekai_parse_episode_id(episode_id: str) -> tuple[str, str]:
    parts = (episode_id or "").split(":")
    if len(parts) != 3 or parts[0] != "animekai":
        raise HTTPException(status_code=400, detail="Invalid AnimeKai episode ID")
    return parts[1], parts[2]


def _is_animekai_provider(provider: str) -> bool:
    return (provider or "").lower() in {"animekai", "anikai"}


def _normalize_animekai_watch_slug(slug: str) -> str:
    value = (slug or "").strip()
    if not value:
        raise HTTPException(status_code=404, detail="AnimeKai episode slug missing")

    if "watch/animekai/" in value:
        value = value.split("/")[-1]

    match = re.search(r"animekai-(\d+)", value)
    if match:
        return f"animekai-{match.group(1)}"

    match = re.search(r"(?:^|[-_/])ep(?:isode)?[-_/]?(\d+)$", value, re.IGNORECASE)
    if match:
        return f"animekai-{match.group(1)}"

    if value.isdigit():
        return f"animekai-{value}"

    match = re.search(r"(\d+)", value)
    if match:
        return f"animekai-{match.group(1)}"

    return value


async def _animekai_sources_from_episode_id(episode_id: str, category: str) -> dict:
    slug, episode = _animekai_parse_episode_id(episode_id)
    soup = await _animekai_fetch_soup(f"/watch/{slug}/ep-{episode}")
    servers = []
    subtitles = []
    seen_urls = set()
    seen_subtitles = set()
    for group in soup.select(".server-items.lang-group"):
        lang = (group.get("data-id") or "").lower()
        if lang and lang != category.lower():
            continue
        for server in group.select(".server-video.server"):
            video = server.get("data-video", "").strip()
            if not video:
                continue
            for subtitle in _extract_animekai_subtitles_from_url(video):
                key = (subtitle.get("url"), subtitle.get("lang"), subtitle.get("label"))
                if key in seen_subtitles:
                    continue
                seen_subtitles.add(key)
                subtitles.append(subtitle)
            resolved = await _animekai_resolve_embed(video)
            for source in resolved.get("sources", []):
                src = source.get("url")
                if not src:
                    continue
                source_type = source.get("type") or ("hls" if ".m3u8" in src else "")
                if source_type != "hls" and ".m3u8" not in src:
                    continue
                if src in seen_urls:
                    continue
                seen_urls.add(src)
                servers.append(
                    {
                        "url": src,
                        "type": "hls",
                        "referer": f"{ANIMEKAI_BASE}/",
                        "headers": {
                            "Referer": f"{ANIMEKAI_BASE}/",
                            "User-Agent": ANIMEKAI_UA,
                        },
                    }
                )
            for subtitle in resolved.get("subtitles", []):
                normalized = _normalize_subtitle_entry(subtitle)
                if not normalized:
                    continue
                key = (normalized.get("url"), normalized.get("lang"), normalized.get("label"))
                if key in seen_subtitles:
                    continue
                seen_subtitles.add(key)
                subtitles.append(normalized)
        if servers:
            break

    if not servers:
        raise HTTPException(
            status_code=404,
            detail={
                "message": "No Anikai streams found",
                "animeSlug": slug,
                "episode": episode,
                "category": category,
                "url": f"{ANIMEKAI_BASE}/watch/{slug}/ep-{episode}",
            },
        )

    normalized_streams = []
    for index, stream in enumerate(servers, start=1):
        stream["name"] = f"ThapaSIR-{index}"
        stream["server"] = f"ThapaSIR-{index}"
        stream["quality"] = f"ThapaSIR-{index}"
        normalized_streams.append(stream)

    return {
        "headers": {"Referer": f"{ANIMEKAI_BASE}/", "User-Agent": ANIMEKAI_UA},
        "provider": "animekai",
        "providerName": "ThapaSIR",
        "streams": normalized_streams,
        "subtitles": subtitles,
        "download": None,
    }


async def _animekai_resolve_embed(url: str) -> dict:
    html = await _animekai_fetch_text(url)
    soup = BeautifulSoup(html, "html.parser")
    sources = []
    subtitles = []
    seen = set()
    seen_subs = set()
    for source in soup.select("source[src]"):
        src = source.get("src", "").strip()
        if src and src not in seen:
            seen.add(src)
            sources.append({"url": src, "type": source.get("type") or ("hls" if ".m3u8" in src else None)})

    for track in soup.select("track[src]"):
        src = track.get("src", "").strip()
        if not src:
            continue
        label = track.get("label") or track.get("srclang") or "Subtitle"
        lang = track.get("srclang")
        sub_type = "vtt" if ".vtt" in src.lower() else None
        key = (src, lang, label)
        if key in seen_subs:
            continue
        seen_subs.add(key)
        subtitles.append({"url": src, "label": label, "lang": lang, "type": sub_type})

    for match in re.findall(r"https?://[^\"'\s]+\.m3u8[^\"'\s]*", html):
        if match not in seen:
            seen.add(match)
            sources.append({"url": match, "type": "hls"})

    return {
        "success": True,
        "embed_url": url,
        "sources": sources,
        "subtitles": subtitles,
        "provider": urlparse(url).hostname or "",
    }


# ---------------------------------------------------------------------------
# MKissa provider (allanime / mkissa.to)
# ---------------------------------------------------------------------------

MKISSA_BASE = "https://mkissa.to"
MKISSA_API = "https://api.allanime.day/api"
MKISSA_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
MKISSA_HEADERS = {"User-Agent": MKISSA_UA, "Content-Type": "application/json", "Accept": "application/json"}

MKISSA_SEARCH_QUERY = """
query($search: SearchInput, $limit: Int, $page: Int, $translationType: VaildTranslationTypeEnumType) {
  shows(search: $search, limit: $limit, page: $page, translationType: $translationType) {
    edges { _id name englishName nativeName slugTime thumbnail }
  }
}
"""

MKISSA_EPISODE_QUERY = """
query($showId: String!, $translationType: VaildTranslationTypeEnumType!, $episodeString: String!) {
  episode(showId: $showId, translationType: $translationType, episodeString: $episodeString) {
    episodeString sourceUrls notes
  }
}
"""


def _is_mkissa_provider(provider: str) -> bool:
    return (provider or "").lower() in {"mkissa", "allanime", "allmanga", "anime"}


async def _mkissa_graphql(query: str, variables: dict) -> dict:
    headers = {**MKISSA_HEADERS, "Origin": MKISSA_BASE, "Referer": f"{MKISSA_BASE}/anime"}
    async with httpx.AsyncClient(http2=True, timeout=httpx.Timeout(30.0), follow_redirects=True) as cl:
        r = await cl.post(MKISSA_API, json={"query": query, "variables": variables}, headers=headers)
        if r.status_code != 200:
            raise HTTPException(502, f"MKissa API error: {r.status_code}")
        body = r.json()
        if "errors" in body:
            raise HTTPException(502, f"MKissa API error: {body['errors']}")
        return body["data"]


async def _mkissa_search_by_title(title: str, translation_type: str = "sub") -> dict:
    data = await _mkissa_graphql(MKISSA_SEARCH_QUERY, {
        "search": {"sortBy": "Trending", "query": title},
        "limit": 10,
        "page": 1,
        "translationType": translation_type,
    })
    shows = data.get("shows") or {}
    edges = shows.get("edges") or []
    for edge in edges:
        name = (edge.get("englishName") or edge.get("name") or "").lower()
        if title.lower() in name or name in title.lower():
            return edge
    if edges:
        return edges[0]
    return None


async def _mkissa_sources(show_id: str, episode_num: str, translation_type: str = "sub") -> dict:
    try:
        data = await _mkissa_graphql(MKISSA_EPISODE_QUERY, {
            "showId": show_id,
            "translationType": translation_type,
            "episodeString": episode_num,
        })
    except HTTPException:
        return {"streams": [], "subtitles": [], "error": "Episode not found on MKissa"}
    ep = data.get("episode")
    if not ep:
        return {"streams": [], "subtitles": [], "error": "Episode not found on MKissa"}
    sources = []
    raw = ep.get("sourceUrls")
    if isinstance(raw, list):
        for src in raw:
            if isinstance(src, dict):
                url = (src.get("sourceUrl") or "").strip()
                if url:
                    sources.append({"url": url, "type": "hls" if ".m3u8" in url else "mp4"})
            elif isinstance(src, str):
                url = src.strip()
                if url:
                    sources.append({"url": url, "type": "hls" if ".m3u8" in url else "mp4"})
    elif isinstance(raw, str):
        import json as _json
        try:
            parsed = _json.loads(raw)
            if isinstance(parsed, list):
                for src in parsed:
                    if isinstance(src, dict):
                        url = (src.get("sourceUrl") or src.get("url") or "").strip()
                        if url:
                            sources.append({"url": url, "type": "hls" if ".m3u8" in url else "mp4"})
        except (_json.JSONDecodeError, TypeError):
            url = raw.strip()
            if url:
                sources.append({"url": url, "type": "hls" if ".m3u8" in url else "mp4"})
    return {
        "streams": sources,
        "subtitles": [],
        "headers": {"Referer": f"{MKISSA_BASE}/", "User-Agent": MKISSA_UA},
    }


async def _mkissa_sources_from_episode_id(episode_id: str, category: str, anilist_id: int) -> dict:
    parts = episode_id.split(":", 2)
    if len(parts) == 3 and parts[0] == "mkissa":
        show_id = parts[1]
        episode_num = parts[2]
        return await _mkissa_sources(show_id, episode_num, category)
    episode_num = episode_id.strip().lstrip("0") or "1"
    title = ""
    try:
        gql = f"""query {{ Media(id: {anilist_id}) {{ title {{ english romaji native }} }} }}"""
        info = await _anilist_query(gql)
        media = info.get("Media", {}) or info.get("data", {}).get("Media", {})
        title = (media.get("title", {}).get("english") or media.get("title", {}).get("romaji") or media.get("title", {}).get("native") or "")
    except Exception:
        pass
    if not title:
        return {"streams": [], "subtitles": [], "error": "Cannot resolve title for MKissa search"}
    found = await _mkissa_search_by_title(title, category)
    if not found:
        return {"streams": [], "subtitles": [], "error": "Show not found on MKissa"}
    show_id = found.get("_id") or found.get("slugTime") or ""
    if not show_id:
        return {"streams": [], "subtitles": [], "error": "MKissa show ID not found"}
    return await _mkissa_sources(show_id, episode_num, category)


@app.get("/animekai/search", include_in_schema=False)
async def animekai_search(q: str = Query(..., min_length=1, description="Anime search query")):
    soup = await _animekai_fetch_soup(f"/browser?keyword={quote(q)}")
    results = []
    for item in soup.select("div.aitem"):
        parsed = _animekai_parse_item(item)
        if parsed["title"]:
            results.append(parsed)
    return {"success": True, "query": q, "count": len(results), "results": results}


@app.get("/animekai/home", include_in_schema=False)
async def animekai_home():
    soup = await _animekai_fetch_soup("/home")
    banner = []
    for slide in soup.select(".swiper-slide"):
        style = slide.get("style", "")
        bg_match = re.search(r"url\(([^)]+)\)", style)
        title_el = slide.select_one("p.title")
        watch_btn = slide.select_one("a.watch-btn")
        item = {
            "title": title_el.get_text(" ", strip=True) if title_el else "",
            "japanese_title": title_el.get("data-jp", "") if title_el else "",
            "description": _animekai_text(slide, "p.desc"),
            "poster": bg_match.group(1) if bg_match else "",
            "url": _animekai_absolute_url(watch_btn.get("href", "")) if watch_btn else "",
            "rating": "",
            "release": "",
            "quality": "",
        }
        item.update(_animekai_parse_info_spans(slide))
        for detail in slide.select(".mics > div"):
            label = _animekai_text(detail, "div").lower()
            value = _animekai_text(detail, "span")
            if label == "rating":
                item["rating"] = value
            elif label == "release":
                item["release"] = value
            elif label == "quality":
                item["quality"] = value
        if item["title"]:
            banner.append(item)

    latest_updates = []
    for item in soup.select("div.aitem"):
        parsed = _animekai_parse_item(item)
        anchor = item.select_one("a")
        href = anchor.get("href", "") if anchor else ""
        parsed["current_episode"] = href.split("/ep-")[1] if "/ep-" in href else ""
        parsed["url"] = _animekai_absolute_url(href.split("/ep-")[0]) if href else ""
        if parsed["title"]:
            latest_updates.append(parsed)

    top_trending = {}
    for block in soup.select(".aitem-col.top-anime"):
        tab_id = block.get("data-id", "trending")
        label = {
            "trending": "NOW",
            "day": "DAY",
            "week": "WEEK",
            "month": "MONTH",
        }.get(tab_id, tab_id.upper())
        entries = []
        for item in block.select("div.aitem"):
            parsed = _animekai_parse_item(item)
            rank = _animekai_text(item, ".num")
            style = item.get("style", "")
            bg_match = re.search(r"url\(([^)]+)\)", style)
            if bg_match:
                parsed["poster"] = bg_match.group(1)
            if parsed["title"]:
                entries.append({"rank": rank, **parsed})
        top_trending[label] = entries

    return {
        "success": True,
        "banner": banner,
        "latest_updates": latest_updates,
        "top_trending": top_trending,
    }


@app.get("/animekai/anime/{slug}", include_in_schema=False)
async def animekai_anime(slug: str):
    soup = await _animekai_fetch_soup(f"/watch/{slug}")
    title_el = soup.select_one("h1.title")
    title = title_el.get_text(" ", strip=True) if title_el else slug.replace("-", " ")
    info = _animekai_parse_info_spans(soup)
    bg_node = soup.select_one(".watch-section-bg")
    banner_style = bg_node.get("style", "") if bg_node else ""
    banner_match = re.search(r"url\(([^)]+)\)", banner_style)

    detail = {}
    for row in soup.select(".detail > div > div"):
        text = row.get_text(" ", strip=True)
        if ":" in text:
            key, value = text.split(":", 1)
            detail[key.strip().lower().replace(" ", "_")] = value.strip()

    seasons = []
    for season in soup.select(".swiper-wrapper.season .aitem"):
        poster_link = season.select_one("a.poster")
        seasons.append(
            {
                "title": _animekai_text(season, ".detail span"),
                "episodes": _animekai_text(season, ".btn"),
                "poster": _animekai_extract_image(season.select_one("img")),
                "url": _animekai_absolute_url(poster_link.get("href", "")) if poster_link else "",
                "active": "active" in season.get("class", []),
            }
        )

    episodes = []
    for episode in soup.select(".eplist a, a.eplist, .eplist a[data-num]"):
        number = episode.get("data-num") or episode.get("num") or ""
        href = episode.get("href", "") or f"/watch/{slug}/ep-{number}"
        if not number:
            continue
        title_span = episode.select_one("span")
        episodes.append(
            {
                "number": number,
                "slug": episode.get("data-slug") or episode.get("slug") or number,
                "title": title_span.get_text(" ", strip=True) if title_span else f"Episode {number}",
                "japanese_title": title_span.get("data-jp", "") if title_span else "",
                "has_sub": episode.get("data-sub") != "0",
                "has_dub": episode.get("data-dub") == "1",
                "url": _animekai_absolute_url(href),
            }
        )

    return {
        "success": True,
        "title": title,
        "japanese_title": title_el.get("data-jp", "") if title_el else "",
        "description": _animekai_text(soup, ".desc"),
        "poster": _animekai_extract_image(soup.select_one(".poster img[itemprop='image']")) or _animekai_extract_image(soup.select_one(".poster img")),
        "banner": banner_match.group(1) if banner_match else "",
        "rating": _animekai_text(soup, ".rate-box .value"),
        "detail": detail,
        "seasons": seasons,
        "episodes": episodes,
        **info,
    }


@app.get("/animekai/episode/{slug}/{episode}", include_in_schema=False)
async def animekai_episode(slug: str, episode: int):
    soup = await _animekai_fetch_soup(f"/watch/{slug}/ep-{episode}")
    raw_servers = {}
    for group in soup.select(".server-items.lang-group"):
        lang = group.get("data-id", "unknown")
        entries = []
        for server in group.select(".server-video.server"):
            video = server.get("data-video", "").strip()
            if video:
                entries.append({"name": server.get_text(" ", strip=True), "url": video})
        if entries:
            raw_servers[lang] = entries

    resolved_servers = {}
    for lang, items in raw_servers.items():
        out = []
        for item in items:
            try:
                resolved = await _animekai_resolve_embed(item["url"])
                hls = next((source for source in resolved["sources"] if source["url"].endswith(".m3u8") or source.get("type") == "hls"), None)
                if hls:
                    out.append({"name": item["name"], "url": hls["url"], "type": "hls"})
                else:
                    out.append({"name": item["name"], "url": item["url"], "type": "embed", "note": "could not resolve to HLS"})
            except Exception:
                out.append({"name": item["name"], "url": item["url"], "type": "embed", "note": "resolve failed"})
        if out:
            resolved_servers[lang] = out

    title = _animekai_text(soup, "h1.title") or slug.replace("-", " ")
    bg_node = soup.select_one(".watch-section-bg")
    banner_style = bg_node.get("style", "") if bg_node else ""
    banner_match = re.search(r"url\(([^)]+)\)", banner_style)
    return {
        "success": True,
        "title": title,
        "slug": slug,
        "episode": episode,
        "poster": _animekai_extract_image(soup.select_one(".poster img")),
        "banner": banner_match.group(1) if banner_match else "",
        "servers": resolved_servers,
    }


@app.get("/animekai/stream", include_in_schema=False)
async def animekai_stream(url: str = Query(..., description="AnimeKai embed URL")):
    if not url.startswith("http"):
        raise HTTPException(status_code=400, detail="Invalid URL")
    return await _animekai_resolve_embed(url)

@app.get("/filter")
async def filter_anime(
    genre: Optional[str] = Query(None, description="Genre name, e.g. Action, Romance"),
    tag: Optional[str] = Query(None, description="Tag name, e.g. Isekai, Time Skip"),
    year: Optional[int] = Query(None, description="Season year, e.g. 2025"),
    season: Optional[str] = Query(None, description="WINTER, SPRING, SUMMER, or FALL"),
    format: Optional[str] = Query(None, description="TV, MOVIE, OVA, ONA, SPECIAL, MUSIC"),
    status: Optional[str] = Query(None, description="RELEASING, FINISHED, NOT_YET_RELEASED, CANCELLED, HIATUS"),
    sort: str = Query("POPULARITY_DESC", description="Sort order"),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=50),
):
    """Advanced anime filter with genre, tag, year, season, format, status, and sort."""
    # Build dynamic argument string
    args = ["type: ANIME", f"sort: [{SORT_MAP.get(sort, 'POPULARITY_DESC')}]"]
    variables = {"page": page, "perPage": per_page}

    if genre:
        args.append("genre: $genre")
        variables["genre"] = genre
    if tag:
        args.append("tag: $tag")
        variables["tag"] = tag
    if year:
        args.append("seasonYear: $seasonYear")
        variables["seasonYear"] = year
    if season:
        args.append("season: $season")
        variables["season"] = season.upper()
    if format:
        args.append("format: $format")
        variables["format"] = format.upper()
    if status:
        args.append("status: $status")
        variables["status"] = status.upper()

    # Build variable type declarations
    var_types = ["$page: Int", "$perPage: Int"]
    if genre:
        var_types.append("$genre: String")
    if tag:
        var_types.append("$tag: String")
    if year:
        var_types.append("$seasonYear: Int")
    if season:
        var_types.append("$season: MediaSeason")
    if format:
        var_types.append("$format: MediaFormat")
    if status:
        var_types.append("$status: MediaStatus")

    gql = f"""
    query ({', '.join(var_types)}) {{
        Page(page: $page, perPage: $perPage) {{
            pageInfo {{ total currentPage lastPage hasNextPage perPage }}
            media({', '.join(args)}) {{
                {MEDIA_LIST_FIELDS}
            }}
        }}
    }}
    """
    data = await _anilist_query(gql, variables)
    page_data = data.get("Page", {})
    page_info = page_data.get("pageInfo", {})
    response = {
        "page": page_info.get("currentPage", page),
        "perPage": page_info.get("perPage", per_page),
        "total": page_info.get("total", 0),
        "hasNextPage": page_info.get("hasNextPage", False),
        "results": page_data.get("media", []),
    }
    return _proxy_deep_images(response)


# ─── Collection Endpoints (with pagination) ─────────────────────────────────

async def _fetch_collection(sort_type: str, status: str = None, page: int = 1, per_page: int = 20):
    """Internal helper for fetching collections like trending, popular, etc."""
    status_filter = f", status: {status}" if status else ""
    gql = f"""
    query ($page: Int, $perPage: Int) {{
        Page(page: $page, perPage: $perPage) {{
            pageInfo {{ total currentPage lastPage hasNextPage perPage }}
            media(type: ANIME, sort: [{sort_type}]{status_filter}) {{
                {MEDIA_LIST_FIELDS}
            }}
        }}
    }}
    """
    data = await _anilist_query(gql, {"page": page, "perPage": per_page})
    page_data = data.get("Page", {})
    page_info = page_data.get("pageInfo", {})
    response = {
        "page": page_info.get("currentPage", page),
        "perPage": page_info.get("perPage", per_page),
        "total": page_info.get("total", 0),
        "hasNextPage": page_info.get("hasNextPage", False),
        "results": page_data.get("media", []),
    }
    return _proxy_deep_images(response)


@app.get("/spotlight")
async def get_spotlight():
    """Get the spotlight anime – high-priority trending and popular titles."""
    gql = f"""
    query {{
        Page(page: 1, perPage: 10) {{
            media(sort: [TRENDING_DESC, POPULARITY_DESC], type: ANIME) {{
                {MEDIA_LIST_FIELDS}
            }}
        }}
    }}
    """
    data = await _anilist_query(gql)
    media = data.get("Page", {}).get("media", [])
    return _proxy_deep_images({"results": media})


@app.get("/trending")
async def get_trending(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=50),
):
    """Get trending anime with full metadata and pagination."""
    async def fn():
        return await _fetch_collection("TRENDING_DESC", page=page, per_page=per_page)
    return await _cached_response("trending", 600, fn, str(page), str(per_page))


@app.get("/popular")
async def get_popular(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=50),
):
    """Get most popular anime of all time with full metadata and pagination."""
    async def fn():
        return await _fetch_collection("POPULARITY_DESC", page=page, per_page=per_page)
    return await _cached_response("popular", 600, fn, str(page), str(per_page))


@app.get("/upcoming")
async def get_upcoming(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=50),
):
    """Get upcoming anime with full metadata and pagination."""
    async def fn():
        return await _fetch_collection("POPULARITY_DESC", "NOT_YET_RELEASED", page=page, per_page=per_page)
    return await _cached_response("upcoming", 600, fn, str(page), str(per_page))


@app.get("/recent")
async def get_recent(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=50),
):
    """Get currently airing anime with full metadata and pagination."""
    async def fn():
        return await _fetch_collection("START_DATE_DESC", "RELEASING", page=page, per_page=per_page)
    return await _cached_response("recent", 600, fn, str(page), str(per_page))


@app.get("/home-data")
async def get_home_data():
    """Combined homepage data: trending + popular + recent + upcoming in one request."""

    async def fetch_fn():
        async def fetch_section(sort, status=None, limit=20):
            return await _fetch_collection(sort, status, page=1, per_page=limit)
        trending, popular, recent, upcoming = await asyncio.gather(
            fetch_section("TRENDING_DESC"),
            fetch_section("POPULARITY_DESC"),
            fetch_section("START_DATE_DESC", "RELEASING"),
            fetch_section("POPULARITY_DESC", "NOT_YET_RELEASED", limit=10),
        )
        return {
            "trending": trending.get("results", []),
            "popular": popular.get("results", []),
            "recent": recent.get("results", []),
            "upcoming": upcoming.get("results", []),
        }

    return await _cached_response("home", 600, fetch_fn, "v1")


@app.get("/schedule")
async def get_schedule(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=50),
):
    """Get upcoming airing schedule with UNIX timestamps and full anime metadata."""
    gql = f"""
    query ($page: Int, $perPage: Int) {{
        Page(page: $page, perPage: $perPage) {{
            pageInfo {{ total currentPage lastPage hasNextPage perPage }}
            airingSchedules(notYetAired: true, sort: TIME) {{
                episode
                airingAt
                timeUntilAiring
                media {{
                    {MEDIA_LIST_FIELDS}
                }}
            }}
        }}
    }}
    """
    data = await _anilist_query(gql, {"page": page, "perPage": per_page})
    page_data = data.get("Page", {})
    page_info = page_data.get("pageInfo", {})
    results = []
    for item in page_data.get("airingSchedules", []):
        entry = item.get("media", {})
        entry["next_episode"] = item.get("episode")
        entry["airingAt"] = item.get("airingAt")
        entry["timeUntilAiring"] = item.get("timeUntilAiring")
        results.append(entry)

    if not results:
        return _proxy_deep_images(await _jikan_schedule(page=page, per_page=per_page))

    response = {
        "page": page_info.get("currentPage", page),
        "perPage": page_info.get("perPage", per_page),
        "total": page_info.get("total", 0),
        "hasNextPage": page_info.get("hasNextPage", False),
        "results": results,
    }
    return _proxy_deep_images(response)


# ─── Anime Details ────────────────────────────────────────────────────────

@app.get("/info/mal-{mal_id}")
async def get_anime_info_by_mal(mal_id: int):
    """Resolve a MAL ID, then fetch metadata through the existing AniList info flow."""
    resolution = await _resolve_mal_to_anilist(mal_id)
    if not resolution:
        jikan_item = await _jikan_anime_by_mal(mal_id)
        if not jikan_item:
            return _mal_mapping_required_response(mal_id)
        media = _normalize_jikan_to_anilist(jikan_item)
        media["streamable"] = False
        media["needsMapping"] = True
        media["detail"] = "No MAL-to-AniList mapping found"
        media["malBackup"] = {
            "used": True,
            "malId": mal_id,
            "anilistId": None,
            "source": "jikan",
            "metadataOnly": True,
        }
        return _proxy_deep_images(media)

    anilist_id = resolution["anilistId"]
    try:
        media = await get_anime_info(anilist_id, malId=mal_id)
        if isinstance(media, dict):
            media["malBackup"] = {
                "used": True,
                "malId": mal_id,
                "anilistId": anilist_id,
                "source": resolution.get("source", "mapping"),
            }
        return media
    except HTTPException:
        # If AniList metadata is down but the mapping exists, return MAL metadata instead of 422/404.
        jikan_item = await _jikan_anime_by_mal(mal_id)
        if jikan_item:
            media = _normalize_jikan_to_anilist(jikan_item)
            media["id"] = anilist_id
            media["anilistId"] = anilist_id
            media["idMal"] = mal_id
            media["malBackup"] = {
                "used": True,
                "malId": mal_id,
                "anilistId": anilist_id,
                "source": resolution.get("source", "mapping"),
                "metadataSource": "jikan",
            }
            return _proxy_deep_images(media)
        raise


@app.get("/info/{anilist_id}")
async def get_anime_info(
    anilist_id: int,
    malId: Optional[int] = Query(None, description="Optional MAL ID for fallback when AniList is unavailable"),
):
    """Get complete anime page data — everything AniList has to offer."""
    async def fetch_fn():
        gql = f"""
        query ($id: Int) {{
            Media(id: $id, type: ANIME) {{
                {MEDIA_FULL_FIELDS}
            }}
        }}
        """
        data = await _anilist_query(gql, {"id": anilist_id})
        anilist_error = data.get("__anilist_error__")
        media = data.get("Media")
        if anilist_error in {404, 429, 500, 502, 503, 504} or not media:
            query_mal_id = malId if isinstance(malId, int) else None
            local_mal_id = _get_local_mal_id_for_anilist(anilist_id)
            fallback_mal_id = query_mal_id or local_mal_id or await _resolve_anilist_to_mal_with_anizip(anilist_id)
            if fallback_mal_id is not None:
                jikan_item = await _jikan_anime_by_mal(fallback_mal_id)
                if jikan_item:
                    media = _normalize_jikan_to_anilist(jikan_item)
                    media["id"] = anilist_id
                    media["anilistId"] = anilist_id
                    media["idMal"] = fallback_mal_id
                    media["malBackup"] = {
                        "used": True,
                        "malId": fallback_mal_id,
                        "anilistId": anilist_id,
                        "source": "query_param" if query_mal_id else ("local_mapping" if local_mal_id else "ani_zip_reverse"),
                        "metadataSource": "jikan",
                        "fallbackReason": f"anilist_{anilist_error}" if anilist_error else "anilist_empty_media",
                    }
                    return _proxy_deep_images(media)

            if anilist_error == 429:
                raise HTTPException(
                    status_code=503,
                    detail="AniList is rate-limiting right now. Please retry shortly.",
                )
            if anilist_error == 404:
                raise HTTPException(status_code=404, detail="Anime not found")
            raise HTTPException(
                status_code=503,
                detail="AniList is temporarily unavailable. Please retry shortly.",
            )
        return _proxy_deep_images(media)

    return await _cached_response("anime_info", 21600, fetch_fn, str(anilist_id))


@app.get("/anime/{anilist_id}/characters")
async def get_anime_characters(
    anilist_id: int,
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=50),
):
    """Get paginated character list with voice actors for an anime."""
    gql = """
    query ($id: Int, $page: Int, $perPage: Int) {
        Media(id: $id, type: ANIME) {
            id
            title { romaji english }
            characters(sort: [ROLE, RELEVANCE], page: $page, perPage: $perPage) {
                pageInfo { total currentPage lastPage hasNextPage perPage }
                edges {
                    role
                    node {
                        id
                        name { full native userPreferred }
                        image { large medium }
                        description
                        gender
                        dateOfBirth { year month day }
                        age
                        favourites
                        siteUrl
                    }
                    voiceActors {
                        id
                        name { full native }
                        image { large }
                        languageV2
                    }
                }
            }
        }
    }
    """
    data = await _anilist_query(gql, {"id": anilist_id, "page": page, "perPage": per_page})
    media = data.get("Media")
    if not media:
        raise HTTPException(status_code=404, detail="Anime not found")
    chars = media.get("characters", {})
    page_info = chars.get("pageInfo", {})
    response = {
        "page": page_info.get("currentPage", page),
        "perPage": page_info.get("perPage", per_page),
        "total": page_info.get("total", 0),
        "hasNextPage": page_info.get("hasNextPage", False),
        "characters": chars.get("edges", []),
    }
    return _proxy_deep_images(response)


@app.get("/anime/{anilist_id}/relations")
async def get_anime_relations(anilist_id: int):
    """Get all related anime/manga for an anime (sequels, prequels, side stories, etc.)."""
    gql = """
    query ($id: Int) {
        Media(id: $id, type: ANIME) {
            id
            title { romaji english }
            relations {
                edges {
                    relationType(version: 2)
                    node {
                        id
                        title { romaji english native }
                        coverImage { large }
                        bannerImage
                        format
                        type
                        status
                        episodes
                        chapters
                        meanScore
                        averageScore
                        popularity
                        startDate { year month day }
                    }
                }
            }
        }
    }
    """
    data = await _anilist_query(gql, {"id": anilist_id})
    media = data.get("Media")
    if not media:
        raise HTTPException(status_code=404, detail="Anime not found")
    response = {
        "id": media["id"],
        "title": media["title"],
        "relations": media.get("relations", {}).get("edges", []),
    }
    return _proxy_deep_images(response)


@app.get("/anime/{anilist_id}/recommendations")
async def get_anime_recommendations(
    anilist_id: int,
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=25),
):
    """Get paginated community recommendations for an anime."""
    gql = """
    query ($id: Int, $page: Int, $perPage: Int) {
        Media(id: $id, type: ANIME) {
            id
            title { romaji english }
            recommendations(sort: RATING_DESC, page: $page, perPage: $perPage) {
                pageInfo { total currentPage lastPage hasNextPage perPage }
                nodes {
                    rating
                    mediaRecommendation {
                        id
                        title { romaji english native }
                        coverImage { large extraLarge }
                        bannerImage
                        format
                        episodes
                        status
                        meanScore
                        averageScore
                        popularity
                        genres
                        startDate { year }
                    }
                }
            }
        }
    }
    """
    data = await _anilist_query(gql, {"id": anilist_id, "page": page, "perPage": per_page})
    media = data.get("Media")
    if not media:
        raise HTTPException(status_code=404, detail="Anime not found")
    recs = media.get("recommendations", {})
    page_info = recs.get("pageInfo", {})
    response = {
        "page": page_info.get("currentPage", page),
        "perPage": page_info.get("perPage", per_page),
        "total": page_info.get("total", 0),
        "hasNextPage": page_info.get("hasNextPage", False),
        "recommendations": recs.get("nodes", []),
    }
    return _proxy_deep_images(response)


# ─── Streaming (Pipe-based with MAL backup) ─────────────────────────────────

async def _anizone_search_cached(query: str) -> dict:
    async def fetch_fn():
        return {"results": await _anizone_provider.search(query)}

    return await _cached_response("anizone_search", ANIZONE_CACHE_SEARCH_TTL, fetch_fn, query.strip().lower())


async def _anizone_episodes_cached(anime_url: str) -> dict:
    normalized_url = normalize_anizone_url(anime_url)

    async def fetch_fn():
        return {"episodes": await _anizone_provider.get_episodes(normalized_url)}

    return await _cached_response("anizone_episodes:v2", ANIZONE_CACHE_EPISODES_TTL, fetch_fn, normalized_url)


async def _anizone_sources_cached(episode_url: str) -> dict:
    normalized_url = normalize_anizone_url(episode_url)

    async def fetch_fn():
        return await _anizone_provider.get_sources(normalized_url)

    return await _cached_response("anizone_sources", ANIZONE_CACHE_SOURCES_TTL, fetch_fn, normalized_url)


async def _anizone_health_cached() -> dict:
    async def fetch_fn():
        return await _anizone_provider.health()

    return await _cached_response("anizone_health", ANIZONE_CACHE_HEALTH_TTL, fetch_fn, "status")


async def _anizone_title_candidates(anilist_id: int) -> dict:
    gql = """
    query ($id: Int) {
      Media(id: $id, type: ANIME) {
        id
        title { romaji english native }
        synonyms
        seasonYear
        episodes
        startDate { year }
      }
    }
    """
    data = await _anilist_query(gql, {"id": anilist_id})
    media = data.get("Media") or {}
    title_data = media.get("title") or {}
    titles = []
    for value in (title_data.get("english"), title_data.get("romaji"), title_data.get("native")):
        if value and value.isascii() and value not in titles:
            titles.append(value)
    for value in media.get("synonyms") or []:
        if value and value.isascii() and value not in titles:
            titles.append(value)
    return {
        "titles": titles,
        "year": media.get("seasonYear") or (media.get("startDate") or {}).get("year"),
        "episodes": media.get("episodes"),
    }


def _score_anizone_match(candidate: dict, titles: list[str], year=None, episode_count=None) -> int:
    candidate_title = _normalize_title_for_match(candidate.get("title", ""))
    normalized_titles = [_normalize_title_for_match(title) for title in titles if title]
    if not candidate_title or not normalized_titles:
        return 0

    score = 0
    if candidate_title in normalized_titles:
        score += 100
    elif any(candidate_title in title or title in candidate_title for title in normalized_titles):
        score += 70
    elif any(set(candidate_title.split()) & set(title.split()) for title in normalized_titles):
        score += 25

    info = str(candidate.get("info") or "")
    if year and str(year) in info:
        score += 15
    available = candidate.get("availableEpisodes") or 0
    if episode_count and available and int(available) == int(episode_count):
        score += 15
    return score


async def _find_anizone_match(anilist_id: int) -> Optional[dict]:
    if _is_disabled_stream_provider("anizone"):
        print(f"[Anizone] fallback reason=disabled anilistId={anilist_id}")
        return None

    info = await _anizone_title_candidates(anilist_id)
    titles = info.get("titles") or []
    if not titles:
        print(f"[Anizone] fallback reason=no_titles anilistId={anilist_id}")
        return None

    title_hash = _anizone_title_hash(titles)

    async def fetch_fn():
        best = None
        best_score = 0
        for title in titles[:5]:
            try:
                search_data = await _anizone_search_cached(title)
                for candidate in search_data.get("results", []):
                    score = _score_anizone_match(candidate, titles, info.get("year"), info.get("episodes"))
                    if score > best_score:
                        best = candidate
                        best_score = score
                if best_score >= 100:
                    break
            except Exception as exc:
                print(f"[Anizone] search failed title={_safe_log_value(title)}: {exc}")
                continue
        if not best or best_score < 70:
            print(f"[Anizone] fallback reason=no_confident_match anilistId={anilist_id} score={best_score}")
            return {}
        print(f"[Anizone] match anilistId={anilist_id} title={_safe_log_value(best.get('title'))} score={best_score}")
        return {"match": best, "score": best_score}

    cached = await _cached_response("anizone_match", ANIZONE_CACHE_MATCH_TTL, fetch_fn, str(anilist_id), title_hash)
    return cached.get("match")


async def _inject_anizone_provider(data: dict, anilist_id: int) -> dict:
    if _is_disabled_stream_provider("anizone"):
        return data
    try:
        match = await _find_anizone_match(anilist_id)
        if not match:
            return data
        episodes_data = await _anizone_episodes_cached(match["id"])
        episodes = episodes_data.get("episodes") or []
        if not episodes:
            print(f"[Anizone] fallback reason=no_episodes anilistId={anilist_id}")
            return data
        providers = data.setdefault("providers", {})
        providers["anizone"] = _anizone_episode_response(anilist_id, episodes)
        return _order_stream_providers(data)
    except Exception as exc:
        print(f"[Anizone] fallback reason=exception anilistId={anilist_id} error={exc}")
        return data


@app.get("/episodes/mal-{mal_id}")
async def get_episodes_by_mal_slug(mal_id: int):
    """Get episodes from a MAL ID by resolving to numeric AniList internally."""
    backup = await _fetch_mal_backup_episode_data(mal_id)
    if "response" in backup:
        return backup["response"]

    data = backup["data"]
    data["malBackup"] = {
        "used": True,
        "malId": mal_id,
        "source": backup["source"],
        "anilistId": backup["anilistId"],
    }
    data = await _inject_anizone_provider(data, backup["anilistId"])
    return _proxy_deep_images(_order_stream_providers(_remove_disabled_stream_providers(_inject_mal_source_slugs(data, mal_id))))


@app.get("/episodes/{anilist_id}")
async def get_episodes(
    anilist_id: int,
    malId: Optional[int] = Query(None, description="Optional MAL ID to use only if the AniList lookup fails"),
):
    """Get the episode list for an anime, with MAL backup only after AniList fails."""
    async def fetch_fn():
        anizone_data = await _inject_anizone_provider({"providers": {}}, anilist_id)
        has_anizone = bool(anizone_data.get("providers", {}).get("anizone"))
        if has_anizone:
            return _proxy_deep_images(_order_stream_providers(anizone_data))

        data, error = await _try_episode_fetch({"anilistId": anilist_id})
        if data is not None:
            data = await _inject_anizone_provider(data, anilist_id)
            return _proxy_deep_images(_order_stream_providers(_inject_source_slugs(data, anilist_id)))

        try:
            animekai_only = await _animekai_only_episode_payload(anilist_id)
        except Exception as exc:
            print(f"[ANIMEKAI WARN] AnimeKai-only fallback failed for {anilist_id}: {exc}")
            animekai_only = None
        if animekai_only is not None:
            animekai_only = await _inject_anizone_provider(animekai_only, anilist_id)
            return _proxy_deep_images(_order_stream_providers(_inject_source_slugs(animekai_only, anilist_id)))

        if malId is None:
            _raise_pipe_lookup_error(error, "AniList episode lookup failed")

        backup = await _fetch_mal_backup_episode_data(malId)
        if "response" in backup:
            return backup["response"]

        data = backup["data"]
        data["malBackup"] = {
            "used": True,
            "malId": malId,
            "source": backup["source"],
            "anilistId": backup["anilistId"],
        }
        data = await _inject_anizone_provider(data, backup["anilistId"])
        return _proxy_deep_images(_order_stream_providers(_inject_mal_source_slugs(data, malId)))

    data = await _cached_response("episodes:v2", 43200, fetch_fn, str(anilist_id))
    return _order_stream_providers(_remove_disabled_stream_providers(data))


@app.get("/episodes-by-mal/{malId}")
async def get_episodes_by_mal(malId: int):
    """Get episodes using the explicit MAL backup route."""
    return await get_episodes_by_mal_slug(malId)


@app.get("/anizone/health")
async def anizone_health():
    return await _anizone_health_cached()


@app.get("/anizone/search")
async def anizone_search(q: str = Query(..., min_length=1)):
    return await _anizone_search_cached(q)


@app.get("/anizone/episodes")
async def anizone_episodes(url: str = Query(..., description="Anizone anime URL or base64-url encoded URL")):
    try:
        return await _anizone_episodes_cached(url)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid Anizone URL")
    except Exception:
        raise HTTPException(status_code=502, detail="Anizone episodes unavailable")


@app.get("/anizone/sources")
async def anizone_sources(url: str = Query(..., description="Anizone episode URL or base64-url encoded URL")):
    try:
        return await _anizone_sources_cached(url)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid Anizone URL")
    except Exception:
        raise HTTPException(status_code=502, detail="Anizone source unavailable")


@app.get("/sources")
async def get_sources(
    episodeId: str = Query(..., description="Plain-text episode ID from /episodes response"),
    provider: str = Query(..., description="Provider name, e.g. kiwi, arc, telli"),
    anilistId: int = Query(..., description="AniList anime ID"),
    category: str = Query("sub", description="sub or dub"),
):
    """Get M3U8 streaming sources for a specific episode."""
    if _is_disabled_stream_provider(provider):
        raise HTTPException(status_code=410, detail=f"Provider '{provider}' is disabled")

    if (provider or "").lower() == "anizone":
        try:
            episode_url = _decode_anizone_episode_id(episodeId)
            data = await _anizone_sources_cached(episode_url)
            data["provider"] = "anizone"
            return _proxy_deep_images(data)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid Anizone URL")
        except HTTPException:
            raise
        except Exception:
            return JSONResponse(
                status_code=502,
                content={
                    "detail": "Anizone source unavailable",
                    "provider": "anizone",
                    "fallbackAvailable": True,
                },
            )

    if _is_animekai_provider(provider):
        data = await _animekai_sources_from_episode_id(episodeId, category)
        data = await _prepare_source_payload(
            data,
            provider="animekai",
            category=category,
            episode_id=episodeId,
            anilist_id=anilistId,
            anime_query={"anilistId": anilistId},
        )
        return _proxy_deep_images(data)

    if _is_mkissa_provider(provider):
        data = await _mkissa_sources_from_episode_id(episodeId, category, anilistId)
        data = await _prepare_source_payload(
            data,
            provider="mkissa",
            category=category,
            episode_id=episodeId,
            anilist_id=anilistId,
            anime_query={"anilistId": anilistId},
        )
        return _proxy_deep_images(data)

    data = await _fetch_raw_sources(
        episode_id=episodeId,
        provider=provider,
        category=category,
        anime_query={"anilistId": anilistId},
    )
    data = await _prepare_source_payload(
        data,
        provider=provider,
        category=category,
        episode_id=episodeId,
        anilist_id=anilistId,
        anime_query={"anilistId": anilistId},
    )
    return _proxy_deep_images(data)

@app.get("/watch/{provider}/{anilist_id}/{category}/{slug:path}")
async def get_watch_sources(provider: str, anilist_id: str, category: str, slug: str):
    """The super simple sources endpoint resolving slugs (prefix-number) back to provider IDs."""
    if isinstance(anilist_id, str) and anilist_id.startswith("mal-"):
        try:
            mal_id = int(anilist_id.split("-", 1)[1])
        except (IndexError, ValueError):
            raise HTTPException(status_code=422, detail="Invalid MAL route segment")
        return await get_watch_sources_by_mal(mal_id, provider, category, slug)

    try:
        resolved_anilist_id = int(anilist_id)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="Invalid AniList route segment")

    if (provider or "").lower() == "anizone":
        return await get_sources(episodeId=slug, provider="anizone", anilistId=resolved_anilist_id, category=category)

    if _is_animekai_provider(provider):
        normalized_slug = _normalize_animekai_watch_slug(slug)
        match = re.search(r"animekai-(\d+)", normalized_slug)
        if not match:
            raise HTTPException(status_code=404, detail=f"Episode slug '{slug}' not found for provider {provider}")
        episode_number = match.group(1)
        anime_slug = await _animekai_lookup_slug(resolved_anilist_id)
        if not anime_slug:
            raise HTTPException(
                status_code=404,
                detail={"message": "Anikai slug lookup failed", "anilistId": resolved_anilist_id},
            )
        target_id = f"animekai:{anime_slug}:{episode_number}"
        return await get_sources(episodeId=target_id, provider="animekai", anilistId=resolved_anilist_id, category=category)

    if _is_mkissa_provider(provider):
        episode_number = slug.strip().lstrip("0") or "1"
        return await get_sources(episodeId=episode_number, provider="mkissa", anilistId=resolved_anilist_id, category=category)

    data = await _fetch_raw_episodes(resolved_anilist_id)
    target_id = _resolve_slug_to_episode_id(data, provider, category, slug)

    if not target_id:
        raise HTTPException(status_code=404, detail=f"Episode slug '{slug}' not found for provider {provider}")

    try:
        return await get_sources(episodeId=target_id, provider=provider, anilistId=resolved_anilist_id, category=category)
    except HTTPException:
        raise
    except Exception as e:
        print(f"[WATCH WARN] Provider {provider} failed for {anilist_id}: {e}")
        return {"streams": [], "subtitles": [], "provider": provider, "error": "Provider unavailable"}


@app.get("/watch-by-mal/{malId}/{provider}/{category}/{episodeId:path}")
async def get_watch_sources_by_mal(malId: int, provider: str, category: str, episodeId: str):
    """Resolve MAL to numeric AniList, then use the existing watch/source logic."""
    resolution = await _resolve_mal_to_anilist(malId)
    if not resolution:
        return _mal_mapping_required_response(malId)

    anilist_id = resolution["anilistId"]
    if (provider or "").lower() == "anizone":
        return await get_sources(episodeId=episodeId, provider="anizone", anilistId=anilist_id, category=category)

    if _is_animekai_provider(provider):
        normalized_slug = _normalize_animekai_watch_slug(episodeId)
        match = re.search(r"animekai-(\d+)", normalized_slug)
        if not match:
            raise HTTPException(status_code=404, detail=f"Episode slug '{episodeId}' not found for provider {provider}")
        episode_number = match.group(1)
        anime_slug = await _animekai_lookup_slug(anilist_id)
        if not anime_slug:
            raise HTTPException(
                status_code=404,
                detail={"message": "Anikai slug lookup failed", "anilistId": anilist_id, "malId": malId},
            )
        target_id = f"animekai:{anime_slug}:{episode_number}"
        return await get_sources(episodeId=target_id, provider="animekai", anilistId=anilist_id, category=category)

    if _is_mkissa_provider(provider):
        episode_number = episodeId.strip().lstrip("0") or "1"
        return await get_sources(episodeId=episode_number, provider="mkissa", anilistId=anilist_id, category=category)

    data = await _fetch_raw_episodes(anilist_id)
    target_id = _resolve_slug_to_episode_id(data, provider, category, episodeId)
    if not target_id:
        raise HTTPException(status_code=404, detail=f"Episode slug '{episodeId}' not found for provider {provider}")

    try:
        return await get_sources(
            episodeId=target_id,
            provider=provider,
            anilistId=anilist_id,
            category=category,
        )
    except HTTPException:
        raise
    except Exception as e:
        print(f"[WATCH WARN] Provider {provider} failed for {anilist_id}: {e}")
        return {"streams": [], "subtitles": [], "provider": provider, "error": "Provider unavailable"}


# ─── Health / Debug Endpoints ──────────────────────────────────────────

@app.get("/health/redis")
async def health_redis():
    """Check Redis connection health."""
    t0 = time.time()
    r = await _get_redis()
    if not r:
        return {
            "redis_connected": False,
            "error": "REDIS_URL not configured or connection failed",
            "response_time_ms": round((time.time() - t0) * 1000),
        }
    try:
        pong = await r.ping()
        info = await r.info("server")
        return {
            "redis_connected": True,
            "ping": str(pong),
            "redis_version": info.get("redis_version", "unknown"),
            "response_time_ms": round((time.time() - t0) * 1000),
        }
    except Exception as e:
        return {
            "redis_connected": False,
            "ping": str(e),
            "error": str(e),
            "response_time_ms": round((time.time() - t0) * 1000),
        }
