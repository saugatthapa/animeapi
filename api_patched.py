"""Runtime safety patch layer for the main FastAPI app.

This module imports the existing api.py app, then applies targeted fixes
without touching the large api.py file directly:
- selected /watch provider routes no longer run extra provider injection
- /episodes MAL routes no longer run slow extra provider injection
- AnimeKai network timeouts become non-fatal warnings
- AniZone direct routes use stronger title-based lookup and MAL fallback support

Run with: uvicorn api_patched:app
"""

import asyncio
import re
from copy import deepcopy
from typing import Optional

from bs4 import BeautifulSoup
from fastapi import HTTPException

try:
    from curl_cffi.requests.exceptions import Timeout as CurlTimeout, RequestException
except Exception:  # pragma: no cover - keeps import safe if curl_cffi changes
    CurlTimeout = TimeoutError
    RequestException = Exception

import api as _api

app = _api.app


def _empty_provider_response(provider: str, error: str = "Provider unavailable") -> dict:
    return {
        "streams": [],
        "subtitles": [],
        "provider": provider,
        "error": error,
    }


async def _safe_animekai_fetch_text(path: str, extra_headers: Optional[dict] = None) -> str:
    """Fetch AnimeKai HTML without allowing timeout/network errors to crash requests."""
    url = path if str(path).startswith("http") else _api._animekai_absolute_url(path)
    headers = dict(_api.ANIMEKAI_HEADERS)
    if extra_headers:
        headers.update(extra_headers)

    def _request():
        return _api.curl_requests.get(
            url,
            headers=headers,
            impersonate="chrome124",
            timeout=8,
            allow_redirects=True,
        )

    try:
        response = await asyncio.to_thread(_request)
    except CurlTimeout as exc:
        print(f"[ANIMEKAI TIMEOUT] {url}: {exc}")
        return ""
    except RequestException as exc:
        print(f"[ANIMEKAI REQUEST ERROR] {url}: {exc}")
        return ""
    except Exception as exc:
        print(f"[ANIMEKAI UNKNOWN ERROR] {url}: {exc}")
        return ""

    if getattr(response, "status_code", 0) >= 400:
        print(f"[ANIMEKAI HTTP {response.status_code}] {url}")
        return ""

    return getattr(response, "text", "") or ""


async def _safe_animekai_fetch_soup(path: str, extra_headers: Optional[dict] = None) -> BeautifulSoup:
    html = await _safe_animekai_fetch_text(path, extra_headers=extra_headers)
    return BeautifulSoup(html or "", "html.parser")


async def _safe_inject_animekai_provider(data: dict, anilist_id: int) -> dict:
    """Keep AnimeKai injection non-fatal anywhere old code still calls it."""
    try:
        provider = await asyncio.wait_for(_api._animekai_build_provider(anilist_id), timeout=8)
    except Exception as exc:
        print(f"[ANIMEKAI WARN] Failed to inject provider for {anilist_id}: {exc}")
        return data

    if provider is None:
        return data

    providers = data.setdefault("providers", {})
    providers["animekai"] = deepcopy(provider)
    return data


async def _safe_noop_extra_stream_provider_injection(data: dict, anilist_id: int) -> dict:
    """Never let background provider injection block /episodes or /watch routes."""
    return data


# Patch helper functions used by the imported app.
_api._animekai_fetch_text = _safe_animekai_fetch_text
_api._animekai_fetch_soup = _safe_animekai_fetch_soup
_api._inject_animekai_provider = _safe_inject_animekai_provider
_api._inject_extra_stream_providers = _safe_noop_extra_stream_provider_injection


async def safe_get_episodes_by_mal_slug(mal_id: int):
    """Fast MAL episode route without AnimeKai/AniZone injection."""
    backup = await _api._fetch_mal_backup_episode_data(mal_id)
    if "response" in backup:
        return backup["response"]

    data = backup["data"]
    data["malBackup"] = {
        "used": True,
        "malId": mal_id,
        "source": backup["source"],
        "anilistId": backup["anilistId"],
    }
    data = await _api._inject_anizone_provider(data, backup["anilistId"])
    return _api._proxy_deep_images(_api._order_stream_providers(_api._inject_mal_source_slugs(data, mal_id)))


async def safe_get_episodes_by_mal(malId: int):
    return await safe_get_episodes_by_mal_slug(malId)


async def safe_get_watch_sources(provider: str, anilist_id: str, category: str, slug: str):
    """Resolve only the selected provider for AniList routes."""
    if isinstance(anilist_id, str) and anilist_id.startswith("mal-"):
        try:
            mal_id = int(anilist_id.split("-", 1)[1])
        except (IndexError, ValueError):
            raise HTTPException(status_code=422, detail="Invalid MAL route segment")
        return await safe_get_watch_sources_by_mal(mal_id, provider, category, slug)

    try:
        resolved_anilist_id = int(anilist_id)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="Invalid AniList route segment")

    try:
        if (provider or "").lower() == "anizone":
            return await _api.get_sources(
                episodeId=slug,
                provider="anizone",
                anilistId=resolved_anilist_id,
                category=category,
            )

        if _api._is_animekai_provider(provider):
            normalized_slug = _api._normalize_animekai_watch_slug(slug)
            match = re.search(r"animekai-(\d+)", normalized_slug)
            if not match:
                raise HTTPException(status_code=404, detail=f"Episode slug '{slug}' not found for provider {provider}")
            episode_number = match.group(1)
            anime_slug = await _api._animekai_lookup_slug(resolved_anilist_id)
            if not anime_slug:
                raise HTTPException(
                    status_code=404,
                    detail={"message": "AnimeKai slug lookup failed", "anilistId": resolved_anilist_id},
                )
            target_id = f"animekai:{anime_slug}:{episode_number}"
            return await _api.get_sources(
                episodeId=target_id,
                provider="animekai",
                anilistId=resolved_anilist_id,
                category=category,
            )

        if _api._is_mkissa_provider(provider):
            episode_number = slug.strip().lstrip("0") or "1"
            return await _api.get_sources(episodeId=episode_number, provider="mkissa", anilistId=resolved_anilist_id, category=category)

        data = await _api._fetch_raw_episodes(resolved_anilist_id)
        target_id = _api._resolve_slug_to_episode_id(data, provider, category, slug)
        if not target_id:
            raise HTTPException(status_code=404, detail=f"Episode slug '{slug}' not found for provider {provider}")

        return await _api.get_sources(
            episodeId=target_id,
            provider=provider,
            anilistId=resolved_anilist_id,
            category=category,
        )
    except HTTPException:
        raise
    except Exception as exc:
        print(f"[WATCH WARN] Provider {provider} failed for {anilist_id}: {exc}")
        return _empty_provider_response(provider)


async def safe_get_watch_sources_by_mal(malId: int, provider: str, category: str, episodeId: str):
    """Resolve only the selected provider for MAL routes."""
    resolution = await _api._resolve_mal_to_anilist(malId)
    if not resolution:
        return _api._mal_mapping_required_response(malId)

    anilist_id = resolution["anilistId"]

    try:
        if (provider or "").lower() == "anizone":
            return await _api.get_sources(
                episodeId=episodeId,
                provider="anizone",
                anilistId=anilist_id,
                category=category,
            )

        if _api._is_animekai_provider(provider):
            normalized_slug = _api._normalize_animekai_watch_slug(episodeId)
            match = re.search(r"animekai-(\d+)", normalized_slug)
            if not match:
                raise HTTPException(status_code=404, detail=f"Episode slug '{episodeId}' not found for provider {provider}")
            episode_number = match.group(1)
            anime_slug = await _api._animekai_lookup_slug(anilist_id)
            if not anime_slug:
                raise HTTPException(
                    status_code=404,
                    detail={"message": "AnimeKai slug lookup failed", "anilistId": anilist_id, "malId": malId},
                )
            target_id = f"animekai:{anime_slug}:{episode_number}"
            return await _api.get_sources(
                episodeId=target_id,
                provider="animekai",
                anilistId=anilist_id,
                category=category,
            )

        if _api._is_mkissa_provider(provider):
            episode_number = episodeId.strip().lstrip("0") or "1"
            return await _api.get_sources(episodeId=episode_number, provider="mkissa", anilistId=anilist_id, category=category)

        data = await _api._fetch_raw_episodes(anilist_id)
        target_id = _api._resolve_slug_to_episode_id(data, provider, category, episodeId)
        if not target_id:
            raise HTTPException(status_code=404, detail=f"Episode slug '{episodeId}' not found for provider {provider}")

        return await _api.get_sources(
            episodeId=target_id,
            provider=provider,
            anilistId=anilist_id,
            category=category,
        )
    except HTTPException:
        raise
    except Exception as exc:
        print(f"[WATCH WARN] Provider {provider} failed for MAL {malId}: {exc}")
        return _empty_provider_response(provider)


def _remove_route(path: str, method: str = "GET") -> None:
    app.router.routes = [
        route
        for route in app.router.routes
        if not (getattr(route, "path", None) == path and method in getattr(route, "methods", set()))
    ]


def _move_route_before(path: str, before_path: str, method: str = "GET") -> None:
    """Move a just-added route before a more general route.

    FastAPI matches routes in order. If /episodes/{anilist_id} appears before
    /episodes/mal-{mal_id}, then /episodes/mal-21 is caught by the int route and
    returns 422. This fixes that order after patch routes are registered.
    """
    routes = app.router.routes
    target_index = next(
        (i for i, route in enumerate(routes) if getattr(route, "path", None) == path and method in getattr(route, "methods", set())),
        None,
    )
    if target_index is None:
        return
    route = routes.pop(target_index)
    before_index = next(
        (i for i, candidate in enumerate(routes) if getattr(candidate, "path", None) == before_path and method in getattr(candidate, "methods", set())),
        len(routes),
    )
    routes.insert(before_index, route)


# Replace original routes with safe versions.
_remove_route("/episodes/mal-{mal_id}")
_remove_route("/episodes-by-mal/{malId}")
_remove_route("/watch/{provider}/{anilist_id}/{category}/{slug:path}")
_remove_route("/watch-by-mal/{malId}/{provider}/{category}/{episodeId:path}")
app.add_api_route(
    "/episodes/mal-{mal_id}",
    safe_get_episodes_by_mal_slug,
    methods=["GET"],
)
app.add_api_route(
    "/episodes-by-mal/{malId}",
    safe_get_episodes_by_mal,
    methods=["GET"],
)
app.add_api_route(
    "/watch/{provider}/{anilist_id}/{category}/{slug:path}",
    safe_get_watch_sources,
    methods=["GET"],
)
app.add_api_route(
    "/watch-by-mal/{malId}/{provider}/{category}/{episodeId:path}",
    safe_get_watch_sources_by_mal,
    methods=["GET"],
)

# Keep specific MAL episodes above the generic /episodes/{anilist_id} route.
_move_route_before("/episodes/mal-{mal_id}", "/episodes/{anilist_id}")

print("[API PATCH] Safe watch routes, fast MAL episodes, and AnimeKai timeout handling enabled.")
