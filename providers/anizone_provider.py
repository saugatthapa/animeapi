import asyncio
import base64
import os
import re
import time
from typing import Optional
from urllib.parse import quote_plus, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup


ANIZONE_BASE_URL = os.getenv("ANIZONE_BASE_URL", "https://anizone.to").rstrip("/")
ANIZONE_TIMEOUT_SECONDS = float(os.getenv("ANIZONE_TIMEOUT_SECONDS", "8"))
ANIZONE_SEARCH_TIMEOUT_SECONDS = float(os.getenv("ANIZONE_SEARCH_TIMEOUT_SECONDS", "6"))
ANIZONE_EPISODES_TIMEOUT_SECONDS = float(os.getenv("ANIZONE_EPISODES_TIMEOUT_SECONDS", str(ANIZONE_TIMEOUT_SECONDS)))
ANIZONE_SOURCES_TIMEOUT_SECONDS = float(os.getenv("ANIZONE_SOURCES_TIMEOUT_SECONDS", str(ANIZONE_TIMEOUT_SECONDS)))

ANIZONE_HEADERS = {
    "User-Agent": os.getenv(
        "ANIZONE_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Referer": ANIZONE_BASE_URL,
}


class AnizoneProviderError(Exception):
    pass


def safe_text(element) -> str:
    if element is None:
        return ""
    return re.sub(r"\s+", " ", element.get_text(" ", strip=True)).strip()


def safe_log_value(value: str) -> str:
    return str(value or "").encode("ascii", errors="backslashreplace").decode("ascii")


def absolute_url(path_or_url: str) -> str:
    return urljoin(f"{ANIZONE_BASE_URL}/", path_or_url or "")


def _decode_possible_base64(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return raw
    if raw.startswith(("http://", "https://", "/")):
        return raw
    try:
        padding = "=" * (-len(raw) % 4)
        decoded = base64.urlsafe_b64decode((raw + padding).encode()).decode()
        if decoded.startswith(("http://", "https://", "/")):
            return decoded
    except Exception:
        pass
    return raw


def encode_anizone_url(value: str) -> str:
    normalized = normalize_anizone_url(value)
    return base64.urlsafe_b64encode(normalized.encode()).decode().rstrip("=")


def is_allowed_anizone_url(value: str) -> bool:
    raw = _decode_possible_base64(value)
    if raw.startswith("/"):
        return True
    parsed = urlparse(raw)
    return parsed.scheme in {"http", "https"} and parsed.netloc.lower() == urlparse(ANIZONE_BASE_URL).netloc.lower()


def normalize_anizone_url(value: str) -> str:
    raw = _decode_possible_base64(value)
    if not is_allowed_anizone_url(raw):
        raise ValueError("Invalid Anizone URL")
    return absolute_url(raw)


def extract_episode_number(title: str) -> str:
    match = re.search(r"Episode\s+(\d+(?:\.\d+)?)", title or "", re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r"(\d+(?:\.\d+)?)", title or "")
    return match.group(1) if match else (title or "").strip()


def detect_stream_type(url: str) -> str:
    clean = (url or "").split("?", 1)[0].lower()
    if clean.endswith(".mp4"):
        return "mp4"
    return "hls"


def detect_subtitle_format(url: str, data_type: Optional[str] = None) -> str:
    if data_type:
        return data_type.strip().lower()
    clean = (url or "").split("?", 1)[0].lower()
    for ext in (".ass", ".srt", ".vtt"):
        if clean.endswith(ext):
            return ext[1:]
    return "unknown"


def _episode_sort_key(item: dict):
    try:
        return (0, float(item.get("number")))
    except (TypeError, ValueError):
        return (1, str(item.get("number") or item.get("title") or ""))


class AnizoneProvider:
    name = "anizone"
    display_name = "Anizone"
    base_url = ANIZONE_BASE_URL

    async def _fetch_soup(self, url: str, timeout: float) -> BeautifulSoup:
        if not is_allowed_anizone_url(url):
            raise ValueError("Invalid Anizone URL")
        normalized_url = normalize_anizone_url(url)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=ANIZONE_HEADERS) as client:
            response = await client.get(normalized_url)
            response.raise_for_status()
            return BeautifulSoup(response.text or "", "html.parser")

    async def search(self, query: str) -> list[dict]:
        query = (query or "").strip()
        if not query:
            return []
        print(f"[Anizone] search q={safe_log_value(query)}")
        url = f"{ANIZONE_BASE_URL}/anime?search={quote_plus(query)}"
        soup = await asyncio.wait_for(self._fetch_soup(url, ANIZONE_SEARCH_TIMEOUT_SECONDS), timeout=ANIZONE_SEARCH_TIMEOUT_SECONDS + 1)

        results = []
        for item in soup.select("div.grid > div.relative.overflow-hidden"):
            title_node = item.select_one("a[title]")
            if title_node is None:
                continue
            href = title_node.get("href") or ""
            title = (title_node.get("title") or safe_text(title_node)).strip()
            if not href or not title:
                continue
            info = safe_text(item.select_one(".text-xs"))
            eps_match = re.search(r"(\d+)\s*Eps", info, re.IGNORECASE)
            available_episodes = int(eps_match.group(1)) if eps_match else 0
            results.append(
                {
                    "id": absolute_url(href),
                    "provider": self.name,
                    "title": title,
                    "availableEpisodes": available_episodes,
                    "info": info,
                }
            )
        return results

    async def get_episodes(self, anime_url: str, start: int = 1, limit: int = 100) -> tuple[list, int, bool]:
        normalized_url = normalize_anizone_url(anime_url)

        all_episodes = []
        seen_urls = set()
        page = 1
        max_pages = 50
        has_more = False
        target_count = start + limit - 1

        async def fetch_page(p: int) -> BeautifulSoup:
            page_url = f"{normalized_url}?page={p}" if p > 1 else normalized_url
            return await asyncio.wait_for(
                self._fetch_soup(page_url, ANIZONE_EPISODES_TIMEOUT_SECONDS),
                timeout=ANIZONE_EPISODES_TIMEOUT_SECONDS + 1,
            )

        while page <= max_pages:
            soup = await fetch_page(page)

            candidates = list(soup.select("ul.grid li a"))
            if not candidates:
                anime_path = urlparse(normalized_url).path.rstrip("/")
                candidates = [
                    element
                    for element in soup.select("a[href]")
                    if urlparse(absolute_url(element.get("href") or "")).path.startswith(f"{anime_path}/")
                ]

            if not candidates:
                break

            page_has_new = False
            for element in candidates:
                href = element.get("href") or ""
                if not href:
                    continue
                source_id = absolute_url(href)
                if source_id in seen_urls:
                    continue
                page_has_new = True
                seen_urls.add(source_id)
                title = safe_text(element.select_one("h3")) or safe_text(element) or ""
                if "episode" not in title.lower():
                    title = f"Episode {extract_episode_number(source_id) or (len(all_episodes) + 1)}"
                number = extract_episode_number(title) or str(len(all_episodes) + 1)
                all_episodes.append(
                    {
                        "id": encode_anizone_url(source_id),
                        "provider": self.name,
                        "animeId": normalized_url,
                        "number": number,
                        "title": title,
                        "sourceId": source_id,
                    }
                )

            if not page_has_new:
                break

            # If we have enough episodes to cover the requested range,
            # peek at next page to determine has_more, then stop
            if len(all_episodes) >= target_count:
                try:
                    next_soup = await fetch_page(page + 1)
                    next_candidates = list(next_soup.select("ul.grid li a"))
                    if not next_candidates:
                        anime_path = urlparse(normalized_url).path.rstrip("/")
                        next_candidates = [
                            element
                            for element in next_soup.select("a[href]")
                            if urlparse(absolute_url(element.get("href") or "")).path.startswith(f"{anime_path}/")
                        ]
                    has_more = len(next_candidates) > 0
                except Exception:
                    has_more = False
                break

            page += 1

        all_episodes.sort(key=_episode_sort_key)
        total_known = len(all_episodes)
        sliced = all_episodes[max(0, start - 1):start - 1 + limit]
        print(f"[Anizone] episodes count={total_known} pages={page} has_more={has_more}")
        return sliced, total_known, has_more

    async def get_sources(self, episode_url: str) -> dict:
        normalized_url = normalize_anizone_url(episode_url)
        soup = await asyncio.wait_for(
            self._fetch_soup(normalized_url, ANIZONE_SOURCES_TIMEOUT_SECONDS),
            timeout=ANIZONE_SOURCES_TIMEOUT_SECONDS + 1,
        )

        streams = []
        subtitles = []
        player = soup.select_one("media-player")
        stream_url = absolute_url(player.get("src") or "") if player is not None else ""
        if stream_url:
            streams.append(
                {
                    "url": stream_url,
                    "type": detect_stream_type(stream_url),
                    "quality": "default",
                    "headers": {
                        "User-Agent": ANIZONE_HEADERS["User-Agent"],
                        "Referer": ANIZONE_BASE_URL,
                    },
                }
            )

        for track in soup.select('track[src][kind="subtitles"]'):
            src = track.get("src") or ""
            if not src:
                continue
            url = absolute_url(src)
            label = track.get("label") or "Subtitle"
            language = track.get("srclang") or "und"
            subtitles.append(
                {
                    "file": url,
                    "url": url,
                    "label": label,
                    "kind": "captions",
                    "language": language,
                    "lang": language,
                    "format": detect_subtitle_format(url, track.get("data-type")),
                }
            )

        print(f"[Anizone] sources streams={len(streams)} subtitles={len(subtitles)}")
        result = {
            "provider": self.name,
            "streams": streams,
            "subtitles": subtitles,
            "intro": None,
            "outro": None,
        }
        if not streams:
            result["warning"] = "No Anizone stream found"
        return result

    async def health(self) -> dict:
        started = time.time()
        try:
            soup = await asyncio.wait_for(self._fetch_soup(ANIZONE_BASE_URL, min(ANIZONE_TIMEOUT_SECONDS, 6)), timeout=min(ANIZONE_TIMEOUT_SECONDS, 6) + 1)
            ok = bool(soup.select_one("body"))
            return {
                "provider": self.name,
                "ok": ok,
                "response_time_ms": round((time.time() - started) * 1000),
            }
        except Exception as exc:
            return {
                "provider": self.name,
                "ok": False,
                "error": str(exc),
                "response_time_ms": round((time.time() - started) * 1000),
            }
