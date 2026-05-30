"""
AniZone Unified — Native scrapers + CDN proxy, no AniList.
Designed to be mounted alongside the Miruro API.
"""

import re
import json
from typing import Optional
import httpx
from fastapi import HTTPException, Query, APIRouter
from starlette.concurrency import run_in_threadpool
from bs4 import BeautifulSoup

ANIZONE_BASE = "https://anizone.to"
ANIZONE_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
ANIZONE_HEADERS = {"User-Agent": ANIZONE_UA, "Accept": "text/html, */*", "Accept-Language": "en-US,en;q=0.9"}

router = APIRouter(prefix="/anizone", tags=["AniZone"])


def _fetch(url: str, referer: str = "", params: dict | None = None, timeout: float = 30.0) -> str:
    headers = dict(ANIZONE_HEADERS)
    if referer:
        headers["Referer"] = referer
    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as c:
            r = c.get(url, headers=headers, params=params or {})
    except Exception as e:
        raise HTTPException(502, f"Fetch failed: {e}")
    if r.status_code == 403 and "cdn-cgi" in r.text:
        raise HTTPException(503, "Cloudflare challenge")
    if r.status_code != 200:
        raise HTTPException(r.status_code, f"Failed: {r.status_code}")
    return r.text


def _parse_search(html: str) -> list:
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for item in soup.select("[wire\\:key]"):
        wk = item.get("wire:key", "")
        if not wk.startswith("a-") or "-t-" in wk:
            continue
        slug = wk[2:]
        if not slug:
            continue
        link = item.select_one("a[href*='/anime/" + slug + "']")
        if not link:
            continue
        title = link.get("title", "") or link.get_text(strip=True) or slug
        img = item.select_one("div.absolute img, img")
        img_src = img.get("src", "") if img else ""
        meta_div = item.select_one(".line-clamp-1")
        meta_spans = meta_div.find_all("span") if meta_div else []
        meta = [s.get_text(strip=True) for s in meta_spans if s.get_text(strip=True)]
        results.append({
            "slug": slug, "title": title, "image": img_src,
            "type": meta[0] if len(meta) > 0 else "",
            "year": meta[1] if len(meta) > 1 else "",
            "episodes": meta[2] if len(meta) > 2 else "",
            "status": meta[3] if len(meta) > 3 else "",
        })
    return results


def _parse_total_pages(html: str) -> int:
    """Detect total episode pages from pagination links. Returns 1 if no pagination."""
    soup = BeautifulSoup(html, "html.parser")
    nums = set()
    for link in soup.select("a[href*='?page='], a[href*='&page=']"):
        m = re.search(r"[?&]page=(\d+)", link.get("href", ""))
        if m:
            nums.add(int(m.group(1)))
    if not nums:
        for link in soup.select("nav a[href]"):
            m = re.search(r"/anime/[^/]+(?:\?.*)?$", link.get("href", ""))
            if m:
                nums.add(1)
    return max(nums) if nums else 1


def _parse_episode_cards(soup, slug, episodes: list, seen_nums: set):
    """Extract episode cards from a parsed page into the given lists."""
    for card in soup.select(f"a[href*='/anime/{slug}/']"):
        href = card.get("href", "")
        m = re.search(rf"/anime/{re.escape(slug)}/(\d+)\b", href)
        if not m:
            continue
        num = int(m.group(1))
        if num in seen_nums:
            continue
        seen_nums.add(num)
        h3 = card.select_one("h3")
        ep_title = h3.get_text(strip=True) if h3 else ""
        ep_img = card.select_one("img[src]")
        ep_img_src = ep_img.get("src", "") if ep_img else ""
        episodes.append({"number": num, "title": ep_title, "image": ep_img_src})


def _parse_episodes_from_scripts(soup, slug, episodes: list, seen_nums: set):
    """Fallback: extract episode numbers from embedded JSON/JS in script tags."""
    for script in soup.select("script"):
        text = script.string or ""
        # Look for Livewire initial-data attribute JSON
        for el in soup.select("[wire\\:initial-data]"):
            raw = el.get("wire:initial-data", "")
            if not raw:
                continue
            try:
                data = json.loads(raw)
                for key in ("episodes", "episodeList", "data"):
                    items = data.get(key) or {}
                    if isinstance(items, dict):
                        for k, v in items.items():
                            if isinstance(v, dict) and v.get("number"):
                                num = int(v["number"])
                                if num not in seen_nums:
                                    seen_nums.add(num)
                                    episodes.append({"number": num, "title": v.get("title", f"Episode {num}"), "image": v.get("image", "") or v.get("poster", "")})
                    elif isinstance(items, list):
                        for v in items:
                            if isinstance(v, dict) and v.get("number"):
                                num = int(v["number"])
                                if num not in seen_nums:
                                    seen_nums.add(num)
                                    episodes.append({"number": num, "title": v.get("title", f"Episode {num}"), "image": v.get("image", "") or v.get("poster", "")})
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
        # Look for JSON arrays in script content
        for m in re.finditer(r'(?:episodes|episodeList|data)\s*[:=]\s*(\[[\s\S]*?\])\s*[;,]', text):
            try:
                items = json.loads(m.group(1))
                for v in items:
                    if isinstance(v, dict) and v.get("number"):
                        num = int(v["number"])
                        if num not in seen_nums:
                            seen_nums.add(num)
                            episodes.append({"number": num, "title": v.get("title", f"Episode {num}"), "image": v.get("image", "") or v.get("poster", "")})
            except (json.JSONDecodeError, TypeError, ValueError):
                pass


def _parse_mal_id(html: str) -> Optional[int]:
    """Extract MyAnimeList ID from an anizone anime page, if present."""
    soup = BeautifulSoup(html, "html.parser")
    for link in soup.select("a[href*='myanimelist.net/anime/']"):
        m = re.search(r"myanimelist\.net/anime/(\d+)", link.get("href", ""))
        if m:
            return int(m.group(1))
    return None


def _parse_info(html: str, slug: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    title_el = soup.select_one("h1")
    title = title_el.get_text(strip=True) if title_el else slug
    poster = soup.select_one("div.flex.items-start > div img[src]")
    image = poster.get("src", "") if poster else ""
    info_div = soup.select_one("div.grid.grid-cols-1.gap-6")
    anime_type = status = year = ""
    if info_div:
        for span in info_div.find_all("span"):
            t = span.get_text(strip=True)
            if not t or len(t) > 30:
                continue
            if t in ("TV Series", "OVA", "Movie", "ONA", "Special", "Web", "Music", "Unknown", "TV Special"):
                anime_type = t
            elif t in ("Completed", "Airing", "Not yet aired"):
                status = t
            elif re.match(r"^\d{4}$", t):
                year = t
    episodes = []
    seen_nums = set()
    _parse_episode_cards(soup, slug, episodes, seen_nums)
    _parse_episodes_from_scripts(soup, slug, episodes, seen_nums)
    episodes.sort(key=lambda e: e["number"])
    return {
        "slug": slug, "title": title, "image": image,
        "type": anime_type, "year": year, "status": status,
        "episodes": episodes,
    }


def _parse_episode(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    media_player = soup.select_one("media-player")
    src = ""
    if media_player:
        src = media_player.get("src", "")
    if not src:
        for s in soup.find_all("script"):
            text = s.string or ""
            m = re.search(r'src["\']?\s*[:=]\s*["\']([^"\']+master\.m3u8[^"\']*)', text)
            if m:
                src = m.group(1)
                break
    uuid = ""
    cdn_base = "https://seiryuu.vid-cdn.xyz"
    if src:
        m = re.search(r"(https?://[^/]+)", src)
        if m:
            cdn_base = m.group(1).rstrip("/")
        m = re.search(r"/([a-f0-9-]{36})/", src)
        if m:
            uuid = m.group(1)
    if not uuid or not src:
        raise HTTPException(404, "No video source found")
    subs = []
    for track in soup.select("track"):
        ts = track.get("src", "")
        label = track.get("label", "")
        kind = track.get("kind", "")
        if ts and kind == "subtitles":
            subs.append({"url": ts, "label": label, "kind": kind})
    poster = ""
    poster_el = soup.select_one("media-poster")
    if poster_el:
        poster = poster_el.get("src", "")
    cdn_domain = cdn_base.split("://", 1)[-1]
    proxy_pref = f"/anizone/cdn/{cdn_domain}/{uuid}"
    def to_proxy(url_str: str) -> str:
        if not url_str:
            return ""
        m = re.search(r"//([^/]+)", url_str)
        if m:
            rest = url_str.split("://", 1)[-1]
            return f"/anizone/cdn/{rest}"
        return f"{proxy_pref}/{url_str.split('/')[-1]}"
    return {
        "url": to_proxy(src), "url_original": src, "uuid": uuid,
        "subtitles": [{"url": to_proxy(s["url"]), "url_original": s["url"], "label": s["label"], "kind": s["kind"]} for s in subs],
        "chapters": f"{proxy_pref}/chapters.vtt",
        "storyboard": f"{proxy_pref}/storyboard.vtt",
        "poster": to_proxy(poster) or f"{proxy_pref}/snapshot.webp",
        "teaser": f"{proxy_pref}/teaser.webp",
    }


# ─── Native Endpoints (slug-based, for frontend) ──────────────────────

@router.get("/search")
async def search(q: str = Query(..., min_length=1), page: int = Query(1, ge=1)):
    try:
        html = await run_in_threadpool(_fetch, f"{ANIZONE_BASE}/anime", f"{ANIZONE_BASE}/", {"search": q, "page": str(page)})
    except HTTPException as e:
        return {"error": str(e.detail), "results": []}
    results = _parse_search(html)
    return {"query": q, "page": page, "results": results, "total": len(results)}


@router.get("/info/{slug}")
async def info(slug: str):
    slug = slug.strip().split("/")[0]
    try:
        html = await run_in_threadpool(_fetch, f"{ANIZONE_BASE}/anime/{slug}", f"{ANIZONE_BASE}/anime")
    except HTTPException as e:
        raise HTTPException(e.status_code, detail=f"Failed: {e.detail}")
    return _parse_info(html, slug)


@router.get("/episode/{slug}/{episode_num}")
async def episode(slug: str, episode_num: int):
    slug = slug.strip().split("/")[0]
    try:
        html = await run_in_threadpool(_fetch, f"{ANIZONE_BASE}/anime/{slug}/{episode_num}", f"{ANIZONE_BASE}/anime/{slug}")
    except HTTPException as e:
        raise HTTPException(e.status_code, detail=f"Failed: {e.detail}")
    data = _parse_episode(html)
    data["slug"] = slug
    data["episode"] = episode_num
    return data


@router.get("/popular")
async def popular(page: int = Query(1, ge=1)):
    try:
        html = await run_in_threadpool(_fetch, f"{ANIZONE_BASE}/anime?page={page}", f"{ANIZONE_BASE}/")
    except HTTPException as e:
        return {"error": str(e.detail), "results": []}
    results = _parse_search(html)
    return {"page": page, "results": results}


# ─── CDN Proxy ────────────────────────────────────────────────────────

@router.get("/cdn/{path:path}")
async def cdn_proxy(path: str):
    from fastapi.responses import Response
    url = f"https://{path}"
    headers = {"User-Agent": ANIZONE_UA, "Origin": ANIZONE_BASE, "Referer": f"{ANIZONE_BASE}/"}
    try:
        with httpx.Client(timeout=30.0, follow_redirects=True) as c:
            r = c.get(url, headers=headers)
    except Exception as e:
        raise HTTPException(500, str(e))
    if r.status_code != 200:
        raise HTTPException(r.status_code, "CDN proxy error")
    ct = r.headers.get("content-type", "")
    is_m3u8 = "m3u8" in ct or path.endswith(".m3u8")
    is_vtt = "vtt" in ct or path.endswith(".vtt")
    if is_m3u8 or is_vtt:
        lines = []
        for line in r.text.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or stripped.startswith("WEBVTT") or stripped.startswith("NOTE"):
                lines.append(line)
                continue
            if stripped.startswith("http://") or stripped.startswith("https://"):
                lines.append(f"/anizone/cdn/{stripped.split('://',1)[1]}")
            elif stripped.startswith("/"):
                lines.append(f"/anizone/cdn{stripped}")
            else:
                base_path = path.rsplit("/", 1)[0] if "/" in path else ""
                resolved = f"{base_path}/{stripped}" if base_path else stripped
                lines.append(f"/anizone/cdn/{resolved}")
        media_type = "application/vnd.apple.mpegurl" if is_m3u8 else "text/vtt"
        return Response(content="\n".join(lines), media_type=media_type)
    return Response(content=r.content, media_type=ct or "application/octet-stream")
