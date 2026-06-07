<div align="center">
  <img src="https://www.miruro.to/icon-512x512.png" alt="Miruro API" width="150" style="border-radius: 20%; box-shadow: 0 0 20px rgba(56, 189, 248, 0.5);">
  <br><br>
  
  # Miruro API v2.7
  
  **The ultimate, decrypted, and fully reverse-engineered native Python backend for Miruro.**
  
  [https://github.com/walterwhite-69/Miruro-API](https://github.com/walterwhite-69/Miruro-API)
</div>

<br>

---

## What This Does

Miruro's frontend communicates with its backend through a `secure/pipe` tunnel that base64-encodes, gzip-compresses, and encrypts every request. This project bypasses all of that and gives you simple, direct REST endpoints to:

1. **Search & filter** anime with full AniList metadata
2. **Get complete anime info** — characters, staff, relations, recommendations, trailer, stats, and all metadata in one request
3. **Browse collections** — trending, popular, upcoming, recent, schedule, and spotlight — all paginated
4. **List episodes** with decoded episode IDs from multiple providers
5. **Get M3U8 streaming URLs** for any episode
6. **Autocomplete** search suggestions for dropdown UIs
7. **Read manga** through MangaDex search, metadata, chapter, and page endpoints

No headless browsers, no Selenium — just lightweight async HTTP requests.

<br>

## Access Control

This API supports two access patterns:

1. Allowed browser origins via `ALLOWED_ORIGINS`
2. API-key access from any website or mobile app via `x-api-key`

Environment variables:

```txt
ALLOWED_ORIGINS=https://animio.co,https://www.animio.co,https://animio.qzz.io,http://localhost:3000,http://localhost:5173,http://127.0.0.1:3000,http://127.0.0.1:5173
API_KEY=your-secret-key
ALLOW_API_KEY_ANY_ORIGIN=1
```

If `API_KEY` is set:

- requests with a valid `x-api-key` are accepted even if their website/app origin is not listed in `ALLOWED_ORIGINS`
- mobile apps can use the API without needing a browser origin
- requests without a valid API key still fall back to the normal origin/referer checks

Send the key with:

```txt
x-api-key: your-secret-key
```

Set `ALLOW_API_KEY_ANY_ORIGIN=0` if you ever want to disable this behavior.

<br>

## All Endpoints

### 🔍 Search & Discovery

| Endpoint | Description | Params |
|---|---|---|
| `GET /search?query={name}` | Full-text anime search with rich metadata (20+ fields per result) | `query` (required), `page`=1, `per_page`=20 |
| `GET /suggestions?query={name}` | Lightweight autocomplete for dropdowns — returns id, title, poster, format, status, year. Max 8 results. | `query` (required) |
| `GET /filter` | Advanced browse/filter by any combination of genre, tag, year, season, format, status, sort | All optional — see below |

#### Filter Parameters

| Param | Values |
|---|---|
| `genre` | Action, Romance, Comedy, Drama, Fantasy, Sci-Fi, etc. |
| `tag` | Isekai, Time Skip, Reincarnation, etc. |
| `year` | 2025, 2024, etc. |
| `season` | WINTER · SPRING · SUMMER · FALL |
| `format` | TV · MOVIE · OVA · ONA · SPECIAL |
| `status` | RELEASING · FINISHED · NOT_YET_RELEASED · CANCELLED |
| `sort` | SCORE_DESC · POPULARITY_DESC · TRENDING_DESC · START_DATE_DESC |
| `page` / `per_page` | Pagination (defaults: 1 / 20, max per_page: 50) |

---

### 📊 Collections (All Paginated)

| Endpoint | Description |
|---|---|
| `GET /trending` | Currently trending anime |
| `GET /popular` | Most popular anime of all time |
| `GET /upcoming` | Most anticipated upcoming anime |
| `GET /recent` | Currently airing / this season's anime |
| `GET /spotlight` | Curated "What's Hot" list (trending + popular) |
| `GET /schedule` | Airing schedule, AniList primary with Jikan/MAL fallback |

All collection endpoints accept `page` and `per_page` query params and return:

```json
{
  "page": 1,
  "perPage": 20,
  "total": 5000,
  "hasNextPage": true,
  "results": [ ... ]
}
```

Each anime in `results` includes 20+ fields: title (romaji/english/native), coverImage, bannerImage, format, season, seasonYear, episodes, duration, status, averageScore, meanScore, popularity, favourites, genres, source, countryOfOrigin, studios, nextAiringEpisode, startDate, endDate, and more.

---

### 📖 Anime Details

| Endpoint | Description |
|---|---|
| `GET /info/{anilist_id}` | **Complete anime page** — everything in one request |
| `GET /anime/{id}/characters` | Paginated character list with voice actors |
| `GET /anime/{id}/relations` | All related media (sequels, prequels, side stories, spin-offs) |
| `GET /anime/{id}/recommendations` | Community recommendations sorted by rating |

#### What `/info/{id}` Returns

Everything you need to build a full anime detail page:

- **Core**: id, idMal, title (romaji/english/native), description, coverImage, bannerImage
- **Metadata**: format, season, seasonYear, episodes, duration, status, source, countryOfOrigin
- **Scores**: averageScore, meanScore, popularity, favourites, trending
- **Taxonomy**: genres, tags (with rank & spoiler flag), synonyms, hashtag
- **People**: characters (25, with voice actors), staff (25, with roles)
- **Related**: relations (sequels/prequels/etc.), recommendations (10, with ratings)
- **Media**: trailer (YouTube/Dailymotion), streamingEpisodes, externalLinks
- **Stats**: scoreDistribution, statusDistribution
- **Studios**: name, isAnimationStudio, siteUrl
- **Dates**: startDate, endDate, nextAiringEpisode
- **Links**: siteUrl, externalLinks (MAL, official site, etc.)

---

### Manga Reader

Manga support is separate from anime streaming and uses MangaDex for readable chapters/pages. Jikan and AniList are used only for metadata-to-MangaDex resolution.

| Endpoint | Description |
|---|---|
| `GET /manga/search?q={title}` | Search MangaDex manga by title |
| `GET /manga/resolve?malId={mal_id}` | Resolve MAL manga ID to MangaDex UUID |
| `GET /manga/resolve?anilistId={anilist_id}` | Resolve AniList manga ID to MangaDex UUID |
| `GET /manga/resolve?title={title}` | Resolve by title when no ID exists |
| `GET /manga/{mangadex_id}` | Manga metadata, cover, links, tags, authors |
| `GET /manga/{mangadex_id}/chapters?lang=en` | Chapter list for a language |
| `GET /manga/chapter/{chapter_id}` | Single chapter metadata |
| `GET /manga/read/{chapter_id}` | Ordered MangaDex page image URLs |

Example flow:

```txt
GET /manga/search?q=witch+hat+atelier
GET /manga/{mangadex_id}/chapters?lang=en
GET /manga/read/{chapter_id}?quality=data-saver
```

Resolve response:

```json
{
  "resolved": true,
  "mangadexId": "67e7453b-9ee5-4ae5-9316-215b03e4a71d",
  "malId": 100035,
  "source": "local_mal_mapping"
}
```

Reader response:

```json
{
  "chapterId": "a8cfe9d2-57f2-4552-b415-fdbc6806aae3",
  "readable": true,
  "quality": "data-saver",
  "total": 23,
  "pages": [
    { "index": 1, "url": "https://.../data-saver/{hash}/1-page.jpg" }
  ]
}
```

If a chapter is only hosted by an official external source, `/manga/read/{chapter_id}` returns `readable: false` with `externalUrl`.

---

### ▶️ Streaming (3-Step Flow)

To get a video stream, follow these 3 steps in order:

#### Step 1: Get Episodes — `GET /episodes/{anilist_id}`

Returns all episodes from multiple providers organized by audio type. Anizone is the primary/default provider when a confident AniList title match is found; existing providers remain fallback.

AniList ID is always the primary streaming ID. MAL ID is backup-only:

- Normal flow: `GET /episodes/{anilist_id}`
- Backup-aware flow: `GET /episodes/{anilist_id}?malId={mal_id}`
- Explicit MAL backup: `GET /info/mal-{mal_id}` and `GET /episodes/mal-{mal_id}`
- Mapping check: `GET /resolve?malId={mal_id}`

Successful resolve response:

```json
{
  "malId": 59983,
  "anilistId": 182300,
  "resolved": true
}
```

MAL backup resolves `mal_id` to a numeric AniList ID before calling the streaming pipe. Resolution uses cache, `mappings.json`, ani.zip, then AniList `idMal` lookup only when AniList is reachable. Without a mapping it returns:

```json
{
  "streamable": false,
  "needsMapping": true,
  "detail": "No MAL-to-AniList mapping found"
}
```

```json
{
  "mappings": { "anilistId": 178005, "malId": 56885, "kitsuId": ... },
  "providers": {
    "anizone": {
      "episodes": {
        "sub": [
          {
            "id": "watch/anizone/178005/sub/ENCODED_EPISODE_URL",
            "provider": "anizone",
            "number": 1,
            "title": "Episode Title",
            "image": null,
            "airDate": null,
            "duration": null,
            "description": null,
            "filler": false
          }
        ],
        "dub": []
      }
    },
    "bonk": { "...": "fallback providers stay available" }
  }
}
```

When the MAL backup route is used, returned episode IDs use the explicit backup route:

```json
{
  "id": "watch-by-mal/6594/kiwi/sub/animepahe-1"
}
```

#### Step 2: Get Sources [SUPER SIMPLE]

Just take the direct `id` from the Step 1 response and use it as the URL. No manual parameters or complex IDs needed!

**Endpoint:** `GET /{id}`
**Example:** `GET /watch/anizone/178005/sub/ENCODED_EPISODE_URL`

For MAL backup responses, use the emitted backup ID, for example:
`GET /watch-by-mal/6594/kiwi/sub/animepahe-1`

Do not open `/watch/mal-{id}`; MAL playback is supported only through `/watch-by-mal/...`.

```json
{
  "streams": [
    { "url": "https://.../master.m3u8", "type": "hls", "quality": "1080p" }
  ],
  "subtitles": [
    { "file": "https://...", "label": "English", "kind": "captions" }
  ],
  "intro": { "start": 0, "end": 90 },
  "outro": { "start": 1300, "end": 1420 }
}
```

> [!TIP]
> This endpoint automatically handles provider selection and category matching. The frontend should call `/episodes/{anilistId}` first and then request the returned episode `id`; do not manually build watch URLs unless debugging.

<details>
<summary><b>Fallback / Detailed Option</b></summary>
If you need manual control, you can use the traditional endpoint:
`GET /sources?episodeId=...&provider=...&anilistId=...&category=...`

Anizone is also supported:
`GET /sources?provider=anizone&episodeId=ENCODED_EPISODE_URL&anilistId=21&category=sub`
</details>

#### Step 3: Play

Feed `streams[0].url` into any HLS player (Video.js, hls.js, VLC, mpv). Cached responses include `cached` and `response_time_ms` when served through the API cache. Anizone subtitles are returned in the `subtitles` array when the source page exposes subtitle tracks.

#### Anizone Debug Endpoints

```bash
curl "http://localhost:8000/anizone/health"
curl "http://localhost:8000/anizone/search?q=naruto"
curl "http://localhost:8000/anizone/episodes?url=ENCODED_ANIZONE_ANIME_URL"
curl "http://localhost:8000/anizone/sources?url=ENCODED_ANIZONE_EPISODE_URL"
curl "http://localhost:8000/episodes/21"
curl "http://localhost:8000/watch/anizone/21/sub/ENCODED_EPISODE_URL"
curl "http://localhost:8000/sources?provider=anizone&episodeId=ENCODED_EPISODE_URL&anilistId=21&category=sub"
```

#### Streaming Environment

```txt
ANIZONE_BASE_URL=https://anizone.to
ANIZONE_TIMEOUT_SECONDS=8
PRIMARY_STREAM_PROVIDER=anizone
STREAM_PROVIDER_ORDER=anizone,bonk,ally,dune,bee,hop,arc,zoro,jet,kiwi
DISABLED_STREAM_PROVIDERS=kiwi
ANIZONE_CACHE_SEARCH_TTL=1800
ANIZONE_CACHE_EPISODES_TTL=21600
ANIZONE_CACHE_SOURCES_TTL=600
```

AniList ID remains the main streaming ID. MAL IDs are used only for backup/resolution flows.

<br>

## Setup

```bash
git clone https://github.com/walterwhite-69/Miruro-API.git
cd Miruro-API
pip install -r requirements.txt  
uvicorn api:app --host 0.0.0.0 --port 8000
```

Then open `http://localhost:8000/` for interactive API docs.

<br>

## Disclaimer

This project is for educational purposes and API integrity research only. The author takes absolutely zero responsibility for network usage. Code contains zero skiddable artifacts.

<br>

**Author:** Walter | **GitHub:** [walterwhite-69](https://github.com/walterwhite-69)
