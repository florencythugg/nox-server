import re
import asyncio
import yt_dlp
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

app = FastAPI(title="Nox Audio Stream Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

PIPED_INSTANCES = [
    "https://pipedapi.kavin.rocks",
    "https://api.piped.private.coffee",
    "https://pipedapi.adminforge.de",
    "https://pipedapi.drgns.space",
    "https://pipedapi.leptons.xyz",
]

INVIDIOUS_INSTANCES = [
    "https://inv.nadeko.net",
    "https://invidious.nerdvpn.de",
    "https://yt.artemislena.eu",
    "https://invidious.f5.si",
    "https://iv.ggtyler.dev",
]

STREAM_OPTS = {
    "format": "bestaudio[protocol^=http][protocol!=m3u8][protocol!=m3u8_native]/bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "extract_flat": False,
}

def norm(s: str) -> str:
    s = (s or "").lower().replace("ё", "е")
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def is_strict_match(candidate_title: str, candidate_author: str, artist: str, title: str) -> bool:
    """Проверяем, что найденный аудиопоток реально соответствует нужному артисту и треку."""
    haystack = f"{norm(candidate_title)} {norm(candidate_author)}"
    n_title = norm(title)
    n_artist = norm(artist)

    if not n_title:
        return False

    # Отсекаем каверы, караоке, замедленные версии и ремиксы, если их не просили
    bad_words = ["karaoke", "караоке", "cover", "кавер", "instrumental", "минус", "slowed", "nightcore", "8d audio", "reaction", "реакция"]
    for bw in bad_words:
        if bw in haystack and bw not in n_title:
            return False

    # Проверяем совпадение названия трека
    title_words = [w for w in n_title.split() if len(w) >= 2]
    if title_words:
        matched_title = sum(1 for w in title_words if w in haystack)
        if matched_title / len(title_words) < 0.7:
            return False
    elif n_title not in haystack:
        return False

    # Проверяем совпадение артиста
    if n_artist:
        artist_words = [w for w in n_artist.split() if len(w) >= 2]
        if artist_words:
            matched_artist = sum(1 for w in artist_words if w in haystack)
            if matched_artist == 0:
                return False
        elif n_artist not in haystack:
            return False

    return True

async def check_piped(host: str, artist: str, title: str):
    q = f"{artist} - {title}".strip(" -")
    async with httpx.AsyncClient(follow_redirects=True, timeout=6.5) as client:
        for filt in ["music_songs", "videos"]:
            s = await client.get(f"{host}/search", params={"q": q, "filter": filt})
            if s.status_code != 200:
                continue
            items = s.json().get("items", [])[:5]
            for item in items:
                cand_title = item.get("title", "")
                cand_author = item.get("uploaderName", "")
                if not is_strict_match(cand_title, cand_author, artist, title):
                    continue
                vid_url = item.get("url", "")
                vid_id = vid_url.split("v=")[-1] if "v=" in vid_url else vid_url.strip("/")
                if vid_id:
                    st = await client.get(f"{host}/streams/{vid_id}")
                    if st.status_code == 200:
                        streams = st.json().get("audioStreams", [])
                        direct = [a for a in streams if a.get("url") and ".m3u8" not in a["url"]]
                        if direct:
                            direct.sort(key=lambda x: x.get("bitrate", 0), reverse=True)
                            return direct[0]["url"], direct[0].get("mimeType", "audio/mp4"), False
    return None

async def check_invidious(host: str, artist: str, title: str):
    q = f"{artist} - {title}".strip(" -")
    async with httpx.AsyncClient(follow_redirects=True, timeout=6.5) as client:
        s = await client.get(f"{host}/api/v1/search", params={"q": q, "type": "video"})
        if s.status_code == 200 and isinstance(s.json(), list):
            for item in s.json()[:5]:
                cand_title = item.get("title", "")
                cand_author = item.get("author", "")
                if not is_strict_match(cand_title, cand_author, artist, title):
                    continue
                vid_id = item.get("videoId")
                if vid_id:
                    st = await client.get(f"{host}/api/v1/videos/{vid_id}")
                    if st.status_code == 200:
                        fmts = st.json().get("adaptiveFormats", [])
                        audio_fmts = [f for f in fmts if str(f.get("type", "")).startswith("audio/") and f.get("url")]
                        if audio_fmts:
                            return audio_fmts[0]["url"], "audio/mp4", False
    return None

def check_ytdlp_sync(artist: str, title: str):
    q = f"{artist} - {title}".strip(" -")
    for prefix in [f"scsearch5:{q}", f"ytsearch3:{q} audio"]:
        try:
            with yt_dlp.YoutubeDL(STREAM_OPTS) as ydl:
                info = ydl.extract_info(prefix, download=False)
                entries = info.get("entries") if isinstance(info, dict) and "entries" in info else [info]
                for track in (entries or []):
                    if not track:
                        continue
                    cand_title = track.get("title", "")
                    cand_author = track.get("uploader", "") or track.get("artist", "")
                    if not is_strict_match(cand_title, cand_author, artist, title):
                        continue
                    url = track.get("url", "")
                    if url and ".m3u8" not in url:
                        return url, "audio/mpeg", False
        except Exception:
            continue
    return None

async def resolve_exact_track(artist: str, title: str):
    tasks = [
        *[asyncio.create_task(check_piped(h, artist, title)) for h in PIPED_INSTANCES],
        *[asyncio.create_task(check_invidious(h, artist, title)) for h in INVIDIOUS_INSTANCES],
    ]
    for fut in asyncio.as_completed(tasks):
        try:
            res = await fut
            if res and res[0]:
                for t in tasks:
                    if not t.done():
                        t.cancel()
                return res
        except Exception:
            continue

    loop = asyncio.get_event_loop()
    yt_res = await loop.run_in_executor(None, check_ytdlp_sync, artist, title)
    if yt_res and yt_res[0]:
        return yt_res
    return None, None, False

@app.get("/")
def root():
    return {"status": "ok", "service": "Nox Cloud Audio Server"}

@app.get("/health")
def health():
    return {"status": "ok", "service": "Nox Full HQ"}

@app.get("/api/resolve")
async def resolve_endpoint(q: str = "", artist: str = "", title: str = ""):
    if not title and q:
        if " - " in q:
            artist, title = q.split(" - ", 1)
        else:
            title = q
    if not title.strip():
        raise HTTPException(status_code=400, detail="Track title required")

    url, mime, direct_cors = await resolve_exact_track(artist.strip(), title.strip())
    if not url:
        raise HTTPException(status_code=404, detail="Exact full track not found")
    return {"url": url, "mime": mime, "direct": direct_cors}

@app.get("/api/stream")
async def stream_track(q: str = "", artist: str = "", title: str = "", request: Request = None):
    if not title and q:
        if " - " in q:
            artist, title = q.split(" - ", 1)
        else:
            title = q
    if not title.strip():
        raise HTTPException(status_code=400, detail="Track title required")

    audio_url, mime_type, _ = await resolve_exact_track(artist.strip(), title.strip())
    if not audio_url:
        raise HTTPException(status_code=502, detail="Exact stream not found")

    headers = {"User-Agent": "Mozilla/5.0"}
    if request and "range" in request.headers:
        headers["Range"] = request.headers["range"]

    async def iter_stream():
        async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            async with client.stream("GET", audio_url, headers=headers) as r:
                async for chunk in r.aiter_bytes(chunk_size=65536):
                    yield chunk

    return StreamingResponse(
        iter_stream(),
        media_type=mime_type or "audio/mpeg",
        headers={
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-cache",
        },
    )
