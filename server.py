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
    "https://pipedapi.tokhmi.xyz",
    "https://api.piped.projectsegfau.lt",
    "https://pipedapi.smnz.de",
    "https://pipedapi.adminforge.de",
    "https://api.piped.yt",
]

INVIDIOUS_INSTANCES = [
    "https://invidious.nerdvpn.de",
    "https://inv.nadeko.net",
    "https://yt.artemislena.eu",
    "https://invidious.privacyredirect.com",
    "https://iv.ggtyler.dev",
]

STREAM_OPTS = {
    "format": "bestaudio[protocol^=http][protocol!=m3u8][protocol!=m3u8_native]/bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "extract_flat": False,
}

def clean_query_text(q: str) -> str:
    # Убираем (feat. ...), [Explicit], (Remastered) для более точного поиска
    cleaned = re.sub(r"\s*[\(\[].*?[\)\]]", "", q)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or q

async def check_piped(host: str, query: str):
    async with httpx.AsyncClient(follow_redirects=True, timeout=6.0) as client:
        s = await client.get(f"{host}/search", params={"q": f"{query} audio", "filter": "music_songs"})
        items = s.json().get("items", []) if s.status_code == 200 else []
        if not items:
            s2 = await client.get(f"{host}/search", params={"q": query, "filter": "videos"})
            items = s2.json().get("items", []) if s2.status_code == 200 else []
        if items:
            vid_url = items[0].get("url", "")
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

async def check_invidious(host: str, query: str):
    async with httpx.AsyncClient(follow_redirects=True, timeout=6.0) as client:
        s = await client.get(f"{host}/api/v1/search", params={"q": f"{query} audio", "type": "video"})
        if s.status_code == 200 and s.json():
            vid_id = s.json()[0].get("videoId")
            if vid_id:
                st = await client.get(f"{host}/api/v1/videos/{vid_id}")
                if st.status_code == 200:
                    fmts = st.json().get("adaptiveFormats", [])
                    audio_fmts = [f for f in fmts if str(f.get("type", "")).startswith("audio/") and f.get("url")]
                    if audio_fmts:
                        return audio_fmts[0]["url"], "audio/mp4", False
    return None

async def check_audius(query: str):
    async with httpx.AsyncClient(follow_redirects=True, timeout=6.0) as client:
        au = await client.get("https://discoveryprovider.audius.co/v1/tracks/search", params={"query": query, "limit": 5})
        if au.status_code == 200:
            for item in au.json().get("data", []):
                tid = item.get("id")
                if tid:
                    stream_endpoint = f"https://discoveryprovider.audius.co/v1/tracks/{tid}/stream"
                    head_res = await client.head(stream_endpoint)
                    final_url = str(head_res.url)
                    if final_url and head_res.status_code in (200, 206):
                        return final_url, "audio/mpeg", True
    return None

def check_ytdlp_sync(query: str):
    for prefix in [f"scsearch3:{query}", f"ytsearch2:{query} audio"]:
        try:
            with yt_dlp.YoutubeDL(STREAM_OPTS) as ydl:
                info = ydl.extract_info(prefix, download=False)
                entries = info.get("entries") if isinstance(info, dict) and "entries" in info else [info]
                for track in (entries or []):
                    url = (track or {}).get("url", "")
                    if url and ".m3u8" not in url:
                        return url, "audio/mpeg", False
        except Exception:
            continue
    return None

async def resolve_fast(raw_query: str):
    query = clean_query_text(raw_query)
    tasks = [
        *[asyncio.create_task(check_piped(h, query)) for h in PIPED_INSTANCES],
        *[asyncio.create_task(check_invidious(h, query)) for h in INVIDIOUS_INSTANCES],
        asyncio.create_task(check_audius(query)),
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
    yt_res = await loop.run_in_executor(None, check_ytdlp_sync, query)
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
async def resolve_endpoint(q: str):
    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="Query required")
    url, mime, direct_cors = await resolve_fast(q.strip())
    if not url:
        raise HTTPException(status_code=404, detail="Track not found in full catalogs")
    return {"url": url, "mime": mime, "direct": direct_cors}

@app.get("/api/stream")
async def stream_track(q: str, request: Request):
    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="Parameter 'q' is required")

    audio_url, mime_type, _ = await resolve_fast(q.strip())
    if not audio_url:
        raise HTTPException(status_code=502, detail="No playable full stream found")

    headers = {"User-Agent": "Mozilla/5.0"}
    if "range" in request.headers:
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
