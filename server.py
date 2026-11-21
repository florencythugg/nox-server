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

STREAM_OPTS = {
    "format": "bestaudio[protocol^=http][protocol!=m3u8][protocol!=m3u8_native]/bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "extract_flat": False,
}

def norm(s: str) -> str:
    s = (s or "").lower().replace("ё", "е")
    s = re.sub(r"\s*[\(\[].*?[\)\]]", "", s)
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def is_match(cand_title: str, cand_artist: str, artist: str, title: str) -> bool:
    hay = f"{norm(cand_title)} {norm(cand_artist)}"
    nt = norm(title)
    na = norm(artist)
    if not nt:
        return False
    t_words = [w for w in nt.split() if len(w) >= 2]
    if t_words and sum(1 for w in t_words if w in hay) / len(t_words) < 0.7:
        return False
    if na:
        a_words = [w for w in na.split() if len(w) >= 2]
        if a_words and sum(1 for w in a_words if w in hay) == 0:
            return False
    return True

async def check_saavn(artist: str, title: str):
    q = f"{artist} {title}".strip()
    endpoints = [
        "https://saavn.dev/api/search/songs",
        "https://jiosaavn-api-privatecvc2.vercel.app/search/songs",
    ]
    async with httpx.AsyncClient(follow_redirects=True, timeout=6.0) as client:
        for ep in endpoints:
            try:
                res = await client.get(ep, params={"query": q, "limit": 5})
                if res.status_code != 200:
                    continue
                body = res.json()
                results = body.get("data", {}).get("results", []) if isinstance(body.get("data"), dict) else body.get("results", [])
                for item in results:
                    name = item.get("name") or item.get("title") or ""
                    primary = item.get("primaryArtists") or ""
                    if isinstance(item.get("artists"), dict):
                        primary = ", ".join(a.get("name", "") for a in item["artists"].get("primary", []))
                    if not is_match(name, primary, artist, title):
                        continue
                    dl = item.get("downloadUrl") or []
                    if isinstance(dl, list) and dl:
                        best = dl[-1].get("url") or dl[-1].get("link")
                        if best:
                            return best, "audio/mp4"
            except Exception:
                continue
    return None

async def check_audius(artist: str, title: str):
    q = f"{artist} {title}".strip()
    async with httpx.AsyncClient(follow_redirects=True, timeout=6.0) as client:
        try:
            au = await client.get("https://discoveryprovider.audius.co/v1/tracks/search", params={"query": q, "limit": 5})
            if au.status_code == 200:
                for item in au.json().get("data", []):
                    t_name = item.get("title", "")
                    u_name = (item.get("user") or {}).get("name", "")
                    if is_match(t_name, u_name, artist, title) and item.get("id"):
                        return f"https://discoveryprovider.audius.co/v1/tracks/{item['id']}/stream", "audio/mpeg"
        except Exception:
            pass
    return None

def check_ytdlp_sync(artist: str, title: str):
    q = f"{artist} - {title}".strip(" -")
    for prefix in [f"scsearch3:{q}", f"ytsearch2:{q} audio"]:
        try:
            with yt_dlp.YoutubeDL(STREAM_OPTS) as ydl:
                info = ydl.extract_info(prefix, download=False)
                entries = info.get("entries") if isinstance(info, dict) and "entries" in info else [info]
                for track in (entries or []):
                    if not track:
                        continue
                    if not is_match(track.get("title", ""), track.get("uploader", ""), artist, title):
                        continue
                    url = track.get("url", "")
                    if url and ".m3u8" not in url:
                        return url, "audio/mpeg"
        except Exception:
            continue
    return None

async def resolve_full(artist: str, title: str):
    res = await check_saavn(artist, title)
    if res:
        return res
    res = await check_audius(artist, title)
    if res:
        return res
    loop = asyncio.get_event_loop()
    res = await loop.run_in_executor(None, check_ytdlp_sync, artist, title)
    if res:
        return res
    return None, None

@app.get("/")
def root():
    return {"status": "ok", "service": "Nox Cloud Audio Server"}

@app.get("/health")
def health():
    return {"status": "ok", "service": "Nox Full HQ"}

@app.get("/api/stream")
async def stream_track(q: str = "", artist: str = "", title: str = "", request: Request = None):
    if not title and q:
        if " - " in q:
            artist, title = q.split(" - ", 1)
        else:
            title = q
    if not title.strip():
        raise HTTPException(status_code=400, detail="Track title required")

    audio_url, mime_type = await resolve_full(artist.strip(), title.strip())
    if not audio_url:
        raise HTTPException(status_code=404, detail="Full track not found")

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
