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
]

INVIDIOUS_INSTANCES = [
    "https://invidious.nerdvpn.de",
    "https://inv.nadeko.net",
    "https://yt.artemislena.eu",
]

STREAM_OPTS = {
    "format": "bestaudio[protocol^=http][protocol!=m3u8][protocol!=m3u8_native]/bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "extract_flat": False,
}

async def resolve_audio_url(query: str):
    async with httpx.AsyncClient(follow_redirects=True, timeout=10.0) as client:
        # 1. Пробуем Piped API (чистые IP, отдаёт прямой audioStreams URL без блокировки Render)
        for host in PIPED_INSTANCES:
            try:
                s = await client.get(f"{host}/search", params={"q": f"{query} audio", "filter": "music_songs"})
                if s.status_code != 200:
                    continue
                items = s.json().get("items", [])
                if not items:
                    items_all = (await client.get(f"{host}/search", params={"q": query, "filter": "videos"})).json().get("items", [])
                    items = items_all
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
                                return direct[0]["url"], direct[0].get("mimeType", "audio/mp4")
            except Exception:
                continue

        # 2. Пробуем Invidious API
        for host in INVIDIOUS_INSTANCES:
            try:
                s = await client.get(f"{host}/api/v1/search", params={"q": f"{query} audio", "type": "video"})
                if s.status_code == 200 and s.json():
                    vid_id = s.json()[0].get("videoId")
                    if vid_id:
                        st = await client.get(f"{host}/api/v1/videos/{vid_id}")
                        if st.status_code == 200:
                            fmts = st.json().get("adaptiveFormats", [])
                            audio_fmts = [f for f in fmts if str(f.get("type", "")).startswith("audio/") and f.get("url")]
                            if audio_fmts:
                                return audio_fmts[0]["url"], "audio/mp4"
            except Exception:
                continue

        # 3. Пробуем Audius (официальный каталог полных треков)
        try:
            au = await client.get("https://discoveryprovider.audius.co/v1/tracks/search", params={"query": query, "limit": 1})
            if au.status_code == 200:
                data = au.json().get("data", [])
                if data and data[0].get("id"):
                    tid = data[0]["id"]
                    return f"https://discoveryprovider.audius.co/v1/tracks/{tid}/stream", "audio/mpeg"
        except Exception:
            pass

    # 4. Резерв через yt-dlp
    for prefix in [f"scsearch2:{query}", f"ytsearch1:{query} audio"]:
        try:
            with yt_dlp.YoutubeDL(STREAM_OPTS) as ydl:
                info = ydl.extract_info(prefix, download=False)
                entries = info.get("entries") if isinstance(info, dict) and "entries" in info else [info]
                for track in (entries or []):
                    url = (track or {}).get("url", "")
                    if url and ".m3u8" not in url:
                        return url, "audio/mpeg"
        except Exception:
            continue

    return None, None

@app.get("/")
def root():
    return {"status": "ok", "service": "Nox Cloud Audio Server"}

@app.get("/health")
def health():
    return {"status": "ok", "service": "Nox Full HQ"}

@app.get("/api/stream")
async def stream_track(q: str, request: Request):
    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="Parameter 'q' is required")

    audio_url, mime_type = await resolve_audio_url(q.strip())
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
