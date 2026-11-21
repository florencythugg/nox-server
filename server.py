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
    # Берём строго прямой HTTP-поток (mp3/m4a), исключаем m3u8/hls плейлисты, которые не играет <audio>
    "format": "bestaudio[protocol^=http][protocol!=m3u8][protocol!=m3u8_native]/bestaudio[ext=mp3]/bestaudio[ext=m4a]/bestaudio",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "extract_flat": False,
}

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

    audio_url = None
    mime_type = "audio/mpeg"
    last_err = ""

    for prefix in [f"scsearch3:{q}", f"ytsearch2:{q} audio"]:
        try:
            with yt_dlp.YoutubeDL(STREAM_OPTS) as ydl:
                info = ydl.extract_info(prefix, download=False)
                entries = info.get("entries") if isinstance(info, dict) and "entries" in info else [info]
                for track in (entries or []):
                    if not track:
                        continue
                    url = track.get("url") or ""
                    # Пропускаем .m3u8 плейлисты, ищем прямой аудиофайл
                    if url and ".m3u8" not in url:
                        audio_url = url
                        ext = track.get("ext", "")
                        if ext in ("m4a", "mp4"):
                            mime_type = "audio/mp4"
                        elif ext == "webm":
                            mime_type = "audio/webm"
                        else:
                            mime_type = "audio/mpeg"
                        break
                if audio_url:
                    break
        except Exception as e:
            last_err = str(e)
            continue

    if not audio_url:
        raise HTTPException(status_code=502, detail=f"Direct audio not found: {last_err}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    range_header = request.headers.get("range")
    if range_header:
        headers["Range"] = range_header

    async def iter_stream():
        async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            async with client.stream("GET", audio_url, headers=headers) as r:
                async for chunk in r.aiter_bytes(chunk_size=65536):
                    yield chunk

    return StreamingResponse(
        iter_stream(),
        media_type=mime_type,
        headers={
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-cache",
        },
    )
