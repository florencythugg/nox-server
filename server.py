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
    "format": "bestaudio/best",
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
    last_err = ""

    # Пробуем сначала SoundCloud (не блокирует IP Render), затем YouTube
    for prefix in [f"scsearch1:{q}", f"ytsearch1:{q} official audio"]:
        try:
            with yt_dlp.YoutubeDL(STREAM_OPTS) as ydl:
                info = ydl.extract_info(prefix, download=False)
                track = info["entries"][0] if ("entries" in info and info["entries"]) else info
                if track and track.get("url"):
                    audio_url = track["url"]
                    break
        except Exception as e:
            last_err = str(e)
            continue

    if not audio_url:
        raise HTTPException(status_code=502, detail=f"Stream not found: {last_err}")

    headers = {}
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
        media_type="audio/mpeg",
        headers={
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-cache",
        },
    )
