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
    "format": "bestaudio[ext=m4a]/bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "default_search": "ytsearch1",
    "extract_flat": False,
    "extractor_args": {
        "youtube": {
            "player_client": ["android", "ios", "web"]
        }
    }
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

    try:
        with yt_dlp.YoutubeDL(STREAM_OPTS) as ydl:
            info = ydl.extract_info(f"ytsearch1:{q} audio", download=False)
            if "entries" in info and info["entries"]:
                track = info["entries"][0]
            else:
                track = info
            audio_url = track.get("url")
            if not audio_url:
                raise HTTPException(status_code=404, detail="Audio stream URL unavailable")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
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
        media_type="audio/mp4",
        headers={
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-cache",
        },
    )
