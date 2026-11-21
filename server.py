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

SEARCH_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "extract_flat": True,
    "skip_download": True,
}

STREAM_OPTS = {
    "format": "bestaudio[ext=m4a]/bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "extract_flat": False,
}

def format_duration(seconds):
    if not seconds:
        return "3:15"
    seconds = int(seconds)
    return f"{seconds // 60}:{seconds % 60:02d}"

@app.get("/health")
def health():
    return {"status": "ok", "service": "Nox Full Catalog Engine"}

@app.get("/api/search")
def search_tracks(q: str, limit: int = 20):
    if not q or not q.strip():
        return {"tracks": []}
    try:
        # Ищем сразу до 20 популярных треков по запросу
        query = f"ytsearch{limit}:{q} официальное аудио трек" if any(
            "а" <= c.lower() <= "я" for c in q
        ) else f"ytsearch{limit}:{q} official audio"

        with yt_dlp.YoutubeDL(SEARCH_OPTS) as ydl:
            info = ydl.extract_info(query, download=False)
            entries = info.get("entries", []) if info else []

        results = []
        for item in entries:
            if not item or not item.get("id"):
                continue
            vid_id = item["id"]
            raw_title = item.get("title", "Без названия")
            uploader = item.get("uploader") or item.get("channel") or "Исполнитель"

            # Разделяем "Артист - Название", если есть тире в заголовке
            if " - " in raw_title:
                parts = raw_title.split(" - ", 1)
                artist = parts[0].strip()
                title = parts[1].strip()
            else:
                artist = uploader.replace(" - Topic", "").strip()
                title = raw_title.strip()

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

SEARCH_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "extract_flat": True,
    "skip_download": True,
}

STREAM_OPTS = {
    "format": "bestaudio[ext=m4a]/bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "extract_flat": False,
}

def format_duration(seconds):
    if not seconds:
        return "3:15"
    seconds = int(seconds)
    return f"{seconds // 60}:{seconds % 60:02d}"

@app.get("/health")
def health():
    return {"status": "ok", "service": "Nox Full Catalog Engine"}

@app.get("/api/search")
def search_tracks(q: str, limit: int = 25, sort_by_popularity: bool = False):
    if not q or not q.strip():
        return {"tracks": []}
    try:
        is_ru = any("а" <= c.lower() <= "я" for c in q)
        query = f"ytsearch{limit}:{q} трек аудио" if is_ru else f"ytsearch{limit}:{q} official audio"

        with yt_dlp.YoutubeDL(SEARCH_OPTS) as ydl:
            info = ydl.extract_info(query, download=False)
            entries = info.get("entries", []) if info else []

        results = []
        for item in entries:
            if not item or not item.get("id"):
                continue
            vid_id = item["id"]
            raw_title = item.get("title", "Без названия")
            uploader = item.get("uploader") or item.get("channel") or "Исполнитель"
            views = int(item.get("view_count") or 0)

            if " - " in raw_title:
                parts = raw_title.split(" - ", 1)
                artist = parts[0].strip()
                title = parts[1].strip()
            else:
                artist = uploader.replace(" - Topic", "").strip()
                title = raw_title.strip()

            for tag in [
                "(Official Audio)", "[Official Audio]", "(Official Video)",
                "[Official Video]", "(Audio)", "(Lyrics)", "(Клип)", "(Премьера)"
            ]:
                title = title.replace(tag, "").strip()

            results.append({
                "id": f"yt-{vid_id}",
                "videoId": vid_id,
                "title": title,
                "artist": artist,
                "genre": "Популярное",
                "duration": format_duration(item.get("duration")),
                "durationSec": int(item.get("duration") or 180),
                "cover": f"https://i.ytimg.com/vi/{vid_id}/hqdefault.jpg",
                "views": views,
                "source": "server",
            })

        if sort_by_popularity:
            results.sort(key=lambda x: x["views"], reverse=True)

        return {"tracks": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stream")
async def stream_track(request: Request, q: str = "", id: str = ""):
    if not q and not id:
        raise HTTPException(status_code=400, detail="Parameter 'id' or 'q' is required")

    target = f"https://www.youtube.com/watch?v={id}" if id else f"ytsearch1:{q} audio"

    try:
        with yt_dlp.YoutubeDL(STREAM_OPTS) as ydl:
            info = ydl.extract_info(target, download=False)
            track = info["entries"][0] if ("entries" in info and info["entries"]) else info
            audio_url = track.get("url")
            if not audio_url:
                raise HTTPException(status_code=404, detail="Audio stream URL unavailable")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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
        media_type="audio/mp4",
        headers={
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-cache",
        },
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)