import asyncio
import re
import threading
import time
from datetime import datetime
from urllib.parse import urlparse, parse_qs

import grpc
import requests
from aiohttp import web

import stream_list_pb2
import stream_list_pb2_grpc

clients = set()
main_loop = None
stream_status = "Desconectado"
reader_stop = threading.Event()
reader_thread = None
active_call = None
call_lock = threading.Lock()


def extract_video_id(value: str) -> str:
    value = value.strip()
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", value):
        return value
    parsed = urlparse(value)
    host = (parsed.hostname or "").lower()
    if host in ("youtu.be", "www.youtu.be"):
        return parsed.path.strip("/").split("/")[0]
    if host.endswith("youtube.com"):
        if parsed.path == "/watch":
            return parse_qs(parsed.query).get("v", [""])[0]
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) >= 2 and parts[0] in ("live", "shorts", "embed"):
            return parts[1]
    raise ValueError("No pude identificar el ID. Pegá la URL completa del directo.")


def get_live_chat_id(video_id: str, api_key: str) -> str:
    response = requests.get(
        "https://www.googleapis.com/youtube/v3/videos",
        params={"part": "liveStreamingDetails", "id": video_id, "key": api_key},
        timeout=20,
    )
    try:
        data = response.json()
    except Exception:
        data = {}
    if response.status_code != 200:
        msg = data.get("error", {}).get("message", response.text)
        raise RuntimeError(f"YouTube API ({response.status_code}): {msg}")
    items = data.get("items", [])
    if not items:
        raise RuntimeError("No encontré el video. Comprobá que la URL sea correcta y que el live esté activo.")
    chat_id = items[0].get("liveStreamingDetails", {}).get("activeLiveChatId")
    if not chat_id:
        raise RuntimeError("No hay un chat activo. Iniciá el directo y habilitá el chat en YouTube Studio.")
    return chat_id


async def broadcast(payload):
    dead = []
    for ws in list(clients):
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


def publish_from_thread(payload):
    if main_loop and not main_loop.is_closed():
        asyncio.run_coroutine_threadsafe(broadcast(payload), main_loop)


def grpc_reader(api_key: str, chat_id: str, stop_event: threading.Event):
    global stream_status, active_call
    page_token = None
    first_connection = True
    creds = grpc.ssl_channel_credentials()

    while not stop_event.is_set():
        try:
            stream_status = "Conectando a YouTube (streamList)..."
            publish_from_thread({"type": "status", "status": stream_status})
            with grpc.secure_channel("dns:///youtube.googleapis.com:443", creds) as channel:
                stub = stream_list_pb2_grpc.V3DataLiveChatMessageServiceStub(channel)
                metadata = (("x-goog-api-key", api_key),)
                request = stream_list_pb2.LiveChatMessageListRequest(
                    part=["snippet", "authorDetails"],
                    live_chat_id=chat_id,
                    profile_image_size=128,
                    page_token=page_token,
                )
                stream_status = "Conectado a YouTube. Leyendo respuestas del chat..."
                publish_from_thread({"type": "status", "status": stream_status})
                call = stub.StreamList(request, metadata=metadata)
                with call_lock:
                    active_call = call
                for response in call:
                    if stop_event.is_set():
                        return
                    if response.next_page_token:
                        page_token = response.next_page_token

                    for item in response.items:
                        if not item.HasField("snippet"):
                            continue
                        snippet = item.snippet
                        if not snippet.HasField("text_message_details"):
                            continue
                        text = snippet.text_message_details.message_text.strip()
                        if not text:
                            continue
                        # streamList starts by sending recent history; skip it only on first connection.
                        if first_connection and snippet.HasField("published_at"):
                            try:
                                published = datetime.fromisoformat(snippet.published_at.replace("Z", "+00:00"))
                                if published.timestamp() < time.time() - 3:
                                    continue
                            except Exception:
                                pass
                        author = ""
                        author_id = ""
                        avatar = ""
                        if item.HasField("author_details"):
                            if item.author_details.HasField("display_name"):
                                author = item.author_details.display_name
                            if item.author_details.HasField("channel_id"):
                                author_id = item.author_details.channel_id
                            if item.author_details.HasField("profile_image_url"):
                                avatar = item.author_details.profile_image_url
                        publish_from_thread({
                            "type": "chat",
                            "author": author or "Usuario de YouTube",
                            "authorId": author_id,
                            "avatar": avatar,
                            "text": text,
                            "source": "youtube",
                        })
                    first_connection = False

            if not stop_event.is_set():
                stream_status = "Stream cerrado; reconectando..."
                publish_from_thread({"type": "status", "status": stream_status})
                stop_event.wait(1)
        except grpc.RpcError as exc:
            if stop_event.is_set():
                return
            detail = exc.details() or str(exc)
            stream_status = f"gRPC: {detail}. Reintentando en 3 segundos..."
            publish_from_thread({"type": "status", "status": stream_status})
            stop_event.wait(3)
        except Exception as exc:
            if stop_event.is_set():
                return
            stream_status = f"Error: {exc}. Reintentando en 3 segundos..."
            publish_from_thread({"type": "status", "status": stream_status})
            stop_event.wait(3)
        finally:
            with call_lock:
                if active_call is not None:
                    active_call = None


async def index(_request):
    return web.FileResponse("index.html")


async def preguntas_csv(_request):
    return web.FileResponse("preguntas.csv")


async def tts_handler(request):
    text = request.query.get("text", "").strip()[:220]
    voice = request.query.get("voice", "es-MX-JorgeNeural").strip()
    rate = request.query.get("rate", "+16%").strip()
    pitch = request.query.get("pitch", "+12Hz").strip()
    if not text:
        return web.Response(status=400, text="Missing text")
    try:
        import edge_tts
        comm = edge_tts.Communicate(text, voice=voice, rate=rate, pitch=pitch)
        audio_bytes = bytearray()
        async for chunk in comm.stream():
            if chunk["type"] == "audio":
                audio_bytes.extend(chunk["data"])
        if not audio_bytes:
            return web.Response(status=502, text="Empty TTS audio")
        return web.Response(body=bytes(audio_bytes), content_type="audio/mpeg")
    except Exception as exc:
        return web.Response(status=500, text=str(exc))


async def connect(request):
    global stream_status, reader_stop, reader_thread
    body = await request.json()
    api_key = body.get("apiKey", "").strip()
    video_url = body.get("videoUrl", "").strip()
    if not api_key or not video_url:
        return web.json_response({"error": "Completá la API Key y la URL del directo."}, status=400)
    try:
        video_id = extract_video_id(video_url)
        chat_id = await asyncio.to_thread(get_live_chat_id, video_id, api_key)
    except Exception as exc:
        return web.json_response({"error": str(exc)}, status=400)

    if reader_stop:
        reader_stop.set()
    with call_lock:
        if active_call is not None:
            try:
                active_call.cancel()
            except Exception:
                pass
    reader_stop = threading.Event()
    stream_status = "Chat encontrado. Iniciando stream..."
    reader_thread = threading.Thread(target=grpc_reader, args=(api_key, chat_id, reader_stop), daemon=True)
    reader_thread.start()
    return web.json_response({"ok": True, "videoId": video_id, "status": stream_status})


async def websocket_handler(request):
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    clients.add(ws)
    await ws.send_json({"type": "status", "status": stream_status})
    async for _msg in ws:
        pass
    clients.discard(ws)
    return ws


async def on_startup(_app):
    global main_loop
    main_loop = asyncio.get_running_loop()


app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/preguntas.csv", preguntas_csv)
app.router.add_get("/tts", tts_handler)
app.router.add_post("/connect", connect)
app.router.add_get("/ws", websocket_handler)
app.on_startup.append(on_startup)

if __name__ == "__main__":
    print("Abrí http://127.0.0.1:8080")
    web.run_app(app, host="127.0.0.1", port=8080)
