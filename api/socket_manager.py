"""
Socket.IO manager — singleton AsyncServer + thread-safe emit helper.

Background migration threads call `emit_from_thread()` to push batch
progress events to connected frontend clients.

Usage (background thread):
    from api.socket_manager import emit_from_thread
    emit_from_thread("job:batch", {"run_id": ..., "batch_num": 1, ...})

Frontend connects to:
    ws://host:8000/ws/socket.io/
"""
from __future__ import annotations

import asyncio
import logging

import socketio

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Singleton AsyncServer
# ---------------------------------------------------------------------------

sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins=[],   # [] = disable engineio CORS check entirely
    logger=False,
    engineio_logger=False,
)

# Placeholder — real other_asgi_app (FastAPI) is injected by main.py after
# the FastAPI app is fully constructed, to avoid circular imports.
# Client URL: ws://host:8000/ws/socket.io/
socket_asgi = socketio.ASGIApp(sio, socketio_path="ws/socket.io")

# Event loop reference — captured at FastAPI startup so background threads
# can schedule coroutines onto the main async loop.
_loop: asyncio.AbstractEventLoop | None = None


def set_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    global _loop
    _loop = loop


# ---------------------------------------------------------------------------
# Thread-safe emit
# ---------------------------------------------------------------------------

def emit_from_thread(event: str, data: dict, room: str | None = None) -> None:
    """Emit a socket.io event from a synchronous background thread.

    Non-blocking fire-and-forget with a 2-second timeout.
    Silently ignores errors so a socket failure never breaks migration.
    """
    if _loop is None or _loop.is_closed():
        return
    try:
        future = asyncio.run_coroutine_threadsafe(
            sio.emit(event, data, room=room),
            _loop,
        )
        future.result(timeout=2)
    except Exception as exc:
        logger.debug("socket emit error (non-critical): %s", exc)


# ---------------------------------------------------------------------------
# Connection lifecycle events
# ---------------------------------------------------------------------------

@sio.event
async def connect(sid, environ, auth):
    logger.debug("socket connected: %s", sid)


@sio.event
async def disconnect(sid):
    logger.debug("socket disconnected: %s", sid)
