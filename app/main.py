"""FastAPI application factory."""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .config import Settings, get_settings
from .db import create_db, create_tables
from .delivery import build_client
from .errors import install_error_handlers
from .keepalive import keep_awake
from .logging_setup import configure_logging, log
from .ratelimit import SlidingWindowLimiter
from .repository import Repository
from .routes import mock_receiver, webhooks
from .worker import Worker

logger = logging.getLogger("app")


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        configure_logging(settings.log_level)
        engine, session_factory = create_db(settings)
        await create_tables(engine)
        client = build_client(settings)

        app.state.settings = settings
        app.state.repo = Repository(session_factory)
        app.state.ingest_limiter = SlidingWindowLimiter()
        app.state.worker = None
        app.state.worker_task = None
        keepalive_task: asyncio.Task | None = None
        if settings.run_worker:
            app.state.worker = Worker(session_factory, client, settings)
            app.state.worker_task = asyncio.create_task(app.state.worker.run(), name="worker")
        if settings.render_external_url:
            keepalive_task = asyncio.create_task(
                keep_awake(settings.render_external_url, settings.self_ping_interval_s), name="keepalive"
            )
        log(logger, "app_started", run_worker=settings.run_worker, mock_receiver=settings.enable_mock_receiver)
        try:
            yield
        finally:
            if keepalive_task is not None:
                keepalive_task.cancel()
            if app.state.worker is not None:
                app.state.worker.stop()
                await asyncio.gather(app.state.worker_task, return_exceptions=True)  # drains in-flight work
            await client.aclose()
            await engine.dispose()

    app = FastAPI(
        title="Distributed Webhook Dispatcher",
        version="1.0.0",
        description=(
            "Accepts events, queues them durably in Postgres, and delivers them in the background with "
            "exponential-backoff retries and per-destination rate limiting."
        ),
        lifespan=lifespan,
    )
    install_error_handlers(app)

    @app.middleware("http")
    async def guard_and_log(request: Request, call_next):
        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > settings.max_body_bytes:
            return JSONResponse(
                {"error": "payload_too_large", "message": f"Body exceeds {settings.max_body_bytes} bytes"}, 413
            )
        started = time.perf_counter()
        response = await call_next(request)
        if request.url.path != "/health":
            log(
                logger, "http_request",
                method=request.method, path=request.url.path, status=response.status_code,
                duration_ms=round((time.perf_counter() - started) * 1000, 1),
            )
        return response

    app.include_router(webhooks.router)
    if settings.enable_mock_receiver:
        app.include_router(mock_receiver.router)

    # HEAD is allowed too, because uptime monitors usually check with HEAD requests.
    @app.api_route("/health", methods=["GET", "HEAD"], tags=["ops"], summary="Liveness + database check")
    async def health(request: Request) -> JSONResponse:
        try:
            await request.app.state.repo.ping()
        except Exception:
            return JSONResponse({"status": "unhealthy", "db": "down"}, 503)

        # If the worker task has died, report unhealthy: the host's health check will then restart us.
        worker_task = request.app.state.worker_task
        if settings.run_worker and (worker_task is None or worker_task.done()):
            return JSONResponse({"status": "unhealthy", "db": "up", "worker": "stopped"}, 503)
        return JSONResponse({"status": "ok", "db": "up", "worker": "running" if settings.run_worker else "disabled"})

    @app.api_route("/", methods=["GET", "HEAD"], tags=["ops"], include_in_schema=False)
    async def root() -> dict:
        return {
            "service": "webhook-dispatcher",
            "docs": "/docs",
            "dispatch": "POST /api/webhooks/dispatch",
            "jobStatus": "GET /api/webhooks/jobs/{jobId}",
            "mockReceiver": "POST /mock/receiver" if settings.enable_mock_receiver else None,
        }

    return app


def app_factory() -> FastAPI:
    """Entry point for `uvicorn app.main:app_factory --factory`."""
    return create_app()
