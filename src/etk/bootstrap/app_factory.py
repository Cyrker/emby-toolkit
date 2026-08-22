from __future__ import annotations

from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path
from typing import Any

from fastapi import FastAPI

from etk.compatibility.flask_app import (
    LazyCompatibilityApplication,
    create_flask_app,
)
from etk.compatibility.response_contract import fastapi_json_response

from .runtime_supervisor import RuntimeRole, RuntimeState, RuntimeSupervisor, get_runtime_supervisor


def create_app(
    *,
    start_runtime: bool = True,
    static_dir: Path | None = None,
    data_dir: Path | None = None,
    secret_key: bytes | None = None,
    supervisor: RuntimeSupervisor | None = None,
) -> FastAPI:
    """Build the ASGI application without importing or starting legacy runtime modules."""
    runtime = supervisor or get_runtime_supervisor()
    compatibility_app = LazyCompatibilityApplication(
        partial(
            create_flask_app,
            static_dir=static_dir,
            data_dir=data_dir,
            secret_key=secret_key,
            supervisor=runtime,
        )
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if start_runtime:
            runtime.start(role=RuntimeRole.API, background=True)
        try:
            if start_runtime:
                compatibility_app.materialize()
            yield
        finally:
            if start_runtime:
                runtime.stop()

    app = FastAPI(title="Emby Toolkit", lifespan=lifespan)
    app.state.runtime_supervisor = runtime

    @app.get("/api/health", include_in_schema=False)
    def health() -> Any:
        if start_runtime and runtime.state is not RuntimeState.RUNNING:
            return fastapi_json_response(
                {"status": "error", "message": f"runtime is {runtime.state.value}"},
                status_code=503,
            )
        return fastapi_json_response({"status": "ok"})

    app.state.compatibility_app = compatibility_app

    # The compatibility mount must remain the final route so new FastAPI routes win.
    app.mount("/", compatibility_app, name="legacy-compatibility")
    return app
