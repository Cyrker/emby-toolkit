from __future__ import annotations

from uuid import UUID

from sqlalchemy import text

from etk.domains.media.effect_execution import PersistentMediaEffectExecutor
from etk.domains.media.effects import PostgresMediaEffectLedger
from etk.domains.media.image_editing import (
    ImageUpdateCommand,
    ImageUpdateOutcome,
    TmdbImageCatalog,
)
from etk.domains.media.image_editing_effects import ManualImageEffectDelivery, PersistentMediaImageEditingGateway
from etk.platform.db import DatabaseRuntime


class FakeCompatibilityGateway:
    def __init__(self) -> None:
        self.commands: list[ImageUpdateCommand] = []
        self.error: Exception | None = None

    @staticmethod
    def processor_ready() -> bool:
        return True

    def tmdb_images(self, item_id: str) -> TmdbImageCatalog:
        del item_id
        return TmdbImageCatalog((), (), ())

    def update_image(self, command: ImageUpdateCommand) -> ImageUpdateOutcome:
        self.commands.append(command)
        if self.error is not None:
            raise self.error
        return ImageUpdateOutcome(True, f"{command.image_type} 替换成功")


def test_pg18_manual_image_url_effect_is_durable_confirmed_and_idempotent(
    p5_runtime: DatabaseRuntime,
) -> None:
    compatibility = FakeCompatibilityGateway()
    gateway = PersistentMediaImageEditingGateway(
        p5_runtime.unit_of_work,
        compatibility,
        worker_id="fixture-api:image",
    )
    command = ImageUpdateCommand(
        item_id="fixture-movie",
        image_type="poster",
        image_url="https://fixture.invalid/poster.jpg",
        image_bytes=None,
        content_type=None,
    )

    first = gateway.update_image(command)
    second = gateway.update_image(command)

    assert first.success is True
    assert first.effect_status == "confirmed"
    assert second == first
    assert compatibility.commands == [command]
    with p5_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM media_effects")) == 1
        assert connection.scalar(text("SELECT status FROM media_effects")) == "confirmed"
        assert (
            connection.scalar(text("SELECT status FROM outbox_events WHERE topic = 'media.effect.requested'"))
            == "processed"
        )


def test_pg18_manual_image_upload_roundtrips_bytes_and_unknown_outcome_never_replays(
    p5_runtime: DatabaseRuntime,
) -> None:
    compatibility = FakeCompatibilityGateway()
    gateway = PersistentMediaImageEditingGateway(
        p5_runtime.unit_of_work,
        compatibility,
        worker_id="fixture-api:image",
    )
    upload = ImageUpdateCommand(
        item_id="fixture-series",
        image_type="clearlogo",
        image_url="",
        image_bytes=b"fixture-image-bytes",
        content_type="image/png",
    )

    confirmed = gateway.update_image(upload)

    assert confirmed.success is True
    assert compatibility.commands == [upload]

    compatibility.error = RuntimeError("provider token=fixture-secret outcome lost")
    unknown_command = ImageUpdateCommand(
        item_id="fixture-series",
        image_type="fanart",
        image_url="https://fixture.invalid/fanart.jpg",
        image_bytes=None,
        content_type=None,
    )
    first_unknown = gateway.update_image(unknown_command)
    second_unknown = gateway.update_image(unknown_command)

    assert first_unknown.success is False
    assert first_unknown.effect_status == "indeterminate"
    assert "fixture-secret" not in first_unknown.message
    assert second_unknown == first_unknown
    assert compatibility.commands == [upload, unknown_command]
    with p5_runtime.engine.connect() as connection:
        errors = connection.scalars(
            text("SELECT error_json::text FROM media_effects WHERE status = 'indeterminate'")
        ).all()
    assert len(errors) == 1
    assert "fixture-secret" not in str(errors[0])


def test_pg18_manual_image_api_defers_and_worker_delivery_confirms_once(
    p5_runtime: DatabaseRuntime,
) -> None:
    native = FakeCompatibilityGateway()
    api = PersistentMediaImageEditingGateway(
        p5_runtime.unit_of_work,
        native,
        defer=True,
    )
    command = ImageUpdateCommand(
        item_id="fixture-worker-image",
        image_type="poster",
        image_url="https://fixture.invalid/worker.jpg",
        image_bytes=None,
        content_type=None,
    )

    pending = api.update_image(command)
    assert pending.success is False
    assert pending.effect_status == "pending"
    assert native.commands == []

    executor = PersistentMediaEffectExecutor(
        PostgresMediaEffectLedger(p5_runtime.unit_of_work),
        ManualImageEffectDelivery(native),
        worker_id="fixture-worker:image",
    )
    confirmed = executor.execute(UUID(str(pending.effect_id)))
    replay = executor.execute(confirmed.id)

    assert confirmed.status.value == "confirmed"
    assert replay == confirmed
    assert native.commands == [command]
