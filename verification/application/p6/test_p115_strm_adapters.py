from __future__ import annotations

import os
from pathlib import Path
from typing import cast

import pytest

from etk.domains.p115 import AtomicP115LocalStrmLibraryGateway


def _library(root: Path):
    library = AtomicP115LocalStrmLibraryGateway().open(str(root))
    assert library is not None
    return library


def test_atomic_strm_and_subtitle_projection_preserves_exact_bytes(tmp_path: Path) -> None:
    library = _library(tmp_path)

    first = library.write_strm(
        "Movies/Feature/Movie.strm",
        "https://etk.example.test/api/p115/play/pick",
    )
    replay = library.write_strm(
        "Movies/Feature/Movie.strm",
        "https://etk.example.test/api/p115/play/pick",
    )
    subtitle = library.write_subtitle("Movies/Feature/Movie.chs.srt", b"fixture subtitle\n")
    subtitle_replay = library.write_subtitle("Movies/Feature/Movie.chs.srt", b"different")
    strm_path = tmp_path / "Movies/Feature/Movie.strm"
    strm_path.write_bytes(b"https://etk.example.test/api/p115/play/pick\n")
    newline_rewrite = library.write_strm(
        "Movies/Feature/Movie.strm",
        "https://etk.example.test/api/p115/play/pick",
    )

    assert first.changed is True
    assert replay.changed is False
    assert subtitle.changed is True
    assert subtitle_replay.changed is False
    assert newline_rewrite.changed is True
    assert strm_path.read_bytes() == b"https://etk.example.test/api/p115/play/pick"
    assert (tmp_path / "Movies/Feature/Movie.chs.srt").read_bytes() == b"fixture subtitle\n"
    assert not list(tmp_path.rglob(".*.strm.*"))


def test_exclusive_strm_create_never_replaces_an_existing_file(tmp_path: Path) -> None:
    library = _library(tmp_path)
    target = tmp_path / "Movies/Feature/Movie.strm"
    target.parent.mkdir(parents=True)
    target.write_text("user-owned", encoding="utf-8")

    with pytest.raises(RuntimeError, match="拒绝覆盖"):
        library.create_strm("Movies/Feature/Movie.strm", "virtual")

    assert target.read_text(encoding="utf-8") == "user-owned"
    assert not list(target.parent.glob(".Movie.strm.*"))


def test_exclusive_strm_create_loses_a_path_race_without_overwriting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    library = _library(tmp_path)
    original_link = os.link

    def racing_link(
        source: object,
        target: object,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        assert isinstance(target, str)
        assert dst_dir_fd is not None
        descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644, dir_fd=dst_dir_fd)
        try:
            os.write(descriptor, b"racing-owner")
        finally:
            os.close(descriptor)
        original_link(
            source,
            target,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )

    monkeypatch.setattr(os, "link", racing_link)

    with pytest.raises(FileExistsError):
        library.create_strm("Movies/Feature/Movie.strm", "virtual")

    target = tmp_path / "Movies/Feature/Movie.strm"
    assert target.read_bytes() == b"racing-owner"
    assert not list(target.parent.glob(".Movie.strm.*"))


def test_interrupted_atomic_subtitle_replace_leaves_no_final_or_temp_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    library = _library(tmp_path)

    def fail_replace(_source: object, _target: object, **_kwargs: object) -> None:
        raise OSError("fixture replace interruption")

    monkeypatch.setattr(os, "replace", fail_replace)
    with pytest.raises(OSError, match="replace interruption"):
        library.write_subtitle("Movies/Movie.srt", b"partial subtitle")

    assert not (tmp_path / "Movies/Movie.srt").exists()
    assert not list((tmp_path / "Movies").glob(".Movie.srt.*"))


def test_reconcile_removes_only_stale_managed_strm_without_cleanup(tmp_path: Path) -> None:
    library = _library(tmp_path)
    library.write_strm("Movies/Keep.strm", "https://etk.example.test/api/p115/play/keep")
    library.write_strm("Movies/Stale.strm", "https://etk.example.test/api/p115/play/stale")
    library.write_strm("Movies/Virtual.strm", "https://etk.example.test/api/p115/virtual-play/asset")
    sidecar = tmp_path / "Movies/unmanaged.txt"
    sidecar.write_text("keep", encoding="utf-8")

    preview = library.preview_reconcile(
        roots=("Movies",),
        desired_paths=("Movies/Keep.strm",),
        cleanup=False,
    )
    assert (tmp_path / "Movies/Stale.strm").exists()
    result = library.reconcile(
        roots=("Movies",),
        desired_paths=("Movies/Keep.strm",),
        cleanup=False,
        allowed_deletions=preview.planned_deletions,
    )

    assert result.deleted_paths == ("Movies/Stale.strm",)
    assert (tmp_path / "Movies/Keep.strm").exists()
    assert (tmp_path / "Movies/Virtual.strm").exists()
    assert sidecar.exists()


def test_cleanup_preserves_metadata_and_desired_derivatives(tmp_path: Path) -> None:
    library = _library(tmp_path)
    library.write_strm("Series/Season 01/Episode.strm", "https://etk.example.test/api/p115/play/episode")
    season = tmp_path / "Series/Season 01"
    (season / "Episode.chs.srt").write_text("subtitle", encoding="utf-8")
    (season / "season01-poster.jpg").write_bytes(b"poster")
    orphan = season / "orphan.bin"
    orphan.write_bytes(b"orphan")

    preview = library.preview_reconcile(
        roots=("Series",),
        desired_paths=("Series/Season 01/Episode.strm",),
        cleanup=True,
    )
    assert orphan.exists()
    result = library.reconcile(
        roots=("Series",),
        desired_paths=("Series/Season 01/Episode.strm",),
        cleanup=True,
        allowed_deletions=preview.planned_deletions,
    )

    assert result.deleted_paths == ("Series/Season 01/orphan.bin",)
    assert (season / "Episode.chs.srt").exists()
    assert (season / "season01-poster.jpg").exists()


def test_cleanup_plans_legacy_removal_of_a_metadata_only_directory(tmp_path: Path) -> None:
    library = _library(tmp_path)
    library.write_strm("Series/Keep/Episode.strm", "https://etk.example.test/api/p115/play/episode")
    empty_shell = tmp_path / "Series/Empty"
    empty_shell.mkdir()
    metadata = empty_shell / "season.nfo"
    metadata.write_text("fixture metadata", encoding="utf-8")

    preview = library.preview_reconcile(
        roots=("Series",),
        desired_paths=("Series/Keep/Episode.strm",),
        cleanup=True,
    )

    assert preview.deleted_paths == ("Series/Empty/season.nfo",)
    assert preview.cleaned_directory_paths == ("Series/Empty",)
    assert metadata.exists()
    result = library.reconcile(
        roots=("Series",),
        desired_paths=("Series/Keep/Episode.strm",),
        cleanup=True,
        allowed_deletions=preview.planned_deletions,
    )
    assert result.deleted_paths == ("Series/Empty/season.nfo",)
    assert result.cleaned_directory_paths == ("Series/Empty",)
    assert not empty_shell.exists()


def test_cleanup_never_schedules_a_desired_subtitle_or_its_directory(tmp_path: Path) -> None:
    library = _library(tmp_path)
    subtitle = tmp_path / "Series/Subtitles/Only.srt"
    subtitle.parent.mkdir(parents=True)
    subtitle.write_text("fixture subtitle", encoding="utf-8")
    (subtitle.parent / "season.nfo").write_text("fixture metadata", encoding="utf-8")

    preview = library.preview_reconcile(
        roots=("Series",),
        desired_paths=("Series/Subtitles/Only.srt",),
        cleanup=True,
    )

    assert preview.deleted_paths == ()
    assert preview.cleaned_directory_paths == ()
    assert subtitle.exists()


def test_reconcile_never_deletes_a_file_added_after_the_persisted_preview(tmp_path: Path) -> None:
    library = _library(tmp_path)
    library.write_strm("Movies/Keep.strm", "https://etk.example.test/api/p115/play/keep")
    library.write_strm("Movies/Planned.strm", "https://etk.example.test/api/p115/play/planned")
    preview = library.preview_reconcile(
        roots=("Movies",),
        desired_paths=("Movies/Keep.strm",),
        cleanup=False,
    )
    library.write_strm("Movies/Late.strm", "https://etk.example.test/api/p115/play/late")

    result = library.reconcile(
        roots=("Movies",),
        desired_paths=("Movies/Keep.strm",),
        cleanup=False,
        allowed_deletions=preview.planned_deletions,
    )

    assert result.deleted_paths == ("Movies/Planned.strm",)
    assert result.deferred_paths == ("Movies/Late.strm",)
    assert not (tmp_path / "Movies/Planned.strm").exists()
    assert (tmp_path / "Movies/Late.strm").exists()


def test_reconcile_defers_a_same_path_file_replacement_after_persisted_preview(
    tmp_path: Path,
) -> None:
    library = _library(tmp_path)
    library.write_strm("Movies/Keep.strm", "https://etk.example.test/api/p115/play/keep")
    library.write_strm("Movies/Planned.strm", "https://etk.example.test/api/p115/play/planned")
    preview = library.preview_reconcile(
        roots=("Movies",),
        desired_paths=("Movies/Keep.strm",),
        cleanup=False,
    )
    planned = tmp_path / "Movies/Planned.strm"
    planned.unlink()
    planned.write_text("new ordinary file at the audited path", encoding="utf-8")

    result = library.reconcile(
        roots=("Movies",),
        desired_paths=("Movies/Keep.strm",),
        cleanup=False,
        allowed_deletions=preview.planned_deletions,
    )

    assert result.deleted_paths == ()
    assert result.deferred_paths == ("Movies/Planned.strm",)
    assert planned.read_text(encoding="utf-8") == "new ordinary file at the audited path"


def test_reconcile_defers_when_fresh_virtual_strm_context_now_protects_a_sidecar(
    tmp_path: Path,
) -> None:
    library = _library(tmp_path)
    sidecar = tmp_path / "Movies/Virtual.srt"
    sidecar.parent.mkdir()
    sidecar.write_text("fixture subtitle", encoding="utf-8")
    preview = library.preview_reconcile(
        roots=("Movies",),
        desired_paths=(),
        cleanup=True,
    )
    assert preview.deleted_paths == ("Movies/Virtual.srt",)
    library.write_strm(
        "Movies/Virtual.strm",
        "https://etk.example.test/api/p115/virtual-play/asset",
    )

    result = library.reconcile(
        roots=("Movies",),
        desired_paths=(),
        cleanup=True,
        allowed_deletions=preview.planned_deletions,
    )

    assert result.deleted_paths == ()
    assert result.deferred_paths == ("Movies/Virtual.srt",)
    assert sidecar.exists()


def test_local_projection_rejects_traversal_and_symlink_escape(tmp_path: Path) -> None:
    library = _library(tmp_path)
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    outside.mkdir()
    (tmp_path / "linked").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="超出配置根目录"):
        library.write_strm("../escape.strm", "fixture")
    with pytest.raises(RuntimeError, match="符号链接"):
        library.write_strm("linked/escape.strm", "fixture")
    assert not (outside / "escape.strm").exists()


def test_open_library_rejects_root_replaced_by_a_symlink_before_creating_parents(
    tmp_path: Path,
) -> None:
    root = tmp_path / "library"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    library = _library(root)
    original = tmp_path / "library-original"
    root.rename(original)
    root.symlink_to(outside, target_is_directory=True)

    with pytest.raises(RuntimeError, match="根目录.*发生变化"):
        library.write_strm("Movies/escape.strm", "fixture")

    assert not (outside / "Movies").exists()


def test_reconcile_unlink_stays_anchored_when_configured_root_is_swapped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    outside = tmp_path / "outside"
    original = tmp_path / "library-original"
    root.mkdir()
    outside.mkdir()
    library = _library(root)
    library.write_strm("Movies/Stale.strm", "https://etk.example.test/api/p115/play/stale")
    outside_stale = outside / "Movies/Stale.strm"
    outside_stale.parent.mkdir()
    outside_stale.write_text("outside must survive", encoding="utf-8")
    preview = library.preview_reconcile(roots=("Movies",), desired_paths=(), cleanup=False)
    original_rename = os.rename
    swapped = False

    def racing_rename(source: object, target: object, *args: object, **kwargs: object) -> None:
        nonlocal swapped
        if source == "Stale.strm" and not swapped:
            swapped = True
            original_rename(root, original)
            root.symlink_to(outside, target_is_directory=True)
        original_rename(source, target, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "rename", racing_rename)

    with pytest.raises(RuntimeError, match="根目录.*发生变化"):
        library.reconcile(
            roots=("Movies",),
            desired_paths=(),
            cleanup=False,
            allowed_deletions=preview.planned_deletions,
        )

    assert outside_stale.read_text(encoding="utf-8") == "outside must survive"
    assert (original / "Movies/Stale.strm").exists()


def test_reconcile_quarantines_and_restores_a_same_path_swap_at_delete_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    library = _library(tmp_path)
    library.write_strm("Movies/Stale.strm", "https://etk.example.test/api/p115/play/stale")
    preview = library.preview_reconcile(roots=("Movies",), desired_paths=(), cleanup=False)
    stale = tmp_path / "Movies/Stale.strm"
    original_rename = os.rename
    original_unlink = os.unlink
    swapped = False

    def racing_rename(source: object, target: object, *args: object, **kwargs: object) -> None:
        nonlocal swapped
        if source == "Stale.strm" and not swapped:
            swapped = True
            original_unlink(stale)
            stale.write_text("replacement at audited path", encoding="utf-8")
        original_rename(source, target, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "rename", racing_rename)

    result = library.reconcile(
        roots=("Movies",),
        desired_paths=(),
        cleanup=False,
        allowed_deletions=preview.planned_deletions,
    )

    assert result.deleted_paths == ()
    assert result.deferred_paths == ("Movies/Stale.strm",)
    assert stale.read_text(encoding="utf-8") == "replacement at audited path"
    assert not list(tmp_path.glob(".etk-delete-file-*"))


def test_reconcile_quarantines_and_restores_a_directory_swap_at_delete_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    library = _library(tmp_path)
    library.write_strm("Movies/Keep.strm", "https://etk.example.test/api/p115/play/keep")
    empty = tmp_path / "Movies/Empty"
    empty.mkdir()
    preview = library.preview_reconcile(
        roots=("Movies",),
        desired_paths=("Movies/Keep.strm",),
        cleanup=True,
    )
    original_rename = os.rename
    swapped = False

    def racing_rename(source: object, target: object, *args: object, **kwargs: object) -> None:
        nonlocal swapped
        if source == "Empty" and not swapped:
            swapped = True
            parent = cast(int, kwargs["src_dir_fd"])
            original_rename(
                "Empty",
                "Empty-before-race",
                src_dir_fd=parent,
                dst_dir_fd=parent,
            )
            os.mkdir("Empty", dir_fd=parent)
        original_rename(source, target, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "rename", racing_rename)

    result = library.reconcile(
        roots=("Movies",),
        desired_paths=("Movies/Keep.strm",),
        cleanup=True,
        allowed_deletions=preview.planned_deletions,
    )

    assert result.cleaned_directory_paths == ()
    assert result.deferred_paths == ("Movies/Empty",)
    assert empty.is_dir()
    assert (tmp_path / "Movies/Empty-before-race").is_dir()
    assert not list(tmp_path.glob(".etk-delete-directory-*"))


def test_reconcile_restores_quarantine_when_open_parent_moves_outside_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    library = _library(root)
    library.write_strm("Movies/Stale.strm", "https://etk.example.test/api/p115/play/stale")
    preview = library.preview_reconcile(roots=("Movies",), desired_paths=(), cleanup=False)
    original_rename = os.rename
    moved = outside / "MovedMovies"
    detached = False

    def racing_rename(source: object, target: object, *args: object, **kwargs: object) -> None:
        nonlocal detached
        if source == "Stale.strm" and not detached:
            detached = True
            original_rename(root / "Movies", moved)
            (root / "Movies").mkdir()
        original_rename(source, target, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "rename", racing_rename)

    result = library.reconcile(
        roots=("Movies",),
        desired_paths=(),
        cleanup=False,
        allowed_deletions=preview.planned_deletions,
    )

    assert result.deleted_paths == ()
    assert result.deferred_paths == ("Movies/Stale.strm",)
    assert (moved / "Stale.strm").exists()
    assert not list(root.glob(".etk-delete-file-*"))


def test_gateway_open_rejects_a_root_swapped_to_symlink_during_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "library"
    outside = tmp_path / "outside"
    original = tmp_path / "library-original"
    root.mkdir()
    outside.mkdir()
    original_resolve = Path.resolve
    swapped = False

    def racing_resolve(path: Path, *args: object, **kwargs: object) -> Path:
        nonlocal swapped
        if path == root and not swapped:
            swapped = True
            root.rename(original)
            root.symlink_to(outside, target_is_directory=True)
        return original_resolve(path, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(Path, "resolve", racing_resolve)

    assert AtomicP115LocalStrmLibraryGateway().open(str(root)) is None


def test_bulk_replace_is_atomic_and_keeps_legacy_message_counts_input(tmp_path: Path) -> None:
    library = _library(tmp_path)
    library.write_strm("A/one.strm", "https://old.example/api/p115/play/one")
    library.write_strm("A/two.strm", "https://other.example/api/p115/play/two")

    fixed, skipped = library.replace_strm(
        mode="plain",
        search="https://old.example",
        replacement="https://new.example",
    )
    regex_fixed, regex_skipped = library.replace_strm(
        mode="regex",
        search=r"/play/(one|two)$",
        replacement=r"/play/replaced-\1",
    )

    assert (fixed, skipped) == (1, 1)
    assert (regex_fixed, regex_skipped) == (2, 0)
    assert (tmp_path / "A/one.strm").read_text(encoding="utf-8") == ("https://new.example/api/p115/play/replaced-one")


def test_bulk_replace_skips_a_symlinked_strm_file(tmp_path: Path) -> None:
    library = _library(tmp_path)
    outside = tmp_path.parent / f"{tmp_path.name}-outside.strm"
    outside.write_text("https://old.example/api/p115/play/outside", encoding="utf-8")
    directory = tmp_path / "Movies"
    directory.mkdir()
    (directory / "linked.strm").symlink_to(outside)

    fixed, skipped = library.replace_strm(
        mode="plain",
        search="https://old.example",
        replacement="https://new.example",
    )

    assert (fixed, skipped) == (0, 1)
    assert outside.read_text(encoding="utf-8") == "https://old.example/api/p115/play/outside"
