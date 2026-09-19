"""Tests for the analysis-bucket customTime stamping helper.

See specs/data-retention.md §Analysis bucket: every object the worker
writes to the analysis bucket must carry the retention deadline of the
source asset version it derives from, stamped at creation (the pipeline
SA holds create-only IAM — no follow-up metadata update is possible).
"""

from __future__ import annotations

from datetime import UTC, datetime

from continuous_gtfs.analysis_storage import _StampingFilesystem, stamped_filesystem


class _SpyFilesystem:
    """Records every open() call's mode and kwargs without touching real
    storage — lets tests assert exactly what metadata a write would carry
    without needing a real (or emulated) GCS backend."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict]] = []

    def open(self, path, mode="rb", *args, **kwargs):
        self.calls.append((path, mode, kwargs))
        return object()  # never used as a context manager in these tests

    def mkdirs(self, path, exist_ok=True):
        # Delegation smoke test target — never called by the stamping
        # wrapper itself, just proves __getattr__ reaches through.
        self.mkdirs_calls = getattr(self, "mkdirs_calls", [])
        self.mkdirs_calls.append(path)


class TestStampingFilesystemWrapsWrites:
    def test_write_mode_gets_fixed_key_metadata_custom_time(self):
        spy = _SpyFilesystem()
        deadline = datetime(2026, 6, 1, tzinfo=UTC)
        wrapped = _StampingFilesystem(spy, deadline)

        wrapped.open("bucket/_feed_digest=abc/stops.parquet", "wb")

        assert len(spy.calls) == 1
        _, mode, kwargs = spy.calls[0]
        assert mode == "wb"
        assert kwargs["fixed_key_metadata"] == {"custom_time": deadline.isoformat()}

    def test_metadata_json_commit_marker_is_stamped_identically(self):
        """The commit marker gets no special-case handling — it's just
        another write through the same wrapped filesystem, so it's
        stamped the same way as every table (specs/data-retention.md:
        "every object in a directory carries the directory's deadline,
        the commit marker included")."""
        spy = _SpyFilesystem()
        deadline = datetime(2026, 6, 1, tzinfo=UTC)
        wrapped = _StampingFilesystem(spy, deadline)

        wrapped.open("bucket/_feed_digest=abc/agency.parquet", "wb")
        wrapped.open("bucket/_feed_digest=abc/metadata.json", "wb")

        assert len(spy.calls) == 2
        for _, _, kwargs in spy.calls:
            assert kwargs["fixed_key_metadata"] == {"custom_time": deadline.isoformat()}

    def test_read_mode_is_not_stamped(self):
        spy = _SpyFilesystem()
        wrapped = _StampingFilesystem(spy, datetime(2026, 6, 1, tzinfo=UTC))

        wrapped.open("bucket/_feed_digest=abc/metadata.json", "rb")

        assert len(spy.calls) == 1
        _, mode, kwargs = spy.calls[0]
        assert mode == "rb"
        assert "fixed_key_metadata" not in kwargs

    def test_preserves_caller_supplied_fixed_key_metadata(self):
        """A caller-supplied fixed_key_metadata (e.g. content_disposition)
        is merged with, not clobbered by, custom_time."""
        spy = _SpyFilesystem()
        wrapped = _StampingFilesystem(spy, datetime(2026, 6, 1, tzinfo=UTC))

        wrapped.open(
            "bucket/x",
            "wb",
            fixed_key_metadata={"content_disposition": "attachment"},
        )

        _, _, kwargs = spy.calls[0]
        assert kwargs["fixed_key_metadata"]["content_disposition"] == "attachment"
        assert "custom_time" in kwargs["fixed_key_metadata"]

    def test_delegates_other_methods_to_wrapped_filesystem(self):
        spy = _SpyFilesystem()
        wrapped = _StampingFilesystem(spy, datetime(2026, 6, 1, tzinfo=UTC))

        wrapped.mkdirs("bucket/_feed_digest=abc", exist_ok=True)

        assert spy.mkdirs_calls == ["bucket/_feed_digest=abc"]


class TestStampedFilesystem:
    def test_none_deadline_returns_plain_unwrapped_filesystem(self):
        fs, resolved = stamped_filesystem("memory://analysis/schedule", None)

        assert not isinstance(fs, _StampingFilesystem)
        assert resolved.endswith("analysis/schedule")

    def test_concrete_deadline_returns_wrapped_filesystem(self):
        deadline = datetime(2026, 6, 1, tzinfo=UTC)
        fs, resolved = stamped_filesystem("memory://analysis/schedule", deadline)

        assert isinstance(fs, _StampingFilesystem)
        assert resolved.endswith("analysis/schedule")

    def test_wrapped_filesystem_actually_stamps_writes_end_to_end(self):
        """Using the real in-memory fsspec backend (not a spy): writes
        through the resolved+wrapped filesystem succeed and carry the
        metadata kwarg, proving the wrapper composes with a real
        AbstractFileSystem rather than only the spy's loose interface."""
        deadline = datetime(2026, 6, 1, tzinfo=UTC)
        fs, resolved = stamped_filesystem("memory://analysis-e2e/schedule", deadline)

        path = f"{resolved}/_feed_digest=abc/metadata.json"
        with fs.open(path, "wb") as f:
            f.write(b'{"ok": true}')

        # MemoryFileSystem doesn't retain fixed_key_metadata (it's a GCS-
        # specific concept), but it must not have rejected the kwarg, and
        # the bytes must have landed.
        assert fs.cat(path) == b'{"ok": true}'
