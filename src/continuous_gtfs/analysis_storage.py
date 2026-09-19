"""GCS customTime stamping for analysis-bucket writes.

Every object the pipeline worker writes to the analysis bucket (canonical
form today; semantic/pairing tables later) must carry the retention
deadline of the source asset version it derives from, per
specs/data-retention.md §Analysis bucket. The deadline rides the run
dispatch (orchestrator's ``retention.ts`` ``dispatchDeadline()``, wired
onto ``DispatchRequest.deadline``) since the worker has no way to compute
a retention class itself.

Stamping happens at OBJECT CREATION, never as a follow-up metadata patch:
the pipeline service account holds create-only IAM on the analysis bucket
(``tf/modules/platform/analysis-storage.tf`` — objectCreator + objectViewer,
no update/delete), so ``customTime`` must be part of each object's initial
upload. gcsfs's ``fixed_key_metadata={"custom_time": ...}`` does exactly
that (see gcsfs.GCSFile — the value is folded into the initial
simple/resumable upload body, not a PATCH), so the helper here wraps
whatever filesystem ``gtfs_digester.write_exploded()`` would otherwise
resolve, injecting that metadata into every file it opens for write. A
``None`` deadline (permanent/null-window source version) is a pure
passthrough: no customTime is ever set, matching "permanent and
null-window derived objects carry no customTime" in the spec.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import fsspec


class _StampingFilesystem:
    """Wraps an fsspec filesystem so every file opened for write is created
    with a fixed GCS ``customTime``. Wraps rather than subclasses — fsspec
    filesystem instances are commonly cached per URL, and wrapping avoids
    fighting that cache. Delegates every other attribute/method (mkdirs,
    exists, ls, ...) straight through to the wrapped filesystem.
    """

    def __init__(self, fs: fsspec.AbstractFileSystem, deadline: datetime) -> None:
        self._fs = fs
        self._deadline = deadline

    def __getattr__(self, name: str) -> Any:
        return getattr(self._fs, name)

    def open(self, path: str, mode: str = "rb", *args: Any, **kwargs: Any):
        if "w" in mode or "x" in mode:
            fixed = dict(kwargs.pop("fixed_key_metadata", None) or {})
            fixed.setdefault("custom_time", self._deadline.isoformat())
            kwargs["fixed_key_metadata"] = fixed
        return self._fs.open(path, mode, *args, **kwargs)


def stamped_filesystem(
    base_path: str,
    deadline: datetime | None,
) -> tuple[fsspec.AbstractFileSystem, str]:
    """Resolve the fsspec filesystem for ``base_path``, wrapped so every
    subsequently-written file is stamped with ``customTime = deadline`` at
    creation. Pass both return values straight through to
    ``gtfs_digester.write_exploded(filesystem=fs, base_path=resolved, ...)``
    — ``write_exploded`` only resolves its own filesystem when ``None`` is
    passed in, so handing it an already-resolved (and already-wrapped) one
    skips that and uses ours for every write.

    ``deadline=None`` (a permanent/null-window source version) returns the
    plain, unwrapped filesystem: no customTime is ever set, matching
    specs/data-retention.md's "permanent and null-window derived objects
    carry no customTime."
    """
    fs, resolved = fsspec.core.url_to_fs(base_path)
    if deadline is None:
        return fs, resolved
    return _StampingFilesystem(fs, deadline), resolved
