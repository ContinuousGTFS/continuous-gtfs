"""The canonical tree packer — Python twin of the orchestrator's.

specs/asset-registry.md §Tree paths: a deterministic zip encoding such
that identical trees yield byte-identical archives, regardless of when,
where, or by which implementation they were packed. The platform
orchestrator's TypeScript packer is the fetch path; this one serves the
local CLI's ``--input`` directory packing (specs/transform-framework.md
§Running Pipelines Locally), so fixtures stay loose files on disk and
every local run exercises the packer's determinism. Both are pinned to
the same golden vector (tests/fixtures/file-tree-vector/vector.json);
either drifting breaks its own suite.

The encoding, stated once (mirrors the TS module's header):

- **File entries only** — no directory entries; structure is implicit in
  entry paths, exactly as in a git tree. Paths are relative to the tree
  root, forward slashes, UTF-8 (general-purpose flag bit 11 always set).
- **Bytewise order** — entries sorted by the UTF-8 bytes of their path.
- **STORE** (method 0) as the one fixed compression setting — what makes
  cross-language byte-identity structural rather than hopeful.
- **Fixed timestamps and attributes** — DOS date 1980-01-01 00:00:00,
  version made-by/needed 20, internal/external attributes 0, no extra
  fields, no comments.

**DEFLATE variant** (``compress=True``) — the schedule packager's
encoding for published outputs (specs/asset-registry.md §Output Assets,
"Schedule packaging is byte-deterministic"). Every rule above holds
except the method: entries are raw DEFLATE (method 8) at a pinned level.
Its bytes are stable for one zlib build, which is what the output dedup
needs; the cross-language golden vector covers the STORE encoding only.

No ZIP64: more than 65,535 entries or any 32-bit overflow is refused
with a clear error rather than written as a corrupt archive. A
zero-entry archive is the 22-byte end-of-central-directory record alone.

Hand-rolled rather than ``zipfile.ZipFile`` with pinned options: the
stdlib writer varies the UTF-8 flag with each name's content and stamps
versions/attributes it owns, so its bytes are not a stable cross-language
contract. This is ~60 lines of struct packing pinned by a digest test.
"""

from __future__ import annotations

import io
import struct
import zlib
from collections.abc import Callable, Iterable
from pathlib import Path

_LFH_SIG = 0x04034B50
_CDH_SIG = 0x02014B50
_EOCD_SIG = 0x06054B50
_FLAGS = 0x0800  # UTF-8 names, always — never varied per entry
_VERSION = 20
_DOS_TIME = 0x0000  # 00:00:00
_DOS_DATE = 0x0021  # 1980-01-01
_MAX_ENTRIES = 0xFFFF
_MAX_U32 = 0xFFFFFFFF
_STORE = 0
_DEFLATE = 8
_DEFLATE_LEVEL = 6  # pinned, never inherited from zlib's default


class CanonicalZipError(ValueError):
    """A tree the canonical encoding cannot represent, or a caller bug."""


def _validate_path(path: str) -> None:
    if not path or path.startswith("/") or "\\" in path:
        raise CanonicalZipError(f"invalid entry path: {path!r}")
    for segment in path.split("/"):
        if segment in ("", ".", ".."):
            raise CanonicalZipError(f"invalid entry path: {path!r}")


def _deflate(content: bytes) -> bytes:
    compressor = zlib.compressobj(_DEFLATE_LEVEL, zlib.DEFLATED, -15)
    return compressor.compress(content) + compressor.flush()


Content = bytes | Callable[[], bytes]


def pack_entries(
    entries: Iterable[tuple[str, Content]], *, compress: bool = False
) -> bytes:
    """Pack ``(path, content)`` pairs into the canonical archive.

    Input order is irrelevant — entries are sorted here, by the UTF-8
    bytes of their path. Duplicate or invalid paths are refused before
    any content is read. ``content`` may be a zero-argument callable
    returning the bytes: it is called only when its entry is written, so
    a caller encoding large members holds one member's bytes at a time.
    ``compress=True`` selects the DEFLATE variant (module docstring);
    the default is the STORE encoding tree paths are pinned to.
    """
    method = _DEFLATE if compress else _STORE
    items = [(path, content) for path, content in entries]
    if len(items) > _MAX_ENTRIES:
        raise CanonicalZipError(
            f"tree has {len(items)} file entries; the canonical archive "
            f"supports at most {_MAX_ENTRIES} (no ZIP64)"
        )
    for path, _ in items:
        _validate_path(path)
    items.sort(key=lambda kv: kv[0].encode("utf-8"))
    for (a, _), (b, _) in zip(items, items[1:], strict=False):
        if a == b:
            raise CanonicalZipError(f"duplicate entry path: {a!r}")

    out = io.BytesIO()
    central = bytearray()
    for i, (path, source) in enumerate(items):
        items[i] = (
            path,
            b"",
        )  # drop the reference so each member is freed once written
        content = source() if callable(source) else source
        del source
        name = path.encode("utf-8")
        if len(content) > _MAX_U32:
            raise CanonicalZipError(f"entry {path!r} is {len(content)} bytes; no ZIP64")
        crc = zlib.crc32(content) & 0xFFFFFFFF
        data = _deflate(content) if compress else content
        if len(data) > _MAX_U32:
            raise CanonicalZipError(f"entry {path!r} compresses past 4 GiB; no ZIP64")
        offset = out.tell()
        if offset > _MAX_U32:
            raise CanonicalZipError(
                f"archive exceeds 4 GiB at entry {path!r}; no ZIP64"
            )
        local_header = struct.pack(
            "<IHHHHHIIIHH",
            _LFH_SIG,
            _VERSION,
            _FLAGS,
            method,
            _DOS_TIME,
            _DOS_DATE,
            crc,
            len(data),
            len(content),
            len(name),
            0,  # extra length
        )
        out.write(local_header)
        out.write(name)
        out.write(data)
        central += struct.pack(
            "<IHHHHHHIIIHHHHHII",
            _CDH_SIG,
            _VERSION,  # version made by
            _VERSION,  # version needed
            _FLAGS,
            method,
            _DOS_TIME,
            _DOS_DATE,
            crc,
            len(data),
            len(content),
            len(name),
            0,  # extra length
            0,  # comment length
            0,  # disk number start
            0,  # internal attributes
            0,  # external attributes
            offset,
        )
        central += name
        # Release this member before the next one is encoded.
        del content, data

    cd_offset = out.tell()
    out.write(central)
    out.write(
        struct.pack(
            "<IHHHHIIH",
            _EOCD_SIG,
            0,
            0,
            len(items),
            len(items),
            len(central),
            cd_offset,
            0,
        )
    )
    # getvalue() hands back the buffer without a further copy.
    return out.getvalue()


def pack_tree(root: Path) -> bytes:
    """Pack a directory on disk into the canonical archive.

    Walks ``root`` recursively; every regular file becomes an entry whose
    path is relative to ``root`` with forward slashes. A symlink anywhere
    under the tree is refused — the fetch path refuses links in a
    committed tree (specs/asset-registry.md §Tree paths), and a local run
    must not produce an archive the platform never would.
    """
    root = Path(root)
    if not root.is_dir():
        raise CanonicalZipError(f"not a directory: {root}")
    entries: list[tuple[str, bytes]] = []
    for path in sorted(root.rglob("*")):
        rel = path.relative_to(root).as_posix()
        if path.is_symlink():
            raise CanonicalZipError(
                f"{rel!r} is a symbolic link — the canonical archive holds regular "
                f"files only; replace the link with the real file"
            )
        if path.is_file():
            entries.append((rel, path.read_bytes()))
    return pack_entries(entries)
