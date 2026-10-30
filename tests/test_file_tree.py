"""FileTree and the canonical packer (specs/transform-framework.md
§PipelineContext; specs/asset-registry.md §Tree paths).

Two contracts under test:

- The **canonical packer** produces byte-identical archives for identical
  trees — pinned against the shared golden vector
  (tests/fixtures/file-tree-vector/vector.json), whose digest the
  platform orchestrator's TypeScript packer suite states as the same
  literal, so either packer drifting breaks its own CI.
- **FileTree**'s guarantees beyond its engine: name-sorted iteration
  regardless of archive entry order, and a zero-entry archive yielding a
  valid empty root.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import zipfile
from importlib.resources.abc import Traversable
from pathlib import Path

import pytest

from continuous_gtfs.canonical_zip import CanonicalZipError, pack_entries, pack_tree
from continuous_gtfs.file_tree import FileTree
from continuous_gtfs.parsers import parse

# GOLDEN VECTOR: shared with the platform orchestrator's packer suite
# (a TypeScript port of the same encoding). If this
# assertion fails, the canonical encoding changed — a breaking change to
# every stored file_tree version's identity, not a snapshot to refresh.
GOLDEN_SHA256 = "6f2849c7020529fac4cf6e9fdf6165c9399d80cf915f3bf15fb556544cbdc1c9"
# A zero-entry archive: the 22-byte end-of-central-directory record.
EMPTY_SHA256 = "8739c76e681f900923b900c9df0ef75cf421d39cabb54650c4b9ad19b6a76d85"

VECTOR_PATH = (
    Path(__file__).resolve().parent / "fixtures" / "file-tree-vector" / "vector.json"
)


def _vector() -> tuple[list[tuple[str, bytes]], str]:
    manifest = json.loads(VECTOR_PATH.read_text())
    entries = [(path, base64.b64decode(b64)) for path, b64 in manifest["files"].items()]
    return entries, manifest["sha256"]


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# --- Canonical packer -------------------------------------------------------


def test_golden_vector_digest():
    entries, manifest_digest = _vector()
    # The manifest's own digest must agree with this suite's literal — a
    # drifted manifest should fail HERE, not only in the other language.
    assert manifest_digest == GOLDEN_SHA256
    assert _sha256(pack_entries(entries)) == GOLDEN_SHA256


def test_input_order_is_irrelevant():
    entries, _ = _vector()
    assert _sha256(pack_entries(reversed(entries))) == GOLDEN_SHA256


def test_pack_tree_matches_pack_entries_for_a_materialized_directory(tmp_path):
    # The CLI's directory route: materialize the vector as loose files and
    # pack the directory — byte-identical to packing the entries, so every
    # local run exercises the same determinism the fetcher promises.
    entries, _ = _vector()
    for rel, content in entries:
        target = tmp_path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    assert _sha256(pack_tree(tmp_path)) == GOLDEN_SHA256


def test_pack_tree_refuses_a_symlink(tmp_path):
    (tmp_path / "real.txt").write_bytes(b"x")
    (tmp_path / "alias.txt").symlink_to("real.txt")
    with pytest.raises(CanonicalZipError, match="symbolic link"):
        pack_tree(tmp_path)


def test_empty_archive_is_the_eocd_alone():
    packed = pack_entries([])
    assert len(packed) == 22
    assert _sha256(packed) == EMPTY_SHA256


def test_invalid_and_duplicate_paths_are_refused():
    for bad in ("/abs.txt", "a/../b.txt", "a\\b.txt", "", "a//b.txt", "./a.txt"):
        with pytest.raises(CanonicalZipError):
            pack_entries([(bad, b"")])
    with pytest.raises(CanonicalZipError, match="duplicate"):
        pack_entries([("a.txt", b"1"), ("a.txt", b"2")])


def test_stdlib_zipfile_reads_the_canonical_archive():
    entries, _ = _vector()
    zf = zipfile.ZipFile(io.BytesIO(pack_entries(entries)))
    ordered = sorted(entries, key=lambda e: e[0].encode())
    assert zf.namelist() == [path for path, _ in ordered]
    assert zf.read("a/nested.txt") == b"nested\n"
    assert zf.testzip() is None


# --- FileTree ----------------------------------------------------------------


def _reverse_order_zip(files: dict[str, bytes]) -> bytes:
    """A zip whose entries are written in REVERSE name order — the shape
    the sorted-iteration guarantee exists for."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name in sorted(files, reverse=True):
            zf.writestr(name, files[name])
    return buf.getvalue()


def test_root_is_a_traversable_directory():
    tree = FileTree(pack_entries([("a.txt", b"x")]))
    assert isinstance(tree, Traversable)
    assert tree.is_dir()
    assert not tree.is_file()


def test_iterdir_is_name_sorted_regardless_of_archive_order():
    raw = _reverse_order_zip(
        {"zeta.txt": b"z", "alpha.txt": b"a", "mid/inner.txt": b"i", "beta.txt": b"b"}
    )
    tree = FileTree(raw)
    assert [child.name for child in tree.iterdir()] == [
        "alpha.txt",
        "beta.txt",
        "mid",
        "zeta.txt",
    ]
    # And the guarantee holds below the root too.
    mid = tree / "mid"
    assert mid.is_dir()
    assert [c.name for c in mid.iterdir()] == ["inner.txt"]


def test_navigation_and_reads_mirror_gits_model():
    entries, _ = _vector()
    tree = FileTree(pack_entries(entries))
    # `/` descends; a multi-segment path is accepted in one join.
    nested = tree / "a" / "nested.txt"
    assert nested.is_file() and not nested.is_dir()
    assert nested.read_bytes() == b"nested\n"
    assert tree.joinpath("a/nested.txt").read_text() == "nested\n"
    # An interior directory node answers is_dir, like a git tree.
    assert (tree / "a").is_dir()
    # An empty file is a valid blob.
    assert (tree / "a" / "empty.bin").read_bytes() == b""
    # An absent path exists() False.
    assert not (tree / "no-such").exists()


def test_rglob_walks_deterministically():
    raw = _reverse_order_zip({"b/x.txt": b"1", "a/y.txt": b"2", "top.txt": b"3"})
    tree = FileTree(raw)
    # Depth-first, shallow before deep, directories in name order.
    assert [m.name for m in tree.rglob("*.txt")] == ["top.txt", "y.txt", "x.txt"]
    assert [m.name for m in tree.glob("*.txt")] == ["top.txt"]


def test_zero_entry_archive_is_a_valid_empty_root():
    tree = FileTree(pack_entries([]))
    assert tree.is_dir()
    assert list(tree.iterdir()) == []
    assert list(tree.rglob("*")) == []


def test_explicit_directory_entries_change_nothing():
    # A hand-pushed zip may carry explicit directory entries; they add
    # nothing and change nothing (specs/asset-registry.md §Tree paths).
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("dir/", b"")
        zf.writestr("dir/a.txt", b"x")
    with_dirs = FileTree(buf.getvalue())
    without_dirs = FileTree(pack_entries([("dir/a.txt", b"x")]))
    assert [c.name for c in with_dirs.iterdir()] == [
        c.name for c in without_dirs.iterdir()
    ]
    assert (with_dirs / "dir" / "a.txt").read_bytes() == b"x"


def test_parser_registry_routes_file_tree():
    entries, _ = _vector()
    tree = parse("file_tree", pack_entries(entries))
    assert isinstance(tree, FileTree)
    assert (tree / "README.md").read_text() == "golden tree fixture\n"


def test_unparseable_bytes_refuse_loudly():
    with pytest.raises(zipfile.BadZipFile):
        parse("file_tree", b"not an archive")
