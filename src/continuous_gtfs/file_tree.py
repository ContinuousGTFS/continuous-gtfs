"""FileTree — the ``file_tree`` content kind's parsed shape.

specs/transform-framework.md §PipelineContext: a read-only root
implementing the stdlib Traversable protocol
(importlib.resources.abc.Traversable) over a zip archive — a typed
mirror of git's object model, where every node answers ``is_dir()``
(tree) or ``is_file()`` (blob). Get-at-path via ``/`` / ``joinpath``
(multi-segment paths accepted), shallow listing via ``iterdir()``, deep
listing via ``rglob()``; entry bytes via ``read_bytes()`` /
``read_text()``, decompressed lazily on access.

Two guarantees beyond the engine underneath (``zipfile.Path`` — an
implementation detail, pinned by this module's own tests and swappable):

- **Iteration is name-sorted**, regardless of archive entry order, so
  behavior never differs between a canonical git-tree archive and a
  hand-pushed zip (transport-independent determinism).
- **The root of a zero-entry archive is a valid, empty directory node**
  — a deliberately emptied tree is a legitimate state
  (specs/asset-registry.md §Tree paths).

Read-only: the class exposes no write surface, and the archive bytes it
wraps are never touched.
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Iterator
from typing import IO, Any


class FileTree:
    """A node in an archived file tree: the root (built from the archive
    bytes), or a descendant reached through ``/`` / ``joinpath`` /
    iteration. Directory nodes exist implicitly through entry paths,
    exactly as directories exist in a git tree."""

    __slots__ = ("_node",)

    def __init__(self, raw: bytes) -> None:
        """Parse archive bytes into the tree's root node.

        Raises ``zipfile.BadZipFile`` when the bytes aren't a readable
        archive — the same loud refusal every other content kind's
        parser gives malformed bytes.
        """
        self._node = zipfile.Path(zipfile.ZipFile(io.BytesIO(raw)))

    @classmethod
    def _wrap(cls, node: zipfile.Path) -> FileTree:
        tree = object.__new__(cls)
        tree._node = node
        return tree

    # --- Traversable -------------------------------------------------------

    @property
    def name(self) -> str:
        """The node's base name; the root's is the empty-ish engine default."""
        return self._node.name

    def is_dir(self) -> bool:
        """A tree node — the root, or an interior directory."""
        return self._node.is_dir()

    def is_file(self) -> bool:
        """A blob node — an archive entry."""
        return self._node.is_file()

    def iterdir(self) -> Iterator[FileTree]:
        """The node's children, **name-sorted** regardless of the order the
        archive's entries were written in."""
        for child in sorted(self._node.iterdir(), key=lambda p: p.name):
            yield FileTree._wrap(child)

    def joinpath(self, *descendants: str) -> FileTree:
        """Descend to a child or deeper descendant; each segment may itself
        contain ``/`` (multi-segment paths accepted)."""
        return FileTree._wrap(self._node.joinpath(*descendants))

    def __truediv__(self, child: str) -> FileTree:
        return self.joinpath(child)

    def open(self, mode: str = "r", *args: Any, **kwargs: Any) -> IO[Any]:
        """Open a file node for reading (``r`` or ``rb``)."""
        return self._node.open(mode, *args, **kwargs)

    def read_bytes(self) -> bytes:
        return self._node.read_bytes()

    def read_text(self, *args: Any, **kwargs: Any) -> str:
        return self._node.read_text(*args, **kwargs)

    # --- Beyond the protocol ----------------------------------------------

    def exists(self) -> bool:
        return self._node.exists()

    def glob(self, pattern: str) -> Iterator[FileTree]:
        """Patterned matching under this node, deterministically ordered
        (sorted by archive path) regardless of archive entry order."""
        for match in sorted(self._node.glob(pattern), key=lambda p: p.at):
            yield FileTree._wrap(match)

    def rglob(self, pattern: str) -> Iterator[FileTree]:
        """Recursive matching: the pattern applied at this node and at every
        directory below it, depth-first in name order (shallow matches
        before deep ones) — deterministic regardless of archive entry
        order, and independent of any engine quirk about whether ``**``
        matches the empty prefix."""
        yield from self.glob(pattern)
        for child in self.iterdir():
            if child.is_dir():
                yield from child.rglob(pattern)

    def __repr__(self) -> str:
        return f"FileTree({str(self._node)!r})"
