"""Pipeline folder scanner and DAG resolver."""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import TypedDict

from .parsers import known_kinds
from .step import Step, definition_hash


class InputSpec(TypedDict):
    """One normalized `INPUTS` manifest entry.

    `load_inputs_manifest` reduces both manifest forms — a bare
    content_kind string (required) and a `{"content_kind": ..., "optional":
    True}` mapping — to this shape so every consumer (worker dispatch
    validation, registration, the local CLI) reads one structure. See
    specs/transform-framework.md §INPUTS manifest.
    """

    content_kind: str
    optional: bool


def scan_pipeline(folder: str | Path, root: str | Path | None = None) -> list[Step]:
    """Scan a folder for Step instances and return them with names inferred.

    Imports all .py files in the folder (non-recursive), finds module-level
    objects that are isinstance(obj, Step), and infers the step name from
    the variable name.

    The folder is treated as a Python package so files can use relative
    imports (e.g., `from .feed_info import update_feed`).

    Every step leaves with its definition provenance stamped
    (specs/transform-framework.md §Step Snapshot at Registration):
    `source_file` relative to `root` — the pipeline codebase root, which
    defaults to the parent of a `pipelines/` folder or else the folder's
    parent — `source_hash` (set here from `definition_params()` for a
    builtin instance; already set by @step for a function) and
    `module_hash` over the defining module's bytes.
    """
    folder = Path(folder).resolve()
    root = _codebase_root(folder, root)
    # Use path hash to avoid collisions between folders with the same name
    path_hash = hashlib.sha256(str(folder).encode()).hexdigest()[:8]
    pkg_name = f"_pipeline_{folder.name}_{path_hash}"
    steps: list[Step] = []
    seen_names: set[str] = set()
    seen_ids: set[int] = set()
    module_hashes: dict[str, str] = {}

    _register_package(folder, pkg_name)

    for py_file in sorted(folder.glob("*.py")):
        if py_file.name.startswith("_"):
            continue

        module = _import_module(py_file, pkg_name)
        if module is None:
            continue

        for attr_name in dir(module):
            if attr_name.startswith("_"):
                continue
            obj = getattr(module, attr_name)
            if isinstance(obj, Step) and id(obj) not in seen_ids:
                if attr_name in seen_names:
                    raise ValueError(
                        f"Duplicate step name '{attr_name}' found in {py_file.name}"
                    )
                obj.name = attr_name
                if obj.source_path is None:
                    # No construction site recorded (a REPL-built step):
                    # the module the scanner met it in is the best answer.
                    obj.source_path = str(py_file)
                _stamp_provenance(obj, root, module_hashes)
                seen_names.add(attr_name)
                seen_ids.add(id(obj))
                steps.append(obj)

    return steps


def _codebase_root(folder: Path, root: str | Path | None) -> Path:
    if root is not None:
        return Path(root).resolve()
    if folder.parent.name == "pipelines":
        return folder.parent.parent
    return folder.parent


def _stamp_provenance(
    step_obj: Step, root: Path, module_hashes: dict[str, str]
) -> None:
    """Fill source_file (root-relative), source_hash, module_hash from the
    step's absolute `source_path`. Idempotent — a re-scan of cached modules
    reads the same absolute path again, never the relative form a prior
    scan emitted. Never raises: a step whose source cannot be read registers
    with the hashes empty rather than failing the scan."""
    if step_obj.source_hash is None and step_obj.is_builtin:
        # A builtin instance IS its parameters. A custom function whose
        # source was unavailable stays unknown — hashing its attributes would
        # make two different bodies read as one definition.
        try:
            step_obj.source_hash = definition_hash(step_obj)
        except Exception:  # noqa: BLE001 — provenance is best-effort
            step_obj.source_hash = None
    if step_obj.source_path is None:
        return
    src = Path(step_obj.source_path)
    key = str(src)
    if key not in module_hashes:
        try:
            module_hashes[key] = hashlib.sha256(src.read_bytes()).hexdigest()
        except OSError:
            module_hashes[key] = ""
    step_obj.module_hash = module_hashes[key] or None
    try:
        step_obj.source_file = src.resolve().relative_to(root).as_posix()
    except ValueError:
        # Defined outside the codebase root (an installed package's step):
        # keep the path as found rather than invent a relative one.
        step_obj.source_file = str(src)


VALID_FEED_TYPES = ("schedule", "realtime")


def load_feed_type(folder: str | Path) -> str:
    """Load the FEED_TYPE constant from a pipeline folder's __init__.py.

    Every pipeline declares FEED_TYPE alongside INPUTS so the framework
    can route dispatches to the right execution path (DataFrame mutation
    vs FeedMessage transform) and the worker can advertise per-pipeline
    capabilities at registration time. See
    specs/transform-framework.md §INPUTS manifest.

    Raises:
        FileNotFoundError: if the folder has no __init__.py.
        ValueError: if FEED_TYPE is missing or not one of the valid kinds.
    """
    folder = Path(folder).resolve()
    init_file = folder / "__init__.py"
    if not init_file.exists():
        raise FileNotFoundError(
            f"Pipeline folder {folder} is missing __init__.py with a FEED_TYPE "
            f"constant. See specs/transform-framework.md §INPUTS manifest."
        )

    path_hash = hashlib.sha256(str(folder).encode()).hexdigest()[:8]
    pkg_name = f"_pipeline_{folder.name}_{path_hash}"
    _register_package(folder, pkg_name)
    pkg_module = sys.modules.get(pkg_name)
    if pkg_module is None:
        raise ValueError(f"Failed to load pipeline package at {folder}")

    feed_type = getattr(pkg_module, "FEED_TYPE", None)
    if feed_type is None:
        raise ValueError(
            f"Pipeline {folder} must define a top-level FEED_TYPE constant in "
            f"__init__.py (one of {VALID_FEED_TYPES}). See "
            f"specs/transform-framework.md §INPUTS manifest."
        )
    if feed_type not in VALID_FEED_TYPES:
        raise ValueError(
            f"Pipeline {folder} FEED_TYPE = {feed_type!r} is not valid. "
            f"Must be one of {VALID_FEED_TYPES}."
        )
    return feed_type


def discover_pipelines(repo_root: str | Path) -> list[Path]:
    """Find every pipeline directory under <repo_root>/pipelines/.

    A pipeline is any immediate subdirectory of <repo_root>/pipelines/
    that contains an __init__.py. Returned paths are sorted by name for
    deterministic load order. Hidden directories (starting with '.' or
    '_') and the __pycache__ directory are skipped.

    Raises:
        FileNotFoundError: if <repo_root>/pipelines/ doesn't exist.
    """
    repo_root = Path(repo_root).resolve()
    pipelines_dir = repo_root / "pipelines"
    if not pipelines_dir.is_dir():
        raise FileNotFoundError(
            f"Agency repo {repo_root} has no pipelines/ subdirectory. "
            f"See specs/configuration.md §Agency Project Structure."
        )

    results: list[Path] = []
    for child in sorted(pipelines_dir.iterdir()):
        if not child.is_dir():
            continue
        if child.name.startswith(".") or child.name.startswith("_"):
            continue
        if not (child / "__init__.py").exists():
            continue
        results.append(child)
    return results


def load_inputs_manifest(folder: str | Path) -> dict[str, InputSpec]:
    """Load the INPUTS manifest from a pipeline folder's __init__.py.

    Every pipeline must declare its named inputs as a module-level
    `INPUTS` dict in the folder's `__init__.py` (see
    specs/transform-framework.md §INPUTS manifest). Each value is either
    a bare `content_kind` string — a **required** input — or a mapping
    `{"content_kind": <kind>, "optional": True}` — an **optional** input.
    Both forms are normalized to `InputSpec` here so callers never see
    the raw shape; `manifest_kinds()` gives the flat name → content_kind
    view the parser registry wants.

    The manifest is the pipeline's input contract — the worker validates
    dispatches against it and reports it at registration, and the CLI
    uses it to resolve content_kind for local --input flags.

    Raises:
        FileNotFoundError: if the folder has no __init__.py.
        ValueError: if the manifest is missing, malformed, or references
            an unknown content_kind.
    """
    folder = Path(folder).resolve()
    init_file = folder / "__init__.py"
    if not init_file.exists():
        raise FileNotFoundError(
            f"Pipeline folder {folder} is missing __init__.py with an INPUTS "
            f"manifest. See specs/transform-framework.md §INPUTS manifest."
        )

    path_hash = hashlib.sha256(str(folder).encode()).hexdigest()[:8]
    pkg_name = f"_pipeline_{folder.name}_{path_hash}"
    _register_package(folder, pkg_name)
    pkg_module = sys.modules.get(pkg_name)
    if pkg_module is None:
        raise ValueError(f"Failed to load pipeline package at {folder}")

    manifest = getattr(pkg_module, "INPUTS", None)
    if manifest is None:
        raise ValueError(
            f"Pipeline {folder} must define a top-level INPUTS dict in "
            f"__init__.py declaring each named input and its content_kind. "
            f"See specs/transform-framework.md §INPUTS manifest."
        )
    if not isinstance(manifest, dict):
        raise ValueError(
            f"Pipeline {folder} INPUTS must be a dict, got "
            f"{type(manifest).__name__}. "
            f"See specs/transform-framework.md §INPUTS manifest."
        )

    valid_kinds = known_kinds()
    normalized: dict[str, InputSpec] = {}
    for name, value in manifest.items():
        if not isinstance(name, str):
            raise ValueError(
                f"Pipeline {folder} INPUTS keys must be input names (str); "
                f"got {name!r}. See specs/transform-framework.md §INPUTS manifest."
            )
        spec = _normalize_input_spec(folder, name, value)
        if spec["content_kind"] not in valid_kinds:
            raise ValueError(
                f"Pipeline {folder} INPUTS[{name!r}] content_kind = "
                f"{spec['content_kind']!r} is not a known content_kind. "
                f"Known: {sorted(valid_kinds)}"
            )
        normalized[name] = spec

    return normalized


def _normalize_input_spec(folder: Path, name: str, value: object) -> InputSpec:
    """Reduce one raw INPUTS value to an InputSpec, rejecting bad shapes."""
    where = f"Pipeline {folder} INPUTS[{name!r}]"
    see = "See specs/transform-framework.md §INPUTS manifest."
    if isinstance(value, str):
        return {"content_kind": value, "optional": False}
    if isinstance(value, Mapping):
        kind = value.get("content_kind")
        if not isinstance(kind, str):
            raise ValueError(
                f"{where} mapping is missing a 'content_kind' string "
                f"(got {dict(value)!r}). {see}"
            )
        optional = value.get("optional", False)
        if not isinstance(optional, bool):
            raise ValueError(
                f"{where} 'optional' must be a bool, got {optional!r}. {see}"
            )
        unknown_keys = set(value) - {"content_kind", "optional"}
        if unknown_keys:
            raise ValueError(
                f"{where} has unknown key(s) {sorted(unknown_keys)}; only "
                f"'content_kind' and 'optional' are allowed. {see}"
            )
        return {"content_kind": kind, "optional": optional}
    raise ValueError(
        f"{where} must be a content_kind string or a "
        f"{{'content_kind': ..., 'optional': ...}} mapping; got {value!r}. {see}"
    )


def manifest_kinds(manifest: Mapping[str, InputSpec]) -> dict[str, str]:
    """Flat name → content_kind view of a normalized INPUTS manifest.

    The parser registry only cares about kinds; this is what
    `load_inputs` and the worker's per-slot parse consume.
    """
    return {name: spec["content_kind"] for name, spec in manifest.items()}


def required_input_names(manifest: Mapping[str, InputSpec]) -> list[str]:
    """Names of the manifest's required (non-optional) inputs, in order."""
    return [name for name, spec in manifest.items() if not spec["optional"]]


def _register_package(folder: Path, pkg_name: str) -> None:
    """Register a folder as a Python package for relative imports."""
    if pkg_name in sys.modules:
        return
    parent = str(folder.parent)
    if parent not in sys.path:
        sys.path.insert(0, parent)
    init_file = folder / "__init__.py"
    if init_file.exists():
        spec = importlib.util.spec_from_file_location(
            pkg_name,
            init_file,
            submodule_search_locations=[str(folder)],
        )
    else:
        spec = importlib.util.spec_from_file_location(
            pkg_name,
            None,
            submodule_search_locations=[str(folder)],
        )
    if spec is None:
        return
    pkg_module = importlib.util.module_from_spec(spec)
    sys.modules[pkg_name] = pkg_module
    if spec.loader and init_file.exists():
        spec.loader.exec_module(pkg_module)


def _import_module(py_file: Path, pkg_name: str):
    """Import a Python file as a submodule of the pipeline package."""
    module_name = f"{pkg_name}.{py_file.stem}"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, py_file)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    module.__package__ = pkg_name
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def dag_edges(steps: list[Step]) -> list[tuple[Step, Step]]:
    """The DAG's effective edge list: (a, b) means a must run before b.

    Wildcard declarations expand here exactly as execution orders them —
    an `after="*"` step gains an edge from every other step (except fellow
    after-wildcards, which would cycle), symmetrically for `before="*"` —
    so every renderer of the DAG (resolve_dag itself, the ReactFlow export,
    the CLI's mermaid view) shows the edges that actually execute rather
    than each re-deriving (or crashing on) the `"*"` sentinel (#683).
    """
    step_set = set(id(s) for s in steps)
    edges: list[tuple[Step, Step]] = []
    for s in steps:
        if s.after == "*":
            for other in steps:
                if other is s or other.after == "*":
                    continue
                edges.append((other, s))
        else:
            for dep in s.after:
                if id(dep) in step_set:
                    edges.append((dep, s))
        if s.before == "*":
            for other in steps:
                if other is s or other.before == "*":
                    continue
                edges.append((s, other))
        else:
            for dep in s.before:
                if id(dep) in step_set:
                    edges.append((s, dep))
    return edges


def resolve_dag(steps: list[Step]) -> list[Step]:
    """Topological sort of steps using after/before references.

    Returns steps in execution order. Raises ValueError on cycles.
    Within a dependency level, sorts by (priority, name) for determinism.

    A step declared with `before="*"` is expanded into implicit edges to
    every other step in the input — use it for init steps that must run
    before any other step the scanner discovers (e.g. one that seeds
    ctx.output from a specific input). `after="*"` is symmetric — the
    step runs after every other step, useful for teardown / summary
    transforms. Multiple wildcards of the same kind don't edge to each
    other (that would be a cycle); they order among themselves via
    (priority, name) in the ready queue.
    """
    edges = dag_edges(steps)

    # Kahn's algorithm
    in_degree = {id(s): 0 for s in steps}
    adjacency: dict[int, list[Step]] = {id(s): [] for s in steps}
    for src, dst in edges:
        adjacency[id(src)].append(dst)
        in_degree[id(dst)] += 1

    queue = sorted(
        [s for s in steps if in_degree[id(s)] == 0],
        key=lambda s: (s.priority, s.name),
    )
    result: list[Step] = []

    while queue:
        current = queue.pop(0)
        result.append(current)
        for neighbor in adjacency[id(current)]:
            in_degree[id(neighbor)] -= 1
            if in_degree[id(neighbor)] == 0:
                queue.append(neighbor)
                queue.sort(key=lambda s: (s.priority, s.name))

    if len(result) != len(steps):
        resolved_names = {s.name for s in result}
        unresolved = [s.name for s in steps if s.name not in resolved_names]
        raise ValueError(f"Cycle detected in DAG. Unresolved steps: {unresolved}")

    return result


def select_with_ancestors(steps: list[Step], target_name: str) -> list[Step]:
    """Return the target step plus all its transitive dependencies, in DAG order.

    Ancestors are computed from:

    - `after` references (direct) — Y has X in its `after` list means Y
      depends on X.
    - Reverse `before` references — Y has X in its `before` list means Y
      runs before X, i.e. X depends on Y.
    - `before="*"` wildcards — any step declared with `before="*"`
      implicitly runs before every other step in the DAG, so it's an
      ancestor of every selected step (including selected `after="*"`
      steps). Without this, `--select` on a pipeline with a wildcard
      init step (seeding `ctx.output`, for example) would run the
      target against an un-seeded context.

    `after="*"` is deliberately NOT pulled in as a descendant —
    `select_with_ancestors` walks the UP direction only (things the
    target needs). Teardown steps run after everything else; they're
    downstream of the target, not prerequisites for it.

    Args:
        steps: The full resolved DAG (output of resolve_dag).
        target_name: Name of the step to select.

    Raises:
        ValueError: if target_name matches no step.
    """
    by_name = {s.name: s for s in steps}
    target = by_name.get(target_name)
    if target is None:
        available = sorted(by_name.keys())
        raise ValueError(
            f"No step named '{target_name}'. Available: {', '.join(available)}"
        )

    # Reverse-lookup index: step_id -> list of steps that must run before it
    # because some other step has it in its `before` list.
    reverse_before: dict[int, list[Step]] = {id(s): [] for s in steps}
    for s in steps:
        if s.before == "*":
            continue  # wildcard handled separately below
        for b in s.before:
            if id(b) in reverse_before:
                reverse_before[id(b)].append(s)

    # Every before="*" step is an ancestor of every other step. Collect
    # them once and seed the frontier with them when any non-wildcard
    # target is selected — and as ancestors of other wildcard targets
    # with a different priority / name (the resolver handles cycle
    # avoidance between same-kind wildcards; select_with_ancestors just
    # includes them all).
    wildcard_before = [s for s in steps if s.before == "*"]

    selected_ids: set[int] = {id(target)}
    frontier: list[Step] = [target]
    while frontier:
        cur = frontier.pop()
        if cur.after == "*":
            # after="*" on the target means it runs after everything —
            # no upstream dependencies to walk via that edge.
            after_deps: list[Step] = []
        else:
            after_deps = list(cur.after)
        deps = after_deps + reverse_before[id(cur)]
        # Every before="*" step is an implicit prerequisite of cur
        # (unless cur is itself a before="*" step — they don't edge
        # to each other per the resolver rules).
        if cur.before != "*":
            deps += wildcard_before
        for dep in deps:
            if id(dep) not in selected_ids:
                selected_ids.add(id(dep))
                frontier.append(dep)

    # Preserve DAG order from the resolved list
    return [s for s in steps if id(s) in selected_ids]


def export_reactflow(steps: list[Step]) -> dict:
    """Export the resolved DAG as ReactFlow-compatible JSON."""
    nodes = []
    for s in steps:
        nodes.append(
            {
                "id": s.name,
                "label": s.label,
                "type": "builtin" if s.is_builtin else "custom",
                "builtin": s.builtin_name,
                "files": list(s.files),
                "source_file": s.source_file,
                "source_line": s.source_line,
                "source_hash": s.source_hash,
                "module_hash": s.module_hash,
                "enabled": s.enabled,
                "priority": s.priority,
                "data_owner": s.data_owner,
                "tags": s.tags,
                # Declared finding vocabulary, so a DAG view can show what a
                # step is capable of reporting without running it
                # (specs/transform-framework.md §Declaring a finding vocabulary).
                "findings": [
                    {"code": code, "subject": list(subject)}
                    for code, subject in s.declared_findings.items()
                ],
            }
        )

    # dag_edges expands wildcards, so an after="*" summary step's edges
    # appear here instead of being silently dropped (#683).
    edges = [
        {"source": a.name, "target": b.name}
        for a, b in dag_edges(steps)
        if a.name and b.name
    ]

    return {"nodes": nodes, "edges": edges}
