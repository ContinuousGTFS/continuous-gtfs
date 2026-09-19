"""Step base class and @step decorator."""

from __future__ import annotations

import dataclasses
import hashlib
import inspect
import json
import os
import sys
import textwrap
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from .context import PipelineContext


BeforeSpec = "list[Step] | Literal['*']"

# A step's declared finding vocabulary, as authored: either a bare code
# ("one issue for the whole condition") or a (code, {"subject": [...]}) pair
# naming the context keys that identify what the finding is about. See
# specs/transform-framework.md §Declaring a finding vocabulary.
FindingsSpec = "list[str | tuple[str, dict[str, Any]]]"


def normalize_findings(
    spec: list[Any] | None, *, step_name: str = ""
) -> dict[str, list[str]]:
    """Normalize a `findings=[...]` declaration into {code: subject_keys}.

    Accepts a bare code (no subject → class grain) or a
    ``(code, {"subject": [...]})`` pair. Never raises on a malformed entry:
    declaring is discoverability, not enforcement
    (specs/transform-framework.md §Declaring a finding vocabulary), so a
    typo'd declaration degrades to "no subject" and warns rather than
    breaking pipeline load. A duplicate code keeps the last entry.
    """
    declared: dict[str, list[str]] = {}
    for entry in spec or []:
        code: Any
        subject: Any = []
        if isinstance(entry, str):
            code = entry
        elif isinstance(entry, (tuple, list)) and len(entry) == 2:
            code, options = entry
            if isinstance(options, dict):
                subject = options.get("subject", [])
            else:
                _warn_findings_entry(step_name, entry)
        else:
            _warn_findings_entry(step_name, entry)
            continue

        if not isinstance(code, str) or not code:
            _warn_findings_entry(step_name, entry)
            continue
        if isinstance(subject, str):
            subject = [subject]
        if not isinstance(subject, (list, tuple)) or not all(
            isinstance(k, str) for k in subject
        ):
            _warn_findings_entry(step_name, entry)
            subject = []
        declared[code] = list(subject)
    return declared


def _warn_findings_entry(step_name: str, entry: Any) -> None:
    import warnings

    where = f" on step {step_name}" if step_name else ""
    warnings.warn(
        f"Malformed findings declaration{where}: {entry!r}. Expected a code "
        'string or a (code, {"subject": [...]}) pair; treating it as having '
        "no subject.",
        stacklevel=3,
    )


class Step:
    """Base class for all pipeline steps.

    Builtins subclass this and set `files` and `description` as class attributes.
    Custom code uses the @step decorator which returns a Step instance.
    """

    files: list[str] = []
    description: str = ""
    #: Declared finding vocabulary, as authored (see `normalize_findings`).
    #: Builtins that emit findings set this as a class attribute.
    findings: list[Any] = []

    def __init__(
        self,
        *,
        after: list[Step] | Literal["*"] | None = None,
        before: list[Step] | Literal["*"] | None = None,
        priority: int = 100,
        tags: list[str] | None = None,
        data_owner: str | None = None,
        enabled: bool = True,
        findings: list[Any] | None = None,
        **kwargs: Any,
    ):
        if before == "*" and after == "*":
            raise ValueError(
                "Step cannot have both before='*' and after='*' — that asks "
                "the step to run before AND after every other step, which is "
                "a contradiction (every pair would form a cycle)."
            )

        # Copy class-level defaults to instance to avoid shared mutable state
        self.files = list(type(self).files)
        self.description = type(self).description
        # Finding vocabulary: the authored form stays on `.findings` (what the
        # scanner/CLI shows), the resolved {code: subject_keys} map on
        # `.declared_findings` (what emission and the registration snapshot
        # read).
        self.findings = list(findings if findings is not None else type(self).findings)
        self.declared_findings: dict[str, list[str]] = normalize_findings(self.findings)
        # `before` / `after` accept either a list of Step references or the
        # literal "*" meaning "runs before / after every other step in the
        # DAG". The resolver expands "*" into implicit edges at DAG-build
        # time so the rest of the framework continues to work with
        # explicit edge lists.
        self.after: list[Step] | Literal["*"] = "*" if after == "*" else (after or [])
        self.before: list[Step] | Literal["*"] = (
            "*" if before == "*" else (before or [])
        )
        self.priority = priority
        self.tags = tags or []
        self.data_owner = data_owner
        self.enabled = enabled
        # Set by scanner from variable name
        self.name: str = ""
        # Source info: `source_path` is the ABSOLUTE defining location the
        # framework keeps for itself (module hashing, re-scans); `source_file`
        # is what registration emits — the same location made relative to the
        # pipeline codebase root by the scanner. A builtin instance records
        # where it was constructed (the first frame outside this package), so
        # provenance does not depend on which module the scanner meets it in;
        # @step overrides both with the decorated function's own location.
        site = _definition_site()
        self.source_path: str | None = site[0] if site else None
        self.source_file: str | None = site[0] if site else None
        self.source_line: int | None = site[1] if site else None
        # Definition provenance for cross-image comparison
        # (specs/transform-framework.md §Step Snapshot at Registration,
        # "Step identity across images"). `source_hash` is set by @step from
        # the function's source text, or by the scanner from
        # `definition_params()` for a builtin instance; `module_hash` is the
        # scanner's hash of the defining module's bytes.
        self.source_hash: str | None = None
        self.module_hash: str | None = None

    @property
    def is_builtin(self) -> bool:
        return type(self) is not Step and not hasattr(self, "_source_func")

    @property
    def builtin_name(self) -> str | None:
        return type(self).__name__ if self.is_builtin else None

    @property
    def label(self) -> str:
        """Human-readable label for UI display."""
        if self.description:
            return self.description.split("\n")[0]
        return self.name.replace("_", " ").title()

    def apply(self, ctx: PipelineContext) -> None:
        raise NotImplementedError(f"Step {self.name} has no apply() implementation")

    # Instance attributes that are bookkeeping, not definition: excluded from
    # `definition_params()` so two identical builtin instances hash the same
    # regardless of where the scanner found them or what they were wired to.
    _NON_DEFINITION_ATTRS = frozenset(
        {
            "name",
            "source_path",
            "source_file",
            "source_line",
            "source_hash",
            "module_hash",
            "after",
            "before",
            "apply",
            "_source_func",
        }
    )

    def definition_params(self) -> dict[str, Any]:
        """What this instance was built from, as a JSON-canonicalizable dict.

        The default reads every instance attribute except DAG wiring and
        scanner bookkeeping, so a builtin's constructor parameters (files,
        conditions, description, ...) are covered without each builtin
        declaring them. A builtin holding state that does not canonicalize
        (a callable, a DataFrame) overrides this to name what defines it.
        """
        return {
            k: v for k, v in vars(self).items() if k not in self._NON_DEFINITION_ATTRS
        }


_PACKAGE_DIR = Path(__file__).resolve().parent


def _definition_site() -> tuple[str, int] | None:
    """The first caller frame outside this package: where an agency module
    constructed the step. None when no such frame has a real file (a REPL,
    exec'd code)."""
    frame = sys._getframe(1)
    while frame is not None:
        filename = frame.f_code.co_filename
        if os.path.isabs(filename) and not filename.startswith("<"):
            try:
                inside = Path(filename).resolve().is_relative_to(_PACKAGE_DIR)
            except (OSError, ValueError):
                inside = False
            if not inside:
                return filename, frame.f_lineno
        frame = frame.f_back
    return None


def _canonical(value: Any) -> Any:
    """Reduce a definition parameter to JSON-stable data. Never raises: a
    value with no stable representation reduces to its type name, so a
    registration cannot fail on a hash."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            "__type__": type(value).__name__,
            **_canonical(dataclasses.asdict(value)),
        }
    if isinstance(value, dict):
        return {
            str(k): _canonical(v)
            for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_canonical(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted(_canonical(v) for v in value)
    if isinstance(value, Step):
        return {"__step__": value.name or type(value).__name__}
    r = repr(value)
    # A default object repr carries a memory address; that is noise, not
    # definition.
    if " at 0x" in r:
        return {"__type__": type(value).__name__}
    return r


def definition_hash(step_obj: Step) -> str:
    """sha256 over the canonical JSON of `step_obj.definition_params()`."""
    payload = json.dumps(
        _canonical(step_obj.definition_params()), sort_keys=True, default=str
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def source_text_hash(source: str) -> str:
    """sha256 over source text normalized so a re-indent or trailing
    whitespace is not a change: dedented, each line right-stripped."""
    lines = [line.rstrip() for line in textwrap.dedent(source).splitlines()]
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


def step(
    files: list[str] | None = None,
    *,
    after: list[Step] | Literal["*"] | None = None,
    before: list[Step] | Literal["*"] | None = None,
    priority: int = 100,
    tags: list[str] | None = None,
    data_owner: str | None = None,
    enabled: bool = True,
    description: str | None = None,
    findings: list[Any] | None = None,
) -> Callable[[Callable], Step]:
    """Decorator that wraps a function as a Step instance.

    Usage:
        @step(files=["stops.txt"], after=[some_other_step])
        def my_transform(ctx):
            ...

        @step(
            files=["stops.txt"],
            findings=[("stop_outside_area", {"subject": ["stop_id"]})],
        )
        def check_stops(ctx):
            ctx.emit_finding("stop_outside_area", ...)

    The decorated name becomes a Step instance, not a function.
    """

    def decorator(func: Callable) -> Step:
        s = Step(
            after=after,
            before=before,
            priority=priority,
            tags=tags,
            data_owner=data_owner,
            enabled=enabled,
            findings=findings,
        )
        s.files = files or []
        s.description = description or func.__doc__ or ""
        s.apply = func  # type: ignore[assignment]
        s._source_func = func  # type: ignore[attr-defined]
        try:
            filename = inspect.getfile(func)
            if os.path.isabs(filename) and not filename.startswith("<"):
                s.source_path = s.source_file = filename
                s.source_line = inspect.getsourcelines(func)[1]
            # else: exec'd or generated code ("<string>") — keep the
            # construction site Step.__init__ recorded as the location.
            s.source_hash = source_text_hash(inspect.getsource(func))
        except (TypeError, OSError):
            # Source unavailable: the location may be known, the definition
            # is not — leave source_hash None so it registers as unknown
            # rather than as some other hash.
            pass
        return s

    return decorator
