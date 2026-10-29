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

#: A step's role (specs/transform-framework.md §Checks): a transform may
#: change the output; a check only reads it and emits findings. Set by the
#: declaration form (@step / Step vs @check / Check), never passed.
StepRole = Literal["transform", "check"]
TRANSFORM: StepRole = "transform"
CHECK: StepRole = "check"


#: Finding codes the framework emits on a check's behalf
#: (specs/transform-framework.md §Dev-time mutation detection, §Checks phase).
#: The platform treats them as declared on every check; no step declares
#: them.
CHECK_MUTATED_VIEW = "check_mutated_view"
CHECK_FAILED = "check_failed"
RESERVED_FINDING_CODES = frozenset({CHECK_MUTATED_VIEW, CHECK_FAILED})


def normalize_findings(
    spec: list[Any] | None, *, step_name: str = ""
) -> dict[str, list[str]]:
    """Normalize a `findings=[...]` declaration into {code: subject_keys}.

    Accepts a bare code (no subject → class grain) or a
    ``(code, {"subject": [...]})`` pair. Never raises on a malformed entry:
    declaring is discoverability, not enforcement
    (specs/transform-framework.md §Declaring a finding vocabulary), so a
    typo'd declaration degrades to "no subject" and warns rather than
    breaking pipeline load. A duplicate code keeps the last entry. A
    framework-reserved code (RESERVED_FINDING_CODES) is dropped with a
    warning: the framework owns it, and the platform already counts it as
    declared on every check.
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
        if code in RESERVED_FINDING_CODES:
            import warnings

            where = f" on step {step_name}" if step_name else ""
            warnings.warn(
                f"findings declaration{where} names {code!r}, a code the "
                "framework reserves for checks; dropping it. See "
                "specs/transform-framework.md §Dev-time mutation detection.",
                stacklevel=3,
            )
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
    """Base class for every pipeline step.

    Builtins subclass it and set `files`, `description`, `findings` and
    `category` as class attributes; `@step` and `@check` wrap functions into
    instances. Every step, builtin or custom, takes the shared parameters
    below, so a builtin's own `Args:` lists only what it adds.

    Args:
        after: Steps this one runs after, or `"*"` to run after every other
            step that touches its files.
        before: Steps this one runs before, or `"*"` to run before every
            other step. A step cannot be both `before="*"` and `after="*"`.
        priority: Tiebreak among steps at the same DAG level; lower runs
            earlier.
        tags: Free labels for grouping steps.
        data_owner: Email of the person responsible for the step's data.
        enabled: `False` skips the step while leaving it in the DAG so
            dependency chains still hold.
        findings: Finding codes the step may emit, overriding the class
            declaration (specs/transform-framework.md §Declaring a finding
            vocabulary).
        **kwargs: Reserved for subclasses.
    """

    #: Transform or check (specs/transform-framework.md §Checks). A class
    #: attribute: the declaration form decides it, so it is never part of a
    #: step's definition parameters.
    role: StepRole = TRANSFORM
    #: The catalog section a builtin belongs to, one of its package's
    #: CATEGORIES (specs/transform-framework.md §The builtin catalog).
    #: Declaration only: never part of the definition hash or execution.
    category: str | None = None
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
        # scanner's hash of the defining module's bytes. `source` and
        # `module_source` are the texts those two hashes were computed from,
        # set exactly when their hash is — the definition text travels with
        # the image so the platform never fetches it from a repository.
        self.source_hash: str | None = None
        self.module_hash: str | None = None
        self.source: str | None = None
        self.module_source: str | None = None

    @property
    def is_builtin(self) -> bool:
        """True for a parameterized builtin (a `Step` subclass instance); False
        for a function wrapped by `@step` / `@check`."""
        return type(self) is not Step and not hasattr(self, "_source_func")

    @property
    def builtin_name(self) -> str | None:
        """The builtin's class name (`RemoveRows`, `CombineFeeds`, ...), or None
        for a decorated function."""
        return type(self).__name__ if self.is_builtin else None

    @property
    def label(self) -> str:
        """Human-readable label for UI display."""
        if self.description:
            return self.description.split("\n")[0]
        return self.name.replace("_", " ").title()

    @property
    def is_check(self) -> bool:
        """True when this step is a check: it reads the output and emits
        findings, never changes it (specs/transform-framework.md §Checks)."""
        return self.role == CHECK

    def apply(self, ctx: PipelineContext) -> None:
        """Do the step's work against `ctx`. A builtin overrides this; a
        decorated function supplies it. Transforms mutate `ctx.output`; checks
        read it and call `ctx.finding(...)`."""
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
            "source",
            "module_source",
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


def _check_ordering_error(label: str, after: Any, before: Any) -> str | None:
    """The load error for a check declared with a position, or None."""
    given = [
        name
        for name, value in (("before", before), ("after", after))
        if value is not None and value != []
    ]
    if not given:
        return None
    return (
        f"Check {label} declares {' and '.join(given)}=...; a check runs after "
        "every transform and takes no before/after. Drop it, or declare the "
        "step with @step (a transform) if it needs a position in the DAG. "
        "See specs/transform-framework.md §Checks."
    )


class Check(Step):
    """Base class for builtin checks (specs/transform-framework.md §Checks).

    A check reads the output, emits findings, and cannot change the data: it
    runs after every transform, concurrently with the other checks, against
    its own read-only view of the output. It takes `Step`'s arguments except
    `before` / `after`, which raise — a check has no position to declare.
    """

    role: StepRole = CHECK

    def __init__(
        self,
        *,
        after: Any = None,
        before: Any = None,
        **kwargs: Any,
    ):
        if _check_ordering_error("", after, before):
            raise ValueError(
                _check_ordering_error(_construction_label(type(self)), after, before)
            )
        super().__init__(**kwargs)


def _construction_label(cls: type) -> str:
    site = _definition_site()
    where = f" (constructed at {site[0]}:{site[1]})" if site else ""
    return f"{cls.__name__}{where}"


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


def _definition_payload(step_obj: Step) -> str:
    """The canonical JSON `definition_hash` hashes: keys sorted, separators
    `", "` / `": "`, no indentation, non-ASCII escaped — Python's
    `json.dumps` defaults, spelled out because the spec pins them
    (specs/transform-framework.md §Step Snapshot at Registration)."""
    return json.dumps(
        _canonical(step_obj.definition_params()),
        sort_keys=True,
        separators=(", ", ": "),
        ensure_ascii=True,
        default=str,
    )


def definition_hash(step_obj: Step) -> str:
    """sha256 over the canonical JSON of `step_obj.definition_params()`."""
    return hashlib.sha256(_definition_payload(step_obj).encode()).hexdigest()


def definition_text(step_obj: Step) -> str:
    """A builtin instance's definition text: the same canonical serialization
    `definition_hash` hashes, laid out one key per line so a changed
    parameter diffs as the lines that hold it (specs/transform-framework.md
    §Step Snapshot at Registration, `source`). Compacting it again —
    `json.dumps(json.loads(text), sort_keys=True)` — reproduces the hashed
    payload exactly, so the layout never moves a hash."""
    return json.dumps(
        json.loads(_definition_payload(step_obj)),
        sort_keys=True,
        indent=2,
        ensure_ascii=True,
    )


def normalized_source(source: str) -> str:
    """Source text as `source_text_hash` hashes it: dedented, each line
    right-stripped, so a re-indent or trailing whitespace is not a change.
    This is the text a `@step` function's snapshot `source` carries."""
    return "\n".join(line.rstrip() for line in textwrap.dedent(source).splitlines())


def source_text_hash(source: str) -> str:
    """sha256 over `normalized_source(source)`."""
    return hashlib.sha256(normalized_source(source).encode()).hexdigest()


# Exact texts of the @step misuse errors. Every callable-in-configuration
# situation other than clean bare decoration raises loudly with one of these
# instead of silently registering nothing (the bare-@step foot-gun that once
# silently dropped a transform from a live pipeline).
_STEP_BARE_WITH_CONFIG_ERROR = (
    "@step received a function together with configuration arguments — "
    "refusing to guess which was intended. Bare @step takes no arguments; "
    "to configure the step, write @step(files=[...], ...) with parentheses "
    "and pass every option by keyword."
)
_STEP_TOO_MANY_POSITIONAL_ERROR = (
    "@step takes at most one positional argument (the files list). Pass "
    "configuration by keyword, or use bare @step / @step() for a step with "
    "defaults."
)
_STEP_FILES_CALLABLE_ERROR = (
    "@step files= must be a list of filenames, got a callable. Use bare "
    "@step or @step() to decorate with defaults; files= only accepts "
    "filenames."
)


def step(
    *args: Any,
    files: list[str] | None = None,
    after: list[Step] | Literal["*"] | None = None,
    before: list[Step] | Literal["*"] | None = None,
    priority: int = 100,
    tags: list[str] | None = None,
    data_owner: str | None = None,
    enabled: bool = True,
    description: str | None = None,
    findings: list[Any] | None = None,
) -> Step | Callable[[Callable], Step]:
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

    ``@step()`` is the documented idiom. Bare ``@step`` (no parentheses) is
    also accepted and is identical to ``@step()``: the decorated function
    arrives as the sole positional argument with no configuration, which is
    unambiguous because the positional slot otherwise only ever holds a
    files list. Any other callable in the configuration — a function
    alongside keyword options, or ``files=`` given a callable — raises
    TypeError rather than silently registering nothing.
    """
    return _decorate(
        TRANSFORM,
        args,
        files=files,
        after=after,
        before=before,
        priority=priority,
        tags=tags,
        data_owner=data_owner,
        enabled=enabled,
        description=description,
        findings=findings,
    )


def check(
    *args: Any,
    files: list[str] | None = None,
    priority: int = 100,
    tags: list[str] | None = None,
    data_owner: str | None = None,
    enabled: bool = True,
    description: str | None = None,
    findings: list[Any] | None = None,
    after: Any = None,
    before: Any = None,
) -> Step | Callable[[Callable], Step]:
    """Decorator that wraps a function as a check (specs/transform-framework.md
    §Checks): a step that reads the output and emits findings and cannot
    change the data.

    Usage:
        @check(
            files=["stops.txt"],
            findings=[("stop_outside_area", {"subject": ["stop_id"]})],
        )
        def check_stops(ctx):
            ctx.emit_finding("stop_outside_area", ...)

    Takes @step's arguments and forms (bare ``@check`` is ``@check()``)
    except ``before`` / ``after``: a check runs after every transform, so
    either one raises ValueError naming the step. ``after``/``before`` are in
    the signature only so that error can name the step instead of Python
    reporting an unexpected keyword. ``priority`` has no effect on a check;
    checks order among themselves by name.
    """
    return _decorate(
        CHECK,
        args,
        files=files,
        after=after,
        before=before,
        priority=priority,
        tags=tags,
        data_owner=data_owner,
        enabled=enabled,
        description=description,
        findings=findings,
    )


def _decorate(
    role: StepRole,
    args: tuple[Any, ...],
    *,
    files: list[str] | None,
    after: Any,
    before: Any,
    priority: int,
    tags: list[str] | None,
    data_owner: str | None,
    enabled: bool,
    description: str | None,
    findings: list[Any] | None,
) -> Step | Callable[[Callable], Step]:
    """The shared body of @step and @check: argument-form handling plus the
    function → Step wrapping, for either role."""

    def decorator(func: Callable) -> Step:
        if role == CHECK:
            error = _check_ordering_error(
                getattr(func, "__name__", repr(func)), after, before
            )
            if error:
                raise ValueError(error)
            # Check.__init__ takes no before/after; the decorator rejected
            # them above with the function's name.
            s: Step = Check(
                priority=priority,
                tags=tags,
                data_owner=data_owner,
                enabled=enabled,
                findings=findings,
            )
        else:
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
            text = normalized_source(inspect.getsource(func))
            s.source = text
            s.source_hash = hashlib.sha256(text.encode()).hexdigest()
        except (TypeError, OSError):
            # Source unavailable: the location may be known, the definition
            # is not — leave source_hash None so it registers as unknown
            # rather than as some other hash.
            pass
        return s

    config_supplied = not (
        files is None
        and after is None
        and before is None
        and priority == 100
        and tags is None
        and data_owner is None
        and enabled is True
        and description is None
        and findings is None
    )

    if len(args) > 1:
        raise TypeError(_STEP_TOO_MANY_POSITIONAL_ERROR)
    if args:
        (sole,) = args
        if callable(sole):
            # Bare @step: the decorated function is the sole positional
            # argument. Only the clean form is accepted — one callable,
            # nothing else supplied — and it decorates with all defaults,
            # identical to @step().
            if config_supplied:
                raise TypeError(_STEP_BARE_WITH_CONFIG_ERROR)
            return decorator(sole)
        # Positional files list: @step(["stops.txt"])
        if files is not None:
            raise TypeError("step() got multiple values for argument 'files'")
        files = sole
    if callable(files):
        raise TypeError(_STEP_FILES_CALLABLE_ERROR)
    return decorator
