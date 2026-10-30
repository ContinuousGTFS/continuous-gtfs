"""The builtin catalog contract (specs/transform-framework.md §The builtin
catalog): every exported builtin declares a category its package lists, a
summary line, an `Args:` entry for every constructor parameter it adds, and a
description on every finding it declares. The docs site renders the catalog
from exactly these; a gap here is a gap on the page."""

from __future__ import annotations

import inspect
import re

import pytest

from continuous_gtfs.builtins import realtime, schedule
from continuous_gtfs.step import Step

PACKAGES = [schedule, realtime]


def _exports(pkg):
    return [(name, getattr(pkg, name)) for name in pkg.__all__]


def _builtins(pkg):
    return [
        (name, obj)
        for name, obj in _exports(pkg)
        if isinstance(obj, type) and issubclass(obj, Step)
    ]


CASES = [
    pytest.param(pkg, name, cls, id=f"{pkg.__name__.rsplit('.', 1)[1]}.{name}")
    for pkg in PACKAGES
    for name, cls in _builtins(pkg)
]

HELPER_CASES = [
    pytest.param(pkg, name, obj, id=f"{pkg.__name__.rsplit('.', 1)[1]}.{name}")
    for pkg in PACKAGES
    for name, obj in _exports(pkg)
    if not (isinstance(obj, type) and issubclass(obj, Step))
]


def _documented_args(doc: str) -> set[str]:
    """Parameter names under the docstring's `Args:` section."""
    lines = doc.splitlines()
    try:
        start = next(i for i, line in enumerate(lines) if line.strip() == "Args:")
    except StopIteration:
        return set()
    names: set[str] = set()
    for line in lines[start + 1 :]:
        if line.strip() and not line.startswith(" "):
            break  # next top-level section
        match = re.match(r"^\s{4}(\*{0,2}\w+)(?:\s*\(.*?\))?:", line)
        if match:
            names.add(match.group(1))
        elif line.strip() and re.match(r"^\s{0,3}\S", line):
            break
    return names


SHARED = {p.name for p in inspect.signature(Step.__init__).parameters.values()} - {
    "kwargs"
}


def _added_params(cls: type) -> set[str]:
    """Constructor parameters beyond the shared step parameters, which are
    documented once on `Step`; `**kwargs` stands for them on a builtin."""
    sig = inspect.signature(cls.__init__)
    names = set()
    for p in sig.parameters.values():
        if p.name in SHARED:
            continue
        names.add("**kwargs" if p.kind is p.VAR_KEYWORD else p.name)
    return names


@pytest.mark.parametrize("pkg,name,cls", CASES)
def test_builtin_declares_a_listed_category(pkg, name, cls):
    assert cls.category, f"{name} has no category"
    assert cls.category in pkg.CATEGORIES, (
        f"{name}.category={cls.category!r} is not in {pkg.__name__}.CATEGORIES"
    )


@pytest.mark.parametrize("pkg,name,cls", CASES)
def test_builtin_docstring_has_summary_and_every_arg(pkg, name, cls):
    doc = inspect.getdoc(cls) or ""
    assert doc.splitlines()[0].strip(), f"{name} has no summary line"
    expected = _added_params(cls)
    documented = _documented_args(doc)
    missing = expected - documented
    assert not missing, f"{name} Args: section does not describe {sorted(missing)}"


@pytest.mark.parametrize("pkg,name,cls", CASES)
def test_builtin_findings_carry_descriptions(pkg, name, cls):
    for entry in cls.findings:
        code, options = (entry, {}) if isinstance(entry, str) else entry
        assert options.get("description"), (
            f"{name} declares finding {code!r} without a description"
        )


@pytest.mark.parametrize("pkg,name,obj", HELPER_CASES)
def test_helper_type_has_summary(pkg, name, obj):
    doc = inspect.getdoc(obj) or ""
    assert doc.splitlines()[0].strip(), f"{name} has no summary line"


def test_every_category_is_used():
    for pkg in PACKAGES:
        used = {cls.category for _, cls in _builtins(pkg)}
        unused = set(pkg.CATEGORIES) - used
        assert not unused, f"{pkg.__name__}.CATEGORIES lists unused {sorted(unused)}"
