"""mkdocs hook: render the builtins catalog pages from the packages.

One page per builtins package, rendered from the package's `CATEGORIES`,
each builtin's `category`, docstring, signature, and declared findings. The
page opens with a menu, then one section per category holding a tightly
scoped mkdocstrings directive per builtin plus a findings table, then the
helper types. Nothing under `docs/` is its source.

A package without `CATEGORIES` fails the build. A builtin whose category is
missing or not listed renders under "Uncategorized" with the gap called out.
"""

from __future__ import annotations

import importlib
import inspect

from mkdocs.config.defaults import MkDocsConfig
from mkdocs.exceptions import PluginError
from mkdocs.plugins import event_priority
from mkdocs.structure.files import File, Files

PAGES = [
    {
        "uri": "reference/builtins-schedule.md",
        "module": "continuous_gtfs.builtins.schedule",
        "title": "Schedule builtins",
        "lede": (
            "Parameterized steps for GTFS schedule tables, operating on Polars "
            "DataFrames in `ctx.output`. How conditions combine, how a missing "
            "column is reported, and the parameters every step shares are in the "
            "[builtins guide](../guide/builtins.md)."
        ),
    },
    {
        "uri": "reference/builtins-realtime.md",
        "module": "continuous_gtfs.builtins.realtime",
        "title": "Realtime builtins",
        "lede": (
            "Parameterized steps for GTFS-RT feeds, operating in place on the "
            "protobuf `FeedMessage` objects in `ctx.output`, one per feed type. "
            "How outputs are keyed and how entity types are handled is in the "
            "[builtins guide](../guide/builtins.md)."
        ),
    },
]

DIRECTIVE_OPTIONS = """    options:
      heading_level: 3
      show_root_heading: true
      show_root_toc_entry: true
      show_root_full_path: false
      members: false
      show_bases: false
"""


def _summary(obj) -> str:
    doc = inspect.getdoc(obj) or ""
    first = doc.splitlines()[0].strip() if doc else ""
    return first or "*(no docstring)*"


def _findings_table(cls) -> str:
    entries = list(getattr(cls, "findings", []) or [])
    if not entries:
        return ""
    rows = ["**Findings**\n\n| Finding | Subject | When |\n| --- | --- | --- |\n"]
    for entry in entries:
        code, options = (entry, {}) if isinstance(entry, str) else entry
        subject = ", ".join(f"`{k}`" for k in options.get("subject", [])) or "—"
        when = options.get("description") or "*(no description)*"
        rows.append(f"| `{code}` | {subject} | {when} |\n")
    return "".join(rows) + "\n"


def _directive(path: str) -> str:
    return f"::: {path}\n{DIRECTIVE_OPTIONS}\n"


def _sections(mod, builtins: list) -> list[tuple[str, list]]:
    categories = getattr(mod, "CATEGORIES", None)
    if not categories:
        raise PluginError(f"{mod.__name__} declares no CATEGORIES")
    by_category: dict[str, list] = {c: [] for c in categories}
    uncategorized = []
    for name, cls in builtins:
        cat = getattr(cls, "category", None)
        (by_category[cat] if cat in by_category else uncategorized).append((name, cls))
    sections = [(c, by_category[c]) for c in categories if by_category[c]]
    if uncategorized:
        sections.append(("Uncategorized", uncategorized))
    return sections


def _render(page: dict) -> str:
    from continuous_gtfs.step import Step

    mod = importlib.import_module(page["module"])
    builtins, helpers = [], []
    for name in mod.__all__:
        obj = getattr(mod, name)
        if isinstance(obj, type) and issubclass(obj, Step):
            builtins.append((name, obj))
        else:
            helpers.append((name, obj))
    sections = _sections(mod, builtins)

    def link(name: str) -> str:
        return f"[`{name}`][{page['module']}.{name}]"

    out = [
        f"# {page['title']}\n\n",
        '!!! note ""\n    Rendered at build time from the `'
        f"{page['module']}` package: its categories, each builtin's "
        "docstring, signature and declared findings. To change this page, "
        "change the code; the site is never edited by hand.\n\n",
        page["lede"] + "\n\n",
        "| Builtin | Category | What it does |\n| --- | --- | --- |\n",
    ]
    for cat, items in sections:
        for name, cls in items:
            out.append(f"| {link(name)} | {cat} | {_summary(cls)} |\n")
    for name, obj in helpers:
        out.append(f"| {link(name)} | *helper* | {_summary(obj)} |\n")
    out.append("\n")

    for cat, items in sections:
        out.append(f"## {cat}\n\n")
        if cat == "Uncategorized":
            out.append(
                '!!! warning "Catalog gap"\n    These builtins declare no '
                "`category`, or one their package does not list. Fix the "
                "declaration.\n\n"
            )
        for name, cls in items:
            out.append(_directive(f"{page['module']}.{name}"))
            out.append(_findings_table(cls))
    if helpers:
        out.append("## Helpers\n\n")
        out.append("Types the builtins above take as parameters.\n\n")
        for name, _ in helpers:
            out.append(_directive(f"{page['module']}.{name}"))
    return "".join(out)


# Runs ahead of the plugins' own on_files (hooks otherwise run last), so
# mkdocs-redirects sees the generated pages when it validates its targets.
@event_priority(50)
def on_files(files: Files, config: MkDocsConfig) -> Files:
    for page in PAGES:
        files.append(File.generated(config, page["uri"], content=_render(page)))
    return files
