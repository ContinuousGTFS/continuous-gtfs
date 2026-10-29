"""mkdocs hook: render the CLI reference from the argparse tree.

The `continuous-gtfs` CLI page is rendered from the parser itself, so every
subcommand and option shows with the help text and default the command
actually carries. `docs/guide/cli.md` stays the hand-written narrative.

The parser comes from `continuous_gtfs.cli.build_parser()`. A package that
predates that factory builds its parser inside `main()`; the hook then runs
`main()` with `parse_args` intercepted, takes the parser it was about to
parse with, and stops before any command runs.
"""

from __future__ import annotations

import argparse
import sys

from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import File, Files

PAGE_URI = "reference/cli.md"


class _Captured(Exception):
    def __init__(self, parser: argparse.ArgumentParser) -> None:
        super().__init__("parser captured")
        self.parser = parser


def _build_parser() -> argparse.ArgumentParser:
    from continuous_gtfs import cli

    factory = getattr(cli, "build_parser", None)
    if factory is not None:
        return factory()

    original = argparse.ArgumentParser.parse_args

    def _capture(self, *args, **kwargs):
        raise _Captured(self)

    argparse.ArgumentParser.parse_args = _capture
    saved_argv = sys.argv
    sys.argv = ["continuous-gtfs"]
    try:
        cli.main()
    except _Captured as captured:
        return captured.parser
    finally:
        argparse.ArgumentParser.parse_args = original
        sys.argv = saved_argv
    raise RuntimeError("continuous_gtfs.cli.main() returned without parsing")


def _help(action: argparse.Action) -> str:
    text = action.help or ""
    try:
        text = text % dict(vars(action), prog="continuous-gtfs")
    except (KeyError, TypeError, ValueError):
        pass
    return text.replace("\n", " ").replace("|", "\\|")


def _default(action: argparse.Action) -> str:
    """The default worth showing: not None/SUPPRESS, not a flag's False, not
    a repeatable option's empty list."""
    if action.default in (None, argparse.SUPPRESS, False, []):
        return ""
    return f"`{action.default}`"


def _metavar(action: argparse.Action) -> str:
    if isinstance(action.metavar, tuple):
        return " ".join(action.metavar)
    return action.metavar or action.dest.upper()


def _usage(action: argparse.Action) -> str:
    if not action.option_strings:
        return f"`{_metavar(action)}`"
    flags = ", ".join(f"`{f}`" for f in action.option_strings)
    if action.nargs != 0:
        flags += f" `{_metavar(action)}`"
    return flags


def _subcommand_section(name: str, sub: argparse.ArgumentParser, summary: str) -> str:
    out = [f"## `continuous-gtfs {name}`\n\n"]
    if summary:
        out.append(summary + "\n\n")
    if sub.description and sub.description != summary:
        out.append(sub.description + "\n\n")
    positionals = [a for a in sub._actions if not a.option_strings]
    options = [
        a
        for a in sub._actions
        if a.option_strings and not isinstance(a, argparse._HelpAction)
    ]
    if positionals:
        out.append("| Argument | Description |\n| --- | --- |\n")
        out.extend(f"| {_usage(a)} | {_help(a)} |\n" for a in positionals)
        out.append("\n")
    if options:
        out.append("| Option | Description | Default |\n| --- | --- | --- |\n")
        out.extend(f"| {_usage(a)} | {_help(a)} | {_default(a)} |\n" for a in options)
        out.append("\n")
    return "".join(out)


def _render() -> str:
    parser = _build_parser()
    subparsers = next(
        a for a in parser._actions if isinstance(a, argparse._SubParsersAction)
    )
    summaries = {c.dest: c.help or "" for c in subparsers._choices_actions}
    out = [
        "# CLI reference\n\n",
        '!!! note ""\n    Rendered at build time from the `continuous-gtfs` '
        "command's own parser. To change this page, change the command; the "
        "site is never edited by hand. For what the commands are for and "
        "worked examples, see the [CLI guide](../guide/cli.md).\n\n",
        f"{parser.description}\n\n",
        "| Subcommand | Summary |\n| --- | --- |\n",
    ]
    out.extend(
        f"| [`{name}`](#continuous-gtfs-{name}) | {summaries.get(name, '')} |\n"
        for name in subparsers.choices
    )
    out.append("\n")
    for name, sub in subparsers.choices.items():
        out.append(_subcommand_section(name, sub, summaries.get(name, "")))
    return "".join(out)


def on_files(files: Files, config: MkDocsConfig) -> Files:
    files.append(File.generated(config, PAGE_URI, content=_render()))
    return files
