# Contributing

Thanks for your interest in Continuous GTFS!

## Development setup

The project uses [uv](https://docs.astral.sh/uv/) for everything Python:

```console
git clone https://github.com/continuousgtfs/continuous-gtfs
cd continuous-gtfs
uv sync
uv run pytest
uv run ruff check src tests
```

## Ground rules

- **Branches and releases**: the default branch is `develop`; `main` is the release target. Open pull requests against `develop`. Releases are cut by merging the automated `develop → main` Release PR.
- **Tests**: every behavior change comes with tests. The suite is hermetic — no network, no downloaded feeds; fixtures are small synthetic in-memory feeds built with `continuous_gtfs.testing`.
- **Style**: `ruff` (config in `pyproject.toml`) is the arbiter of formatting and lint questions.
- **Commits**: conventional commits (`feat: ...`, `fix: ...`, `docs: ...`) are appreciated — they feed the release changelog.

## Documentation

The docs site is built with [MkDocs](https://www.mkdocs.org/) and Material for MkDocs from `docs/` and `mkdocs.yml`, and published to GitHub Pages from `main`. Preview it with live reload from the repository root:

```console
uv run --group docs mkdocs serve
```

CI builds every pull request that touches the site's sources with `uv run --frozen --group docs mkdocs build --strict`, so a broken link, a broken anchor, or a page missing from the navigation fails the pull request.

- **Navigation is explicit.** `nav:` in `mkdocs.yml` lists every page; a new page needs an entry in the same change.
- **The Reference is generated.** Module pages render docstrings and signatures through [mkdocstrings](https://mkdocstrings.github.io/). The two builtins catalogs come from each package's `CATEGORIES` and each builtin's category, docstring and declared findings (`docs/_hooks/builtins.py`). The CLI page comes from `continuous_gtfs.cli.build_parser()` (`docs/_hooks/cli_reference.py`). Fix a wrong entry in the code, never on the page.
- **URLs are stable.** A page that moves keeps its old URL working through an entry in the `redirects` plugin's `redirect_maps`, in the same change.

## Reporting issues

Use [GitHub issues](https://github.com/continuousgtfs/continuous-gtfs/issues). For bugs, a failing test or a minimal feed excerpt that reproduces the problem is the fastest path to a fix.

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
