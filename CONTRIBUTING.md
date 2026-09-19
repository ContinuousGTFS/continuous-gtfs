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

## Reporting issues

Use [GitHub issues](https://github.com/continuousgtfs/continuous-gtfs/issues). For bugs, a failing test or a minimal feed excerpt that reproduces the problem is the fastest path to a fix.

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
