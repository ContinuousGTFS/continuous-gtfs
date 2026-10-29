# Continuous GTFS SDK

`continuous-gtfs` is a Python package for transforming GTFS schedule feeds and GTFS-Realtime feeds. A pipeline is a folder of small steps, each one declaring the files it touches and the steps it runs after. The package scans the folder, orders the steps, and runs them, and its `continuous-gtfs` command runs a pipeline on your machine, diffs feeds, and draws the step graph.

This site is for **pipeline authors**: the people who write and test the transforms an agency's feeds pass through. It covers writing steps, the builtin steps, testing, running pipelines locally, and the library and CLI reference.

## Where to start

| If you want to | Read |
| --- | --- |
| Know what the job looks like end to end | [The author's job](jobs/make-a-change.md) |
| Run a first pipeline in five minutes | [Getting Started](guide/index.md) |
| Understand steps, the DAG and the context | [Concepts](guide/concepts.md) |
| Write a pipeline | [Writing Transforms](guide/writing-transforms.md), then [Builtins](guide/builtins.md) |
| Set up a pipeline repository and iterate on real data | [Local Development](guide/local-development.md) |
| Unit-test a step | [Testing Transforms](guide/testing-transforms.md) |
| Look up a builtin, a class or a CLI option | the [Reference](reference/step.md) section |

## The platform

The same pipelines run unchanged on the [Continuous GTFS platform](https://continuousgtfs.com), which runs them continuously against live feeds and adds environments, reviews, validation and publishing. How to operate the platform is documented with the platform, not here. This site stops where a change leaves the author's machine.

## How this site is built

Most pages are hand-written Markdown under `docs/`. The **Reference** section is generated from the package on every build: the module pages from docstrings and signatures (via [mkdocstrings](https://mkdocstrings.github.io/)), the two builtins catalogs from each builtin's category, docstring and declared findings (`docs/_hooks/builtins.py`), and the CLI page from the command's own parser (`docs/_hooks/cli_reference.py`). A wrong reference entry is fixed in the code, never on the page.

Preview locally with live reload, from the repository root:

```bash
uv run --group docs mkdocs serve
```

CI builds with `uv run --frozen --group docs mkdocs build --strict`, so a broken link or a page missing from the navigation fails the pull request. The navigation is listed by hand in `mkdocs.yml`: a new page needs a `nav:` entry in the same change. A page that moves keeps its old URL working with an entry in the `redirects` plugin's map.
