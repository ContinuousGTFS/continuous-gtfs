# Continuous GTFS SDK

`continuous-gtfs` is a Python package for transforming GTFS schedule feeds and GTFS-Realtime feeds. A pipeline is a folder of small steps, each one declaring the files it touches and the steps it runs after. The package scans the folder, orders the steps, and runs them, and its `continuous-gtfs` command runs a pipeline on your machine, diffs feeds, and draws the step graph.

This site is for **pipeline authors**, the people who write and test the transforms an agency's feeds pass through. It covers writing steps, the builtin steps, testing, running pipelines locally, and the library and CLI reference.

## Where to start

| If you want to | Read |
| --- | --- |
| Know what the job looks like end to end | [Making a change](jobs/make-a-change.md) |
| Run a first pipeline in five minutes | [Getting Started](guide/index.md) |
| Understand steps, the DAG and the context | [Concepts](guide/concepts.md) |
| Write a pipeline | [Writing Transforms](guide/writing-transforms.md), then [Builtins](guide/builtins.md) |
| Set up a pipeline repository and iterate on real data | [Local Development](guide/local-development.md) |
| Unit-test a step | [Testing Transforms](guide/testing-transforms.md) |
| Look up a builtin, a class or a CLI option | the [Reference](reference/step.md) section |

## The platform

The same pipelines run unchanged on the [Continuous GTFS platform](https://continuousgtfs.com), which runs them continuously against live feeds and adds environments, reviews, validation and publishing. How to operate the platform is documented with the platform, not here. This site stops where a change leaves the author's machine.

## Working on these docs

The Reference section is generated from the package's docstrings and CLI parser on every build, so a wrong reference entry is fixed in the code. How to build and preview the site is in [CONTRIBUTING.md](https://github.com/ContinuousGTFS/continuous-gtfs/blob/develop/CONTRIBUTING.md#documentation).
