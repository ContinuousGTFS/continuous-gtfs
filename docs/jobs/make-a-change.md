# The author's job: make a pipeline produce different output

| | |
| --- | --- |
| **When** | The published feed needs to change: something upstream is wrong and the source will not fix it, a transform is doing the wrong thing, or the agency has decided to present service differently. |
| **I want to** | make the pipeline produce the output I intend, and understand the change fully on my own machine before anyone else looks at it. |
| **So that** | the change I hand on is scoped to the rows I meant, guarded against upstream drift, covered by tests, and ready for a colleague to review by its output data. |

This SDK covers the part of the job that happens on the author's machine. Once the change is pushed, running it in a shared environment, reviewing it and publishing it are platform work, documented with the platform.

## The path

| # | Step | Where |
| - | --- | --- |
| 1 | Branch the pipeline repository and get sample input data: a recent copy of each feed the pipeline declares in its `INPUTS` manifest. | your editor, git |
| 2 | Write or amend the step. Use a [builtin](../guide/builtins.md) where one fits and a custom `@step` where none does. New reference data enters as a declared input, never as a file the step opens by path. | your editor |
| 3 | Run the pipeline on the sample data and compare its output with what is published today (`--diff-against`, `diff --detail`, `rt-compare`). Narrow to one step with `--select` while iterating. | [CLI](../guide/cli.md) |
| 4 | Pin the behavior in a unit test: the rows the step should change, and that everything else is untouched. | [testing kit](../guide/testing-transforms.md) |
| 5 | Push and ask for review. | git, your code host |

[Local Development](../guide/local-development.md) walks through steps 1 to 4 with commands.

## Judgment points

- **Is the step scoped to exactly the intended rows?** A condition that matches today's rows can match tomorrow's too. Prefer exact ids to patterns, and assert in a test that rows outside the change are untouched (`assert_unchanged`).
- **What catches upstream drift?** When the source changes shape (a renamed id, a new route), a step written against the old shape can quietly do nothing or do too much. A [check](../guide/writing-transforms.md#checks) or a finding emitted from the step makes that visible in every run.
- **Is the output right?** Review judges the output data, not the code. Read the diff of the output, not only the diff of the step.
- **Should this be a builtin or custom code?** A builtin documents itself in the catalog and is tested upstream. Custom code is the right call when no builtin says what you mean.

## Measures

- How long one edit, run and compare cycle takes on real data.
- How much of the change is understood before anything is pushed.
- Whether review comments are about the output data or about the code.
