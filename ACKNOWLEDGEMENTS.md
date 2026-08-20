# Acknowledgements

This project is a port of **[aden-hive/hive](https://github.com/aden-hive/hive)**, read and
run at commit `f71da3f` (2026-08-18).

## Licence

hive is **Apache License 2.0**, © 2024 Aden. A copy of that licence is included as
`LICENSE-hive`, which Apache-2.0 requires of any work carrying its material, along with the
notice of what was changed that section 4(b) asks for — this whole file is that notice.

## What was copied

**No source was copied.** No Python file, fragment or expression from hive appears here;
every file in `src/` was written for this project.

Four kinds of thing were taken across deliberately, and all four are values a caller reads
or compares against rather than code:

- **The two summaries a worker gets when it never runs** — `Colony was stopped - this task
  was never started.` and `Worker was queued behind the concurrency cap and never started -
  stopped before its slot opened.` hive's are the same sentences with a typographic dash
  where this port uses a hyphen.
- **The cap's range, `[1, 32]`** — hive's own clamp, taken from the route that writes a
  colony's metadata. This port enforces it at the command boundary, which hive does not.
- **The default cap, 4** — hive's laptop-safe default.
- **The status vocabulary** — `PENDING`, `RUNNING`, `QUEUED`, and three terminal outcomes.
  hive spells its terminal ones `completed` / `failed` / `stopped`; this port spells them
  `SUCCEEDED` / `FAILED` / `STOPPED`, and `hive-port/bench/compare.py` maps between the two
  when comparing answers.

One number was deliberately **not** taken: hive retains 1000 results, this port retains 200.
The reason is in `README.md` under `Where it differs from aden-hive/hive`.

## What is derived

The behaviour is. Every rule in `hive-port/specs/SPEC-001-hive.md` was established by
reading hive and then running it — its `ColonyRuntime`, its `Worker`, its pending queue and
its stop sweep, driven directly with only the language model stood in for. The record of
what was checked and how is `hive-port/docs/question-log.md`, and hive's own
`core/tests/test_colony_scheduler.py` was run as part of it.

## Also used

- **Akka** (Akka SDK for Java, BSL 1.1) — the platform this port is built on.
- **pytest** and **uv** were used to run hive, not copied.
