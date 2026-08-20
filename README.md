# hive-akka

Decides which of a group of agents runs now, which waits, and which never starts.

A port of [aden-hive/hive](https://github.com/aden-hive/hive) onto **Akka**, built with
**Akka Specify**.

---

## Where it came from

aden-hive/hive is a runtime for groups of AI agents, where one lead agent creates copies of
itself to work on parts of a job at the same time. It was ported to derive a specification
format precise enough to regenerate a system on a different stack — the port is the vehicle,
the specification is the deliverable.

Only one piece of hive is rebuilt here: the part that decides how many of those agents may
work at once. The specifications the port was generated from are in
[TylerJewell/akka-specify-harness](https://github.com/TylerJewell/akka-specify-harness) under
`hive-port/`.

---

## aden-hive/hive → this port

📉 398 Python lines → **471 Java lines**<br>
📁 2 files → **9 files**<br>
⚡ 6.4 milliseconds → **2.1 milliseconds** per decision<br>
🧪 6 tests → **25 tests**<br>
🔁 0 of 6 tests made to fail on purpose → **12 of 12 rules broken and caught**<br>
🎯 21 of 21 shared checks give the same answer<br>
💾 waiting work is lost when the program stops → **waiting work survives**<br>
🔒 1 lock → **0 locks**

Full method and the numbers that did *not* make this list: [`bench/REPORT.md`](https://github.com/TylerJewell/akka-specify-harness/blob/main/hive-port/bench/REPORT.md).

---

## What it took to build

⏱️ **1.8 hours** from the first command to the published repository, **1.0** of them active<br>
💬 **316** exchanges with the model<br>
✍️ **271,047** tokens written by the model, **53,250,345** counting everything sent and re-sent<br>
🙋 **0** questions to a human<br>
🧪 **25** tests

```bash
python toolkit/tokens.py --port hive    # turns, tokens, elapsed and active time
```

The record of every question, and where the time went, is in
[`port-log/`](https://github.com/TylerJewell/akka-specify-harness/tree/main/port-log).

---

## What it does

A group of agents has one number: how many of them may be working at once. Ask it to start
some work, and for each piece it answers one of three things — start it now, put it in line,
or refuse it outright.

From the specification:

- **Work over the limit waits; it is never turned away.** Ask for twenty when only four may
  run, and all twenty are accepted — four start, sixteen wait their turn.
- **One line, first in first out.** Two separate requests share the same line, and nothing
  jumps it.
- **Every finish lets the next one in.** The moment a running agent finishes, the one that
  has waited longest starts, and the limit is never exceeded while that happens.
- **Anything that never runs still says so.** A refused or cancelled piece of work reports
  back exactly like a finished one, so whoever is counting the batch home is never left
  waiting on something that never started.
- **Stopping means stopping.** A stop shuts the door before it clears the line, so work
  cannot slip in behind it and survive.
- **The lead agent never takes a place.** It is always running and is never counted against
  the limit, and an ordinary stop leaves it alone.

Nothing here calls a language model, opens a browser or runs a tool. The work is a decision
with a name attached; what a piece of work does once it starts belongs to a different part
of hive.

---

## Design decisions

**One owner per group.** Two answers given at the same moment could both say yes and put one
too many agents to work, so every question about a group goes to the same single place and is
answered one at a time. hive needs a lock to get this; here there is nothing to lock, because
there was never more than one answerer.

**Written down as it happens.** The list of who is waiting is kept as a running record of
everything that happened, not as a note that gets rewritten. Turn the program off and on and
the line is exactly where it was.

**A finish and what it lets in are one step.** When someone finishes, the finish and the
start it makes room for are written together, so nobody can ever look and see a group sitting
below its own limit with people waiting.

**A limit of nothing is refused.** hive accepts a limit of zero and then quietly accepts work
it can never start. Here anything outside one to thirty-two is turned down with a reason, so a
group that cannot work says so instead of looking busy.

**Old work is thrown away.** Finished work is kept for a while and then dropped, oldest first,
and the text attached to it is cut short. Without both, a long-running group's record grows
until it is too big to copy between machines.

---

## Running it — the short path

You do not need Java, Maven, or the Akka CLI installed. Akka Specify installs them for you.

**1. Install Akka Specify** in Claude Code:

```
/plugin marketplace add akka/ai-marketplace
/plugin install akka@akka-ai-marketplace
```

Restart Claude Code when it asks.

**2. Give it this prompt:**

> Clone https://github.com/TylerJewell/hive-akka into a new directory and open it.
> Then run /akka:setup to install everything this project needs, and /akka:build to
> compile it, run the tests, and start it locally.

**3. Open** http://localhost:9018.

---

## Running it — the developer path

### Requirements

- Java 21 or newer
- Maven 3.9 or newer
- An Akka download token — run `akka code token` once

### Start the service

```bash
mvn compile
akka local run
```

The service starts on **port 9018**.

### Drive one group

```bash
# at most two at once
curl -X PUT localhost:9018/colonies/demo/cap -H 'Content-Type: application/json' \
  -d '{"cap": 2}'

# ask for four
curl -X POST localhost:9018/colonies/demo/workers -H 'Content-Type: application/json' \
  -d '{"batchId": "A", "workerIds": ["w1","w2","w3","w4"], "persistent": false}'
# -> {"batchId":"A","admissions":["ADMITTED","ADMITTED","QUEUED","QUEUED"]}

# finish one, and the next starts
curl -X POST localhost:9018/colonies/demo/workers/w1/terminate \
  -H 'Content-Type: application/json' \
  -d '{"status": "SUCCEEDED", "summary": "done", "durationSeconds": 1.5}'

curl localhost:9018/colonies/demo
```

---

## Configuration

There are no environment variables. The one setting is the port it listens on, written in
`src/main/resources/application.conf`:

```
akka.javasdk.dev-mode.http-port = 9018
```

The limit on how many may run at once is not configuration either. It belongs to a group,
is set through `PUT /colonies/{id}/cap`, and must be between one and thirty-two.

---

## Where it differs from aden-hive/hive

Everything not listed here behaves the same way on purpose, including the parts that look
like mistakes.

- **Changing the limit while work is waiting.** hive reads a group's limit once, when the
  group starts, and nothing changes it after that, so hive has no answer here. This port lets
  the limit be changed and, when it is raised, starts waiting work straight away — a setting
  whose only effect is to change a number, while the line stays stuck behind the old one, is a
  setting that silently did not apply.
- **Lowering the limit below what is already running.** Same starting point: hive has no
  answer, because hive's limit never moves. This port leaves whatever is already running
  alone, because a change to a setting should not stop work that was already allowed to start.
- **A limit of zero.** hive accepts it. Every piece of work is then accepted, put in line, and
  never started, with no error anywhere. This port refuses any limit outside one to
  thirty-two — the same range hive uses when a group is first created, moved to where the
  limit is actually set.
- **How much finished work is kept.** hive keeps the last thousand results. This port keeps
  two hundred and cuts the text attached to each one at 256 characters, because here that
  record is copied between machines and a thousand entries of unbounded text would be too
  large to copy.
- **Starting a piece of work is a separate step.** In hive, work that is allowed to start
  starts itself. Here something outside says when it started and when it finished, because
  this port decides who may run and does not run anything.
- **Waiting work after a restart.** hive's line lives in memory and is gone if the program
  stops. Here it is written down and is still there afterwards.
- **What a caller can see.** hive shows a person one number — how many agents are busy — and
  one button to stop them. This port also lets a caller read the limit and the line, and set
  the limit, because a limit nobody can read is a limit nobody can work with.
- **Two pieces of work with the same name.** `not checked`. hive gives every piece of work its
  own generated name, so the case cannot arise there; here the caller supplies the name, and
  what happens when the same one is supplied twice was not tested on either side.
- **Very large requests.** `not checked`. Neither side was asked for more work in one go than
  it could hold, so where each one starts refusing is unknown.
- **The wording of what a stopped piece of work reports.** hive writes its two sentences with
  a typographic dash; this port writes the same sentences with an ordinary hyphen.

---

## Licence

aden-hive/hive is Apache License 2.0, © 2024 Aden. This port reimplements the behaviour
without copied source; see [`ACKNOWLEDGEMENTS.md`](ACKNOWLEDGEMENTS.md).
