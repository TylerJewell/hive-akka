package io.akka.hive.domain;

import static org.assertj.core.api.Assertions.assertThat;

import io.akka.hive.domain.SchedulerEvent.WorkerPromoted;
import io.akka.hive.domain.SchedulerEvent.WorkerQueued;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 rules R1, R2, R3, R4, R11 and R14 — the admission decision and the queue, with no
 * runtime in the way.
 *
 * <p>Every rule that can be settled by a pure fold is settled here rather than in the entity
 * test, because a rule checked without a runtime is a rule that stays checked when the runtime
 * changes.
 */
public class SchedulerStateTest {

  private static final String COLONY = "c1";

  private SchedulerState colony(int cap) {
    return SchedulerState.empty(COLONY).onEvent(new SchedulerEvent.CapSet(cap));
  }

  private static List<TaskSpec> tasks(String batch, int n) {
    var out = new ArrayList<TaskSpec>();
    for (int i = 1; i <= n; i++) {
      out.add(new TaskSpec(batch + "-w" + i, "task " + batch + " " + i, false));
    }
    return out;
  }

  private SchedulerState apply(SchedulerState state, List<SchedulerEvent> events) {
    var s = state;
    for (var e : events) {
      s = s.onEvent(e);
    }
    return s;
  }

  /** R1. */
  @Test
  public void admitsWhileUnderTheCapAndQueuesAtIt() {
    var s = colony(2);
    s = apply(s, s.planSpawn(tasks("b", 5), "b"));

    assertThat(s.occupiedSlots()).isEqualTo(2);
    assertThat(s.queue()).containsExactly("b-w3", "b-w4", "b-w5");
    assertThat(s.worker("b-w1").orElseThrow().status()).isEqualTo(WorkerStatus.PENDING);
    assertThat(s.worker("b-w3").orElseThrow().status()).isEqualTo(WorkerStatus.QUEUED);
  }

  /** R2 — and specifically that a worker does not count itself. */
  @Test
  public void queuedAndTerminalWorkersDoNotOccupyASlot() {
    var s = colony(2);
    s = apply(s, s.planSpawn(tasks("b", 4), "b"));
    assertThat(s.occupiedSlots()).isEqualTo(2);

    s = apply(s, s.planTerminate("b-w1", WorkerStatus.SUCCEEDED, "done", 1.5));
    // One terminal, one promoted: still exactly two occupied, never three.
    assertThat(s.occupiedSlots()).isEqualTo(2);
    assertThat(s.worker("b-w1").orElseThrow().status().occupiesSlot()).isFalse();
    assertThat(s.worker("b-w4").orElseThrow().status()).isEqualTo(WorkerStatus.QUEUED);

    // A single spawn into a colony with one free slot is admitted, which it would not be
    // if the candidate counted itself against the cap.
    var one = colony(1);
    one = apply(one, one.planSpawn(tasks("s", 1), "s"));
    assertThat(one.worker("s-w1").orElseThrow().status()).isEqualTo(WorkerStatus.PENDING);
  }

  /** R3. */
  @Test
  public void overCapWorkIsQueuedRatherThanRefused() {
    var s = colony(1);
    var events = s.planSpawn(tasks("b", 4), "b");

    assertThat(events).filteredOn(e -> e instanceof SchedulerEvent.WorkerRefused).isEmpty();
    assertThat(events).filteredOn(e -> e instanceof WorkerQueued).hasSize(3);
  }

  /** R4 — one queue, no per-batch fairness. */
  @Test
  public void promotesInQueueOrderAcrossBatches() {
    var s = colony(1);
    s = apply(s, s.planSpawn(tasks("A", 3), "A"));
    s = apply(s, s.planSpawn(tasks("B", 3), "B"));

    assertThat(s.queue()).containsExactly("A-w2", "A-w3", "B-w1", "B-w2", "B-w3");

    var promoted = new ArrayList<String>();
    var running = "A-w1";
    for (int i = 0; i < 5; i++) {
      var events = s.planTerminate(running, WorkerStatus.SUCCEEDED, "done", 0.1);
      s = apply(s, events);
      running =
          events.stream()
              .filter(e -> e instanceof WorkerPromoted)
              .map(e -> ((WorkerPromoted) e).workerId())
              .findFirst()
              .orElseThrow();
      promoted.add(running);
    }
    assertThat(promoted).containsExactly("A-w2", "A-w3", "B-w1", "B-w2", "B-w3");
  }

  /**
   * R11 — the invariant that makes hive's "this queue entry is no longer QUEUED" guard
   * unnecessary here. Driven through a mixed sequence rather than one transition, because the
   * rule is about every way out of QUEUED, not one of them.
   */
  @Test
  public void everyQueueEntryIsAQueuedWorkerThroughoutAMixedSequence() {
    var s = colony(2);
    var checkpoints = new ArrayList<SchedulerState>();

    s = apply(s, s.planSpawn(tasks("A", 5), "A"));
    checkpoints.add(s);
    s = apply(s, s.planTerminate("A-w1", WorkerStatus.SUCCEEDED, "done", 0.1));
    checkpoints.add(s);
    s = apply(s, s.planStop(Set.of("A-w3"), false, false));
    checkpoints.add(s);
    s = apply(s, s.planSetCap(4));
    checkpoints.add(s);
    s = apply(s, s.planSpawn(tasks("B", 3), "B"));
    checkpoints.add(s);
    s = apply(s, s.planStop(null, false, true));
    checkpoints.add(s);

    for (var cp : checkpoints) {
      assertThat(cp.queue()).doesNotHaveDuplicates();
      for (var id : cp.queue()) {
        assertThat(cp.worker(id).orElseThrow().status())
            .as("queue entry %s", id)
            .isEqualTo(WorkerStatus.QUEUED);
      }
      var queuedButNotInQueue =
          cp.workers().values().stream()
              .filter(w -> w.status() == WorkerStatus.QUEUED)
              .map(Worker::id)
              .filter(id -> !cp.queue().contains(id))
              .toList();
      assertThat(queuedButNotInQueue).isEmpty();
    }
  }

  /**
   * R1 and R2 rest on the occupancy set agreeing with the registry, so the agreement is
   * checked directly rather than only through the counts the other tests read.
   */
  @Test
  public void theOccupancySetAgreesWithTheRegistryThroughoutAMixedSequence() {
    var s = colony(2);
    var checkpoints = new ArrayList<SchedulerState>();

    s = apply(s, s.planSpawn(tasks("A", 5), "A"));
    checkpoints.add(s);
    s = apply(s, s.planSpawn(List.of(new TaskSpec("queen", "", true)), "overseer"));
    checkpoints.add(s);
    s = apply(s, s.planStart("A-w1"));
    checkpoints.add(s);
    s = apply(s, s.planTerminate("A-w1", WorkerStatus.SUCCEEDED, "done", 0.1));
    checkpoints.add(s);
    s = apply(s, s.planStop(Set.of("A-w2"), false, false));
    checkpoints.add(s);
    s = apply(s, s.planSetCap(5));
    checkpoints.add(s);
    s = apply(s, s.planStop(null, true, true));
    checkpoints.add(s);

    for (var cp : checkpoints) {
      var fromRegistry =
          cp.workers().values().stream()
              .filter(w -> !w.persistent() && w.status().occupiesSlot())
              .map(Worker::id)
              .toList();
      assertThat(cp.occupied()).containsExactlyInAnyOrderElementsOf(fromRegistry);
      assertThat(cp.occupiedSlots()).isEqualTo(fromRegistry.size());
    }
  }

  /** R14. */
  @Test
  public void evictsTheOldestTerminalWorkersPastTheRetentionBound() {
    var s = colony(1);
    var over = SchedulerState.MAX_RETAINED_TERMINAL + 5;
    for (int i = 1; i <= over; i++) {
      var id = "w" + i;
      s = apply(s, s.planSpawn(List.of(new TaskSpec(id, "t" + i, false)), "b" + i));
      s = apply(s, s.planTerminate(id, WorkerStatus.SUCCEEDED, "done", 0.0));
    }

    assertThat(s.workers()).hasSize(SchedulerState.MAX_RETAINED_TERMINAL);
    assertThat(s.worker("w1")).isEmpty();
    assertThat(s.worker("w" + over)).isPresent();
  }

  /** R14's second half — a live worker is never the one evicted. */
  @Test
  public void neverEvictsAWorkerThatIsQueuedOrHoldingASlot() {
    var s = colony(1);
    s = apply(s, s.planSpawn(List.of(new TaskSpec("live", "still going", false)), "live"));
    s = apply(s, s.planSpawn(List.of(new TaskSpec("waiting", "in line", false)), "wait"));

    for (int i = 1; i <= SchedulerState.MAX_RETAINED_TERMINAL + 5; i++) {
      var id = "t" + i;
      // Refused rather than run: a refusal is terminal on arrival, so this fills the
      // retention window without ever needing the one slot "live" is holding.
      s = s.onEvent(new SchedulerEvent.DispatchBlocked());
      s = apply(s, s.planSpawn(List.of(new TaskSpec(id, "t", false)), "b" + i));
      s = s.onEvent(new SchedulerEvent.DispatchResumed());
    }

    assertThat(s.worker("live")).isPresent();
    assertThat(s.worker("waiting")).isPresent();
    assertThat(s.queue()).containsExactly("waiting");
    assertThat(s.workers()).hasSize(SchedulerState.MAX_RETAINED_TERMINAL + 2);
  }
}
