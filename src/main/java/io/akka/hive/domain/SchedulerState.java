package io.akka.hive.domain;

import io.akka.hive.domain.SchedulerEvent.CapSet;
import io.akka.hive.domain.SchedulerEvent.DispatchBlocked;
import io.akka.hive.domain.SchedulerEvent.DispatchResumed;
import io.akka.hive.domain.SchedulerEvent.QueuedWorkerCancelled;
import io.akka.hive.domain.SchedulerEvent.TerminalWorkersEvicted;
import io.akka.hive.domain.SchedulerEvent.WorkerAdmitted;
import io.akka.hive.domain.SchedulerEvent.WorkerPromoted;
import io.akka.hive.domain.SchedulerEvent.WorkerQueued;
import io.akka.hive.domain.SchedulerEvent.WorkerRefused;
import io.akka.hive.domain.SchedulerEvent.WorkerStarted;
import io.akka.hive.domain.SchedulerEvent.WorkerTerminated;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A colony's whole scheduling state, and every rule that reads it. SPEC-001 §3.
 *
 * <p>The rules live here rather than in the entity so they can be checked without starting a
 * runtime, and so the entity is left deciding only what to persist and what to answer. Each
 * {@code plan*} method returns the events a command should persist, computed by folding this
 * state forward through them — so a command that terminates one worker and promotes two lands
 * as one atomic step with no intermediate state anyone can observe.
 *
 * <p>{@code workers} is insertion-ordered: arrival order is what R14 evicts by and what R4
 * reads for the queue.
 */
public record SchedulerState(
    String colonyId,
    int cap,
    boolean dispatchBlocked,
    Map<String, Worker> workers,
    List<String> queue,
    Set<String> occupied) {

  public static final int MIN_CAP = 1;

  public static final int MAX_CAP = 32;

  /** hive's own laptop-safe default (question-log #1). */
  public static final int DEFAULT_CAP = 4;

  /**
   * SPEC-001 §4 D3. hive retains 1000 results; this retains fewer because the figure means
   * something different here — every retained worker is part of one entity's replicated state,
   * and 1000 of them with the bound on {@link Worker#MAX_TEXT} would sit against the
   * platform's 1 MB replication ceiling rather than comfortably inside it.
   */
  public static final int MAX_RETAINED_TERMINAL = 200;

  private static final String REFUSED_SUMMARY =
      "Colony was stopped - this task was never started.";

  private static final String CANCELLED_SUMMARY =
      "Worker was queued behind the concurrency cap and never started - stopped before its slot opened.";

  private static final String SWEPT_SUMMARY = "Worker was stopped.";

  public static SchedulerState empty(String colonyId) {
    return new SchedulerState(colonyId, DEFAULT_CAP, false, Map.of(), List.of(), Set.of());
  }

  public static boolean capIsAcceptable(int cap) {
    return cap >= MIN_CAP && cap <= MAX_CAP;
  }

  public Optional<Worker> worker(String id) {
    return Optional.ofNullable(workers.get(id));
  }

  /**
   * R1, R2. Held as a set rather than counted from the registry: the drain re-reads it once per
   * promotion, and the registry it would otherwise scan is two orders of magnitude larger than
   * the cap.
   */
  public int occupiedSlots() {
    return occupied.size();
  }

  /** Every terminal worker's report, in arrival order. R7. */
  public List<Report> reports() {
    return workers.values().stream()
        .filter(w -> w.status().isTerminal())
        .map(w -> w.report(colonyId))
        .toList();
  }

  // -- planning ------------------------------------------------------------

  /** R1, R3, R6 — the one admission decision every spawn funnels through. */
  public Admission decide() {
    if (dispatchBlocked) {
      return Admission.REFUSED;
    }
    return occupiedSlots() < cap ? Admission.ADMITTED : Admission.QUEUED;
  }

  public List<SchedulerEvent> planSpawn(List<TaskSpec> specs, String batchId) {
    var fold = new Fold(this);
    for (int i = 0; i < specs.size(); i++) {
      var spec = specs.get(i);
      var admission = spec.persistent() ? Admission.ADMITTED : fold.state.decide();
      var worker =
          Worker.spawned(
              spec,
              batchId,
              i + 1,
              specs.size(),
              switch (admission) {
                case ADMITTED -> WorkerStatus.PENDING;
                case QUEUED -> WorkerStatus.QUEUED;
                case REFUSED -> WorkerStatus.STOPPED;
              });
      fold.emit(
          switch (admission) {
            case ADMITTED -> new WorkerAdmitted(worker);
            case QUEUED -> new WorkerQueued(worker);
            case REFUSED -> new WorkerRefused(worker, REFUSED_SUMMARY);
          });
    }
    return fold.finish();
  }

  public List<SchedulerEvent> planStart(String workerId) {
    return worker(workerId)
        .filter(w -> w.status() == WorkerStatus.PENDING)
        .map(w -> List.<SchedulerEvent>of(new WorkerStarted(workerId)))
        .orElseGet(List::of);
  }

  /** R5 — the terminal and the promotions it makes room for, in one step. */
  public List<SchedulerEvent> planTerminate(
      String workerId, WorkerStatus status, String summary, double durationSeconds) {
    var existing = worker(workerId);
    if (existing.isEmpty() || existing.get().status().isTerminal()) {
      return List.of();
    }
    var fold = new Fold(this);
    fold.emit(new WorkerTerminated(workerId, status, summary, durationSeconds));
    fold.drain();
    return fold.finish();
  }

  /** R13 — a raise is a promotion opportunity; a lower stops nobody. */
  public List<SchedulerEvent> planSetCap(int newCap) {
    var fold = new Fold(this);
    fold.emit(new CapSet(newCap));
    fold.drain();
    return fold.finish();
  }

  public List<SchedulerEvent> planBlockDispatch() {
    return dispatchBlocked ? List.of() : List.of(new DispatchBlocked());
  }

  public List<SchedulerEvent> planResumeDispatch() {
    return dispatchBlocked ? List.of(new DispatchResumed()) : List.of();
  }

  /**
   * R7, R8, R9, R10 — the sweep, in hive's order: block, cancel what is queued, stop what is
   * running, then let the freed slots promote whatever is left.
   *
   * @param workerIds the workers to sweep, or {@code null} for all of them
   * @param includePersistent whether the sweep may take the persistent overseer too
   * @param keepBlocked whether the gate stays shut afterwards, or is restored
   */
  public List<SchedulerEvent> planStop(
      Collection<String> workerIds, boolean includePersistent, boolean keepBlocked) {
    var targeted = workerIds != null;
    var wanted = targeted ? Set.copyOf(workerIds) : Set.<String>of();
    var fold = new Fold(this);

    fold.emitAll(planBlockDispatch());

    // Queued first: they have no slot to give back, and leaving them until after the live
    // sweep would let the promotions below start the very workers this is stopping.
    for (var w : List.copyOf(fold.state.queue)) {
      var worker = fold.state.workers.get(w);
      if (selected(worker, targeted, wanted, includePersistent)) {
        fold.emit(new QueuedWorkerCancelled(w, CANCELLED_SUMMARY));
      }
    }
    // A persistent worker never enters `occupied`, so an includePersistent sweep has to look
    // for it in the registry; every other slot-holder is in the live set.
    var live = new java.util.LinkedHashSet<>(fold.state.occupied);
    if (includePersistent) {
      fold.state.workers.values().stream()
          .filter(w -> w.persistent() && w.status().occupiesSlot())
          .forEach(w -> live.add(w.id()));
    }
    for (var id : live) {
      var worker = fold.state.workers.get(id);
      if (selected(worker, targeted, wanted, includePersistent)) {
        fold.emit(new WorkerTerminated(id, WorkerStatus.STOPPED, SWEPT_SUMMARY, 0.0));
      }
    }
    fold.drain();

    if (!keepBlocked && !dispatchBlocked) {
      fold.emitAll(fold.state.planResumeDispatch());
    }
    return fold.finish();
  }

  private static boolean selected(
      Worker worker, boolean targeted, Set<String> wanted, boolean includePersistent) {
    if (worker == null) {
      return false;
    }
    if (worker.persistent() && !includePersistent) {
      return false;
    }
    return !targeted || wanted.contains(worker.id());
  }

  // -- folding -------------------------------------------------------------

  public SchedulerState onEvent(SchedulerEvent event) {
    return switch (event) {
      case CapSet e ->
          new SchedulerState(colonyId, e.cap(), dispatchBlocked, workers, queue, occupied);
      case DispatchBlocked ignored ->
          new SchedulerState(colonyId, cap, true, workers, queue, occupied);
      case DispatchResumed ignored ->
          new SchedulerState(colonyId, cap, false, workers, queue, occupied);
      case WorkerAdmitted e -> put(e.worker(), queue);
      case WorkerRefused e -> put(e.worker().terminal(WorkerStatus.STOPPED, e.summary(), 0.0), queue);
      case WorkerQueued e -> put(e.worker(), append(queue, e.worker().id()));
      case WorkerStarted e -> put(workers.get(e.workerId()).withStatus(WorkerStatus.RUNNING), queue);
      case WorkerPromoted e ->
          put(workers.get(e.workerId()).withStatus(WorkerStatus.PENDING), without(queue, e.workerId()));
      case WorkerTerminated e ->
          put(
              workers.get(e.workerId()).terminal(e.status(), e.summary(), e.durationSeconds()),
              without(queue, e.workerId()));
      case QueuedWorkerCancelled e ->
          put(
              workers.get(e.workerId()).terminal(WorkerStatus.STOPPED, e.summary(), 0.0),
              without(queue, e.workerId()));
      case TerminalWorkersEvicted e -> evict(e.workerIds());
    };
  }

  private SchedulerState put(Worker worker, List<String> nextQueue) {
    var next = new LinkedHashMap<>(workers);
    next.put(worker.id(), worker);
    var holdsSlot = !worker.persistent() && worker.status().occupiesSlot();
    var nextOccupied = occupied;
    if (holdsSlot != occupied.contains(worker.id())) {
      var mutable = new java.util.LinkedHashSet<>(occupied);
      if (holdsSlot) {
        mutable.add(worker.id());
      } else {
        mutable.remove(worker.id());
      }
      nextOccupied = Collections.unmodifiableSet(mutable);
    }
    return new SchedulerState(colonyId, cap, dispatchBlocked, sealed(next), nextQueue, nextOccupied);
  }

  /** Only terminal workers are ever evicted, so occupancy cannot change here. */
  private SchedulerState evict(List<String> ids) {
    var next = new LinkedHashMap<>(workers);
    ids.forEach(next::remove);
    return new SchedulerState(colonyId, cap, dispatchBlocked, sealed(next), queue, occupied);
  }

  /**
   * Arrival order is what R14 evicts by, so the registry is sealed with
   * {@code Collections.unmodifiableMap} over a {@code LinkedHashMap} rather than
   * {@code Map.copyOf}, which makes no ordering promise.
   */
  private static Map<String, Worker> sealed(LinkedHashMap<String, Worker> map) {
    return Collections.unmodifiableMap(map);
  }

  private static List<String> append(List<String> list, String id) {
    var next = new ArrayList<>(list);
    next.add(id);
    return List.copyOf(next);
  }

  private static List<String> without(List<String> list, String id) {
    return list.stream().filter(x -> !x.equals(id)).toList();
  }

  /** A state and the events that produced it, folded as they are emitted. */
  private static final class Fold {

    private SchedulerState state;

    private final List<SchedulerEvent> events = new ArrayList<>();

    private Fold(SchedulerState state) {
      this.state = state;
    }

    private void emit(SchedulerEvent event) {
      events.add(event);
      state = state.onEvent(event);
    }

    private void emitAll(List<SchedulerEvent> more) {
      more.forEach(this::emit);
    }

    /**
     * R14, run once at the end of every command rather than per event: the registry only has to
     * be within its bound when a caller can next observe it.
     */
    private List<SchedulerEvent> finish() {
      var terminal =
          state.workers.values().stream()
              .filter(w -> w.status().isTerminal())
              .map(Worker::id)
              .toList();
      var excess = terminal.size() - MAX_RETAINED_TERMINAL;
      if (excess > 0) {
        emit(new TerminalWorkersEvicted(List.copyOf(terminal.subList(0, excess))));
      }
      return events;
    }

    /** R4, R5 — promote in queue order until the queue empties or the cap is reached. */
    private void drain() {
      while (!state.queue.isEmpty() && state.occupiedSlots() < state.cap) {
        emit(new WorkerPromoted(state.queue.get(0)));
      }
    }
  }
}
