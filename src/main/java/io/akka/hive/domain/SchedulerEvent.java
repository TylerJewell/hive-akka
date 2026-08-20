package io.akka.hive.domain;

import akka.javasdk.annotations.TypeName;
import java.util.List;

/** Everything that can change a colony's scheduling state. */
public sealed interface SchedulerEvent {

  @TypeName("cap-set")
  record CapSet(int cap) implements SchedulerEvent {}

  @TypeName("worker-admitted")
  record WorkerAdmitted(Worker worker) implements SchedulerEvent {}

  @TypeName("worker-queued")
  record WorkerQueued(Worker worker) implements SchedulerEvent {}

  /**
   * A spawn the gate turned away. The worker is recorded terminal on arrival rather than
   * dropped, because R7 owes it a report.
   */
  @TypeName("worker-refused")
  record WorkerRefused(Worker worker, String summary) implements SchedulerEvent {}

  @TypeName("worker-started")
  record WorkerStarted(String workerId) implements SchedulerEvent {}

  @TypeName("worker-promoted")
  record WorkerPromoted(String workerId) implements SchedulerEvent {}

  @TypeName("worker-terminated")
  record WorkerTerminated(String workerId, WorkerStatus status, String summary, double durationSeconds)
      implements SchedulerEvent {}

  @TypeName("queued-worker-cancelled")
  record QueuedWorkerCancelled(String workerId, String summary) implements SchedulerEvent {}

  @TypeName("dispatch-blocked")
  record DispatchBlocked() implements SchedulerEvent {}

  @TypeName("dispatch-resumed")
  record DispatchResumed() implements SchedulerEvent {}

  /** R14. Recorded rather than pruned silently, so a shrinking registry is readable. */
  @TypeName("terminal-workers-evicted")
  record TerminalWorkersEvicted(List<String> workerIds) implements SchedulerEvent {}
}
