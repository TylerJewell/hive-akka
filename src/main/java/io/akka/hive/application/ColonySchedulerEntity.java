package io.akka.hive.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import akka.javasdk.eventsourcedentity.EventSourcedEntityContext;
import io.akka.hive.domain.Admission;
import io.akka.hive.domain.Report;
import io.akka.hive.domain.SchedulerEvent;
import io.akka.hive.domain.SchedulerState;
import io.akka.hive.domain.TaskSpec;
import io.akka.hive.domain.WorkerStatus;
import java.util.Collection;
import java.util.List;

/**
 * One colony's scheduler — SPEC-001 R1 through R14.
 *
 * <p>The entity id is the colony id. Every rule lives in {@link SchedulerState}, which is
 * tested without a runtime in {@code SchedulerStateTest}; this class decides only what to
 * persist and what to answer.
 *
 * <p>hive holds an {@code asyncio.Lock} across its admission and drain paths because several
 * coroutines can reach them at once. There is no equivalent here: commands to one entity id are
 * processed sequentially by the runtime, which is what {@code SchedulerConcurrencyIntegrationTest}
 * checks rather than assumes (question-log #16).
 */
@Component(id = "colony-scheduler")
public class ColonySchedulerEntity extends EventSourcedEntity<SchedulerState, SchedulerEvent> {

  private final String colonyId;

  public ColonySchedulerEntity(EventSourcedEntityContext context) {
    this.colonyId = context.entityId();
  }

  @Override
  public SchedulerState emptyState() {
    return SchedulerState.empty(colonyId);
  }

  public record SpawnBatch(String batchId, List<TaskSpec> tasks) {}

  /** One admission per task, in the order they were given. */
  public record BatchResult(String batchId, List<Admission> admissions) {}

  public record Terminate(
      String workerId, WorkerStatus status, String summary, double durationSeconds) {}

  public record StopWorkers(
      Collection<String> workerIds, boolean includePersistent, boolean keepBlocked) {}

  public record StopResult(int stopped, int queuedCancelled, boolean dispatchBlocked) {}

  public record ColonyView(SchedulerState state, List<Report> reports) {}

  /** R12 — hive's own clamp range, enforced here because hive's own config does not enforce it. */
  public Effect<Done> setCap(int cap) {
    if (!SchedulerState.capIsAcceptable(cap)) {
      return effects()
          .error(
              "cap must be between "
                  + SchedulerState.MIN_CAP
                  + " and "
                  + SchedulerState.MAX_CAP
                  + ", got "
                  + cap);
    }
    return effects().persistAll(currentState().planSetCap(cap)).thenReply(s -> Done.getInstance());
  }

  public Effect<BatchResult> spawnBatch(SpawnBatch command) {
    var events = currentState().planSpawn(command.tasks(), command.batchId());
    return effects()
        .persistAll(events)
        .thenReply(
            state ->
                new BatchResult(
                    command.batchId(),
                    command.tasks().stream()
                        .map(t -> admissionOf(state, t.workerId()))
                        .toList()));
  }

  private static Admission admissionOf(SchedulerState state, String workerId) {
    return state
        .worker(workerId)
        .map(
            w ->
                switch (w.status()) {
                  case PENDING, RUNNING -> Admission.ADMITTED;
                  case QUEUED -> Admission.QUEUED;
                  default -> Admission.REFUSED;
                })
        .orElse(Admission.REFUSED);
  }

  public Effect<Done> start(String workerId) {
    return effects()
        .persistAll(currentState().planStart(workerId))
        .thenReply(s -> Done.getInstance());
  }

  public Effect<Done> terminate(Terminate command) {
    return effects()
        .persistAll(
            currentState()
                .planTerminate(
                    command.workerId(),
                    command.status(),
                    command.summary(),
                    command.durationSeconds()))
        .thenReply(s -> Done.getInstance());
  }

  public Effect<Done> blockDispatch() {
    return effects()
        .persistAll(currentState().planBlockDispatch())
        .thenReply(s -> Done.getInstance());
  }

  public Effect<Done> resumeDispatch() {
    return effects()
        .persistAll(currentState().planResumeDispatch())
        .thenReply(s -> Done.getInstance());
  }

  public Effect<StopResult> stopWorkers(StopWorkers command) {
    var events =
        currentState()
            .planStop(command.workerIds(), command.includePersistent(), command.keepBlocked());
    var stopped =
        (int) events.stream().filter(e -> e instanceof SchedulerEvent.WorkerTerminated).count();
    var cancelled =
        (int) events.stream().filter(e -> e instanceof SchedulerEvent.QueuedWorkerCancelled).count();
    return effects()
        .persistAll(events)
        .thenReply(state -> new StopResult(stopped, cancelled, state.dispatchBlocked()));
  }

  public ReadOnlyEffect<ColonyView> get() {
    return effects().reply(new ColonyView(currentState(), currentState().reports()));
  }

  @Override
  public SchedulerState applyEvent(SchedulerEvent event) {
    return currentState().onEvent(event);
  }
}
