package io.akka.hive.api;

import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.annotations.http.Put;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.HttpException;
import io.akka.hive.application.ColonySchedulerEntity;
import io.akka.hive.domain.Admission;
import io.akka.hive.domain.Report;
import io.akka.hive.domain.SchedulerState;
import io.akka.hive.domain.TaskSpec;
import io.akka.hive.domain.WorkerStatus;
import java.util.List;
import java.util.Set;

/**
 * The whole surface: a colony's cap, its workers, its queue and its stop gate.
 *
 * <p>hive exposes only part of this to a caller — a Stop button and a live worker count. The
 * cap and the queue are exposed here because a scheduler whose limit nobody can read is a
 * scheduler nobody can operate; the difference is listed in the README.
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint("/colonies")
public class ColonyEndpoint {

  private final ComponentClient componentClient;

  public ColonyEndpoint(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  public record CapRequest(int cap) {}

  /** Worker ids come from the caller, so a retried spawn names the same workers. */
  public record SpawnRequest(String batchId, List<String> workerIds, boolean persistent) {}

  public record SpawnResult(String batchId, List<Admission> admissions) {}

  public record TerminateRequest(WorkerStatus status, String summary, double durationSeconds) {}

  public record StopRequest(List<String> workerIds, boolean includePersistent) {}

  public record StopView(int stopped, int queuedCancelled, boolean dispatchBlocked) {}

  public record ColonyView(
      String colonyId,
      int cap,
      boolean dispatchBlocked,
      int occupiedSlots,
      List<String> queue,
      List<Report> reports) {}

  @Put("/{colonyId}/cap")
  public ColonyView setCap(String colonyId, CapRequest request) {
    if (!SchedulerState.capIsAcceptable(request.cap())) {
      throw HttpException.badRequest(
          "cap must be between "
              + SchedulerState.MIN_CAP
              + " and "
              + SchedulerState.MAX_CAP
              + ", got "
              + request.cap());
    }
    scheduler(colonyId).method(ColonySchedulerEntity::setCap).invoke(request.cap());
    return get(colonyId);
  }

  @Post("/{colonyId}/workers")
  public SpawnResult spawn(String colonyId, SpawnRequest request) {
    var tasks =
        request.workerIds().stream()
            .map(id -> new TaskSpec(id, "task " + id, request.persistent()))
            .toList();
    var result =
        scheduler(colonyId)
            .method(ColonySchedulerEntity::spawnBatch)
            .invoke(new ColonySchedulerEntity.SpawnBatch(request.batchId(), tasks));
    return new SpawnResult(result.batchId(), result.admissions());
  }

  @Post("/{colonyId}/workers/{workerId}/start")
  public ColonyView start(String colonyId, String workerId) {
    scheduler(colonyId).method(ColonySchedulerEntity::start).invoke(workerId);
    return get(colonyId);
  }

  @Post("/{colonyId}/workers/{workerId}/terminate")
  public ColonyView terminate(String colonyId, String workerId, TerminateRequest request) {
    scheduler(colonyId)
        .method(ColonySchedulerEntity::terminate)
        .invoke(
            new ColonySchedulerEntity.Terminate(
                workerId, request.status(), request.summary(), request.durationSeconds()));
    return get(colonyId);
  }

  /**
   * The Stop button. Keeps the gate shut afterwards, which is what a user stop means in hive:
   * the queen is done and nothing more starts until the user speaks again (R9).
   */
  @Post("/{colonyId}/workers/stop-all")
  public StopView stopAll(String colonyId) {
    return toApi(
        scheduler(colonyId)
            .method(ColonySchedulerEntity::stopWorkers)
            .invoke(new ColonySchedulerEntity.StopWorkers(null, false, true)));
  }

  /** Stopping named workers leaves the colony free to keep working, so the gate is restored. */
  @Post("/{colonyId}/workers/stop")
  public StopView stop(String colonyId, StopRequest request) {
    return toApi(
        scheduler(colonyId)
            .method(ColonySchedulerEntity::stopWorkers)
            .invoke(
                new ColonySchedulerEntity.StopWorkers(
                    request.workerIds() == null ? null : Set.copyOf(request.workerIds()),
                    request.includePersistent(),
                    false)));
  }

  private static StopView toApi(ColonySchedulerEntity.StopResult result) {
    return new StopView(result.stopped(), result.queuedCancelled(), result.dispatchBlocked());
  }

  @Post("/{colonyId}/dispatch/block")
  public ColonyView blockDispatch(String colonyId) {
    scheduler(colonyId).method(ColonySchedulerEntity::blockDispatch).invoke();
    return get(colonyId);
  }

  @Post("/{colonyId}/dispatch/resume")
  public ColonyView resumeDispatch(String colonyId) {
    scheduler(colonyId).method(ColonySchedulerEntity::resumeDispatch).invoke();
    return get(colonyId);
  }

  @Get("/{colonyId}")
  public ColonyView get(String colonyId) {
    var view = scheduler(colonyId).method(ColonySchedulerEntity::get).invoke();
    return new ColonyView(
        colonyId,
        view.state().cap(),
        view.state().dispatchBlocked(),
        view.state().occupiedSlots(),
        view.state().queue(),
        view.reports());
  }

  private akka.javasdk.client.EventSourcedEntityClient scheduler(String colonyId) {
    return componentClient.forEventSourcedEntity(colonyId);
  }
}
