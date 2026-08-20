package io.akka.hive.domain;

/**
 * One unit of admitted work. SPEC-001 §2.
 *
 * <p>{@code task} and {@code summary} are caller-supplied and are the only fields here whose
 * length the scheduler does not choose. Both are bounded on the way in: a colony retains
 * {@link SchedulerState#MAX_RETAINED_TERMINAL} terminal workers, and unbounded text on each
 * would put the entity's state against the platform's 1 MB replication ceiling.
 */
public record Worker(
    String id,
    String task,
    String batchId,
    int batchIndex,
    int batchSize,
    boolean persistent,
    WorkerStatus status,
    String summary,
    double durationSeconds) {

  static final int MAX_TEXT = 256;

  public static Worker spawned(
      TaskSpec spec, String batchId, int batchIndex, int batchSize, WorkerStatus status) {
    return new Worker(
        spec.workerId(),
        bounded(spec.task()),
        batchId,
        batchIndex,
        batchSize,
        spec.persistent(),
        status,
        "",
        0.0);
  }

  public Worker withStatus(WorkerStatus next) {
    return new Worker(
        id, task, batchId, batchIndex, batchSize, persistent, next, summary, durationSeconds);
  }

  public Worker terminal(WorkerStatus next, String summary, double durationSeconds) {
    return new Worker(
        id,
        task,
        batchId,
        batchIndex,
        batchSize,
        persistent,
        next,
        bounded(summary),
        durationSeconds);
  }

  private static String bounded(String text) {
    if (text == null) {
      return "";
    }
    return text.length() <= MAX_TEXT ? text : text.substring(0, MAX_TEXT);
  }

  public Report report(String colonyId) {
    return new Report(
        id, colonyId, task, status, summary, durationSeconds, batchId, batchIndex, batchSize);
  }
}
