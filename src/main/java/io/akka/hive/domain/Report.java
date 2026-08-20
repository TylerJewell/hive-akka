package io.akka.hive.domain;

/**
 * What a worker leaves behind on reaching a terminal status. SPEC-001 R7.
 *
 * <p>The batch metadata is carried through unchanged, including for a worker that never ran:
 * a caller counting a batch home resolves it by matching these, so a missing one is a caller
 * waiting forever.
 */
public record Report(
    String workerId,
    String colonyId,
    String task,
    WorkerStatus status,
    String summary,
    double durationSeconds,
    String batchId,
    int batchIndex,
    int batchSize) {}
