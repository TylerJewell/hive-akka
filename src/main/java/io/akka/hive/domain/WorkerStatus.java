package io.akka.hive.domain;

/** SPEC-001 §2 — the six statuses, and the two properties every scheduling rule reads. */
public enum WorkerStatus {
  PENDING,
  RUNNING,
  QUEUED,
  SUCCEEDED,
  FAILED,
  STOPPED;

  /** R2. A queued worker is waiting for a slot; a terminal one has given its slot back. */
  public boolean occupiesSlot() {
    return this == PENDING || this == RUNNING;
  }

  public boolean isTerminal() {
    return this == SUCCEEDED || this == FAILED || this == STOPPED;
  }
}
