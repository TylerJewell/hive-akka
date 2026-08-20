package io.akka.hive.domain;

/** The three outcomes of the one admission decision. SPEC-001 R1, R3, R6. */
public enum Admission {
  ADMITTED,
  QUEUED,
  REFUSED
}
