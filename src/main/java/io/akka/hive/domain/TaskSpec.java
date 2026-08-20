package io.akka.hive.domain;

/**
 * One task handed to the scheduler.
 *
 * <p>The caller supplies the worker id rather than the scheduler minting one, so a spawn is
 * idempotent from the caller's side and a test can name the worker it is about.
 */
public record TaskSpec(String workerId, String task, boolean persistent) {}
