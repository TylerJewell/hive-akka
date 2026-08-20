package io.akka.hive.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.hive.domain.SchedulerEvent;
import io.akka.hive.domain.SchedulerState;
import io.akka.hive.domain.TaskSpec;
import io.akka.hive.domain.WorkerStatus;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 rules R5, R6, R7, R8, R9, R10, R12 and R13 against the entity.
 *
 * <p>What is checked here rather than in {@code SchedulerStateTest} is everything a pure fold
 * cannot answer: that a command rejects rather than persists, that several events from one
 * command land as one atomic step, and that the reports a caller reads back are the ones the
 * rules promise.
 */
public class ColonySchedulerEntityTest {

  private EventSourcedTestKit<SchedulerState, SchedulerEvent, ColonySchedulerEntity> colony(
      int cap) {
    var kit = EventSourcedTestKit.of("colony-1", ColonySchedulerEntity::new);
    kit.method(ColonySchedulerEntity::setCap).invoke(cap);
    return kit;
  }

  private static ColonySchedulerEntity.SpawnBatch batch(String batchId, int n) {
    var specs =
        java.util.stream.IntStream.rangeClosed(1, n)
            .mapToObj(i -> new TaskSpec(batchId + "-w" + i, "task " + i, false))
            .toList();
    return new ColonySchedulerEntity.SpawnBatch(batchId, specs);
  }

  /** R5, and the target claim behind it: several events from one command fold in order. */
  @Test
  public void terminatingAWorkerPromotesTheNextQueuedOneInTheSameCommand() {
    var kit = colony(1);
    kit.method(ColonySchedulerEntity::spawnBatch).invoke(batch("A", 3));

    var result =
        kit.method(ColonySchedulerEntity::terminate)
            .invoke(new ColonySchedulerEntity.Terminate("A-w1", WorkerStatus.SUCCEEDED, "done", 2.0));

    assertThat(result.getAllEvents())
        .hasSize(2)
        .anySatisfy(e -> assertThat(e).isInstanceOf(SchedulerEvent.WorkerTerminated.class))
        .anySatisfy(e -> assertThat(e).isInstanceOf(SchedulerEvent.WorkerPromoted.class));

    var state = kit.getState();
    assertThat(state.worker("A-w1").orElseThrow().status()).isEqualTo(WorkerStatus.SUCCEEDED);
    assertThat(state.worker("A-w2").orElseThrow().status()).isEqualTo(WorkerStatus.PENDING);
    assertThat(state.queue()).containsExactly("A-w3");
  }

  /** R6 — the gate is consulted before the cap, so a wide-open colony still refuses. */
  @Test
  public void aBlockedDispatchRefusesWithoutConsultingTheCap() {
    var kit = colony(32);
    kit.method(ColonySchedulerEntity::blockDispatch).invoke();

    var result = kit.method(ColonySchedulerEntity::spawnBatch).invoke(batch("A", 1));

    assertThat(result.getReply().admissions()).containsExactly(io.akka.hive.domain.Admission.REFUSED);
    assertThat(kit.getState().queue()).isEmpty();
    assertThat(kit.getState().occupiedSlots()).isZero();

    kit.method(ColonySchedulerEntity::resumeDispatch).invoke();
    var after = kit.method(ColonySchedulerEntity::spawnBatch).invoke(batch("B", 1));
    assertThat(after.getReply().admissions()).containsExactly(io.akka.hive.domain.Admission.ADMITTED);
  }

  /** R7 — every worker in a refused batch reports, so a batch counter resolves. */
  @Test
  public void everyWorkerInARefusedBatchReports() {
    var kit = colony(4);
    kit.method(ColonySchedulerEntity::blockDispatch).invoke();

    kit.method(ColonySchedulerEntity::spawnBatch).invoke(batch("A", 3));

    var reports = kit.method(ColonySchedulerEntity::get).invoke().getReply().reports();
    assertThat(reports).hasSize(3);
    assertThat(reports)
        .allSatisfy(
            r -> {
              assertThat(r.status()).isEqualTo(WorkerStatus.STOPPED);
              assertThat(r.durationSeconds()).isZero();
              assertThat(r.batchId()).isEqualTo("A");
              assertThat(r.batchSize()).isEqualTo(3);
              assertThat(r.colonyId()).isEqualTo("colony-1");
            });
    assertThat(reports.stream().map(r -> r.batchIndex()).toList()).containsExactly(1, 2, 3);
  }

  /** R7's second half. */
  @Test
  public void cancellingQueuedWorkersReportsEachOne() {
    var kit = colony(1);
    kit.method(ColonySchedulerEntity::spawnBatch).invoke(batch("A", 4));

    kit.method(ColonySchedulerEntity::stopWorkers)
        .invoke(new ColonySchedulerEntity.StopWorkers(null, false, true));

    var reply = kit.method(ColonySchedulerEntity::get).invoke().getReply();
    assertThat(reply.state().queue()).isEmpty();
    assertThat(reply.reports()).hasSize(4);
    assertThat(reply.reports()).allSatisfy(r -> assertThat(r.status()).isEqualTo(WorkerStatus.STOPPED));
  }

  /** R8 — a cancelled queued worker cannot come back through a later drain. */
  @Test
  public void aCancelledQueuedWorkerIsNotPromotedLater() {
    var kit = colony(1);
    kit.method(ColonySchedulerEntity::spawnBatch).invoke(batch("A", 3));

    kit.method(ColonySchedulerEntity::stopWorkers)
        .invoke(new ColonySchedulerEntity.StopWorkers(Set.of("A-w2"), false, false));

    var result =
        kit.method(ColonySchedulerEntity::terminate)
            .invoke(new ColonySchedulerEntity.Terminate("A-w1", WorkerStatus.SUCCEEDED, "done", 1.0));

    var promoted =
        result.getAllEvents().stream()
            .filter(e -> e instanceof SchedulerEvent.WorkerPromoted)
            .map(e -> ((SchedulerEvent.WorkerPromoted) e).workerId())
            .toList();
    assertThat(promoted).containsExactly("A-w3");
    assertThat(kit.getState().worker("A-w2").orElseThrow().status()).isEqualTo(WorkerStatus.STOPPED);
  }

  /** R9 — the gate's final state, which is the half a caller can observe. */
  @Test
  public void aPlainSweepRestoresTheGateAndAUserStopKeepsItShut() {
    var kit = colony(2);
    kit.method(ColonySchedulerEntity::spawnBatch).invoke(batch("A", 3));

    kit.method(ColonySchedulerEntity::stopWorkers)
        .invoke(new ColonySchedulerEntity.StopWorkers(Set.of("A-w1"), false, false));
    assertThat(kit.getState().dispatchBlocked()).isFalse();

    kit.method(ColonySchedulerEntity::stopWorkers)
        .invoke(new ColonySchedulerEntity.StopWorkers(null, false, true));
    assertThat(kit.getState().dispatchBlocked()).isTrue();
  }

  /** R9's ordering half — the block is persisted before anything it protects. */
  @Test
  public void aSweepBlocksDispatchBeforeItCancelsAnything() {
    var kit = colony(1);
    kit.method(ColonySchedulerEntity::spawnBatch).invoke(batch("A", 3));

    var result =
        kit.method(ColonySchedulerEntity::stopWorkers)
            .invoke(new ColonySchedulerEntity.StopWorkers(null, false, true));

    var events = result.getAllEvents();
    assertThat(events.get(0)).isInstanceOf(SchedulerEvent.DispatchBlocked.class);
  }

  /** R10. */
  @Test
  public void aPersistentWorkerNeitherOccupiesASlotNorIsSweptByDefault() {
    var kit = colony(2);
    kit.method(ColonySchedulerEntity::spawnBatch)
        .invoke(
            new ColonySchedulerEntity.SpawnBatch(
                "overseer", List.of(new TaskSpec("queen", "", true))));
    kit.method(ColonySchedulerEntity::spawnBatch).invoke(batch("A", 2));

    // Cap 2, an overseer and two workers: nothing queued, so the overseer took no slot.
    assertThat(kit.getState().queue()).isEmpty();
    assertThat(kit.getState().occupiedSlots()).isEqualTo(2);

    kit.method(ColonySchedulerEntity::stopWorkers)
        .invoke(new ColonySchedulerEntity.StopWorkers(null, false, true));
    assertThat(kit.getState().worker("queen").orElseThrow().status()).isEqualTo(WorkerStatus.PENDING);

    kit.method(ColonySchedulerEntity::stopWorkers)
        .invoke(new ColonySchedulerEntity.StopWorkers(null, true, true));
    assertThat(kit.getState().worker("queen").orElseThrow().status()).isEqualTo(WorkerStatus.STOPPED);
  }

  /** R12 — hive's own clamp range, enforced where hive does not enforce it. */
  @Test
  public void rejectsACapOutsideOneToThirtyTwo() {
    var kit = colony(4);

    for (var bad : List.of(0, -1, 33, 1000)) {
      var result = kit.method(ColonySchedulerEntity::setCap).invoke(bad);
      assertThat(result.isError()).as("cap %s", bad).isTrue();
      assertThat(result.didPersistEvents()).isFalse();
    }
    assertThat(kit.getState().cap()).isEqualTo(4);

    for (var good : List.of(1, 32)) {
      assertThat(kit.method(ColonySchedulerEntity::setCap).invoke(good).isError()).isFalse();
    }
  }

  /** R13 — both halves, and the difference from hive is the first one. */
  @Test
  public void raisingTheCapPromotesAndLoweringItStopsNobody() {
    var kit = colony(2);
    kit.method(ColonySchedulerEntity::spawnBatch).invoke(batch("A", 5));
    assertThat(kit.getState().queue()).hasSize(3);

    var raised = kit.method(ColonySchedulerEntity::setCap).invoke(4);
    assertThat(
            raised.getAllEvents().stream()
                .filter(e -> e instanceof SchedulerEvent.WorkerPromoted)
                .count())
        .isEqualTo(2);
    assertThat(kit.getState().occupiedSlots()).isEqualTo(4);

    kit.method(ColonySchedulerEntity::setCap).invoke(1);
    assertThat(kit.getState().occupiedSlots()).isEqualTo(4);
    assertThat(kit.getState().queue()).containsExactly("A-w5");
  }

  /** R5 and R6 together: the gate refuses new work but does not hold back a promotion. */
  @Test
  public void aBlockedGateStillPromotesWorkAlreadyAccepted() {
    var kit = colony(1);
    kit.method(ColonySchedulerEntity::spawnBatch).invoke(batch("A", 3));
    kit.method(ColonySchedulerEntity::blockDispatch).invoke();

    kit.method(ColonySchedulerEntity::terminate)
        .invoke(new ColonySchedulerEntity.Terminate("A-w1", WorkerStatus.SUCCEEDED, "done", 1.0));

    assertThat(kit.getState().worker("A-w2").orElseThrow().status()).isEqualTo(WorkerStatus.PENDING);
  }

  /** A slot is held from admission to termination, not from the start signal. */
  @Test
  public void startingAnAdmittedWorkerDoesNotChangeOccupancy() {
    var kit = colony(2);
    kit.method(ColonySchedulerEntity::spawnBatch).invoke(batch("A", 3));
    assertThat(kit.getState().occupiedSlots()).isEqualTo(2);

    kit.method(ColonySchedulerEntity::start).invoke("A-w1");

    assertThat(kit.getState().worker("A-w1").orElseThrow().status()).isEqualTo(WorkerStatus.RUNNING);
    assertThat(kit.getState().occupiedSlots()).isEqualTo(2);
    assertThat(kit.getState().queue()).containsExactly("A-w3");
  }
}
