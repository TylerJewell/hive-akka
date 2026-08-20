package io.akka.hive;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.TestKitSupport;
import io.akka.hive.application.ColonySchedulerEntity;
import io.akka.hive.domain.Admission;
import io.akka.hive.domain.TaskSpec;
import io.akka.hive.domain.WorkerStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Question-log #16 — the claim this whole port rests on: that the target serialises commands
 * per entity id, so a scheduler built as one entity needs no equivalent of hive's
 * {@code _scheduler_lock}.
 *
 * <p>This test starts a runtime. It is the one place the claim can be checked rather than
 * quoted: the documentation says commands to one entity are processed sequentially, and a
 * document is not evidence.
 */
public class SchedulerConcurrencyIntegrationTest extends TestKitSupport {

  private static final int THREADS = 8;

  private static final int SPAWNS = 40;

  private static final int CAP = 5;

  @Test
  public void fortyConcurrentSpawnsNeverExceedTheCapAndLoseNobody() throws Exception {
    var colonyId = "race-" + UUID.randomUUID().toString().substring(0, 8);
    componentClient
        .forEventSourcedEntity(colonyId)
        .method(ColonySchedulerEntity::setCap)
        .invoke(CAP);

    var pool = Executors.newFixedThreadPool(THREADS);
    var start = new CountDownLatch(1);
    var results = new ArrayList<java.util.concurrent.Future<Admission>>();
    try {
      for (int i = 0; i < SPAWNS; i++) {
        var workerId = "w" + i;
        results.add(
            pool.submit(
                () -> {
                  start.await();
                  var reply =
                      componentClient
                          .forEventSourcedEntity(colonyId)
                          .method(ColonySchedulerEntity::spawnBatch)
                          .invoke(
                              new ColonySchedulerEntity.SpawnBatch(
                                  "batch-" + workerId,
                                  List.of(new TaskSpec(workerId, "task " + workerId, false))));
                  return reply.admissions().get(0);
                }));
      }
      start.countDown();
      var admitted = 0;
      for (var f : results) {
        if (f.get(60, TimeUnit.SECONDS) == Admission.ADMITTED) {
          admitted++;
        }
      }
      assertThat(admitted)
          .as("exactly cap admissions, no lost update")
          .isEqualTo(CAP);
    } finally {
      pool.shutdownNow();
    }

    var state =
        componentClient
            .forEventSourcedEntity(colonyId)
            .method(ColonySchedulerEntity::get)
            .invoke()
            .state();

    assertThat(state.workers()).as("every spawn is accounted for").hasSize(SPAWNS);
    assertThat(state.occupiedSlots()).isEqualTo(CAP);
    assertThat(state.queue()).hasSize(SPAWNS - CAP);
    assertThat(state.queue()).doesNotHaveDuplicates();
  }

  /**
   * The same race on the way out: cap slots free concurrently, and the queue must give up
   * exactly cap promotions, never more.
   */
  @Test
  public void concurrentTerminationsPromoteExactlyOnceEach() throws Exception {
    var colonyId = "drain-" + UUID.randomUUID().toString().substring(0, 8);
    var client = componentClient.forEventSourcedEntity(colonyId);
    client.method(ColonySchedulerEntity::setCap).invoke(CAP);

    var specs = new ArrayList<TaskSpec>();
    for (int i = 0; i < SPAWNS; i++) {
      specs.add(new TaskSpec("w" + i, "task " + i, false));
    }
    var spawned = client.method(ColonySchedulerEntity::spawnBatch)
        .invoke(new ColonySchedulerEntity.SpawnBatch("one-batch", specs));
    var running = new ArrayList<String>();
    for (int i = 0; i < spawned.admissions().size(); i++) {
      if (spawned.admissions().get(i) == Admission.ADMITTED) {
        running.add(specs.get(i).workerId());
      }
    }
    assertThat(running).hasSize(CAP);

    var pool = Executors.newFixedThreadPool(THREADS);
    var start = new CountDownLatch(1);
    try {
      var futures = new ArrayList<java.util.concurrent.Future<?>>();
      for (var id : running) {
        futures.add(
            pool.submit(
                () -> {
                  start.await();
                  return componentClient
                      .forEventSourcedEntity(colonyId)
                      .method(ColonySchedulerEntity::terminate)
                      .invoke(
                          new ColonySchedulerEntity.Terminate(
                              id, WorkerStatus.SUCCEEDED, "done", 0.0));
                }));
      }
      start.countDown();
      for (var f : futures) {
        f.get(60, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }

    var state = componentClient
        .forEventSourcedEntity(colonyId)
        .method(ColonySchedulerEntity::get)
        .invoke()
        .state();

    assertThat(state.occupiedSlots()).isEqualTo(CAP);
    assertThat(state.queue()).hasSize(SPAWNS - 2 * CAP);
    // The first cap workers ran, the next cap were promoted, and nobody was promoted twice.
    var promoted =
        state.workers().values().stream()
            .filter(w -> w.status() == WorkerStatus.PENDING)
            .map(w -> w.id())
            .toList();
    assertThat(promoted).doesNotHaveDuplicates().hasSize(CAP);
  }
}
