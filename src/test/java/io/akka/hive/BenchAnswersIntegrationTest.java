package io.akka.hive;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.TestKitSupport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akka.hive.application.ColonySchedulerEntity;
import io.akka.hive.domain.SchedulerState;
import io.akka.hive.domain.TaskSpec;
import io.akka.hive.domain.WorkerStatus;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The port's half of the differential in {@code hive-port/bench/}.
 *
 * <p>Runs the same eight scenarios as {@code hive-port/probes/probe_02.py}, read from the same
 * {@code scenarios.json} so neither side can drift from the other, and writes
 * {@code port-answers.json} next to the source's. {@code bench/compare.py} puts the two side by
 * side; comparing the speed of two schedulers that make different decisions would measure
 * nothing, which is why this runs before any timing is quoted.
 *
 * <p>It is a test rather than a separate program so it drives the real runtime and real journal
 * through the same client an ordinary caller uses, and so it fails the build when the port stops
 * being able to produce an answer at all.
 */
public class BenchAnswersIntegrationTest extends TestKitSupport {

  private static final Path BENCH = Path.of("..", "hive-port", "bench");

  private static final ObjectMapper JSON = new ObjectMapper();

  /** Enough samples to average out a single journal write; kept small so the build stays fast. */
  private static final int TIMED_ADMISSIONS = 200;

  private static final int WARMUP_ADMISSIONS = 50;

  @Test
  public void runsTheSharedScenariosAndWritesThePortsAnswers() throws IOException {
    var scenarios = (ArrayNode) JSON.readTree(Files.readString(BENCH.resolve("scenarios.json")));

    var out = JSON.createObjectNode();
    out.put("system", "hive-akka");
    var results = out.putArray("scenarios");
    for (var scenario : scenarios) {
      results.add(run(scenario));
    }
    out.put("admission_ns_per_op", timeOneAdmission());
    out.put(
        "admission_measurement",
        "one spawnBatch command of a single task into a saturated colony, through the "
            + "component client and the entity's journal, no HTTP");
    out.put("domain_decision_ns_per_op", timeTheDecisionAlone());
    out.put(
        "domain_decision_measurement",
        "SchedulerState.planSpawn for a single task into a saturated colony, in-process, "
            + "nothing persisted - the floor the command above sits on");

    Files.createDirectories(BENCH);
    Files.writeString(
        BENCH.resolve("port-answers.json"), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(out));

    assertThat(results).hasSize(scenarios.size());
  }

  private ObjectNode run(com.fasterxml.jackson.databind.JsonNode scenario) {
    var colonyId = "bench-" + UUID.randomUUID().toString().substring(0, 8);
    var cap = scenario.get("cap").asInt();
    client(colonyId).method(ColonySchedulerEntity::setCap).invoke(cap);

    // The port names workers by id; hive names them by task text. One label per worker on
    // both sides, so the two answer files compare directly.
    var idOf = new LinkedHashMap<String, String>();

    var result = JSON.createObjectNode();
    result.put("name", scenario.get("name").asText());
    result.put("cap", cap);
    var steps = result.putArray("steps");

    for (var op : scenario.get("ops")) {
      var kind = op.get(0).asText();
      switch (kind) {
        case "spawn" -> {
          var batchId = op.get(1).asText();
          var specs = new ArrayList<TaskSpec>();
          for (var label : op.get(2)) {
            var id = colonyId + "-" + label.asText();
            idOf.put(label.asText(), id);
            specs.add(new TaskSpec(id, label.asText(), false));
          }
          client(colonyId)
              .method(ColonySchedulerEntity::spawnBatch)
              .invoke(new ColonySchedulerEntity.SpawnBatch(batchId, specs));
        }
        case "start" ->
            client(colonyId).method(ColonySchedulerEntity::start).invoke(idOf.get(op.get(1).asText()));
        case "terminate" ->
            client(colonyId)
                .method(ColonySchedulerEntity::terminate)
                .invoke(
                    new ColonySchedulerEntity.Terminate(
                        idOf.get(op.get(1).asText()),
                        WorkerStatus.SUCCEEDED,
                        op.get(1).asText() + " done",
                        0.0));
        case "block" -> client(colonyId).method(ColonySchedulerEntity::blockDispatch).invoke();
        case "resume" -> client(colonyId).method(ColonySchedulerEntity::resumeDispatch).invoke();
        case "stop_all" ->
            client(colonyId)
                .method(ColonySchedulerEntity::stopWorkers)
                .invoke(new ColonySchedulerEntity.StopWorkers(null, false, true));
        case "stop" -> {
          var ids = new ArrayList<String>();
          for (var label : op.get(1)) {
            ids.add(idOf.get(label.asText()));
          }
          client(colonyId)
              .method(ColonySchedulerEntity::stopWorkers)
              .invoke(new ColonySchedulerEntity.StopWorkers(Set.copyOf(ids), false, false));
        }
        default -> throw new IllegalArgumentException("unknown op " + kind);
      }
      var step = steps.addObject();
      step.set("op", op);
      step.set("after", observe(colonyId));
    }
    return result;
  }

  private ObjectNode observe(String colonyId) {
    var state = client(colonyId).method(ColonySchedulerEntity::get).invoke().state();
    var node = JSON.createObjectNode();

    var running = node.putArray("running");
    state.workers().values().stream()
        .filter(w -> w.status().occupiesSlot())
        .map(w -> w.task())
        .sorted()
        .forEach(running::add);

    var queued = node.putArray("queued");
    state.queue().forEach(id -> queued.add(state.worker(id).orElseThrow().task()));

    var terminal = node.putObject("terminal");
    state.workers().values().stream()
        .filter(w -> w.status().isTerminal())
        .sorted(java.util.Comparator.comparing(w -> w.task()))
        .forEach(w -> terminal.put(w.task(), w.status().name()));

    node.put("dispatch_blocked", state.dispatchBlocked());
    return node;
  }

  private long timeOneAdmission() {
    var colonyId = "timing-" + UUID.randomUUID().toString().substring(0, 8);
    client(colonyId).method(ColonySchedulerEntity::setCap).invoke(1);
    // Fill the one slot, so every timed spawn below takes the queueing branch - the same
    // branch hive's timing takes, for the same reason.
    spawnOne(colonyId, "filler");
    for (int i = 0; i < WARMUP_ADMISSIONS; i++) {
      spawnOne(colonyId, "warm" + i);
    }
    var start = System.nanoTime();
    for (int i = 0; i < TIMED_ADMISSIONS; i++) {
      spawnOne(colonyId, "t" + i);
    }
    return (System.nanoTime() - start) / TIMED_ADMISSIONS;
  }

  private void spawnOne(String colonyId, String label) {
    client(colonyId)
        .method(ColonySchedulerEntity::spawnBatch)
        .invoke(
            new ColonySchedulerEntity.SpawnBatch(
                "b-" + label, List.of(new TaskSpec(colonyId + "-" + label, label, false))));
  }

  private long timeTheDecisionAlone() {
    var state = SchedulerState.empty("floor").onEvent(new io.akka.hive.domain.SchedulerEvent.CapSet(1));
    for (var events = state.planSpawn(List.of(new TaskSpec("filler", "filler", false)), "b");
        !events.isEmpty();
        events = List.of()) {
      for (var e : events) {
        state = state.onEvent(e);
      }
    }
    for (int i = 0; i < 10_000; i++) {
      state.planSpawn(List.of(new TaskSpec("w" + i, "w" + i, false)), "b");
    }
    var start = System.nanoTime();
    for (int i = 0; i < 100_000; i++) {
      state.planSpawn(List.of(new TaskSpec("w" + i, "w" + i, false)), "b");
    }
    return (System.nanoTime() - start) / 100_000;
  }

  private akka.javasdk.client.EventSourcedEntityClient client(String colonyId) {
    return componentClient.forEventSourcedEntity(colonyId);
  }
}
