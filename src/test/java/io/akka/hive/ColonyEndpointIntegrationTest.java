package io.akka.hive;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.TestKitSupport;
import io.akka.hive.api.ColonyEndpoint;
import io.akka.hive.domain.Admission;
import io.akka.hive.domain.WorkerStatus;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The HTTP surface, end to end against a running runtime — the whole capability driven the way
 * a caller drives it, since this port has no other interface (see {@code gui/manifest.json} in
 * the findings directory).
 */
public class ColonyEndpointIntegrationTest extends TestKitSupport {

  private String colony() {
    return "c-" + UUID.randomUUID().toString().substring(0, 8);
  }

  private ColonyEndpoint.SpawnResult spawn(String colony, String batchId, String... workerIds) {
    return httpClient
        .POST("/colonies/" + colony + "/workers")
        .withRequestBody(new ColonyEndpoint.SpawnRequest(batchId, List.of(workerIds), false))
        .responseBodyAs(ColonyEndpoint.SpawnResult.class)
        .invoke()
        .body();
  }

  private ColonyEndpoint.ColonyView view(String colony) {
    return httpClient
        .GET("/colonies/" + colony)
        .responseBodyAs(ColonyEndpoint.ColonyView.class)
        .invoke()
        .body();
  }

  @Test
  public void drivesAColonyThroughAdmissionQueueingPromotionAndStop() {
    var colony = colony();
    httpClient
        .PUT("/colonies/" + colony + "/cap")
        .withRequestBody(new ColonyEndpoint.CapRequest(2))
        .invoke();

    var spawned = spawn(colony, "A", "w1", "w2", "w3", "w4");
    assertThat(spawned.admissions())
        .containsExactly(
            Admission.ADMITTED, Admission.ADMITTED, Admission.QUEUED, Admission.QUEUED);

    var queued = view(colony);
    assertThat(queued.queue()).containsExactly("w3", "w4");
    assertThat(queued.occupiedSlots()).isEqualTo(2);

    httpClient
        .POST("/colonies/" + colony + "/workers/w1/terminate")
        .withRequestBody(new ColonyEndpoint.TerminateRequest(WorkerStatus.SUCCEEDED, "done", 1.25))
        .invoke();

    var promoted = view(colony);
    assertThat(promoted.queue()).containsExactly("w4");
    assertThat(promoted.occupiedSlots()).isEqualTo(2);
    assertThat(promoted.reports()).hasSize(1);
    assertThat(promoted.reports().get(0).durationSeconds()).isEqualTo(1.25);

    httpClient.POST("/colonies/" + colony + "/workers/stop-all").invoke();

    var stopped = view(colony);
    assertThat(stopped.queue()).isEmpty();
    assertThat(stopped.occupiedSlots()).isZero();
    assertThat(stopped.dispatchBlocked()).isTrue();
    assertThat(stopped.reports()).hasSize(4);

    // Blocked: a spawn now is refused, and still reports.
    var refused = spawn(colony, "B", "w5");
    assertThat(refused.admissions()).containsExactly(Admission.REFUSED);
    assertThat(view(colony).reports()).hasSize(5);

    httpClient.POST("/colonies/" + colony + "/dispatch/resume").invoke();
    assertThat(spawn(colony, "C", "w6").admissions()).containsExactly(Admission.ADMITTED);
  }

  @Test
  public void refusesACapTheSchedulerCouldNotHonour() {
    var colony = colony();
    var response =
        httpClient
            .PUT("/colonies/" + colony + "/cap")
            .withRequestBody(new ColonyEndpoint.CapRequest(0))
            .invoke();
    assertThat(response.httpResponse().status().intValue()).isEqualTo(400);
  }
}
