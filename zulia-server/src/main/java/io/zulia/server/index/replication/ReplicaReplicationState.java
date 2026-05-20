package io.zulia.server.index.replication;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record ReplicaReplicationState(String serverAddress, int servicePort, Long lastReplicatedGeneration, Long lagGenerations, Long lastSuccessMs,
		Long msSinceLastSuccess, int consecutiveFailures, boolean circuitOpen, long backoffUntilMs) {
}
