package io.zulia.server.index.replication;

import io.micronaut.serde.annotation.Serdeable;

import java.util.List;

@Serdeable
public record ReplicationShardState(int shardNumber, Long lastAttemptedGeneration, List<ReplicaReplicationState> replicas) {
}
