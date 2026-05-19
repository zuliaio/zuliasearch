package io.zulia.server.index.replication;

public record ReplicaFileInfo(String name, long length, String checksum) {
}
