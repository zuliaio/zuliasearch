package io.zulia.server.index.replication;

import io.zulia.message.ZuliaBase.Node;
import org.apache.lucene.store.Directory;

import java.util.Collection;

public record PublishRequest(String indexName, int shardNumber, Node replicaNode, Directory sourceDirectory, Collection<String> sourceFiles, long generation,
		boolean taxonomy) {
}
