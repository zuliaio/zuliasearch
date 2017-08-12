package io.zulia.server.index;

import io.zulia.message.ZuliaBase.MasterSlaveSettings;
import io.zulia.message.ZuliaBase.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public abstract class RequestNodeFederator<I, O> extends RequestNodeBase<I, O> {

	private final HashSet<Node> nodes;
	private final ExecutorService pool;

	public RequestNodeFederator(Node thisNode, Collection<Node> otherNodesActive, MasterSlaveSettings masterSlaveSettings, ZuliaIndex index,
			ExecutorService pool) throws IOException {
		this(thisNode, otherNodesActive, masterSlaveSettings, Collections.singleton(index), pool);
	}

	public RequestNodeFederator(Node thisNode, Collection<Node> otherNodesActive, MasterSlaveSettings masterSlaveSettings, Collection<ZuliaIndex> indexes,
			ExecutorService pool) throws IOException {
		super(thisNode, otherNodesActive, masterSlaveSettings);

		this.nodes = new HashSet<>();
		this.pool = pool;

		for (ZuliaIndex index : indexes) {
			io.zulia.message.ZuliaIndex.IndexMapping indexMapping = index.getIndexMapping();

			for (int s = 0; s < index.getNumberOfShards(); s++) {
				for (io.zulia.message.ZuliaIndex.ShardMapping shardMapping : indexMapping.getShardMappingList()) {
					Node selectedNode = getNodeFromShardMapping(index.getIndexName(), shardMapping);
					nodes.add(selectedNode);
				}
			}
		}

	}

	public List<O> send(final I request) throws Exception {

		List<Future<O>> futureResponses = new ArrayList<>();

		for (final Node node : nodes) {

			Future<O> futureResponse = pool.submit(() -> {
				if (nodeIsLocal(node)) {
					return processInternal(request);
				}
				return processExternal(node, request);

			});

			futureResponses.add(futureResponse);
		}

		ArrayList<O> results = new ArrayList<>();
		for (Future<O> response : futureResponses) {
			try {
				O result = response.get();
				results.add(result);
			}
			catch (InterruptedException e) {
				throw e;
			}
			catch (Exception e) {
				Throwable cause = e.getCause();
				if (cause instanceof Exception) {
					throw (Exception) cause;
				}

				throw e;
			}
		}

		return results;

	}
}
