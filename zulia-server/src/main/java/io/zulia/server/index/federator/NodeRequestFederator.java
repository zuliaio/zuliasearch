package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.server.index.NodeRequestBase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public abstract class NodeRequestFederator<I, O> extends NodeRequestBase<I, O> {

	private final ExecutorService pool;

	protected final Set<Node> nodes;

	public NodeRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ExecutorService pool) {
		super(thisNode, otherNodesActive);
		this.pool = pool;
		this.nodes = new HashSet<>();
	}

	public List<O> send(final I request) throws Exception {

		List<Future<O>> futureResponses = new ArrayList<>();

		for (final Node node : nodes) {

			Future<O> futureResponse = pool.submit(() -> {
				if (nodeIsLocal(node)) {
					return processInternal(node, request);
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
