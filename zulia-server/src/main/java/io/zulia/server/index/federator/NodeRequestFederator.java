package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.server.index.NodeRequestBase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
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

		// Join every subtask before reporting a failure. Abandoning still-running subtasks would let them
		// outlive the caller's index lease, so an eviction could unload the index under a live operation.
		ArrayList<O> results = new ArrayList<>();
		Exception firstFailure = null;
		for (Future<O> response : futureResponses) {
			try {
				results.add(response.get());
			}
			catch (InterruptedException e) {
				throw e;
			}
			catch (ExecutionException e) {
				Exception failure = e.getCause() instanceof Exception cause ? cause : e;
				if (firstFailure == null) {
					firstFailure = failure;
				}
				else if (firstFailure != failure) {
					firstFailure.addSuppressed(failure);
				}
			}
		}

		if (firstFailure != null) {
			throw firstFailure;
		}

		return results;

	}
}
