package io.zulia.client.pool;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.zulia.cache.MetaKeys;
import io.zulia.client.command.GetNodes;
import io.zulia.client.command.base.Command;
import io.zulia.client.command.base.RoutableCommand;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.result.GetNodesResult;
import io.zulia.client.result.Result;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.zulia.message.ZuliaBase.Node;
import static io.zulia.message.ZuliaIndex.IndexMapping;

public class ZuliaPool {

	protected class MembershipUpdateThread extends Thread {

		MembershipUpdateThread() {
			setDaemon(true);
			setName("LMMemberUpdateThread" + hashCode());
		}

		@Override
		public void run() {
			while (!isClosed) {
				try {
					try {
						Thread.sleep(memberUpdateInterval);
					}
					catch (InterruptedException e) {

					}
					updateNodes();

				}
				catch (Throwable t) {

				}
			}
		}
	}

	private List<Node> nodes;
	private int retries;
	private int maxIdle;
	private int maxConnections;
	private boolean routingEnabled;
	private boolean isClosed;
	private int memberUpdateInterval;
	private final boolean compressedConnection;

	private ConcurrentHashMap<String, GenericObjectPool<ZuliaConnection>> zuliaConnectionPoolMap;
	private IndexRouting indexRouting;

	public ZuliaPool(final ZuliaPoolConfig zuliaPoolConfig) throws Exception {
		nodes = zuliaPoolConfig.getNodes();
		retries = zuliaPoolConfig.getDefaultRetries();
		maxIdle = zuliaPoolConfig.getMaxIdle();
		maxConnections = zuliaPoolConfig.getMaxConnections();
		routingEnabled = zuliaPoolConfig.isRoutingEnabled();
		memberUpdateInterval = zuliaPoolConfig.getMemberUpdateInterval();
		compressedConnection = zuliaPoolConfig.isCompressedConnection();

		zuliaConnectionPoolMap = new ConcurrentHashMap<>();

		GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig(); //
		poolConfig.setMaxIdlePerKey(maxIdle);
		poolConfig.setMaxTotalPerKey(maxConnections);

		if (zuliaPoolConfig.isMemberUpdateEnabled()) {
			MembershipUpdateThread mut = new MembershipUpdateThread();
			mut.start();
		}
	}

	public int getMaxConnections() {
		return maxConnections;
	}

	public void updateNodes(List<Node> nodes) {

		Set<String> newKeys = new HashSet<>();
		for (Node node : nodes) {
			newKeys.add(getNodeKey(node));
		}

		Set<String> removedNodes = new HashSet<>();
		removedNodes.addAll(zuliaConnectionPoolMap.keySet());
		removedNodes.removeAll(newKeys);
		for (String removedNode : removedNodes) {
			GenericObjectPool<ZuliaConnection> remove = zuliaConnectionPoolMap.remove(removedNode);
			remove.close();
		}

		this.nodes = nodes;
	}

	private String getNodeKey(Node node) {
		return node.getServerAddress() + ":" + node.getServicePort();
	}

	public void updateIndexMappings(List<IndexMapping> list) {
		indexRouting = new IndexRouting(list);
	}

	public void updateNodes() throws Exception {
		GetNodesResult getNodesResult = execute(new GetNodes());
		updateNodes(getNodesResult.getNodes());
		updateIndexMappings(getNodesResult.getIndexMappings());
	}

	public <R extends Result> R execute(Command<R> command) throws Exception {

		int tries = 0;
		while (true) {
			ZuliaConnection zuliaConnection = null;
			Node selectedNode = null;
			try {
				boolean shouldRoute = (command instanceof RoutableCommand) && routingEnabled && (indexRouting != null);

				if (shouldRoute) {
					RoutableCommand rc = (RoutableCommand) command;
					selectedNode = indexRouting.getNode(rc.getIndexName(), rc.getUniqueId());
				}

				if (selectedNode == null) {
					List<Node> tempList = nodes; //stop array index out bounds on updates without locking
					int randomMemberIndex = (int) (Math.random() * tempList.size());
					selectedNode = tempList.get(randomMemberIndex);
				}

				final Node finalSelectedNode = selectedNode;
				GenericObjectPool<ZuliaConnection> nodePool = zuliaConnectionPoolMap.computeIfAbsent(getNodeKey(selectedNode),
						(String key) -> new GenericObjectPool<>(new ZuliaConnectionFactory(finalSelectedNode, compressedConnection)));

				zuliaConnection = nodePool.borrowObject();

				R r;
				try {
					r = command.executeTimed(zuliaConnection);
					nodePool.returnObject(zuliaConnection);
					return r;
				}
				catch (StatusRuntimeException e) {

					if (!Status.INVALID_ARGUMENT.equals(e.getStatus())) {
						nodePool.invalidateObject(zuliaConnection);
					}

					Metadata trailers = e.getTrailers();
					if (trailers.containsKey(MetaKeys.ERROR_KEY)) {
						String errorMessage = trailers.get(MetaKeys.ERROR_KEY);
						if (!Status.INVALID_ARGUMENT.equals(e.getStatus())) {
							throw new Exception(command.getClass().getSimpleName() + ":" + errorMessage);
						}
						else {
							throw new IllegalArgumentException(command.getClass().getSimpleName() + ":" + errorMessage);
						}
					}
					else {
						throw e;
					}
				}

			}
			catch (Exception e) {
				if (tries >= retries) {
					throw e;
				}
				tries++;
			}
		}

	}

	public void close() throws Exception {
		for (GenericObjectPool<ZuliaConnection> pool : zuliaConnectionPoolMap.values()) {
			pool.close();
		}
		isClosed = true;
	}

}
