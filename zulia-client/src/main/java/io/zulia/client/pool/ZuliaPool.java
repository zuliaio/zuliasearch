package io.zulia.client.pool;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.zulia.cache.MetaKeys;
import io.zulia.client.command.GetNodes;
import io.zulia.client.command.base.Command;
import io.zulia.client.command.base.MultiIndexRoutableCommand;
import io.zulia.client.command.base.ShardRoutableCommand;
import io.zulia.client.command.base.SingleIndexRoutableCommand;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.result.GetNodesResult;
import io.zulia.client.result.Result;
import io.zulia.message.ZuliaIndex;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.zulia.message.ZuliaBase.Node;
import static io.zulia.message.ZuliaIndex.IndexMapping;

public class ZuliaPool {

	protected class ZuliaNodeUpdateThread extends Thread {

		ZuliaNodeUpdateThread() {
			setDaemon(true);
			setName("ZuliaNodeUpdateThread" + hashCode());
		}

		@Override
		public void run() {
			while (!isClosed) {
				try {
					try {
						Thread.sleep(nodeUpdateInterval);
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
	private int nodeUpdateInterval;
	private final boolean compressedConnection;

	private ConcurrentHashMap<String, GenericObjectPool<ZuliaConnection>> zuliaConnectionPoolMap;
	private IndexRouting indexRouting;

	public ZuliaPool(final ZuliaPoolConfig zuliaPoolConfig) throws Exception {
		nodes = zuliaPoolConfig.getNodes();
		retries = zuliaPoolConfig.getDefaultRetries();
		maxIdle = zuliaPoolConfig.getMaxIdle();
		maxConnections = zuliaPoolConfig.getMaxConnections();
		routingEnabled = zuliaPoolConfig.isRoutingEnabled();
		nodeUpdateInterval = zuliaPoolConfig.getNodeUpdateInterval();
		compressedConnection = zuliaPoolConfig.isCompressedConnection();

		zuliaConnectionPoolMap = new ConcurrentHashMap<>();

		if (zuliaPoolConfig.isNodeUpdateEnabled()) {
			ZuliaNodeUpdateThread mut = new ZuliaNodeUpdateThread();
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
			System.err.println("Removing not active node: " + removedNode);
			GenericObjectPool<ZuliaConnection> remove = zuliaConnectionPoolMap.remove(removedNode);
			remove.close();
		}

		this.nodes = nodes;
	}

	private String getNodeKey(Node node) {
		return node.getServerAddress() + ":" + node.getServicePort();
	}

	public void updateIndexMappings(List<IndexMapping> indexMappings, List<ZuliaIndex.IndexAlias> indexAliases) {
		indexRouting = new IndexRouting(indexMappings, indexAliases);
	}

	public void updateNodes() throws Exception {
		GetNodesResult getNodesResult = execute(new GetNodes().setActiveOnly(true));
		updateNodes(getNodesResult.getNodes());
		updateIndexMappings(getNodesResult.getIndexMappings(), getNodesResult.getIndexAliases());
	}

	public <R extends Result> R execute(Command<R> command) throws Exception {

		int tries = 0;
		while (true) {
			ZuliaConnection zuliaConnection;
			Node selectedNode = null;
			try {

				if (routingEnabled && (indexRouting != null)) {
					if (command instanceof ShardRoutableCommand) {
						ShardRoutableCommand rc = (ShardRoutableCommand) command;
						selectedNode = indexRouting.getNode(rc.getIndexName(), rc.getUniqueId());
					}
					else if (command instanceof SingleIndexRoutableCommand) {
						SingleIndexRoutableCommand sirc = (SingleIndexRoutableCommand) command;
						selectedNode = indexRouting.getRandomNode(sirc.getIndexName());
					}
					else if (command instanceof MultiIndexRoutableCommand) {
						MultiIndexRoutableCommand mirc = (MultiIndexRoutableCommand) command;
						selectedNode = indexRouting.getRandomNode(mirc.getIndexNames());
					}
				}

				if (selectedNode == null) {
					List<Node> tempList = nodes; //stop array index out bounds on updates without locking

					if (tempList.isEmpty()) {
						throw new IOException("There are no active nodes");
					}

					int randomNodeIndex = (int) (Math.random() * tempList.size());
					selectedNode = tempList.get(randomNodeIndex);
				}

				final Node finalSelectedNode = selectedNode;
				GenericObjectPool<ZuliaConnection> nodePool = zuliaConnectionPoolMap.computeIfAbsent(getNodeKey(selectedNode), (String key) -> {
					GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig(); //
					poolConfig.setMaxIdle(maxIdle);
					poolConfig.setMaxTotal(maxConnections);
					return new GenericObjectPool<>(new ZuliaConnectionFactory(finalSelectedNode, compressedConnection), poolConfig);
				});

				boolean valid = true;

				zuliaConnection = nodePool.borrowObject();
				R r;
				try {
					r = command.executeTimed(zuliaConnection);
					return r;
				}
				catch (StatusRuntimeException e) {

					if (!Status.INVALID_ARGUMENT.equals(e.getStatus())) {
						valid = false;
					}

					Metadata trailers = e.getTrailers();
					if (trailers.containsKey(MetaKeys.ERROR_KEY)) {
						String errorMessage = trailers.get(MetaKeys.ERROR_KEY);
						if (!Status.INVALID_ARGUMENT.equals(e.getStatus())) {
							throw new Exception(command.getClass().getSimpleName() + ": " + errorMessage);
						}
						else {
							throw new IllegalArgumentException(command.getClass().getSimpleName() + ":" + errorMessage);
						}
					}
					else {
						throw e;
					}
				}
				finally {
					if (valid) {
						nodePool.returnObject(zuliaConnection);
					}
					else {
						nodePool.invalidateObject(zuliaConnection);
					}
				}

			}
			catch (Exception e) {
				System.err.println(e.getClass().getSimpleName() + ":" + e.getMessage());
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
