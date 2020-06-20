package io.zulia.client.pool;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.zulia.cache.MetaKeys;
import io.zulia.client.ZuliaRESTClient;
import io.zulia.client.command.GetNodes;
import io.zulia.client.command.base.BaseCommand;
import io.zulia.client.command.base.GrpcCommand;
import io.zulia.client.command.base.MultiIndexRoutableCommand;
import io.zulia.client.command.base.RESTCommand;
import io.zulia.client.command.base.ShardRoutableCommand;
import io.zulia.client.command.base.SingleIndexRoutableCommand;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.result.GetNodesResult;
import io.zulia.client.result.Result;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.zulia.message.ZuliaBase.Node;
import static io.zulia.message.ZuliaIndex.IndexMapping;

public class ZuliaPool {

	private static final Logger LOG = Logger.getLogger(ZuliaPool.class.getName());

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
					catch (InterruptedException ignored) {

					}
					updateNodes();

				}
				catch (Throwable ignored) {

				}
			}
		}
	}

	private final int retries;
	private final int maxIdle;
	private final int maxConnections;
	private final boolean routingEnabled;
	private final int nodeUpdateInterval;
	private final boolean compressedConnection;
	private final ConcurrentHashMap<String, GenericObjectPool<ZuliaConnection>> zuliaConnectionPoolMap;
	private final ConcurrentHashMap<String, ZuliaRESTClient> zuliaRestPoolMap;

	private boolean isClosed;
	private List<Node> nodes;
	private IndexRouting indexRouting;

	public ZuliaPool(final ZuliaPoolConfig zuliaPoolConfig) {
		nodes = zuliaPoolConfig.getNodes();
		retries = zuliaPoolConfig.getDefaultRetries();
		maxIdle = zuliaPoolConfig.getMaxIdle();
		maxConnections = zuliaPoolConfig.getMaxConnections();
		routingEnabled = zuliaPoolConfig.isRoutingEnabled();
		nodeUpdateInterval = zuliaPoolConfig.getNodeUpdateInterval();
		compressedConnection = zuliaPoolConfig.isCompressedConnection();

		zuliaConnectionPoolMap = new ConcurrentHashMap<>();
		zuliaRestPoolMap = new ConcurrentHashMap<>();

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

		Set<String> removedNodes = new HashSet<>(zuliaConnectionPoolMap.keySet());
		removedNodes.addAll(zuliaRestPoolMap.keySet());
		removedNodes.removeAll(newKeys);
		for (String removedNode : removedNodes) {
			try {

				GenericObjectPool<ZuliaConnection> connection = zuliaConnectionPoolMap.remove(removedNode);
				if (connection != null) {
					connection.close();
				}

				ZuliaRESTClient restClient = zuliaRestPoolMap.remove(removedNode);
				if (restClient != null) {
					restClient.close();
				}

			}
			catch (Exception e) {
				LOG.log(Level.SEVERE, "Failed to remove node " + removedNode, e);
			}
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
		GetNodesResult getNodesResult = execute(new GetNodes().setActiveOnly(true));
		updateNodes(getNodesResult.getNodes());
		updateIndexMappings(getNodesResult.getIndexMappings());
	}

	public <R extends Result> R execute(BaseCommand<R> command) throws Exception {

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

				String nodeKey = getNodeKey(selectedNode);
				if (command instanceof RESTCommand) {
					Node finalSelectedNode = selectedNode;
					ZuliaRESTClient restClient = zuliaRestPoolMap
							.computeIfAbsent(nodeKey, s -> new ZuliaRESTClient(finalSelectedNode.getServerAddress(), finalSelectedNode.getRestPort()));
					return ((RESTCommand<R>) command).execute(restClient);
				}
				else {
					GrpcCommand<R> grpcCommand = (GrpcCommand<R>) command;
					final Node finalSelectedNode = selectedNode;
					GenericObjectPool<ZuliaConnection> nodePool = zuliaConnectionPoolMap.computeIfAbsent(nodeKey, (String key) -> {
						GenericObjectPoolConfig<ZuliaConnection> poolConfig = new GenericObjectPoolConfig<>(); //
						poolConfig.setMaxIdle(maxIdle);
						poolConfig.setMaxTotal(maxConnections);
						return new GenericObjectPool<>(new ZuliaConnectionFactory(finalSelectedNode, compressedConnection), poolConfig);
					});

					boolean valid = true;

					zuliaConnection = nodePool.borrowObject();
					R r;
					try {
						r = grpcCommand.executeTimed(zuliaConnection);
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
								throw new Exception(grpcCommand.getClass().getSimpleName() + ": " + errorMessage);
							}
							else {
								throw new IllegalArgumentException(grpcCommand.getClass().getSimpleName() + ":" + errorMessage);
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

	public void close() {
		for (GenericObjectPool<ZuliaConnection> pool : zuliaConnectionPoolMap.values()) {
			pool.close();
		}
		for (ZuliaRESTClient pool : zuliaRestPoolMap.values()) {
			pool.close();
		}
		isClosed = true;
	}

}
