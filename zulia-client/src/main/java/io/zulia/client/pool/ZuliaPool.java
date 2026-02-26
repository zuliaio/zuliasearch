package io.zulia.client.pool;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.zulia.ZuliaRESTConstants;
import io.zulia.cache.MetaKeys;
import io.zulia.client.command.GetNodes;
import io.zulia.client.command.base.BaseCommand;
import io.zulia.client.command.base.GrpcCommand;
import io.zulia.client.command.base.MultiIndexRoutableCommand;
import io.zulia.client.command.base.RESTCommand;
import io.zulia.client.command.base.ShardRoutableCommand;
import io.zulia.client.command.base.SingleIndexRoutableCommand;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.rest.ZuliaRESTClient;
import io.zulia.client.result.GetNodesResult;
import io.zulia.client.result.Result;
import io.zulia.message.ZuliaIndex;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.zulia.message.ZuliaBase.Node;
import static io.zulia.message.ZuliaIndex.IndexShardMapping;

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
					catch (InterruptedException ignored) {

					}
					if (!isClosed) {
						updateNodesAndRouting();
					}

				}
				catch (Throwable ignored) {

				}
			}
		}
	}

	private final int retries;
	private final boolean routingEnabled;
	private final int nodeUpdateInterval;
	private final boolean compressedConnection;
	private final ConcurrentHashMap<String, ZuliaConnection> zuliaConnectionMap;
	private final ConcurrentHashMap<String, ZuliaRESTClient> zuliaRestPoolMap;
	private final ConcurrentHashMap<String, Node> nodeKeyToNode;
	private final ConnectionListener connectionListener;
	private boolean isClosed;
	private List<Node> nodes;
	private IndexRouting indexRouting;

	public ZuliaPool(final ZuliaPoolConfig zuliaPoolConfig) {
		nodes = zuliaPoolConfig.getNodes();
		retries = zuliaPoolConfig.getDefaultRetries();
		routingEnabled = zuliaPoolConfig.isRoutingEnabled();
		nodeUpdateInterval = zuliaPoolConfig.getNodeUpdateInterval();
		compressedConnection = zuliaPoolConfig.isCompressedConnection();

		connectionListener = zuliaPoolConfig.getConnectionListener();

		zuliaConnectionMap = new ConcurrentHashMap<>();
		zuliaRestPoolMap = new ConcurrentHashMap<>();
		nodeKeyToNode = new ConcurrentHashMap<>();

		for (Node node : nodes) {
			nodeKeyToNode.put(getNodeKey(node), node);
		}

		if (zuliaPoolConfig.isNodeUpdateEnabled()) {
			ZuliaNodeUpdateThread mut = new ZuliaNodeUpdateThread();
			mut.start();
		}
	}

	public void updateNodes(List<Node> nodes) {

		Set<String> newKeys = new HashSet<>();
		for (Node node : nodes) {
			String nodeKey = getNodeKey(node);
			newKeys.add(nodeKey);
			nodeKeyToNode.put(nodeKey, node);
		}

		Set<String> removedNodes = new HashSet<>(zuliaConnectionMap.keySet());
		removedNodes.addAll(zuliaRestPoolMap.keySet());
		removedNodes.removeAll(newKeys);
		for (String removedNode : removedNodes) {
			ZuliaConnection connection = zuliaConnectionMap.remove(removedNode);
			if (connection != null) {
				closeConnection(connection);
			}
			ZuliaRESTClient restClient = zuliaRestPoolMap.remove(removedNode);
			if (restClient != null) {
				restClient.close();
			}
		}

		this.nodes = nodes;
	}

	private String getNodeKey(Node node) {
		return node.getServerAddress() + ":" + node.getServicePort();
	}

	public void updateIndexMappings(List<IndexShardMapping> indexShardMappings, List<ZuliaIndex.IndexAlias> indexAliases) {
		indexRouting = new IndexRouting(indexShardMappings, indexAliases);
	}

	public void updateNodesAndRouting() throws Exception {
		GetNodesResult getNodesResult = execute(new GetNodes().setActiveOnly(true));
		updateNodes(getNodesResult.getNodes());
		updateIndexMappings(getNodesResult.getIndexShardMappings(), getNodesResult.getIndexAliases());
	}

	public <R extends Result> R execute(BaseCommand<R> command) throws Exception {

		int tries = 0;
		while (true) {
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
					int restPort = selectedNode.getRestPort();

					if (selectedNode.getRestPort() == 0) {
						Node fullSelectedNode = nodeKeyToNode.get(nodeKey);
						if (fullSelectedNode != null) {
							restPort = fullSelectedNode.getRestPort();
						}
						else {
							restPort = ZuliaRESTConstants.DEFAULT_REST_SERVICE_PORT;
						}
					}

					int finalRestPort = restPort;
					final String finalServer = selectedNode.getServerAddress();
					ZuliaRESTClient restClient = zuliaRestPoolMap.computeIfAbsent(nodeKey, s -> new ZuliaRESTClient(finalServer, finalRestPort));
					return ((RESTCommand<R>) command).execute(restClient);
				}
				else {
					GrpcCommand<R> grpcCommand = (GrpcCommand<R>) command;
					final Node finalSelectedNode = selectedNode;
					ZuliaConnection zuliaConnection = zuliaConnectionMap.computeIfAbsent(nodeKey, (String key) -> {
						ZuliaConnection conn = new ZuliaConnection(finalSelectedNode, compressedConnection);
						if (connectionListener != null) {
							connectionListener.connectionBeforeOpen(conn);
						}
						conn.open();
						if (connectionListener != null) {
							connectionListener.connectionOpened(conn);
						}
						return conn;
					});

					try {
						return grpcCommand.executeTimed(zuliaConnection);
					}
					catch (StatusRuntimeException e) {

						Metadata trailers = e.getTrailers();
						if (trailers != null && trailers.containsKey(MetaKeys.ERROR_KEY)) {
							String errorMessage = trailers.get(MetaKeys.ERROR_KEY);
							if (!Status.INVALID_ARGUMENT.equals(e.getStatus())) {
								throw new Exception(grpcCommand.getClass().getSimpleName() + ": " + errorMessage);
							}
							else {
								throw new IllegalArgumentException(grpcCommand.getClass().getSimpleName() + ": " + errorMessage);
							}
						}
						else {
							throw e;
						}
					}
				}

			}
			catch (Exception e) {
				if (tries >= retries) {
					connectionListener.exception(selectedNode, command, e);
					throw e;
				}
				tries++;
				connectionListener.exceptionWithRetry(selectedNode, command, e, tries);
			}
		}

	}

	private void closeConnection(ZuliaConnection connection) {
		try {
			if (connectionListener != null) {
				connectionListener.connectionBeforeClose(connection);
			}
			connection.close();
			if (connectionListener != null) {
				connectionListener.connectionClosed(connection);
			}
		}
		catch (Exception e) {
			if (connectionListener != null) {
				connectionListener.exceptionClosing(connection, e);
			}
		}
	}

	public void close() {
		for (ZuliaConnection connection : zuliaConnectionMap.values()) {
			closeConnection(connection);
		}

		for (ZuliaRESTClient restClient : zuliaRestPoolMap.values()) {
			restClient.close();
		}

		isClosed = true;
	}

}
