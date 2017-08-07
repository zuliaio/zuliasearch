package io.zulia.client.pool;

import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.zulia.cache.MetaKeys;
import io.zulia.client.command.GetNodes;
import io.zulia.client.command.base.Command;
import io.zulia.client.command.base.RoutableCommand;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.result.GetNodesResult;
import io.zulia.client.result.Result;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;

import java.util.List;

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

	private GenericKeyedObjectPool<Node, ZuliaConnection> connectionPool;
	private IndexRouting indexRouting;

	public ZuliaPool(final ZuliaPoolConfig zuliaPoolConfig) throws Exception {
		nodes = zuliaPoolConfig.getNodes();
		retries = zuliaPoolConfig.getDefaultRetries();
		maxIdle = zuliaPoolConfig.getMaxIdle();
		maxConnections = zuliaPoolConfig.getMaxConnections();
		routingEnabled = zuliaPoolConfig.isRoutingEnabled();
		memberUpdateInterval = zuliaPoolConfig.getMemberUpdateInterval();
		if (memberUpdateInterval < 100) {
			//TODO think about cleaner ways to handle this
			throw new IllegalArgumentException("Member update interval is less than the minimum of 100");
		}

		KeyedPoolableObjectFactory<Node, ZuliaConnection> factory = new KeyedPoolableObjectFactory<Node, ZuliaConnection>() {

			@Override
			public ZuliaConnection makeObject(Node key) throws Exception {
				ZuliaConnection lc = new ZuliaConnection(key);
				lc.open(zuliaPoolConfig.isCompressedConnection());
				return lc;
			}

			@Override
			public void destroyObject(Node key, ZuliaConnection obj) throws Exception {
				obj.close();
			}

			@Override
			public boolean validateObject(Node key, ZuliaConnection obj) {
				return true;
			}

			@Override
			public void activateObject(Node key, ZuliaConnection obj) throws Exception {

			}

			@Override
			public void passivateObject(Node key, ZuliaConnection obj) throws Exception {

			}

		};
		GenericKeyedObjectPool.Config poolConfig = new GenericKeyedObjectPool.Config(); //
		poolConfig.maxIdle = maxIdle;
		poolConfig.maxActive = maxConnections;

		poolConfig.testOnBorrow = false;
		poolConfig.testOnReturn = false;

		connectionPool = new GenericKeyedObjectPool<>(factory, poolConfig);

		if (zuliaPoolConfig.isMemberUpdateEnabled()) {
			MembershipUpdateThread mut = new MembershipUpdateThread();
			mut.start();
		}
	}

	public int getMaxConnections() {
		return maxConnections;
	}

	public void updateNodes(List<Node> members) {
		//TODO handle cleaning up out of the pool?
		this.nodes = members;
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

				zuliaConnection = connectionPool.borrowObject(selectedNode);

				R r;
				try {
					r = command.executeTimed(zuliaConnection);
				}
				catch (StatusRuntimeException e) {
					Metadata trailers = e.getTrailers();
					if (trailers.containsKey(MetaKeys.ERROR_KEY)) {
						throw new Exception(trailers.get(MetaKeys.ERROR_KEY));
					}
					else {
						throw e;
					}
				}

				connectionPool.returnObject(selectedNode, zuliaConnection);
				return r;
			}
			catch (Exception e) {

				if (selectedNode != null && zuliaConnection != null) {
					try {
						connectionPool.invalidateObject(selectedNode, zuliaConnection);
					}
					catch (Exception e1) {
					}
				}
				if (tries >= retries) {
					throw e;
				}
				tries++;
			}
		}

	}

	public void close() throws Exception {
		connectionPool.close();
		isClosed = true;
	}

}
