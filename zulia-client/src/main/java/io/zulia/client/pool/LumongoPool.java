package io.zulia.client.pool;

import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;

import java.util.List;

public class LumongoPool {

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
					updateMembers();

				}
				catch (Throwable t) {

				}
			}
		}
	}

	private List<LMMember> members;
	private int retries;
	private int maxIdle;
	private int maxConnections;
	private boolean routingEnabled;
	private boolean isClosed;
	private int memberUpdateInterval;

	private GenericKeyedObjectPool<LMMember, ZuliaConnection> connectionPool;
	private IndexRouting indexRouting;

	public LumongoPool(final LumongoPoolConfig lumongoPoolConfig) throws Exception {
		members = lumongoPoolConfig.getMembers();
		retries = lumongoPoolConfig.getDefaultRetries();
		maxIdle = lumongoPoolConfig.getMaxIdle();
		maxConnections = lumongoPoolConfig.getMaxConnections();
		routingEnabled = lumongoPoolConfig.isRoutingEnabled();
		memberUpdateInterval = lumongoPoolConfig.getMemberUpdateInterval();
		if (memberUpdateInterval < 100) {
			//TODO think about cleaner ways to handle this
			throw new IllegalArgumentException("Member update interval is less than the minimum of 100");
		}

		KeyedPoolableObjectFactory<LMMember, ZuliaConnection> factory = new KeyedPoolableObjectFactory<LMMember, ZuliaConnection>() {

			@Override
			public ZuliaConnection makeObject(LMMember key) throws Exception {
				ZuliaConnection lc = new ZuliaConnection(key);
				lc.open(lumongoPoolConfig.isCompressedConnection());
				return lc;
			}

			@Override
			public void destroyObject(LMMember key, ZuliaConnection obj) throws Exception {
				obj.close();
			}

			@Override
			public boolean validateObject(LMMember key, ZuliaConnection obj) {
				return true;
			}

			@Override
			public void activateObject(LMMember key, ZuliaConnection obj) throws Exception {

			}

			@Override
			public void passivateObject(LMMember key, ZuliaConnection obj) throws Exception {

			}

		};
		GenericKeyedObjectPool.Config poolConfig = new GenericKeyedObjectPool.Config(); //
		poolConfig.maxIdle = maxIdle;
		poolConfig.maxActive = maxConnections;

		poolConfig.testOnBorrow = false;
		poolConfig.testOnReturn = false;

		connectionPool = new GenericKeyedObjectPool<>(factory, poolConfig);

		if (lumongoPoolConfig.isMemberUpdateEnabled()) {
			MembershipUpdateThread mut = new MembershipUpdateThread();
			mut.start();
		}
	}

	public int getMaxConnections() {
		return maxConnections;
	}

	public void updateMembers(List<LMMember> members) {
		//TODO handle cleaning up out of the pool?
		this.members = members;
	}

	public void updateIndexMappings(List<IndexMapping> list) {
		indexRouting = new IndexRouting(list);
	}

	public void updateMembers() throws Exception {
		GetMembersResult getMembersResult = execute(new GetMembers());
		updateMembers(getMembersResult.getMembers());
		updateIndexMappings(getMembersResult.getIndexMappings());
	}

	public <R extends Result> R execute(Command<R> command) throws Exception {

		int tries = 0;
		while (true) {
			ZuliaConnection zuliaConnection = null;
			LMMember selectedMember = null;
			try {
				boolean shouldRoute = (command instanceof RoutableCommand) && routingEnabled && (indexRouting != null);

				if (shouldRoute) {
					RoutableCommand rc = (RoutableCommand) command;
					selectedMember = indexRouting.getMember(rc.getIndexName(), rc.getUniqueId());
				}

				if (selectedMember == null) {
					List<LMMember> tempList = members; //stop array index out bounds on updates without locking
					int randomMemberIndex = (int) (Math.random() * tempList.size());
					selectedMember = tempList.get(randomMemberIndex);
				}

				zuliaConnection = connectionPool.borrowObject(selectedMember);

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

				connectionPool.returnObject(selectedMember, zuliaConnection);
				return r;
			}
			catch (Exception e) {

				if (selectedMember != null && zuliaConnection != null) {
					try {
						connectionPool.invalidateObject(selectedMember, zuliaConnection);
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
