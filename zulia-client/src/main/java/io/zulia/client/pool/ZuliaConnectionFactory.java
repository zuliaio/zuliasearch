package io.zulia.client.pool;

import io.zulia.message.ZuliaBase.Node;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.util.concurrent.atomic.AtomicLong;

public class ZuliaConnectionFactory extends BasePooledObjectFactory<ZuliaConnection> {

	private final boolean compressedConnection;
	private final Node node;
	private final ConnectionListener connectionListener;

	private static final AtomicLong connectionId = new AtomicLong();

	private final AtomicLong connectionForNodeGen;

	public ZuliaConnectionFactory(Node node, boolean compressedConnection, ConnectionListener connectionListener) {
		this.compressedConnection = compressedConnection;
		this.node = node;
		this.connectionListener = connectionListener;
		this.connectionForNodeGen = new AtomicLong();
	}

	@Override
	public ZuliaConnection create() {
		ZuliaConnection zuliaConnection = new ZuliaConnection(node, compressedConnection, connectionId.getAndIncrement(),
				connectionForNodeGen.getAndIncrement());
		if (connectionListener != null) {
			connectionListener.connectionBeforeOpen(zuliaConnection);
		}
		zuliaConnection.open();
		if (connectionListener != null) {
			connectionListener.connectionOpened(zuliaConnection);
		}
		return zuliaConnection;
	}

	@Override
	public PooledObject<ZuliaConnection> wrap(ZuliaConnection obj) {
		return new DefaultPooledObject<>(obj);
	}

	@Override
	public void destroyObject(PooledObject<ZuliaConnection> p) {
		ZuliaConnection zuliaConnection = p.getObject();
		if (connectionListener != null) {
			connectionListener.connectionBeforeClose(zuliaConnection);
		}
		try {
			zuliaConnection.close();
		}
		catch (Exception e) {
			if (connectionListener != null) {
				connectionListener.exceptionClosing(zuliaConnection, e);
			}
		}
		if (connectionListener != null) {
			connectionListener.connectionClosed(zuliaConnection);
		}
	}
}
