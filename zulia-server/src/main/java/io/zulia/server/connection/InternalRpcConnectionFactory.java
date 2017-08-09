package io.zulia.server.connection;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class InternalRpcConnectionFactory extends BasePooledObjectFactory<InternalRpcConnection> {

	private String memberAddress;
	private int internalServicePort;

	public InternalRpcConnectionFactory(String memberAddress, int internalServicePort) {
		this.memberAddress = memberAddress;
		this.internalServicePort = internalServicePort;
	}

	@Override
	public InternalRpcConnection create() throws Exception {
		return new InternalRpcConnection(memberAddress, internalServicePort);
	}

	@Override
	public PooledObject<InternalRpcConnection> wrap(InternalRpcConnection obj) {
		return new DefaultPooledObject<>(obj);
	}

}