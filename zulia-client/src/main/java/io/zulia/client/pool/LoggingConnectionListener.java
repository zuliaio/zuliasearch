package io.zulia.client.pool;

import io.zulia.message.ZuliaBase;

import java.util.logging.Logger;

public class LoggingConnectionListener implements ConnectionListener {
	private static final Logger LOG = Logger.getLogger(LoggingConnectionListener.class.getName());

	@Override
	public void connectionBeforeOpen(ZuliaConnection zuliaConnection) {
		ZuliaBase.Node node = zuliaConnection.getNode();
		LOG.info("Opening connection #" + zuliaConnection.getConnectionNumberForNode() + " to " + node.getServerAddress() + ":" + node.getServicePort()
				+ " connection id: " + zuliaConnection.getConnectionId());
	}

	@Override
	public void connectionOpened(ZuliaConnection zuliaConnection) {
		ZuliaBase.Node node = zuliaConnection.getNode();
		LOG.info("Opened connection #" + zuliaConnection.getConnectionNumberForNode() + " to " + node.getServerAddress() + ":" + node.getServicePort()
				+ " connection id: " + zuliaConnection.getConnectionId());
	}

	@Override
	public void connectionBeforeClose(ZuliaConnection zuliaConnection) {
		ZuliaBase.Node node = zuliaConnection.getNode();
		LOG.info("Closing connection #" + zuliaConnection.getConnectionNumberForNode() + " to <" + node.getServerAddress() + ":" + node.getServicePort()
				+ "> id: " + zuliaConnection.getConnectionId());
	}

	@Override
	public void connectionClosed(ZuliaConnection zuliaConnection) {
		ZuliaBase.Node node = zuliaConnection.getNode();
		LOG.info("Closed connection #" + zuliaConnection.getConnectionNumberForNode() + " to <" + node.getServerAddress() + ":" + node.getServicePort()
				+ "> id: " + zuliaConnection.getConnectionId());
	}
}
