package io.zulia.client.pool;

import io.zulia.client.command.base.BaseCommand;
import io.zulia.client.result.Result;
import io.zulia.message.ZuliaBase;

import java.util.logging.Level;
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

	@Override
	public void exceptionClosing(ZuliaConnection zuliaConnection, Exception e) {
		ZuliaBase.Node node = zuliaConnection.getNode();
		LOG.log(Level.SEVERE, "Exception closing connection #" + zuliaConnection.getConnectionNumberForNode() + " to <" + node.getServerAddress() + ":"
				+ node.getServicePort() + "> id: " + zuliaConnection.getConnectionId(), e);
	}

	@Override
	public void restClientCreated(String server, int restPort) {
		LOG.info("Created OkHttp client for server <" + server + "> on port <" + restPort + ">");
	}

	@Override
	public <R extends Result> void exceptionWithRetry(ZuliaBase.Node selectedNode, BaseCommand<R> command, Exception exception, int tries) {
		LOG.log(Level.SEVERE,
				"Failed to run " + command.getClass().getSimpleName() + " on " + selectedNode.getServerAddress() + ":" + selectedNode.getServicePort()
						+ " with exception: " + exception.getMessage() + ".  Retrying (" + tries + ")");
	}

	@Override
	public <R extends Result> void exception(ZuliaBase.Node selectedNode, BaseCommand<R> command, Exception exception) {
		LOG.log(Level.SEVERE,
				"Failed to run " + command.getClass().getSimpleName() + " on " + selectedNode.getServerAddress() + ":" + selectedNode.getServicePort()
						+ " with exception: " + exception.getMessage());
	}

}
