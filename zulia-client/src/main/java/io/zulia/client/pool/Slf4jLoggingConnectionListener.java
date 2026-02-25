package io.zulia.client.pool;

import io.zulia.client.command.base.BaseCommand;
import io.zulia.client.result.Result;
import io.zulia.message.ZuliaBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Slf4jLoggingConnectionListener implements ConnectionListener {
	private static final Logger LOG = LoggerFactory.getLogger(Slf4jLoggingConnectionListener.class);

	@Override
	public void connectionBeforeOpen(ZuliaConnection zuliaConnection) {
		ZuliaBase.Node node = zuliaConnection.getNode();
		LOG.info("Opening connection to {}:{}", node.getServerAddress(), node.getServicePort());
	}

	@Override
	public void connectionOpened(ZuliaConnection zuliaConnection) {
		ZuliaBase.Node node = zuliaConnection.getNode();
		LOG.info("Opened connection to {}:{}", node.getServerAddress(), node.getServicePort());
	}

	@Override
	public void connectionBeforeClose(ZuliaConnection zuliaConnection) {
		ZuliaBase.Node node = zuliaConnection.getNode();
		LOG.info("Closing connection to {}:{}", node.getServerAddress(), node.getServicePort());
	}

	@Override
	public void connectionClosed(ZuliaConnection zuliaConnection) {
		ZuliaBase.Node node = zuliaConnection.getNode();
		LOG.info("Closed connection to {}:{}", node.getServerAddress(), node.getServicePort());
	}

	@Override
	public void exceptionClosing(ZuliaConnection zuliaConnection, Exception e) {
		ZuliaBase.Node node = zuliaConnection.getNode();
		LOG.error("Exception closing connection to {}:{}", node.getServerAddress(), node.getServicePort(), e);
	}

	@Override
	public void restClientCreated(String server, int restPort) {
		LOG.info("Created OkHttp client for server {}:{}", server, restPort);
	}

	@Override
	public <R extends Result> void exceptionWithRetry(ZuliaBase.Node selectedNode, BaseCommand<R> command, Exception exception, int tries) {
		LOG.error("Failed to run {} on {}:{} with exception: {}.  Retrying ({})", command.getClass().getSimpleName(), selectedNode.getServerAddress(),
				selectedNode.getServicePort(), exception.getMessage(), tries);
	}

	@Override
	public <R extends Result> void exception(ZuliaBase.Node selectedNode, BaseCommand<R> command, Exception exception) {
		LOG.error("Failed to run {} on {}:{} with exception: {}", command.getClass().getSimpleName(), selectedNode.getServerAddress(),
				selectedNode.getServicePort(), exception.getMessage());
	}

}
