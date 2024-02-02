package io.zulia.client.pool;

import io.zulia.client.command.base.BaseCommand;
import io.zulia.client.result.Result;
import io.zulia.message.ZuliaBase;

public class NoOpConnectionListener implements ConnectionListener {
	@Override
	public void connectionBeforeOpen(ZuliaConnection zuliaConnection) {

	}

	@Override
	public void connectionOpened(ZuliaConnection zuliaConnection) {

	}

	@Override
	public void connectionBeforeClose(ZuliaConnection zuliaConnection) {

	}

	@Override
	public void connectionClosed(ZuliaConnection zuliaConnection) {

	}

	@Override
	public void exceptionClosing(ZuliaConnection zuliaConnection, Exception e) {

	}

	@Override
	public void restClientCreated(String server, int restPort) {

	}

	@Override
	public <R extends Result> void exceptionWithRetry(ZuliaBase.Node selectedNode, BaseCommand<R> command, Exception e, int tries) {

	}

	@Override
	public <R extends Result> void exception(ZuliaBase.Node selectedNode, BaseCommand<R> command, Exception exception) {

	}

}
