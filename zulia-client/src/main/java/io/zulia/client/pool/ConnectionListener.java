package io.zulia.client.pool;

import io.zulia.client.command.base.BaseCommand;
import io.zulia.client.result.Result;
import io.zulia.message.ZuliaBase;

public interface ConnectionListener {

	void connectionBeforeOpen(ZuliaConnection zuliaConnection);

	void connectionOpened(ZuliaConnection zuliaConnection);

	void connectionBeforeClose(ZuliaConnection zuliaConnection);

	void connectionClosed(ZuliaConnection zuliaConnection);

	void exceptionClosing(ZuliaConnection zuliaConnection, Exception e);

	void restClientCreated(String server, int restPort);

	<R extends Result> void exceptionWithRetry(ZuliaBase.Node selectedNode, BaseCommand<R> command, Exception e, int tries);

	<R extends Result> void exception(ZuliaBase.Node selectedNode, BaseCommand<R> command, Exception exception);
}
