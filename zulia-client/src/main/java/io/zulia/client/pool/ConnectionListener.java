package io.zulia.client.pool;

public interface ConnectionListener {

	void connectionBeforeOpen(ZuliaConnection zuliaConnection);

	void connectionOpened(ZuliaConnection zuliaConnection);

	void connectionBeforeClose(ZuliaConnection zuliaConnection);

	void connectionClosed(ZuliaConnection zuliaConnection);
}
