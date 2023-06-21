package io.zulia.client.pool;

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
}
