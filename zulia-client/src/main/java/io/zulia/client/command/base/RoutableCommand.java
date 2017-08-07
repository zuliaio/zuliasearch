package io.zulia.client.command.base;

public interface RoutableCommand {
	String getUniqueId();

	String getIndexName();
}
