package io.zulia.client.command.base;

public interface ShardRoutableCommand {
	String getUniqueId();

	String getIndexName();
}
