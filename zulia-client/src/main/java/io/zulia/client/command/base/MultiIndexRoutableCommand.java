package io.zulia.client.command.base;

import java.util.Collection;

public interface MultiIndexRoutableCommand {

	Collection<String> getIndexNames();
}
