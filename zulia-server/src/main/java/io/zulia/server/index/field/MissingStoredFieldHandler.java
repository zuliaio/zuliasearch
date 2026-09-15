package io.zulia.server.index.field;

import java.util.function.Consumer;

/**
 * Nothing to index, sort or facet.
 */
public enum MissingStoredFieldHandler implements StoredFieldHandler {

	INSTANCE;

	@Override
	public void onUniqueValues(Consumer<? super Object> action) {
	}

	@Override
	public void onAllValues(Consumer<? super Object> action) {
	}

	@Override
	public int valueCount() {
		return 0;
	}

	@Override
	public boolean isPresent() {
		return false;
	}

	@Override
	public boolean existsMarker() {
		return false;
	}

	@Override
	public boolean hasMalformedValues() {
		return false;
	}

	@Override
	public boolean hasDefaultedValues() {
		return false;
	}
}
