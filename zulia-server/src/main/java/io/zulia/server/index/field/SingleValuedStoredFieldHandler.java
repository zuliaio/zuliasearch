package io.zulia.server.index.field;

import java.util.function.Consumer;

/**
 * One value, so every reading is the same reading and deduplication cannot change anything.
 */
public final class SingleValuedStoredFieldHandler implements StoredFieldHandler {

	private final Object value;
	private final boolean existsMarker;
	private final boolean malformed;
	private final boolean defaulted;
	private boolean representationSkipped;

	/**
	 * The resolver never produces a null value, so no consumer here has to check for one.
	 */
	SingleValuedStoredFieldHandler(Object value, boolean existsMarker, boolean malformed, boolean defaulted) {
		this.value = value;
		this.existsMarker = existsMarker;
		this.malformed = malformed;
		this.defaulted = defaulted;
	}

	@Override
	public void onUniqueValues(Consumer<? super Object> action) {
		action.accept(value);
	}

	@Override
	public void onAllValues(Consumer<? super Object> action) {
		action.accept(value);
	}

	@Override
	public int valueCount() {
		return 1;
	}

	@Override
	public boolean isPresent() {
		return true;
	}

	@Override
	public boolean existsMarker() {
		return existsMarker;
	}

	@Override
	public boolean hasMalformedValues() {
		return malformed;
	}

	@Override
	public boolean hasDefaultedValues() {
		return defaulted;
	}

	@Override
	public void skipRepresentation() {
		representationSkipped = true;
	}

	@Override
	public boolean representationSkipped() {
		return representationSkipped;
	}
}
