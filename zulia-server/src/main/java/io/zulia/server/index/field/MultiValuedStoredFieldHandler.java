package io.zulia.server.index.field;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Consumer;

/**
 * The values of a field the resolver did not reduce to exactly one, which includes none at all when lenient handling
 * dropped them. The deduplicated view is built at most once, however many facets and sorts ask for it. One document's
 * one field, so the lazy cache is never shared across threads.
 */
public final class MultiValuedStoredFieldHandler implements StoredFieldHandler {

	private final List<Object> values;
	private final boolean existsMarker;
	private final boolean malformed;
	private final boolean defaulted;
	private Collection<Object> unique;

	/**
	 * The resolver hands over a flat list holding no nulls, which is what lets the readings below skip both checks.
	 */
	MultiValuedStoredFieldHandler(List<Object> values, boolean existsMarker, boolean malformed, boolean defaulted) {
		this.values = values;
		this.existsMarker = existsMarker;
		this.malformed = malformed;
		this.defaulted = defaulted;
	}

	@Override
	public void onUniqueValues(Consumer<? super Object> action) {
		if (unique == null) {
			// a sized copy rather than another walk, because the values are already flat
			unique = new LinkedHashSet<>(values);
		}
		unique.forEach(action);
	}

	@Override
	public void onAllValues(Consumer<? super Object> action) {
		values.forEach(action);
	}

	@Override
	public int valueCount() {
		return values.size();
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
}
