package io.zulia.server.index.field;

import java.util.function.Consumer;

/**
 * One field's resolved values and the ways the indexing, sorting and faceting paths are allowed to read them. Each
 * consumer states which reading it needs, so no consumer has to infer it from the shape of the value.
 */
public sealed interface StoredFieldHandler permits MissingStoredFieldHandler, SingleValuedStoredFieldHandler, MultiValuedStoredFieldHandler {

	/**
	 * Duplicates removed, for facets and sorts. Built once, however, many representations ask for it.
	 */
	void onUniqueValues(Consumer<? super Object> action);

	/**
	 * Every occurrence in document order, for indexing.
	 */
	void onAllValues(Consumer<? super Object> action);

	/**
	 * How many values {@link #onAllValues} passes, which is the list length the indexer records. Zero is a real answer.
	 */
	int valueCount();

	/**
	 * Whether the field config has anything to give the consumers at all. A field can be present and still hold no
	 * values, when lenient handling dropped every one of them, and the malformed marker still has to be written.
	 */
	boolean isPresent();

	/**
	 * Whether the field-exists marker is earned, which a whole value default is not.
	 */
	boolean existsMarker();

	/**
	 * Whether any value failed to parse, which the malformed marker records however the failure was handled.
	 */
	boolean hasMalformedValues();

	boolean hasDefaultedValues();
}
