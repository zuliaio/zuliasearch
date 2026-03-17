package com.datadoghq.sketch.ddsketch.store;

/**
 * Factory for creating {@link UnboundedSizeDenseStore} instances with a pre-allocated index range.
 * <p>
 * Lives in the {@code com.datadoghq.sketch.ddsketch.store} package to access the package-private
 * {@code extendRange(int, int)} method, which allocates the internal {@code double[]} array
 * to cover the full range in a single allocation. Without this, bins added one-by-one each
 * trigger {@code normalize → extendRange → Arrays.copyOf}, causing repeated array copies.
 */
public class PreSizedUnboundedSizeDenseStore {

	private PreSizedUnboundedSizeDenseStore() {
	}

	/**
	 * Creates an {@link UnboundedSizeDenseStore} with the internal array pre-allocated to cover
	 * {@code [minIndex, maxIndex]}. Subsequent {@code add()} calls within this range will not
	 * trigger any array resizing.
	 */
	public static UnboundedSizeDenseStore create(int minIndex, int maxIndex) {
		UnboundedSizeDenseStore store = new UnboundedSizeDenseStore();
		store.extendRange(minIndex, maxIndex);
		return store;
	}
}
