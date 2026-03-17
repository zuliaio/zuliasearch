package io.zulia.server.search.aggregation.stats;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.mapping.IndexMappingProtoBinding;
import com.datadoghq.sketch.ddsketch.store.PreSizedUnboundedSizeDenseStore;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;

import java.util.List;
import java.util.Map;

/**
 * Optimized DDSketch proto deserialization that pre-sizes the internal dense stores
 * based on the proto data, avoiding repeated Arrays.copyOf calls during bin insertion.
 * <p>
 * The standard {@code DDSketchProtoBinding.fromProto} creates an empty store and adds bins
 * one-by-one, causing array resizing via {@code extendRange → Arrays.copyOf}.
 * This class computes the required index range upfront and pre-allocates the store via
 * {@link PreSizedUnboundedSizeDenseStore}, so subsequent {@code add()} calls never resize.
 */
class PreSizedDDSketchProtoBinding {

	private PreSizedDDSketchProtoBinding() {
	}

	/**
	 * Deserializes a DDSketch from its proto representation using pre-sized stores.
	 */
	static DDSketch fromProto(com.datadoghq.sketch.ddsketch.proto.DDSketch proto) {
		IndexMapping indexMapping = IndexMappingProtoBinding.fromProto(proto.getMapping());
		UnboundedSizeDenseStore positiveValueStore = storeFromProto(proto.getPositiveValues());
		UnboundedSizeDenseStore negativeValueStore = storeFromProto(proto.getNegativeValues());
		return DDSketch.of(indexMapping, negativeValueStore, positiveValueStore, proto.getZeroCount());
	}

	/**
	 * Builds an UnboundedSizeDenseStore from a proto Store, pre-allocating the required index range
	 * so that subsequent add() calls never trigger extendRange() / Arrays.copyOf().
	 */
	private static UnboundedSizeDenseStore storeFromProto(com.datadoghq.sketch.ddsketch.proto.Store protoStore) {
		List<Double> contiguousBinCounts = protoStore.getContiguousBinCountsList();
		Map<Integer, Double> sparseBinCounts = protoStore.getBinCountsMap();

		if (contiguousBinCounts.isEmpty() && sparseBinCounts.isEmpty()) {
			return new UnboundedSizeDenseStore();
		}

		// Compute the full index range from both sparse and contiguous encodings
		int minIndex = Integer.MAX_VALUE;
		int maxIndex = Integer.MIN_VALUE;

		if (!contiguousBinCounts.isEmpty()) {
			int contiguousOffset = protoStore.getContiguousBinIndexOffset();
			minIndex = contiguousOffset;
			maxIndex = contiguousOffset + contiguousBinCounts.size() - 1;
		}

		for (int index : sparseBinCounts.keySet()) {
			minIndex = Math.min(minIndex, index);
			maxIndex = Math.max(maxIndex, index);
		}

		// Pre-allocate the store to cover the full range — no Arrays.copyOf during add()
		UnboundedSizeDenseStore store = PreSizedUnboundedSizeDenseStore.create(minIndex, maxIndex);

		// Add contiguous bins
		int index = protoStore.getContiguousBinIndexOffset();
		for (double count : contiguousBinCounts) {
			store.add(index++, count);
		}

		// Add sparse bins
		sparseBinCounts.forEach(store::add);

		return store;
	}
}
