package io.zulia.server.search.distinct;

import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaQuery;
import io.zulia.server.config.SortFieldInfo;
import io.zulia.server.field.FieldTypeUtil;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongHeap;
import org.apache.lucene.util.StringHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Collects the distinct values of one sortable field over every matching document. A string field is collected as
 * segment ordinals and resolved to one sorted set of term hashes per shard, everything else by the hash of its long doc value.
 */
public final class DistinctCountCollectorManager implements CollectorManager<DistinctValueCollector, DistinctValueCollector> {

	/**
	 * Stamped into requests by QueryRequestValidator when the client leaves the cap at 0.
	 */
	public static final int DEFAULT_MAX_GROUPS_PER_SHARD = 1_000_000;
	/**
	 * Each key is 8 bytes on the wire, so this is 128 MB per field per shard, half the internal message limit. Collecting
	 * holds a few times that per search slice while a capped field is folded, so the heap cost scales with the slices too.
	 */
	public static final int MAX_MAX_GROUPS_PER_SHARD = 16_000_000;
	/**
	 * Murmur3 seed for term hashes, part of the cross node contract. Equal to the seed of Lucene's BytesRef overload.
	 */
	static final int HASH_SEED = 104729;
	/**
	 * Past this many set terms summed over a shard's segments, a string field shard keeps only its smallest hashes, so its
	 * memory is a few times the cap instead of growing with the shard. It is 128 MB of hashes, the same bound as the largest cap.
	 */
	static final long MAX_EXACT_SET_TERMS = MAX_MAX_GROUPS_PER_SHARD;

	private final ZuliaQuery.DistinctCountRequest request;
	private final String internalSortFieldName;
	private final FieldType fieldType;
	private final boolean stringField;
	private final ZuliaIndex.SortAs.StringHandling stringHandling;
	private final int cap;
	private final long maxExactSetTerms;

	public DistinctCountCollectorManager(ZuliaQuery.DistinctCountRequest request, SortFieldInfo sortField) {
		this(request, sortField, MAX_EXACT_SET_TERMS);
	}

	/**
	 * Takes the memory guard explicitly, so a test can reach the smallest hashes path without a huge index.
	 */
	DistinctCountCollectorManager(ZuliaQuery.DistinctCountRequest request, SortFieldInfo sortField, long maxExactSetTerms) {
		this.request = request;
		this.internalSortFieldName = sortField.getInternalSortFieldName();
		this.fieldType = sortField.getFieldType();
		this.stringField = FieldTypeUtil.isStringFieldType(fieldType);
		this.stringHandling = sortField.getStringHandling();
		if (request.getMaxGroupsPerShard() <= 0) {
			throw new IllegalArgumentException(
					"Distinct count on <" + request.getField() + "> reached the shard without a per shard cap. QueryRequestValidator should stamp the default");
		}
		this.cap = request.getMaxGroupsPerShard();
		this.maxExactSetTerms = maxExactSetTerms;
	}

	@Override
	public DistinctValueCollector newCollector() {
		return stringField ? new StringDistinctValueCollector(internalSortFieldName) : new NumericDistinctValueCollector(internalSortFieldName, cap);
	}

	@Override
	public DistinctValueCollector reduce(Collection<DistinctValueCollector> collectors) {
		DistinctValueCollector merged = null;
		for (DistinctValueCollector collector : collectors) {
			if (merged == null) {
				merged = collector;
			}
			else {
				merged.merge(collector);
			}
		}
		return merged == null ? newCollector() : merged;
	}

	/**
	 * Builds the shard payload. Keys ship in ascending signed order so the coordinator can union them with a streaming
	 * merge. A shard over the cap ships its count and only its first {@code cap} keys, which are its smallest hashes, so
	 * the payload stays within the cap and the union of what it did send is still a valid floor.
	 */
	public ZuliaQuery.DistinctCountInternal toInternal(DistinctValueCollector collected, List<LeafReaderContext> leaves, TaskExecutor taskExecutor)
			throws IOException {
		ShardKeys shardKeys = switch (collected) {
			case StringDistinctValueCollector strings -> termHashes(strings, leaves, taskExecutor);
			case NumericDistinctValueCollector numerics -> new ShardKeys(numerics.smallestHashes(), numerics.count(), numerics.isCapped());
		};
		ZuliaQuery.DistinctCountInternal.Builder builder = ZuliaQuery.DistinctCountInternal.newBuilder().setField(request.getField()).setFieldType(fieldType)
				.setCount(shardKeys.count()).setCapped(shardKeys.capped());
		if (stringField && stringHandling != null) {
			// two indexes that fold the same field differently hash different strings, so the coordinator must see how each shard normalized
			builder.setStringHandling(stringHandling);
		}
		int shipped = Math.min(shardKeys.keys().length, cap);
		for (int i = 0; i < shipped; i++) {
			builder.addKey(shardKeys.keys()[i]);
		}
		return builder.build();
	}

	/**
	 * One shard's sorted keys with its distinct count, which is a floor rather than exact when capped.
	 */
	private record ShardKeys(long[] keys, long count, boolean capped) {
	}

	/**
	 * Hashes each segment's set terms in parallel into a sorted array, then unites the arrays into one sorted set, so the
	 * shard knows its exact count even when it is over the cap. Past {@link #MAX_EXACT_SET_TERMS} set terms the arrays
	 * would no longer fit the memory budget, so the shard keeps only its smallest hashes instead.
	 */
	private ShardKeys termHashes(StringDistinctValueCollector strings, List<LeafReaderContext> leaves, TaskExecutor taskExecutor) throws IOException {
		List<SegmentOrds> segments = new ArrayList<>();
		long setTerms = 0;
		SegmentOrds largest = null;
		for (Map.Entry<Integer, FixedBitSet> entry : strings.ordsByLeaf().entrySet()) {
			SegmentOrds segment = new SegmentOrds(leaves.get(entry.getKey()), entry.getValue(), entry.getValue().cardinality());
			if (segment.cardinality() == 0) {
				continue;
			}
			segments.add(segment);
			setTerms += segment.cardinality();
			if (largest == null || segment.cardinality() > largest.cardinality()) {
				largest = segment;
			}
		}
		if (largest == null) {
			return new ShardKeys(new long[0], 0, false);
		}
		if (setTerms > maxExactSetTerms) {
			return smallestHashes(segments, largest);
		}
		List<Callable<long[]>> perSegment = new ArrayList<>(segments.size());
		for (SegmentOrds segment : segments) {
			perSegment.add(() -> segmentTermHashes(segment, Integer.MAX_VALUE));
		}
		long[] union = SortedLongUnion.union(taskExecutor.invokeAll(perSegment), taskExecutor);
		return new ShardKeys(union, union.length, union.length > cap);
	}

	/**
	 * Keeps the cap + 1 smallest hashes over all segments, one segment at a time, so memory is a few times the cap however
	 * large the shard is. Keeping one past the cap tells a shard under it, whose kept hashes are then its exact union,
	 * from one over it, whose floor is the larger of its biggest segment and the kept hashes.
	 */
	private ShardKeys smallestHashes(List<SegmentOrds> segments, SegmentOrds largest) throws IOException {
		int keep = (int) Math.min(Integer.MAX_VALUE, cap + 1L);
		long[] kept = new long[0];
		for (SegmentOrds segment : segments) {
			kept = SortedLongUnion.smallestUnion(kept, segmentTermHashes(segment, keep), keep);
		}
		if (kept.length <= cap) {
			// no segment had to drop a hash, so the kept set is every value the shard matched
			return new ShardKeys(kept, kept.length, false);
		}
		return new ShardKeys(kept, Math.max(largest.cardinality(), kept.length), true);
	}

	/**
	 * One segment's set ordinals with their count, so the count is taken once.
	 */
	private record SegmentOrds(LeafReaderContext leaf, FixedBitSet bits, int cardinality) {
	}

	/**
	 * Sorted hashes of one segment's set terms, or only the {@code keep} smallest of them. A dense set walks the term
	 * dictionary once instead of seeking per ordinal.
	 */
	private long[] segmentTermHashes(SegmentOrds segment, int keep) throws IOException {
		SortedSetDocValues values = DocValues.getSortedSet(segment.leaf().reader(), internalSortFieldName);
		FixedBitSet bits = segment.bits();
		int setOrds = segment.cardinality();
		SegmentHashes hashes = new SegmentHashes(setOrds, keep);
		int seen = 0;
		// Past half the terms, one sequential pass over the term dictionary is cheaper than a seek per set ordinal
		if (setOrds * 2L > values.getValueCount()) {
			TermsEnum terms = values.termsEnum();
			int ord = 0;
			for (BytesRef term = terms.next(); term != null && seen < setOrds; term = terms.next(), ord++) {
				if (bits.get(ord)) {
					hashes.add(hash(term));
					seen++;
				}
			}
		}
		else {
			BitSetIterator it = new BitSetIterator(bits, setOrds);
			for (int ord = it.nextDoc(); ord != DocIdSetIterator.NO_MORE_DOCS; ord = it.nextDoc()) {
				hashes.add(hash(values.lookupOrd(ord)));
			}
		}
		return hashes.sorted();
	}

	/**
	 * Collects one segment's hashes, all of them, or the smallest {@code keep} in a bounded heap when the segment has more.
	 */
	private static final class SegmentHashes {
		private final long[] all;
		private final LongHeap smallest;
		private int count;

		SegmentHashes(int setOrds, int keep) {
			boolean bounded = setOrds > keep;
			this.all = bounded ? null : new long[setOrds];
			// LongHeap evicts its smallest value, so hashes go in inverted to make it evict the largest
			this.smallest = bounded ? new LongHeap(keep) : null;
		}

		void add(long hash) {
			if (smallest == null) {
				all[count++] = hash;
			}
			else {
				smallest.insertWithOverflow(~hash);
			}
		}

		long[] sorted() {
			long[] hashes = all;
			if (smallest != null) {
				hashes = new long[smallest.size()];
				for (int i = 0; i < hashes.length; i++) {
					hashes[i] = ~smallest.get(i + 1);
				}
			}
			Arrays.sort(hashes);
			return hashes;
		}
	}

	/**
	 * Hashes a term the same way on every node and across Lucene upgrades. The seed is Lucene's own for the
	 * {@code BytesRef} overload, passed explicitly, so a change there cannot make two nodes disagree.
	 */
	static long hash(BytesRef term) {
		return StringHelper.murmurhash3_x64_128(term.bytes, term.offset, term.length, HASH_SEED)[0];
	}

	/**
	 * Hashes a long doc value the same way on every node, part of the cross node contract like the term hash. This is the
	 * murmur3 finalizer, written out because Lucene keeps its copy private. It is a one to one mix of the 64 bits, so
	 * distinct values never collide and the hashes spread evenly.
	 */
	static long hash(long value) {
		long h = value;
		h ^= h >>> 33;
		h *= 0xff51afd7ed558ccdL;
		h ^= h >>> 33;
		h *= 0xc4ceb9fe1a85ec53L;
		h ^= h >>> 33;
		return h;
	}
}
