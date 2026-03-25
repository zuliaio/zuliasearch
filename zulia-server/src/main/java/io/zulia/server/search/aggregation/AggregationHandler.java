/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zulia.server.search.aggregation;

import com.google.common.util.concurrent.ListenableFuture;
import com.koloboke.collect.map.ObjObjMap;
import com.koloboke.collect.map.hash.HashObjObjMaps;
import io.zulia.ZuliaFieldConstants;
import io.zulia.message.ZuliaQuery;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.config.SortFieldInfo;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.search.aggregation.facets.BinaryFacetReader;
import io.zulia.server.search.aggregation.facets.CountFacetInfo;
import io.zulia.server.search.aggregation.facets.FacetsReader;
import io.zulia.server.search.aggregation.facets.NoOpReader;
import io.zulia.server.search.aggregation.ordinal.FacetHandler;
import io.zulia.server.search.aggregation.ordinal.MapStatOrdinalStorage;
import io.zulia.server.search.aggregation.stats.NumericFieldStatContext;
import io.zulia.server.search.aggregation.stats.NumericFieldStatInfo;
import io.zulia.util.pool.TaskExecutor;
import io.zulia.util.pool.WorkPool;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.TopOrdAndIntQueue;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.function.IntUnaryOperator;

public class AggregationHandler {

	private final TaxonomyReader taxoReader;
	private final NumericFieldStatInfo[] fields;
	private final boolean needsFacets;

	private final CountFacetInfo globalFacetInfo;
	private final int requestedConcurrency;

	private final static Logger LOG = LoggerFactory.getLogger(AggregationHandler.class);

	private final String facetField;
	private final boolean individualFacet;
	private final IntUnaryOperator dimensionChildCount;
	private final AggregationSettings aggregationSettings;

	public AggregationHandler(TaxonomyReader taxoReader, FacetsCollector fc, List<ZuliaQuery.StatRequest> statRequests,
			List<ZuliaQuery.CountRequest> countRequests, ServerIndexConfig serverIndexConfig, int requestedConcurrency,
			IntUnaryOperator dimensionChildCount, boolean debug, long searchId, AggregationSettings aggregationSettings) throws IOException {

		this.dimensionChildCount = dimensionChildCount;
		this.aggregationSettings = aggregationSettings;

		this.taxoReader = taxoReader;
		this.requestedConcurrency = requestedConcurrency;

		ObjObjMap<String, NumericFieldStatInfo> fieldToDimensions = HashObjObjMaps.newMutableMap();

		TreeSet<String> facets = new TreeSet<>();

		boolean needsFacetLocal = false;
		for (ZuliaQuery.StatRequest statRequest : statRequests) {
			String facetLabel = statRequest.getFacetField().getLabel();
			String numericField = statRequest.getNumericField();

			NumericFieldStatInfo fieldStatInfo = fieldToDimensions.computeIfAbsent(numericField, s -> {

				SortFieldInfo numericSortFieldInfo = serverIndexConfig.getSortFieldInfo(numericField);
				if (numericSortFieldInfo == null) {
					//TODO fix this message to mention char list and list length
					throw new IllegalArgumentException("Numeric field " + numericField + " must be indexed as a SORTABLE numeric field");
				}
				if (!FieldTypeUtil.isHandledAsNumericFieldType(numericSortFieldInfo.getFieldType())) {
					//TODO fix this message to mention char list and list length
					throw new IllegalArgumentException("Numeric field " + numericField + " must be indexed as a sortable NUMERIC field");
				}

				NumericFieldStatInfo info = new NumericFieldStatInfo(numericField);
				info.setSortFieldName(numericSortFieldInfo.getInternalSortFieldName());
				info.setNumericFieldType(numericSortFieldInfo.getFieldType());

				return info;
			});

			// Only create DDSketches when percentiles are actually requested
			// This avoids expensive sketch population and serialization on the shard side when only min/max/sum/count are needed (but user spuriously set precision)
			double effectivePrecision = statRequest.getPercentilesList().isEmpty() ? 0.0 : statRequest.getPrecision();

			if (facetLabel.isEmpty()) {
				fieldStatInfo.enableGlobal(effectivePrecision);
			}
			else {
				facets.add(facetLabel);
				fieldStatInfo.addFacet(facetLabel, taxoReader.getOrdinal(new FacetLabel(facetLabel)));
				fieldStatInfo.enableFacetWithPrecision(effectivePrecision);
				needsFacetLocal = true;
			}
		}

		globalFacetInfo = new CountFacetInfo();
		for (ZuliaQuery.CountRequest countRequest : countRequests) {
			ZuliaQuery.Facet facetField = countRequest.getFacetField();
			String facetFieldLabel = facetField.getLabel();
			facets.add(facetFieldLabel);
			globalFacetInfo.addFacet(facetFieldLabel, taxoReader.getOrdinal(new FacetLabel(facetFieldLabel)));
			needsFacetLocal = true;
		}

		if (globalFacetInfo.hasFacets()) {
			globalFacetInfo.computeSortedOrdinalArray();
		}

		this.needsFacets = needsFacetLocal;
		this.fields = fieldToDimensions.values().toArray(new NumericFieldStatInfo[0]);

		for (NumericFieldStatInfo field : fields) {
			if (field.hasFacets()) {
				field.computeSortedOrdinalArray();
			}
		}

		if (facets.size() == 1 && serverIndexConfig.isStoredIndividually(facets.first())) {
			this.facetField = ZuliaFieldConstants.FACET_STORAGE_INDIVIDUAL + facets.first();
			this.individualFacet = true;
			if (debug) {
				LOG.info("Search id {} using individual facet storage for facet <{}>", searchId, facets.first());
			}
		}
		else {
			String facetGroup = serverIndexConfig.getFacetGroupForFacets(facets);
			this.facetField = facetGroup != null ? ZuliaFieldConstants.FACET_STORAGE_GROUP + facetGroup : ZuliaFieldConstants.FACET_STORAGE;
			this.individualFacet = false;
			if (debug) {
				if (facetGroup != null) {
					LOG.info("Search id {} using facet group <{}> for facets {}", searchId, facetGroup, facets);
				}
				else {
					LOG.info("Search id {} using default facet storage for facets {}", searchId, facets);
				}
			}
		}

		sumValues(fc.getMatchingDocs());

	}

	private void sumValues(List<MatchingDocs> matchingDocs) throws IOException {
		if (matchingDocs.isEmpty()) {
			return;
		}

		int concurrency = requestedConcurrency;
		if (concurrency > 1) {
			int totalHits = 0;
			for (MatchingDocs matchingDoc : matchingDocs) {
				totalHits += matchingDoc.totalHits();
			}
			int maxConcurrencyBasedOnHits = (totalHits / aggregationSettings.hitsPerConcurrentRequest()) + 1;
			concurrency = Math.min(maxConcurrencyBasedOnHits, requestedConcurrency);
			concurrency = Math.min(concurrency, matchingDocs.size()); // do not allow more concurrency than the number of segments
		}
		concurrency = Math.min(concurrency, requestedConcurrency);

		if (concurrency > 1) {
			List<ListenableFuture<Object>> futures = new ArrayList<>();
			try (TaskExecutor taskExecutor = WorkPool.virtualBounded(concurrency)) {

				for (MatchingDocs segment : matchingDocs) {
					futures.add(taskExecutor.executeAsync(() -> {
						handleSegmentsThreadSafe(List.of(segment));
						return null;
					}));
				}
			}
			for (ListenableFuture<Object> future : futures) {
				try {
					future.get();
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				catch (ExecutionException e) {
					if (e.getCause() instanceof IOException ioException) {
						throw ioException;
					}
					throw new RuntimeException(e.getCause());
				}
			}
		}
		else {
			handleSegments(matchingDocs, fields, globalFacetInfo);
		}

	}

	private void handleSegmentsThreadSafe(Collection<MatchingDocs> matchingDocsList) throws IOException {

		CountFacetInfo localGlobalFacetInfo = globalFacetInfo.cloneNewCounter();
		NumericFieldStatInfo[] localFields = new NumericFieldStatInfo[fields.length];
		for (int i = 0; i < fields.length; i++) {
			localFields[i] = fields[i].cloneNewStatCount();
		}

		handleSegments(matchingDocsList, localFields, localGlobalFacetInfo);

		globalFacetInfo.merge(localGlobalFacetInfo);
		for (int i = 0; i < fields.length; i++) {
			fields[i].merge(localFields[i]);
		}

	}

	private void handleSegments(Collection<MatchingDocs> matchingDocsList, NumericFieldStatInfo[] localFields, CountFacetInfo localGlobalFacetInfo)
			throws IOException {
		for (MatchingDocs matchingDocs : matchingDocsList) {

			handleSegment(matchingDocs, localFields, localGlobalFacetInfo);
		}
	}

	private void handleSegment(MatchingDocs matchingDocs, NumericFieldStatInfo[] localFields, CountFacetInfo localGlobalFacetInfo) throws IOException {
		LeafReader reader = matchingDocs.context().reader();
		List<NumericFieldStatContext> fieldContexts = new ArrayList<>(localFields.length);
		for (NumericFieldStatInfo field : localFields) {
			NumericFieldStatContext numericFieldStatContext = new NumericFieldStatContext(reader, field);
			fieldContexts.add(numericFieldStatContext);
		}

		DocIdSetIterator docs = matchingDocs.bits().iterator();

		final FacetsReader facetReader = needsFacets ?  new BinaryFacetReader(reader, facetField, individualFacet) : NoOpReader.INSTANCE;

		docs = facetReader.getCombinedIterator(docs);

		for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {

			final FacetHandler facetHandler =  facetReader.getFacetHandler();
			localGlobalFacetInfo.maybeHandleFacets(facetHandler);

			for (NumericFieldStatContext fieldContext : fieldContexts) {
				fieldContext.advanceNumericValues(doc);
				fieldContext.maybeHandleFacet(facetHandler);
				fieldContext.maybeHandleGlobal();
			}
		}
	}

	private NumericFieldStatInfo getFieldStatByName(String field) {
		NumericFieldStatInfo fieldStats = null;
		for (NumericFieldStatInfo fieldStatInfo : fields) {
			if (fieldStatInfo.getNumericFieldName().equals(field)) {
				fieldStats = fieldStatInfo;
				break;
			}
		}
		if (fieldStats == null) {
			throw new IllegalArgumentException("Field " + field + " was not given in constructor");
		}

		return fieldStats;
	}

	private record OrdAndCount(int ord, int count) {
	}

	private static final Comparator<OrdAndCount> COUNT_DESC_ORD_ASC = Comparator.comparingInt(OrdAndCount::count).reversed()
			.thenComparingInt(OrdAndCount::ord);

	// When topN is at least half the dimension cardinality, the priority queue has no meaningful advantage
	// over collect-and-sort (log(topN) ≈ log(n)) and sort has much better cache locality
	public static boolean shouldCollectAndSort(int topN, int dimensionChildCount) {
		return topN >= dimensionChildCount / 2;
	}

	public ZuliaQuery.FacetGroup.Builder getTopChildren(int topN, String dim, String... path) throws IOException {
		FacetLabel countPath = new FacetLabel(dim, path);
		int dimOrd = taxoReader.getOrdinal(countPath);
		if (dimOrd == -1) {
			if (path.length == 0) {
				LOG.warn("Facet {} has not been indexed but was requested by count request", dim);
			}
			else {
				// Do not warm for now if they are requesting a dimension with a path as paths could be sparse
				// Consider checking for the existence of the dimension itself at least without the path in this case
			}
			return ZuliaQuery.FacetGroup.newBuilder();
		}

		int dimChildCount = dimensionChildCount.applyAsInt(dimOrd);

		int[] ords;
		long[] counts;

		if (shouldCollectAndSort(topN, dimChildCount)) {
			// Collect all non-zero entries and sort — avoids large priority queue allocation
			List<OrdAndCount> collected = new ArrayList<>();
			TaxonomyReader.ChildrenIterator childrenIterator = taxoReader.getChildren(dimOrd);
			int child;
			while ((child = childrenIterator.next()) != TaxonomyReader.INVALID_ORDINAL) {
				int count = globalFacetInfo.getOrdinalCount(child);
				if (count != 0) {
					collected.add(new OrdAndCount(child, count));
				}
			}
			collected.sort(COUNT_DESC_ORD_ASC);

			int resultSize = Math.min(collected.size(), topN);
			ords = new int[resultSize];
			counts = new long[resultSize];
			for (int i = 0; i < resultSize; i++) {
				OrdAndCount entry = collected.get(i);
				ords[i] = entry.ord;
				counts[i] = entry.count;
			}
		}
		else {
			// Use priority queue — efficient when topN is small relative to cardinality
			TaxonomyReader.ChildrenIterator childrenIterator = taxoReader.getChildren(dimOrd);
			TopOrdAndIntQueue q = new TopOrdAndIntQueue(Math.min(dimChildCount, topN));
			int bottomValue = Integer.MIN_VALUE;
			int bottomOrd = Integer.MAX_VALUE;
			int child;
			TopOrdAndIntQueue.OrdAndInt reuse = null;
			while ((child = childrenIterator.next()) != TaxonomyReader.INVALID_ORDINAL) {
				int count = globalFacetInfo.getOrdinalCount(child);
				if (count != 0) {
					if (count > bottomValue || (count == bottomValue && child < bottomOrd)) {
						if (reuse == null) {
							reuse = new TopOrdAndIntQueue.OrdAndInt();
						}
						reuse.ord = child;
						reuse.value = count;
						reuse = (TopOrdAndIntQueue.OrdAndInt) q.insertWithOverflow(reuse);
						if (q.size() == topN) {
							bottomValue = q.top().getValue().intValue();
							bottomOrd = q.top().ord;
						}
					}
				}
			}

			int qSize = q.size();
			ords = new int[qSize];
			counts = new long[qSize];
			for (int i = qSize - 1; i >= 0; i--) {
				TopOrdAndIntQueue.OrdAndValue ordValue = q.pop();
				ords[i] = ordValue.ord;
				counts[i] = ordValue.getValue().longValue();
			}
		}

		FacetLabel[] paths = taxoReader.getBulkPath(ords);

		List<ZuliaQuery.FacetCount> facetCounts = new ArrayList<>(ords.length);
		for (int i = 0; i < ords.length; i++) {
			String label = paths[i].components[countPath.length];
			facetCounts.add(ZuliaQuery.FacetCount.newBuilder().setFacet(label).setCount(counts[i]).build());
		}

		return ZuliaQuery.FacetGroup.newBuilder().addAllFacetCount(facetCounts);
	}

	public ZuliaQuery.FacetStatsInternal getGlobalStatsForNumericField(String field) {

		NumericFieldStatInfo fieldStats = getFieldStatByName(field);

		if (!fieldStats.hasGlobal()) {
			throw new IllegalArgumentException("Field " + field + " has not requested as a global stat in the constructor");
		}

		return fieldStats.getGlobalStats().buildResponse().build();
	}

	public List<ZuliaQuery.FacetStatsInternal> getTopChildren(String field, int topN, String dim, String... path) throws IOException {
		NumericFieldStatInfo fieldStats = getFieldStatByName(field);

		if (!fieldStats.hasFacets()) {
			throw new IllegalArgumentException("Field " + field + " has not requested as a facet stat in the constructor");
		}

		MapStatOrdinalStorage<?> facetStatStorage = fieldStats.getFacetStatStorage();

		if (topN <= 0) {
			throw new IllegalArgumentException("topN must be > 0 (got: " + topN + ")");
		}

		FacetLabel countPath = new FacetLabel(dim, path);
		List<ZuliaQuery.FacetStatsInternal> facetStats = facetStatStorage.getFacetStats(taxoReader, countPath, topN, dimensionChildCount);
		if (facetStats == null) {
			if (path.length == 0) {
				LOG.warn("Facet {} has not been indexed but was requested by stat facet request", dim);
			}
			else {
				// Do not warm for now if they are requesting a dimension with a path as paths could be sparse
				// Consider checking for the existence of the dimension itself at least without the path in this case
			}
			return List.of();
		}
		return facetStats;

	}

}
