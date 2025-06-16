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
import io.zulia.message.ZuliaQuery;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.config.SortFieldInfo;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.search.aggregation.facets.BinaryFacetReader;
import io.zulia.server.search.aggregation.facets.CountFacetInfo;
import io.zulia.server.search.aggregation.facets.FacetsReader;
import io.zulia.server.search.aggregation.ordinal.FacetHandler;
import io.zulia.server.search.aggregation.ordinal.MapStatOrdinalStorage;
import io.zulia.server.search.aggregation.stats.NumericFieldStatContext;
import io.zulia.server.search.aggregation.stats.NumericFieldStatInfo;
import io.zulia.server.search.aggregation.stats.Stats;
import io.zulia.util.pool.TaskExecutor;
import io.zulia.util.pool.WorkPool;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.TopOrdAndIntQueue;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class AggregationHandler {

	private final TaxonomyReader taxoReader;
	private final NumericFieldStatInfo[] fields;
	private final boolean needsFacets;

	private final CountFacetInfo globalFacetInfo;
	private final int concurrency;

	public AggregationHandler(TaxonomyReader taxoReader, FacetsCollector fc, List<ZuliaQuery.StatRequest> statRequests,
			List<ZuliaQuery.CountRequest> countRequests, ServerIndexConfig serverIndexConfig, int concurrency) throws IOException {

		this.taxoReader = taxoReader;
		this.concurrency = concurrency;

		ObjObjMap<String, NumericFieldStatInfo> fieldToDimensions = HashObjObjMaps.newMutableMap();

		boolean needsFacetLocal = false;
		for (ZuliaQuery.StatRequest statRequest : statRequests) {
			//global
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

			if (facetLabel.isEmpty()) {
				fieldStatInfo.enableGlobal(statRequest.getPrecision());
			}
			else {
				fieldStatInfo.addFacet(facetLabel, taxoReader.getOrdinal(new FacetLabel(facetLabel)));
				fieldStatInfo.enableFacetWithPrecision(statRequest.getPrecision());
				needsFacetLocal = true;
			}
		}

		globalFacetInfo = new CountFacetInfo();
		for (ZuliaQuery.CountRequest countRequest : countRequests) {
			ZuliaQuery.Facet facetField = countRequest.getFacetField();
			String facetFieldLabel = facetField.getLabel();
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

		sumValues(fc.getMatchingDocs());
	}

	private void sumValues(List<MatchingDocs> matchingDocs) throws IOException {

		List<ListenableFuture<Object>> futures = new ArrayList<>();
		try (TaskExecutor taskExecutor = WorkPool.virtualBounded(concurrency)) {
			for (MatchingDocs hits : matchingDocs) {
				futures.add(taskExecutor.executeAsync(() -> {
					handleMatchingDocsForSegment(hits);
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

	private void handleMatchingDocsForSegment(MatchingDocs hits) throws IOException {
		LeafReader reader = hits.context().reader();

		CountFacetInfo	localGlobalFacetInfo = new CountFacetInfo(globalFacetInfo);

		List<NumericFieldStatContext> fieldContexts = new ArrayList<>();
		for (NumericFieldStatInfo field : fields) {
			// TODO copy numeric stat in this constructor
			NumericFieldStatContext numericFieldStatContext = new NumericFieldStatContext(reader, field);
			fieldContexts.add(numericFieldStatContext);
		}

		DocIdSetIterator docs = hits.bits().iterator();

		final FacetsReader facetReader;

		if (needsFacets) {
			facetReader = new BinaryFacetReader(reader);
			docs = facetReader.getCombinedIterator(docs);
		}
		else {
			facetReader = null;
		}

		for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {

			final FacetHandler facetHandler;
			if (needsFacets) {
				facetHandler = facetReader.getFacetHandler();
				localGlobalFacetInfo.maybeHandleFacets(facetHandler);
			}
			else {
				facetHandler = null;
			}

			for (NumericFieldStatContext fieldContext : fieldContexts) {
				fieldContext.advanceNumericValues(doc);
				fieldContext.maybeHandleFacet(facetHandler);
				fieldContext.maybeHandleGlobal();
			}
		}

		globalFacetInfo.merge(localGlobalFacetInfo);

		//for (NumericFieldStatInfo field : fields) {
			// TODO merge numeric stat back into the main stat here
		//
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

	public ZuliaQuery.FacetGroup.Builder getTopChildren(int topN, String dim, String... path) throws IOException {
		FacetLabel countPath = new FacetLabel(dim, path);
		int dimOrd = taxoReader.getOrdinal(countPath);
		if (dimOrd == -1) {
			return ZuliaQuery.FacetGroup.newBuilder();
		}

		TaxonomyReader.ChildrenIterator childrenIterator = taxoReader.getChildren(dimOrd);

		TopOrdAndIntQueue q = new TopOrdAndIntQueue(Math.min(taxoReader.getSize(), topN));
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

		ZuliaQuery.FacetCount[] facetCounts = new ZuliaQuery.FacetCount[q.size()];
		for (int i = facetCounts.length - 1; i >= 0; i--) {
			TopOrdAndIntQueue.OrdAndValue ordValue = q.pop();
			FacetLabel c = taxoReader.getPath(ordValue.ord);
			String label = c.components[countPath.length];
			facetCounts[i] = ZuliaQuery.FacetCount.newBuilder().setFacet(label).setCount(ordValue.getValue().longValue()).build();
		}

		return ZuliaQuery.FacetGroup.newBuilder().addAllFacetCount(Arrays.stream(facetCounts).toList());
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
		return facetStatStorage.getFacetStats(taxoReader, countPath, topN);

	}

}
