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
package io.zulia.server.search;

import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaQuery;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.search.stat.DoubleStats;
import io.zulia.server.search.stat.LongStats;
import io.zulia.server.search.stat.StatFactory;
import io.zulia.server.search.stat.Stats;
import io.zulia.server.search.stat.TopStatsQueue;
import org.apache.lucene.facet.FacetUtils;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class TaxonomyStatsHandler {

	protected final Stats<?>[][] fieldFacetStats;
	protected final Stats<?>[] fieldStats;

	private final HashMap<String, ZuliaQuery.StatRequest> statRequestMap;
	private final List<String> numericFieldsList;

	private final ArrayList<String> numericFieldSortValueList;
	private final TaxonomyReader taxoReader;
	private final List<ZuliaIndex.FieldConfig.FieldType> fieldTypes;
	private final List<Double> requestPrecisions;

	private int[] children;
	private int[] siblings;

	public TaxonomyStatsHandler(TaxonomyReader taxoReader, FacetsCollector fc, List<ZuliaQuery.StatRequest> statRequests, ServerIndexConfig serverIndexConfig)
			throws IOException {

		boolean facetLevel = statRequests.stream().map(ZuliaQuery.StatRequest::getFacetField).anyMatch(s -> !s.getLabel().isEmpty());
		boolean global = statRequests.stream().map(ZuliaQuery.StatRequest::getFacetField).anyMatch(s -> s.getLabel().isEmpty());

		this.statRequestMap = new HashMap<>();

		for (ZuliaQuery.StatRequest statRequest : statRequests) {
			//global
			if (statRequest.getFacetField().getLabel().isEmpty()) {
				global = true;
			}
			else {
				facetLevel = true;
			}
		}

		statRequests.forEach(statRequest -> this.statRequestMap.put(statRequest.getNumericField(), statRequest));
		Set<String> numericFields = this.statRequestMap.keySet();

		fieldTypes = new ArrayList<>();
		requestPrecisions = new ArrayList<>();
		numericFieldSortValueList = new ArrayList<>();
		for (String numericField : numericFields) {
			ZuliaIndex.FieldConfig.FieldType fieldTypeForSortField = serverIndexConfig.getFieldTypeForSortField(numericField);
			if (fieldTypeForSortField == null) {
				throw new IllegalArgumentException("Numeric field <" + numericField + "> must be indexed as a SORTABLE numeric field");
			}
			if (!FieldTypeUtil.isNumericFieldType(fieldTypeForSortField)) {
				throw new IllegalArgumentException("Numeric field <" + numericField + "> must be indexed as a sortable NUMERIC field");
			}

			fieldTypes.add(fieldTypeForSortField);
			requestPrecisions.add(statRequestMap.get(numericField).getPrecision());
			numericFieldSortValueList.add(serverIndexConfig.getSortField(numericField, fieldTypeForSortField));

			statRequestMap.get(numericField);
		}

		this.numericFieldsList = new ArrayList<>(numericFields);

		if (facetLevel) {
			this.fieldFacetStats = new Stats[numericFieldsList.size()][taxoReader.getSize()];
			this.taxoReader = taxoReader;
		}
		else {
			this.fieldFacetStats = null;
			this.taxoReader = null;
		}

		if (global) {
			this.fieldStats = new Stats[numericFieldsList.size()];
			for (int i = 0; i < fieldStats.length; i++) {
				Double precision = requestPrecisions.get(i);
				ZuliaIndex.FieldConfig.FieldType fieldType = fieldTypes.get(i);
				this.fieldStats[i] = StatFactory.getStatForFieldType(fieldType, precision).get();
			}

		}
		else {
			this.fieldStats = null;
		}

		sumValues(fc.getMatchingDocs(), numericFieldsList);
	}

	private void sumValues(List<MatchingDocs> matchingDocs, List<String> fieldsList) throws IOException {

		final SortedNumericDocValues[] functionValues = new SortedNumericDocValues[fieldsList.size()];

		// temp storage of the doc values for a document
		// because we iterate through the values twice (and the iterator is not resetable) and we do not want to have to create a new array each time
		int[] ordinalDocValues = new int[0];
		long[] numericValues = new long[0];

		List<Supplier<? extends Stats<?>>> statConstructor = new ArrayList<>(fieldsList.size());
		for (int f = 0; f < fieldsList.size(); f++) {
			ZuliaIndex.FieldConfig.FieldType fieldType = fieldTypes.get(f);
			statConstructor.add(StatFactory.getStatForFieldType(fieldType, requestPrecisions.get(f)));
		}

		for (MatchingDocs hits : matchingDocs) {

			for (int f = 0; f < fieldsList.size(); f++) {
				functionValues[f] = DocValues.getSortedNumeric(hits.context.reader(), numericFieldSortValueList.get(f));
			}

			DocIdSetIterator docs = hits.bits.iterator();

			SortedNumericDocValues ordinalValues = null;
			if (fieldFacetStats != null) {
				ordinalValues = FacetUtils.loadOrdinalValues(hits.context.reader(), FacetsConfig.DEFAULT_INDEX_FIELD_NAME);
				docs = ConjunctionUtils.intersectIterators(List.of(hits.bits.iterator(), ordinalValues));
			}

			for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {

				int ordinalCount = -1;
				if (ordinalValues != null) {

					ordinalCount = ordinalValues.docValueCount();

					if (ordinalCount > ordinalDocValues.length) {
						ordinalDocValues = new int[ordinalCount];
					}

					for (int i = 0; i < ordinalCount; i++) {
						int ordIndex = (int) ordinalValues.nextValue();
						ordinalDocValues[i] = ordIndex;
					}
				}

				for (int f = 0; f < fieldsList.size(); f++) {

					SortedNumericDocValues functionValue = functionValues[f];

					if (functionValue.advanceExact(doc)) {
						int numericValueCount = functionValue.docValueCount();
						if (numericValueCount > numericValues.length) {
							numericValues = new long[numericValueCount];
						}
						for (int j = 0; j < numericValueCount; j++) {
							numericValues[j] = functionValue.nextValue();
						}

						if (ordinalCount != -1) {

							for (int i = 0; i < ordinalCount; i++) {
								int ordIndex = ordinalDocValues[i];
								Stats<?> stats = fieldFacetStats[f][ordIndex];
								if (stats == null) {
									stats = statConstructor.get(f).get();
									stats.setOrdinal(ordIndex);
									fieldFacetStats[f][ordIndex] = stats;
								}
								stats.newDoc(true);
							}
							for (int j = 0; j < numericValueCount; j++) {
								for (int i = 0; i < ordinalCount; i++) {
									int ordIndex = ordinalDocValues[i];
									Stats<?> stats = fieldFacetStats[f][ordIndex];
									stats.handleDocValue(numericValues[j]);
								}

							}
						}
						if (fieldStats != null) {
							Stats<?> stats = fieldStats[f];
							stats.newDoc(true);
							for (int j = 0; j < numericValueCount; j++) {
								stats.handleDocValue(numericValues[j]);
							}
						}
					}
					else {
						if (ordinalCount != -1) {
							for (int i = 0; i < ordinalCount; i++) {
								int ordIndex = ordinalDocValues[i];
								Stats<?> stats = fieldFacetStats[f][ordIndex];
								if (stats == null) {
									stats = statConstructor.get(f).get();
									stats.setOrdinal(ordIndex);
									fieldFacetStats[f][ordIndex] = stats;
								}
								stats.newDoc(false);
							}
						}
						if (fieldStats != null) {
							Stats<?> stats = fieldStats[f];
							stats.newDoc(false);
						}
					}
				}
			}
		}
	}

	public ZuliaQuery.FacetStatsInternal getGlobalStatsForNumericField(String field) {
		int fieldIndex = numericFieldsList.indexOf(field);

		if (fieldIndex == -1) {
			throw new IllegalArgumentException("Field <" + field + "> was not given in constructor");
		}

		Stats<?> fieldStat = fieldStats[fieldIndex];
		System.out.println(fieldStat.buildResponse());
		return fieldStat.buildResponse().build();
	}

	public List<ZuliaQuery.FacetStatsInternal> getTopChildren(String field, int topN, String dim, String... path) throws IOException {
		int fieldIndex = numericFieldsList.indexOf(field);

		if (fieldIndex == -1) {
			throw new IllegalArgumentException("Field <" + field + "> was not given in constructor");
		}

		Stats<?>[] stats = fieldFacetStats[fieldIndex];

		if (topN <= 0) {
			throw new IllegalArgumentException("topN must be > 0 (got: " + topN + ")");
		}

		FacetLabel cp = new FacetLabel(dim, path);
		int dimOrd = taxoReader.getOrdinal(cp);
		if (dimOrd == -1) {
			return null;
		}

		ZuliaIndex.FieldConfig.FieldType fieldType = fieldTypes.get(fieldIndex);
		Stats<?> statType = StatFactory.getStatForFieldType(fieldType, 0).get();

		//TODO this will move up
		if (children == null) {
			children = taxoReader.getParallelTaxonomyArrays().children();
		}

		if (siblings == null) {
			siblings = taxoReader.getParallelTaxonomyArrays().siblings();
		}

		int ord = children[dimOrd];

		if (statType instanceof LongStats) {
			TopStatsQueue<LongStats> q = new TopStatsQueue<>(Math.min(taxoReader.getSize(), topN));
			long longBottomValue = Long.MIN_VALUE;

			while (ord != TaxonomyReader.INVALID_ORDINAL) {
				LongStats stat = (LongStats) stats[ord];
				if (stat != null) {
					if (stat.getLongSum() > longBottomValue) {
						q.insertWithOverflow(stat);
						if (q.size() == topN) {
							longBottomValue = q.top().getLongSum();
						}
					}

				}
				ord = siblings[ord];
			}

			return getFacetStatsInternals(cp, q);
		}
		else if (statType instanceof DoubleStats) {
			TopStatsQueue<DoubleStats> q = new TopStatsQueue<>(Math.min(taxoReader.getSize(), topN));
			double doubleBottomValue = Double.NEGATIVE_INFINITY;

			while (ord != TaxonomyReader.INVALID_ORDINAL) {
				DoubleStats stat = (DoubleStats) stats[ord];
				if (stat != null) {
					if (stat.getDoubleSum() > doubleBottomValue) {
						q.insertWithOverflow(stat);
						if (q.size() == topN) {
							doubleBottomValue = q.top().getDoubleSum();
						}
					}

				}

				ord = siblings[ord];
			}

			return getFacetStatsInternals(cp, q);
		}
		else {
			throw new IllegalStateException("Unknown field state type <" + statType + ">");
		}

	}

	private List<ZuliaQuery.FacetStatsInternal> getFacetStatsInternals(FacetLabel cp, TopStatsQueue<?> q) throws IOException {
		ZuliaQuery.FacetStatsInternal[] facetStats = new ZuliaQuery.FacetStatsInternal[q.size()];
		for (int i = facetStats.length - 1; i >= 0; i--) {
			Stats<?> stat = q.pop();
			FacetLabel child = taxoReader.getPath(stat.getOrdinal());
			String label = child.components[cp.length];
			facetStats[i] = stat.buildResponse().setFacet(label).build();
		}

		return Arrays.asList(facetStats);
	}

}
