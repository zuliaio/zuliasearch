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

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketches;
import com.google.protobuf.InvalidProtocolBufferException;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaQuery;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.field.FieldTypeUtil;
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
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class TaxonomyStatsHandler {

	protected final Stats[][] fieldFacetStats;
	protected final Stats[] fieldStats;
	private final HashMap<String, ZuliaQuery.StatRequest> statRequestMap;
	private final List<String> fieldsList;
	private final TaxonomyReader taxoReader;
	private final List<ZuliaIndex.FieldConfig.FieldType> fieldTypes;
	private final List<Double> requestPrecisions;
	private final ServerIndexConfig serverIndexConfig;
	private int[] children;
	private int[] siblings;

	public TaxonomyStatsHandler(TaxonomyReader taxoReader, FacetsCollector fc, List<ZuliaQuery.StatRequest> statRequests, ServerIndexConfig serverIndexConfig)
			throws IOException {
		this.serverIndexConfig = serverIndexConfig;

		boolean facetLevel = statRequests.stream().map(ZuliaQuery.StatRequest::getFacetField).anyMatch(s -> !s.getLabel().isEmpty());
		boolean global = statRequests.stream().map(ZuliaQuery.StatRequest::getFacetField).anyMatch(s -> s.getLabel().isEmpty());

		this.statRequestMap = new HashMap<>();
		statRequests.forEach(statRequest -> this.statRequestMap.put(statRequest.getNumericField(), statRequest));
		Set<String> numericFields = this.statRequestMap.keySet();

		fieldTypes = new ArrayList<>();
		requestPrecisions = new ArrayList<>();
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
		}

		this.fieldsList = new ArrayList<>(numericFields);

		if (facetLevel) {
			this.fieldFacetStats = new Stats[fieldsList.size()][taxoReader.getSize()];
			this.taxoReader = taxoReader;
		}
		else {
			this.fieldFacetStats = null;
			this.taxoReader = null;
		}

		if (global) {
			this.fieldStats = new Stats[fieldsList.size()];
			for (int i = 0; i < fieldStats.length; i++) {
				this.fieldStats[i] = new Stats(FieldTypeUtil.isNumericFloatingPointFieldType(fieldTypes.get(i)), requestPrecisions.get(i));
			}

		}
		else {
			this.fieldStats = null;
		}

		sumValues(fc.getMatchingDocs(), fieldsList);
	}

	private void sumValues(List<MatchingDocs> matchingDocs, List<String> fieldsList) throws IOException {

		final SortedNumericDocValues[] functionValues = new SortedNumericDocValues[fieldsList.size()];

		// temp storage of the doc values for a document
		// because we iterate through the values twice (and the iterator is not resetable) and we do not want to have to create a new array each time
		int[] ordinalDocValues = new int[0];
		long[] numericValues = new long[0];

		for (MatchingDocs hits : matchingDocs) {

			for (int f = 0; f < fieldsList.size(); f++) {
				ZuliaIndex.FieldConfig.FieldType fieldType = fieldTypes.get(f);
				String field = serverIndexConfig.getSortField(fieldsList.get(f), fieldType);
				functionValues[f] = DocValues.getSortedNumeric(hits.context.reader(), field);
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
					ZuliaIndex.FieldConfig.FieldType fieldType = fieldTypes.get(f);
					if (functionValue.advanceExact(doc)) {
						if (functionValue.docValueCount() > numericValues.length) {
							numericValues = new long[functionValue.docValueCount()];
						}
						for (int j = 0; j < functionValue.docValueCount(); j++) {
							long value = functionValue.nextValue();
							numericValues[j] = value;
						}

						if (ordinalCount != -1) {

							for (int i = 0; i < ordinalCount; i++) {
								int ordIndex = ordinalDocValues[i];
								Stats stats = fieldFacetStats[f][ordIndex];
								if (stats == null) {
									stats = new Stats(FieldTypeUtil.isNumericFloatingPointFieldType(fieldType), requestPrecisions.get(f));
									fieldFacetStats[f][ordIndex] = stats;
								}
								stats.newDoc(true);
							}
							for (int j = 0; j < functionValue.docValueCount(); j++) {
								for (int i = 0; i < ordinalCount; i++) {
									int ordIndex = ordinalDocValues[i];
									Stats stats = fieldFacetStats[f][ordIndex];
									addUpValue(fieldType, numericValues[j], stats);
								}

							}
						}
						if (fieldStats != null) {
							Stats stats = fieldStats[f];
							stats.newDoc(true);
							for (int j = 0; j < functionValue.docValueCount(); j++) {
								addUpValue(fieldType, numericValues[j], stats);
							}
						}
					}
					else {
						if (ordinalCount != -1) {
							for (int i = 0; i < ordinalCount; i++) {
								int ordIndex = ordinalDocValues[i];
								Stats stats = fieldFacetStats[f][ordIndex];
								if (stats == null) {
									stats = new Stats(FieldTypeUtil.isNumericFloatingPointFieldType(fieldType), requestPrecisions.get(f));
									fieldFacetStats[f][ordIndex] = stats;
								}
								stats.newDoc(false);
							}
						}
						if (fieldStats != null) {
							Stats stats = fieldStats[f];
							stats.newDoc(false);
						}
					}
				}
			}
		}
	}

	private void addUpValue(ZuliaIndex.FieldConfig.FieldType fieldType, long value, Stats stats) {
		if (FieldTypeUtil.isNumericDoubleFieldType(fieldType)) {
			stats.newValue(NumericUtils.sortableLongToDouble(value));
		}
		else if (FieldTypeUtil.isNumericFloatFieldType(fieldType)) {
			stats.newValue(NumericUtils.sortableIntToFloat((int) value));
		}
		else if (FieldTypeUtil.isNumericLongFieldType(fieldType)) {
			stats.newValue(value);
		}
		else if (FieldTypeUtil.isNumericIntFieldType(fieldType)) {
			stats.newValue((int) value);
		}
		else if (FieldTypeUtil.isBooleanFieldType(fieldType)) {
			stats.newValue((int) value);
		}
	}
	
	public ZuliaQuery.FacetStats getGlobalStatsForNumericField(String field) {
		int fieldIndex = fieldsList.indexOf(field);

		if (fieldIndex == -1) {
			throw new IllegalArgumentException("Field <" + field + "> was not given in constructor");
		}

		return createFacetStat(fieldStats[fieldIndex], "");
	}

	public List<ZuliaQuery.FacetStats> getTopChildren(String field, int topN, String dim, String... path) throws IOException {
		int fieldIndex = fieldsList.indexOf(field);

		if (fieldIndex == -1) {
			throw new IllegalArgumentException("Field <" + field + "> was not given in constructor");
		}

		Stats[] stats = fieldFacetStats[fieldIndex];

		if (topN <= 0) {
			throw new IllegalArgumentException("topN must be > 0 (got: " + topN + ")");
		}

		FacetLabel cp = new FacetLabel(dim, path);
		int dimOrd = taxoReader.getOrdinal(cp);
		if (dimOrd == -1) {
			return null;
		}

		TopStatQueue q = new TopStatQueue(Math.min(taxoReader.getSize(), topN));

		if (children == null) {
			children = taxoReader.getParallelTaxonomyArrays().children();
		}

		if (siblings == null) {
			siblings = taxoReader.getParallelTaxonomyArrays().siblings();
		}

		int ord = children[dimOrd];

		double doubleBottomValue = Double.NEGATIVE_INFINITY;
		long longBottomValue = Long.MIN_VALUE;

		while (ord != TaxonomyReader.INVALID_ORDINAL) {
			Stats stat = stats[ord];
			if (stat != null) {
				stat.ordinal = ord;
				if (stat.isFloatingPoint()) {
					if (stat.doubleSum > doubleBottomValue) {
						q.insertWithOverflow(stat);
						if (q.size() == topN) {
							doubleBottomValue = q.top().doubleSum;
						}
					}
				}
				else {
					if (stat.longSum > longBottomValue) {
						q.insertWithOverflow(stat);
						if (q.size() == topN) {
							longBottomValue = q.top().longSum;
						}
					}
				}
			}

			ord = siblings[ord];
		}

		ZuliaQuery.FacetStats[] facetStats = new ZuliaQuery.FacetStats[q.size()];
		for (int i = facetStats.length - 1; i >= 0; i--) {
			Stats stat = q.pop();
			FacetLabel child = taxoReader.getPath(stat.ordinal);
			String label = child.components[cp.length];
			facetStats[i] = createFacetStat(stat, label);
		}

		return Arrays.asList(facetStats);
	}

	private ZuliaQuery.FacetStats createFacetStat(Stats stat, String label) {
		ZuliaQuery.SortValue sum = ZuliaQuery.SortValue.newBuilder().setLongValue(stat.longSum).setDoubleValue(stat.doubleSum).build();
		ZuliaQuery.SortValue min = ZuliaQuery.SortValue.newBuilder().setLongValue(stat.longMinValue).setDoubleValue(stat.doubleMinValue).build();
		ZuliaQuery.SortValue max = ZuliaQuery.SortValue.newBuilder().setLongValue(stat.longMaxValue).setDoubleValue(stat.doubleMaxValue).build();

		// Add the "always-specified" values first
		ZuliaQuery.FacetStats.Builder builder = ZuliaQuery.FacetStats.newBuilder().setFacet(label).setDocCount(stat.docCount).setAllDocCount(stat.allDocCount)
				.setValueCount(stat.valueCount).setSum(sum).setMin(min).setMax(max);

		// Add stat sketch if specified
		try {
			if (stat.sketch != null) {
				builder.setStatSketch(com.datadoghq.sketch.ddsketch.proto.DDSketch.parseFrom(stat.sketch.serialize()));
			}
		}
		catch (InvalidProtocolBufferException ex) {
			// TODO(Ian): Do we use logging of errors?
		}

		// Build and return
		return builder.build();

	}

	public static class TopStatQueue extends PriorityQueue<Stats> {

		public TopStatQueue(int topN) {
			super(topN);
		}

		@Override
		protected boolean lessThan(Stats a, Stats b) {
			if (a.doubleSum < b.doubleSum || a.longSum < b.longSum) {
				return true;
			}
			else if (a.doubleSum > b.doubleSum || a.longSum > b.longSum) {
				return false;
			}
			else {
				return a.ordinal > b.ordinal;
			}
		}
	}

	public static class Stats {

		private final boolean floatingPoint;
		private int ordinal;
		private long docCount;
		private long allDocCount;
		private long valueCount;

		private double doubleSum;
		private double doubleMinValue = Double.POSITIVE_INFINITY;
		private double doubleMaxValue = Double.NEGATIVE_INFINITY;

		private long longSum;
		private long longMinValue = Long.MAX_VALUE;
		private long longMaxValue = Long.MIN_VALUE;

		private DDSketch sketch = null;

		public Stats(boolean floatingPoint) {
			if (floatingPoint) {
				longMinValue = 0;
				longMaxValue = 0;
			}
			else {
				doubleMinValue = 0;
				doubleMaxValue = 0;
			}
			this.floatingPoint = floatingPoint;
		}

		// overload constructor for building w/ precision
		public Stats(boolean floatingPoint, double precision) {
			this(floatingPoint);
			// Cannot perform DDSketch with perfect precision.
			// May indicate that precision was not requested. This prevents quantile from running.
			if (precision > 0.0) {
				this.sketch = DDSketches.unboundedDense(precision);
			}
		}

		public void newDoc(boolean countNonNull) {
			allDocCount++;
			if (countNonNull) {
				docCount++;
			}
		}

		public void newValue(double newValue) {
			this.doubleSum += newValue;
			if (newValue < doubleMinValue) {
				doubleMinValue = newValue;
			}
			if (newValue > doubleMaxValue) {
				doubleMaxValue = newValue;
			}
			this.valueCount++;
			if (this.sketch != null) {
				this.sketch.accept(newValue);
			}
		}

		public void newValue(long newValue) {
			this.longSum += newValue;
			if (newValue < longMinValue) {
				longMinValue = newValue;
			}
			if (newValue > longMaxValue) {
				longMaxValue = newValue;
			}
			this.valueCount++;
			if (this.sketch != null) {
				this.sketch.accept(newValue);
			}
		}

		public boolean isFloatingPoint() {
			return floatingPoint;
		}
	}

}
