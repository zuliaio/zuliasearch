package io.zulia.server.search.aggregation.stats;

import io.zulia.message.ZuliaIndex;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.search.aggregation.facets.FacetInfo;
import io.zulia.server.search.aggregation.ordinal.DoubleMapStatOrdinalStorage;
import io.zulia.server.search.aggregation.ordinal.LongMapStatOrdinalStorage;
import io.zulia.server.search.aggregation.ordinal.MapStatOrdinalStorage;
import io.zulia.server.search.aggregation.ordinal.OrdinalConsumer;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;

public class NumericFieldStatInfo extends FacetInfo implements OrdinalConsumer {

	private long[] numericValues;

	private int numericValueCount;
	private String numericFieldName;
	private String sortFieldName;

	private Double facetPrecision;

	private ZuliaIndex.FieldConfig.FieldType numericFieldType;
	private Stats<?> globalStats;
	private MapStatOrdinalStorage<?> facetStatStorage;

	private SortedNumericDocValues numericDocValues;

	public NumericFieldStatInfo(String numericFieldName) {
		this.numericValues = new long[1];
		this.numericFieldName = numericFieldName;
	}

	public void setNumericFieldType(ZuliaIndex.FieldConfig.FieldType numericFieldType) {
		this.numericFieldType = numericFieldType;
	}

	public boolean hasGlobal() {
		return globalStats != null;
	}

	public void enableFacetWithPrecision(double facetPrecision) {
		if (this.facetPrecision != null && Double.compare(this.facetPrecision, facetPrecision) != 0) {
			throw new IllegalArgumentException("Facet precision for field <" + numericFieldName + "> must be the same for all facets.");
		}
		this.facetPrecision = facetPrecision;

		if (FieldTypeUtil.isNumericDoubleFieldType(numericFieldType)) {
			facetStatStorage = new DoubleMapStatOrdinalStorage(() -> new DoubleDoubleStats(facetPrecision));
		}
		else if (FieldTypeUtil.isNumericFloatFieldType(numericFieldType)) {
			facetStatStorage = new DoubleMapStatOrdinalStorage(() -> new FloatDoubleStats(facetPrecision));
		}
		else if (FieldTypeUtil.isStoredAsLong(numericFieldType)) {
			facetStatStorage = new LongMapStatOrdinalStorage(() -> new LongLongStats(facetPrecision));
		}
		else if (FieldTypeUtil.isStoredAsInt(numericFieldType)) {
			facetStatStorage = new LongMapStatOrdinalStorage(() -> new IntLongStats(facetPrecision));
		}
		else {
			throw new IllegalArgumentException("Can not generate stat storage for field type <" + numericFieldType + ">");
		}

	}

	public void enableGlobal(double precision) {
		if (FieldTypeUtil.isNumericDoubleFieldType(numericFieldType)) {
			globalStats = new DoubleDoubleStats(precision);
		}
		else if (FieldTypeUtil.isNumericFloatFieldType(numericFieldType)) {
			globalStats = new FloatDoubleStats(precision);
		}
		else if (FieldTypeUtil.isStoredAsLong(numericFieldType)) {
			globalStats = new LongLongStats(precision);
		}
		else if (FieldTypeUtil.isStoredAsInt(numericFieldType)) {
			globalStats = new IntLongStats(precision);
		}
		else {
			throw new IllegalArgumentException("Can not generate stat constructor for field type <" + numericFieldType + ">");
		}
	}

	public void setSortFieldName(String sortFieldName) {
		this.sortFieldName = sortFieldName;
	}

	public void setReader(LeafReader reader) throws IOException {
		numericDocValues = DocValues.getSortedNumeric(reader, sortFieldName);
	}

	public void advanceNumericValues(int doc) throws IOException {
		numericValueCount = -1;
		if (numericDocValues.advanceExact(doc)) {
			numericValueCount = numericDocValues.docValueCount();
			if (numericValueCount > numericValues.length) {
				numericValues = new long[numericValueCount];
			}
			for (int j = 0; j < numericValueCount; j++) {
				numericValues[j] = numericDocValues.nextValue();
			}
		}
	}

	public long[] getNumericValues() {
		return numericValues;
	}

	public int getNumericValueCount() {
		return numericValueCount;
	}

	public Stats<?> getGlobalStats() {
		return globalStats;
	}

	public MapStatOrdinalStorage<?> getFacetStatStorage() {
		return facetStatStorage;
	}

	public String getNumericFieldName() {
		return numericFieldName;
	}

	@Override
	public void handleOrdinal(int ordinal) {
		Stats<?> stats = facetStatStorage.getOrCreateStat(ordinal);
		stats.handleNumericValues(numericValues, numericValueCount);
	}
}
