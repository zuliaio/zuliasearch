package io.zulia.server.index;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;

import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongToDoubleFunction;

/**
 * A {@link DoubleValuesSource} that reads from {@link SortedNumericDocValues}, selecting the
 * minimum value for multi-valued fields. Lucene's built-in factory methods
 * ({@link DoubleValuesSource#fromIntField}, etc.) only work with single-valued
 * {@link org.apache.lucene.index.NumericDocValues}, so this class is needed for sorted numeric
 * (multi-valued) fields.
 */
public class SortedNumericDoubleValuesSource extends DoubleValuesSource {

	private final String field;
	private final SortField.Type sortType;
	private final LongToDoubleFunction decoder;

	public SortedNumericDoubleValuesSource(String field, SortField.Type sortType, LongToDoubleFunction decoder) {
		this.field = field;
		this.sortType = sortType;
		this.decoder = decoder;
	}

	public static SortedNumericDoubleValuesSource fromInt(String field) {
		return new SortedNumericDoubleValuesSource(field, SortField.Type.INT, v -> (double) v);
	}

	public static SortedNumericDoubleValuesSource fromLong(String field) {
		return new SortedNumericDoubleValuesSource(field, SortField.Type.LONG, v -> (double) v);
	}

	public static SortedNumericDoubleValuesSource fromFloat(String field) {
		return new SortedNumericDoubleValuesSource(field, SortField.Type.FLOAT, v -> (double) NumericUtils.sortableIntToFloat((int) v));
	}

	public static SortedNumericDoubleValuesSource fromDouble(String field) {
		return new SortedNumericDoubleValuesSource(field, SortField.Type.DOUBLE, NumericUtils::sortableLongToDouble);
	}

	@Override
	public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
		SortedNumericDocValues sortedNumeric = DocValues.getSortedNumeric(ctx.reader(), field);
		NumericDocValues numericDocValues = SortedNumericSelector.wrap(sortedNumeric, SortedNumericSelector.Type.MIN, sortType);
		return new DoubleValues() {
			@Override
			public double doubleValue() throws IOException {
				return decoder.applyAsDouble(numericDocValues.longValue());
			}

			@Override
			public boolean advanceExact(int target) throws IOException {
				return numericDocValues.advanceExact(target);
			}
		};
	}

	@Override
	public boolean needsScores() {
		return false;
	}

	@Override
	public DoubleValuesSource rewrite(IndexSearcher reader) {
		return this;
	}

	@Override
	public int hashCode() {
		return Objects.hash(field, sortType);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		SortedNumericDoubleValuesSource other = (SortedNumericDoubleValuesSource) obj;
		return Objects.equals(field, other.field) && sortType == other.sortType;
	}

	@Override
	public String toString() {
		return "sortedNumeric(" + field + ")";
	}

	@Override
	public boolean isCacheable(LeafReaderContext ctx) {
		return DocValues.isCacheable(ctx, field);
	}
}
