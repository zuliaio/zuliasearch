package io.zulia.server.search.aggregation.stats;

import com.google.protobuf.ByteString;
import io.zulia.message.ZuliaQuery;
import io.zulia.util.sketch.QuantileSketch;

public abstract class Stats<T extends Stats<T>> implements Comparable<T> {

	private final double precision;
	private final QuantileSketch sketch;

	private int ordinal;
	private long docCount;
	private long allDocCount;
	private long valueCount;

	// overload constructor for building w/ precision
	public Stats(double precision) {
		// A sketch cannot have perfect precision, so no precision means no percentiles
		if (precision > 0.0) {
			this.sketch = new QuantileSketch(precision, QuantileSketch.Mapping.CUBIC);
		}
		else {
			this.sketch = null;
		}
		this.precision = precision;
	}

	protected QuantileSketch getSketch() {
		return sketch;
	}

	protected long getDocCount() {
		return docCount;
	}

	protected long getAllDocCount() {
		return allDocCount;
	}

	protected long getValueCount() {
		return valueCount;
	}

	protected double getPrecision() {
		return precision;
	}

	public abstract void handleDocValue(long docValue);

	public void newDoc(boolean hasValues) {
		allDocCount++;
		if (hasValues) {
			docCount++;
		}
	}

	public void tallyValue(double newValue) {
		this.valueCount++;
		if (this.sketch != null && Double.isFinite(newValue)) {
			this.sketch.accept(newValue);
		}
	}

	public void setOrdinal(int ordinal) {
		this.ordinal = ordinal;
	}

	public int getOrdinal() {
		return ordinal;
	}

	public int compareOrdinal(Stats<T> o) {
		return Integer.compare(o.ordinal, ordinal);
	}

	public ZuliaQuery.FacetStatsInternal.Builder buildResponse() {
		ZuliaQuery.FacetStatsInternal.Builder builder = ZuliaQuery.FacetStatsInternal.newBuilder().setDocCount(docCount).setAllDocCount(allDocCount)
				.setValueCount(valueCount);
		if (sketch != null) {
			builder.setStatSketch(ByteString.copyFrom(sketch.toBytes()));
		}
		return builder;
	}

	public void handleNumericValues(long[] numericValues, int numericValueCount) {
		newDoc(numericValueCount != -1);
		for (int j = 0; j < numericValueCount; j++) {
			handleDocValue(numericValues[j]);
		}
	}

	public void merge(Stats<?> other) {

		synchronized (this) {
			this.allDocCount += other.getAllDocCount();
			this.docCount += other.getDocCount();
			this.valueCount += other.getValueCount();
			mergeExtra((T) other);
		}

		if (this.sketch != null) {
			synchronized (this.sketch) {
				this.sketch.mergeWith(other.getSketch());
			}
		}

	}

	protected abstract void mergeExtra(T other);

}