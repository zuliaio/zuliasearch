package io.zulia.server.search.aggregation.stats;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketchProtoBinding;
import com.datadoghq.sketch.ddsketch.DDSketches;
import io.zulia.message.ZuliaQuery;

public abstract class Stats<T extends Stats<T>> implements Comparable<T> {

	private final double precision;
	private final DDSketch sketch;

	private int ordinal;
	private long docCount;
	private long allDocCount;
	private long valueCount;

	// overload constructor for building w/ precision
	public Stats(double precision) {
		// Cannot perform DDSketch with perfect precision.
		// This prevents quantile from running.
		if (precision > 0.0) {
			this.sketch = DDSketches.unboundedDense(precision);
		}
		else {
			this.sketch = null;
		}
		this.precision = precision;
	}

	protected DDSketch getSketch() {
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
		if (this.sketch != null) {
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
			builder.setStatSketch(DDSketchProtoBinding.toProto(sketch));
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