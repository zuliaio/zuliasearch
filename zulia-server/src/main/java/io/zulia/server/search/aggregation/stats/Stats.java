package io.zulia.server.search.aggregation.stats;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketchProtoBinding;
import com.datadoghq.sketch.ddsketch.DDSketches;
import io.zulia.message.ZuliaQuery;

import java.util.concurrent.atomic.AtomicLong;

public abstract class Stats<T extends Stats<T>> implements Comparable<T> {

	protected int ordinal;
	private final AtomicLong docCount;
	private final AtomicLong allDocCount;
	private final AtomicLong valueCount;

	private final DDSketch sketch;

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
		this.docCount = new AtomicLong();
		this.allDocCount = new AtomicLong();
		this.valueCount = new AtomicLong();
	}

	public abstract void handleDocValue(long docValue);

	public void newDoc(boolean hasValues) {
		allDocCount.getAndIncrement();
		if (hasValues) {
			docCount.getAndIncrement();
		}
	}

	public void tallyValue(double newValue) {
		this.valueCount.getAndIncrement();
		if (this.sketch != null) {
			synchronized (this.sketch) {
				this.sketch.accept(newValue);
			}
		}
	}

	public void setOrdinal(int ordinal) {
		this.ordinal = ordinal;
	}

	public int getOrdinal() {
		return ordinal;
	}

	public ZuliaQuery.FacetStatsInternal.Builder buildResponse() {
		ZuliaQuery.FacetStatsInternal.Builder builder = ZuliaQuery.FacetStatsInternal.newBuilder().setDocCount(docCount.get()).setAllDocCount(allDocCount.get())
				.setValueCount(valueCount.get());
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
}