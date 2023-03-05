package io.zulia.server.search.stat;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketchProtoBinding;
import com.datadoghq.sketch.ddsketch.DDSketches;
import io.zulia.message.ZuliaQuery;

public abstract class Stats<T extends Stats<T>> implements Comparable<T> {

	protected int ordinal;
	private long docCount;
	private long allDocCount;
	private long valueCount;

	private DDSketch sketch;

	// overload constructor for building w/ precision
	public Stats(double precision) {
		// Cannot perform DDSketch with perfect precision.
		// This prevents quantile from running.
		if (precision > 0.0) {
			this.sketch = DDSketches.unboundedDense(precision);
		}
	}

	public abstract void handleDocValue(long docValue);

	public void newDoc(boolean countNonNull) {
		allDocCount++;
		if (countNonNull) {
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

	public ZuliaQuery.FacetStatsInternal.Builder buildResponse() {
		ZuliaQuery.FacetStatsInternal.Builder builder = ZuliaQuery.FacetStatsInternal.newBuilder().setDocCount(docCount).setAllDocCount(allDocCount)
				.setValueCount(valueCount);
		if (sketch != null) {
			builder.setStatSketch(DDSketchProtoBinding.toProto(sketch));
		}
		return builder;
	}

}