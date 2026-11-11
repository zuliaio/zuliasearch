package io.zulia.util.sketch;

/** The exact sum, min and max that ride along with the bins in a sketch's byte form. */
record SketchSummary(double sum, double min, double max) {

	/** What a plain sketch writes, the same as a summarized sketch with no values. */
	static final SketchSummary EMPTY = new SketchSummary(0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);

	/** True when these numbers cover no values, which for a sketch holding values means a plain sketch wrote them. */
	boolean coversNoValues() {
		return min > max;
	}
}
