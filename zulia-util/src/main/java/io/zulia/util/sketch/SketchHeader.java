package io.zulia.util.sketch;

import io.zulia.util.sketch.QuantileSketch.Mapping;

/**
 * The front of a sketch's byte form: format version, mapping, accuracy, then the sum, min and max. It comes before the bins
 * so a reader can build a sketch of the right shape first.
 */
record SketchHeader(Mapping mapping, double requestedAccuracy, SketchSummary summary) {

	private static final int FORMAT_VERSION = 1;

	void write(SketchByteWriter out) {
		out.varLong(FORMAT_VERSION);
		out.varLong(mapping.code);
		out.fixedDouble(requestedAccuracy);
		out.fixedDouble(summary.sum());
		out.fixedDouble(summary.min());
		out.fixedDouble(summary.max());
	}

	static SketchHeader read(SketchByteReader in) {
		long version = in.varLong();
		if (version != FORMAT_VERSION) {
			throw new IllegalArgumentException("Unsupported sketch format version " + version + ", this build reads version " + FORMAT_VERSION);
		}
		Mapping mapping = Mapping.fromCode(Math.toIntExact(in.varLong()));
		double requestedAccuracy = in.fixedDouble();
		return new SketchHeader(mapping, requestedAccuracy, new SketchSummary(in.fixedDouble(), in.fixedDouble(), in.fixedDouble()));
	}
}
