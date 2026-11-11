package io.zulia.util.sketch;

/**
 * Quantile sketch with the DDSketch relative accuracy guarantee: every quantile it returns is within the relative
 * accuracy of the true quantile. Values are counted in exponentially sized bins. The default cubic mapping gives the
 * same bins and quantiles as the Datadog library's unbounded dense sketch, so quantiles do not move when switching.
 * The count is exact. The sum, min and max are not kept, so the per value path is the bin update alone.
 * {@link SummarizedQuantileSketch} keeps them as well. Both share the 5.6.0 byte layout, and this one leaves its sum, min
 * and max slots empty.
 * Counts are ints per bin. The byte form is a small header followed by varint bin counts with runs of empty bins
 * collapsed, and one sketch can be merged straight from another's bytes. Not thread safe.
 */
public final class QuantileSketch extends SketchBins implements Quantiles {

	/** How a value is mapped to a bin. Cubic is the default and the fastest of the exact-guarantee mappings. */
	public enum Mapping {
		/** Cubic interpolation of the logarithm, the library's default. */
		CUBIC(0),
		/** The true logarithm, slowest, fewest bins. */
		LOG(1),
		/** Bitwise linear interpolation, fastest, about 46 percent more bins, quantiles differ within the guarantee. */
		LINEAR(2);

		final int code;

		Mapping(int code) {
			this.code = code;
		}

		static Mapping fromCode(int code) {
			for (Mapping mapping : values()) {
				if (mapping.code == code) {
					return mapping;
				}
			}
			throw new IllegalArgumentException("Unknown sketch mapping code " + code + ", expected 0 (cubic), 1 (log) or 2 (linear)");
		}
	}

	/** A sketch with the cubic mapping. */
	public QuantileSketch(double relativeAccuracy) {
		this(relativeAccuracy, Mapping.CUBIC);
	}

	public QuantileSketch(double relativeAccuracy, Mapping mapping) {
		super(relativeAccuracy, mapping);
	}

	@Override
	public void accept(double value) {
		addToBins(value);
	}

	/** Adds the other sketch's values to this one. */
	public void mergeWith(QuantileSketch other) {
		mergeBins(other);
	}

	public byte[] toBytes() {
		return toBytes(SketchSummary.EMPTY);
	}

	/** Reads a sketch written by either kind of sketch skipping the sum, min and max. */
	public static QuantileSketch fromBytes(byte[] bytes) {
		SketchHeader sketchHeader = readHeader(bytes);
		QuantileSketch sketch = new QuantileSketch(sketchHeader.requestedAccuracy(), sketchHeader.mapping());
		sketch.mergeBinsFrom(bytes);
		return sketch;
	}

	/** Adds a sketch written by either kind of sketch straight into this one without building it first. */
	public void mergeFrom(byte[] bytes) {
		mergeBinsFrom(bytes);
	}
}
