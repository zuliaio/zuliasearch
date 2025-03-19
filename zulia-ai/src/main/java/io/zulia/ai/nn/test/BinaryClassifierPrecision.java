

package io.zulia.ai.nn.test;

public class BinaryClassifierPrecision extends BinaryClassifierEvaluator {
	
	public static final String BC_PRECISION = "BCPrecision";
	
	public BinaryClassifierPrecision(float threshold) {
		this(BC_PRECISION, threshold);
	}
	
	public BinaryClassifierPrecision(String name, float threshold) {
		this(name, 1, threshold);
	}
	
	public BinaryClassifierPrecision(String name, int axis, float threshold) {
		super(name, axis, threshold);
	}
	
	protected float getStat(BinaryClassifierStats binaryClassifierStats) {
		return binaryClassifierStats.getPrecision();
	}
}
