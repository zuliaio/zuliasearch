

package io.zulia.ai.nn.test;

public class BinaryClassifierPrecision extends BinaryClassifierEvaluator {
	
	public BinaryClassifierPrecision(float threshold) {
		this("BCPrecision", threshold);
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
