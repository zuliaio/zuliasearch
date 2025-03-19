

package io.zulia.ai.nn.test;

public class BinaryClassifierF1 extends BinaryClassifierEvaluator {
	
	public static final String BCF1 = "BCF1";
	
	public BinaryClassifierF1(float threshold) {
		this(BCF1, threshold);
	}
	
	public BinaryClassifierF1(String name, float threshold) {
		this(name, 1, threshold);
	}
	
	public BinaryClassifierF1(String name, int axis, float threshold) {
		super(name, axis, threshold);
	}
	
	protected float getStat(BinaryClassifierStats binaryClassifierStats) {
		return binaryClassifierStats.getF1();
	}
}
