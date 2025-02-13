

package io.zulia.ai.nn.test;

public class BinaryClassifierRecall extends BinaryClassifierEvaluator {
	
	public BinaryClassifierRecall(float threshold) {
		this("BCRecall", threshold);
	}
	
	public BinaryClassifierRecall(String name, float threshold) {
		this(name, 1, threshold);
	}
	
	public BinaryClassifierRecall(String name, int axis, float threshold) {
		super(name, axis, threshold);
	}
	
	protected float getStat(BinaryClassifierStats binaryClassifierStats) {
		return binaryClassifierStats.getRecall();
	}
}
