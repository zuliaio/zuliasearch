package io.zulia.ai.nn.test;

public class BinaryClassifierRecall extends BinaryClassifierEvaluator {

	public static final String BC_RECALL = "BCRecall";

	public BinaryClassifierRecall(float threshold) {
		this(BC_RECALL, threshold);
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
