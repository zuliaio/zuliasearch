package io.zulia.ai.nn.test;

/** Accuracy from the confusion counts, so binary training reports it without the DJL Accuracy evaluator. */
public class BinaryClassifierAccuracy extends BinaryClassifierEvaluator {

	public static final String BC_ACCURACY = "BCAccuracy";

	public BinaryClassifierAccuracy(float threshold) {
		this(BC_ACCURACY, threshold);
	}

	public BinaryClassifierAccuracy(String name, float threshold) {
		this(name, 1, threshold);
	}

	public BinaryClassifierAccuracy(String name, int axis, float threshold) {
		super(name, axis, threshold);
	}

	@Override
	protected float getStat(BinaryClassifierStats binaryClassifierStats) {
		return binaryClassifierStats.getAccuracy();
	}
}
