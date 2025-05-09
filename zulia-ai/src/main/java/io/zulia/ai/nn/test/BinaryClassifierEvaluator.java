package io.zulia.ai.nn.test;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.types.DataType;
import ai.djl.training.evaluator.Evaluator;
import ai.djl.util.Preconditions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BinaryClassifierEvaluator extends Evaluator {

	private final float threshold;
	protected Map<String, BinaryClassifierStats> binaryClassifierStatsMap;
	protected int axis;

	public BinaryClassifierEvaluator(String name, float threshold) {
		this(name, 1, threshold);
	}

	public BinaryClassifierEvaluator(String name, int axis, float threshold) {
		super(name);
		this.axis = axis;
		this.threshold = threshold;
		this.binaryClassifierStatsMap = new ConcurrentHashMap<>();
	}

	@Override
	public NDArray evaluate(NDList labels, NDList predictions) {
		BinaryClassifierStats binaryClassifierStats = getBinaryClassifierStats(labels, predictions);
		float stat = getStat(binaryClassifierStats);
		return labels.getManager().create(stat);
	}

	@Override
	public void addAccumulator(String key) {
		binaryClassifierStatsMap.put(key, new BinaryClassifierStats());
	}

	@Override
	public void updateAccumulator(String key, NDList labels, NDList predictions) {
		updateAccumulators(new String[] { key }, labels, predictions);
	}

	@Override
	public void updateAccumulators(String[] keys, NDList labels, NDList predictions) {
		BinaryClassifierStats binaryClassifierStats = getBinaryClassifierStats(labels, predictions);

		for (String key : keys) {
			binaryClassifierStatsMap.get(key).merge(binaryClassifierStats);
		}

	}

	protected BinaryClassifierStats getBinaryClassifierStats(NDList labels, NDList predictions) {
		Preconditions.checkArgument(labels.size() == predictions.size(), "labels and prediction length does not match.");
		NDArray label = labels.head();
		NDArray prediction = predictions.head();
		checkLabelShapes(label, prediction, false);

		NDArray predictionReducedInt64 = prediction.gte(threshold).toType(DataType.INT64, false);
		NDArray notPredictionReducedInt64 = predictionReducedInt64.add(-1).mul(-1);

		NDArray labelInt64 = label.toType(DataType.INT64, false);
		NDArray notLabelInt64 = labelInt64.add(-1).mul(-1);

		long truePositive = predictionReducedInt64.mul(labelInt64).countNonzero().sum().getLong();
		long falseNegative = notPredictionReducedInt64.mul(labelInt64).countNonzero().sum().getLong();

		long falsePositives = predictionReducedInt64.mul(notLabelInt64).countNonzero().sum().getLong();
		long trueNegative = notPredictionReducedInt64.mul(notLabelInt64).countNonzero().sum().getLong();

		return new BinaryClassifierStats(truePositive, falsePositives, trueNegative, falseNegative);
	}

	/** {@inheritDoc} */
	@Override
	public void resetAccumulator(String key) {
		binaryClassifierStatsMap.compute(key, (k, v) -> new BinaryClassifierStats());
	}

	/** {@inheritDoc} */
	@Override
	public float getAccumulator(String key) {
		BinaryClassifierStats binaryClassifierStats = binaryClassifierStatsMap.get(key);
		if (binaryClassifierStats == null) {
			return Float.NaN;
		}

		return getStat(binaryClassifierStats);
	}

	protected abstract float getStat(BinaryClassifierStats binaryClassifierStats);
}
