package io.zulia.ai.nn.test;

public class BinaryClassifierStats extends ClassifierStats<Boolean> {

	private long truePositive;
	private long falsePositive;
	private long trueNegative;
	private long falseNegative;

	public BinaryClassifierStats() {
	}

	public BinaryClassifierStats(long truePositive, long falsePositive, long trueNegative, long falseNegative) {
		this.truePositive = truePositive;
		this.falsePositive = falsePositive;
		this.trueNegative = trueNegative;
		this.falseNegative = falseNegative;
	}

	@Override
	protected void mergeSpecific(ClassifierStats<Boolean> other) {
		if (other instanceof BinaryClassifierStats otherBCS) {
			this.truePositive += otherBCS.truePositive;
			this.falsePositive += otherBCS.falsePositive;
			this.trueNegative += otherBCS.trueNegative;
			this.falseNegative += otherBCS.falseNegative;
		}
	}

	@Override
	protected void evaluateResultSpecific(Boolean predicted, Boolean actual) {
		if (actual) {
			if (predicted) {
				truePositive++;
			}
			else {
				falseNegative++;
			}
		}
		else {
			if (predicted) {
				falsePositive++;
			}
			else {
				trueNegative++;
			}
		}
	}

	// a stat with an empty denominator is 0 rather than NaN, so an epoch with no positive predictions
	// can never win a best-epoch comparison and a zero F1 reads as the worst epoch it is
	@Override
	public float getPrecision() {
		return ratio(truePositive, truePositive + falsePositive);
	}

	@Override
	public float getRecall() {
		return ratio(truePositive, truePositive + falseNegative);
	}

	@Override
	public float getF1() {
		float precision = getPrecision();
		float recall = getRecall();
		return ratio(2 * precision * recall, precision + recall);
	}

	/** Accuracy from the confusion counts, which are filled on every construction path unlike the generic counters. */
	@Override
	public float getAccuracy() {
		return ratio(truePositive + trueNegative, truePositive + trueNegative + falsePositive + falseNegative);
	}

	private static float ratio(double numerator, double denominator) {
		return denominator == 0 ? 0f : (float) (numerator / denominator);
	}

	public long getTruePositive() {
		return truePositive;
	}

	public long getFalsePositive() {
		return falsePositive;
	}

	public long getTrueNegative() {
		return trueNegative;
	}

	public long getFalseNegative() {
		return falseNegative;
	}

	@Override
	public String toString() {
		return "F1: " + getF1() + " Precision: " + getPrecision() + " Recall: " + getRecall();
	}

}
