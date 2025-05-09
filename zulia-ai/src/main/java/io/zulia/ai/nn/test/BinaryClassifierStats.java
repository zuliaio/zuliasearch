package io.zulia.ai.nn.test;

public class BinaryClassifierStats {

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

	public void merge(BinaryClassifierStats other) {
		this.truePositive += other.truePositive;
		this.falsePositive += other.falsePositive;
		this.trueNegative += other.trueNegative;
		this.falseNegative += other.falseNegative;
	}

	public void evaluateResult(boolean predicted, boolean actual) {
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

	public float getPrecision() {
		return (float) truePositive / (truePositive + falsePositive);
	}

	public float getRecall() {
		return (float) truePositive / (truePositive + falseNegative);
	}

	public float getF1() {
		float precision = getPrecision();
		float recall = getRecall();
		return 2 * precision * recall / (precision + recall);
	}

	@Override
	public String toString() {
		return "F1: " + getF1() + " Precision: " + getPrecision() + " Recall: " + getRecall();
	}

}
