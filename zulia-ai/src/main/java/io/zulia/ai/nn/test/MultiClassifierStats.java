package io.zulia.ai.nn.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MultiClassifierStats extends ClassifierStats<Integer> {
	private final List<BinaryClassifierStats> stats;

	public MultiClassifierStats(int stats) {
		this.stats = new ArrayList<>(stats);
	}

	@Override
	protected void mergeSpecific(ClassifierStats<Integer> other) {
		if (other instanceof MultiClassifierStats mcsOther) {
			if (mcsOther.stats.size() != stats.size()) {
				throw new RuntimeException("Stats can't be merged if they don't match dimensions!");
			}

			// Merge each category
			for (int i = 0; i < stats.size(); i++) {
				stats.get(i).merge(mcsOther.stats.get(i));
			}
		}
	}

	@Override
	protected void evaluateResultSpecific(Integer predicted, Integer actual) {
		for (int i = 0; i < stats.size(); i++) {
			// Reduce to a binary classifier on this category
			boolean predPos = predicted == i; // Is the prediction in the category of interest?
			boolean actualPos = actual == i; // Is the actual value in the category of interest?

			// Add to the stats for this category
			stats.get(i).evaluateResult(predPos, actualPos);
		}
		// Help superclass
		countPrediction(Objects.equals(predicted, actual));
	}

	// Nothing uses true negatives, which will be massively over-compiled in a global metric
	public float getPrecision() {
		return combineStats().getPrecision();
	}

	public float getRecall() {
		return combineStats().getRecall();
	}

	public float getF1() {
		return combineStats().getF1();
	}

	public float categoryPrecision(int i) {
		return stats.get(i).getPrecision();
	}

	public float categoryRecall(int i) {
		return stats.get(i).getPrecision();
	}

	public float categoryF1(int i) {
		return stats.get(i).getF1();
	}

	public List<BinaryClassifierStats> getStats() {
		return stats;
	}

	private BinaryClassifierStats combineStats() {
		BinaryClassifierStats combined = new BinaryClassifierStats();
		stats.forEach(combined::merge); // Merge all stats into a single holder
		return combined;
	}

	@Override
	public String toString() {
		return "Categories: " + stats.size() + " Accuracy: " + getAccuracy() + " F1: " + getF1() + " Precision: " + getPrecision() + " Recall: " + getRecall();
	}
}
