package io.zulia.ai.features.scaler;

import io.zulia.ai.features.stat.FeatureStat;

public class PercentileClippingFeatureScaler extends FeatureStatScaler {

	public enum NormalizeRange {
		P25_TO_P75,
		P10_TO_P90,
		P05_TO_P95
	}

	private final Double clip;
	private final NormalizeRange normalizeRange;

	public PercentileClippingFeatureScaler(FeatureStat[] featureStats) {
		this(featureStats, NormalizeRange.P25_TO_P75, 3.0);
	}

	public PercentileClippingFeatureScaler(FeatureStat[] featureStats, NormalizeRange normalizeRange, Double clip) {
		super(featureStats, "PercentileClipping");
		this.normalizeRange = normalizeRange;
		this.clip = clip;
	}

	@Override
	protected double scaleFeature(FeatureStat featureStat, double value) {

		double range = switch (normalizeRange) {
			case P25_TO_P75 -> featureStat.getP75() - featureStat.getP25();
			case P10_TO_P90 -> featureStat.getP90() - featureStat.getP10();
			case P05_TO_P95 -> featureStat.getP95() - featureStat.getP05();
		};
		if (range == 0.0d) {
			range = featureStat.getMax() - featureStat.getMin();
		}

		double retVal = ((value - featureStat.getAvg()) / range);
		if (Double.isNaN(retVal)) {
			retVal = 0;
		}
		if (clip != null) {
			if (retVal > clip) {
				retVal = clip;
			}
			if (retVal < -clip) {
				retVal = -clip;
			}
		}
		return retVal;
	}

	@Override
	public String toString() {
		return "PercentileClippingFeatureScaler(" + normalizeRange + ", " + clip + ")";
	}
}
