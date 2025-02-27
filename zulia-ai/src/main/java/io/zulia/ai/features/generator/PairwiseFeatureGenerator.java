package io.zulia.ai.features.generator;

public interface PairwiseFeatureGenerator<T> extends FeatureGenerator<PairwiseFeatureGenerator.Pair<T>> {
	
	record Pair<T>(T a, T b) {
	}
	
	double[] getFeatures(T e1, T e2);
	
	@Override
	default double[] generateFeatures(Pair<T> tPair) {
		return getFeatures(tPair.a(), tPair.b());
	}
}
