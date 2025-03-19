package io.zulia.ai.nn.translator;

public interface PairwiseDenseFeatureGenerator<T> extends DenseFeatureGenerator<PairwiseDenseFeatureGenerator.Pair<T>> {
	record Pair<R>(R first, R second) {
	
	}
	
	float[] computeFeatures(T first, T second);
	
	@Override
	default float[] apply(Pair<T> tPair) {
		return computeFeatures(tPair.first(), tPair.second());
	}
}
