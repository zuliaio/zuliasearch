package io.zulia.ai.nn.translator;

public class NoOpDenseFeatureGenerator implements DenseFeatureGenerator<float[]> {
	@Override
	public float[] apply(float[] floats) {
		return floats;
	}
}
