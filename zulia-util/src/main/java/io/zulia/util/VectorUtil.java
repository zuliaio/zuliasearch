package io.zulia.util;

public class VectorUtil {

	public static float cosineSimilarity(float[] a, float[] b) {
		if (a.length != b.length) {
			throw new IllegalArgumentException("Vector dimensions must match: " + a.length + " != " + b.length);
		}
		float dot = 0f, normA = 0f, normB = 0f;
		for (int i = 0; i < a.length; i++) {
			dot += a[i] * b[i];
			normA += a[i] * a[i];
			normB += b[i] * b[i];
		}
		double denom = Math.sqrt(normA) * Math.sqrt(normB);
		// A zero-magnitude vector has no direction, so cosine is undefined, but returning 0 is numerically safer
		if (denom == 0.0) {
			return 0f;
		}
		return (float) (dot / denom);
	}

}
