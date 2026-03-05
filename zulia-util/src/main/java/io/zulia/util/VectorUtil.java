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
		return (float) (dot / (Math.sqrt(normA) * Math.sqrt(normB)));
	}

}
