package io.zulia.ai.embedding;

import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

/** Builds a tokenizer for a downloaded model with the configured sequence length. */
public final class ModelTokenizers {

	private ModelTokenizers() {
	}

	public static HuggingFaceTokenizer forModel(Path modelDir, int maxTokens) throws IOException {
		// DJL clamps maxLength to modelMaxLength, and some tokenizer.json files (arctic, gte) carry an
		// upstream 512 there, so both are set from the configured length
		return HuggingFaceTokenizer.newInstance(modelDir,
				Map.of("maxLength", Integer.toString(maxTokens), "modelMaxLength", Integer.toString(maxTokens), "truncation", "true"));
	}
}
