package io.zulia.ai.sparse;

import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.translate.Batchifier;
import ai.djl.translate.Translator;
import ai.djl.translate.TranslatorContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SparseTranslator implements Translator<String, Map<String, Float>> {

	private static final String ATTENTION_MASK_KEY = "attention_mask";

	private final HuggingFaceTokenizer tokenizer;
	private final String[] vocabulary;
	private final float weightThreshold;
	private final int maxTerms;
	private final boolean includeTokenTypes;

	static final Set<String> SPECIAL_TOKENS = Set.of(
			// BERT
			"[CLS]", "[SEP]", "[PAD]", "[UNK]", "[MASK]",
			// RoBERTa
			"<s>", "</s>", "<pad>", "<unk>", "<mask>"
	);

	SparseTranslator(HuggingFaceTokenizer tokenizer, String[] vocabulary, float weightThreshold, int maxTerms,
			boolean includeTokenTypes) {
		this.tokenizer = tokenizer;
		this.vocabulary = vocabulary;
		this.weightThreshold = weightThreshold;
		this.maxTerms = maxTerms;
		this.includeTokenTypes = includeTokenTypes;
	}

	@Override
	public Batchifier getBatchifier() {
		return Batchifier.STACK;
	}

	@Override
	public NDList processInput(TranslatorContext ctx, String input) {
		Encoding encoding = tokenizer.encode(input);
		long[] inputIds = encoding.getIds();
		long[] attentionMask = encoding.getAttentionMask();

		NDManager manager = ctx.getNDManager();
		NDArray ids = manager.create(inputIds);          // [seq_len]
		NDArray mask = manager.create(attentionMask);    // [seq_len]

		ctx.setAttachment(ATTENTION_MASK_KEY, mask);

		if (includeTokenTypes) {
			NDArray types = manager.zeros(ids.getShape(), DataType.INT64);
			return new NDList(ids, mask, types);
		}
		return new NDList(ids, mask);
	}

	@Override
	public Map<String, Float> processOutput(TranslatorContext ctx, NDList list) {
		NDArray logits = list.getFirst(); // [seq_len, vocab_size]
		NDArray mask = (NDArray) ctx.getAttachment(ATTENTION_MASK_KEY); // [seq_len]

		// Sparse activation: log(1 + ReLU(logits)) * attention_mask, max-pooled over sequence
		try (NDManager scope = ctx.getNDManager().newSubManager()) {
			NDArray relu = logits.clip(0, Float.MAX_VALUE);
			NDArray activated = relu.add(1).log();
			NDArray expandedMask = mask.expandDims(1).toType(DataType.FLOAT32, false); // [seq_len, 1]
			NDArray masked = activated.mul(expandedMask);
			NDArray sparse = masked.max(new int[] { 0 }); // [vocab_size]

			float[] weights = sparse.toFloatArray();
			return buildSparseMap(weights);
		}
	}

	private Map<String, Float> buildSparseMap(float[] weights) {
		record TokenWeight(String token, float weight) {
		}

		List<TokenWeight> candidates = new ArrayList<>();
		int limit = Math.min(weights.length, vocabulary.length);

		for (int i = 0; i < limit; i++) {
			if (weights[i] > weightThreshold && vocabulary[i] != null && !isSpecialToken(vocabulary[i])) {
				candidates.add(new TokenWeight(vocabulary[i], weights[i]));
			}
		}

		candidates.sort((a, b) -> Float.compare(b.weight(), a.weight()));

		var result = new LinkedHashMap<String, Float>();
		int count = Math.min(candidates.size(), maxTerms);
		for (int i = 0; i < count; i++) {
			result.put(candidates.get(i).token(), candidates.get(i).weight());
		}
		return result;
	}

	private static boolean isSpecialToken(String token) {
		return SPECIAL_TOKENS.contains(token);
	}
}
