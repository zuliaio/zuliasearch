package io.zulia.ai.embedding;

import ai.djl.ndarray.NDList;
import ai.djl.translate.Batchifier;
import ai.djl.translate.Translator;
import ai.djl.translate.TranslatorContext;

import java.util.List;

/**
 * Names the input tensors of a text translator (input_ids, attention_mask, token_type_ids). DJL leaves the
 * names unset, so OnnxRuntime binds by position and a graph declared in another order silently receives
 * swapped tensors. With names present the engine binds by name and the declared order no longer matters.
 */
public class NamedInputTranslator<O> implements Translator<String, O> {

	static final String[] INPUT_NAMES = { "input_ids", "attention_mask", "token_type_ids" };

	private final Translator<String, O> delegate;

	public NamedInputTranslator(Translator<String, O> delegate) {
		this.delegate = delegate;
	}

	static NDList named(NDList list) {
		for (int i = 0; i < list.size() && i < INPUT_NAMES.length; i++) {
			list.get(i).setName(INPUT_NAMES[i]);
		}
		return list;
	}

	@Override
	public NDList processInput(TranslatorContext ctx, String input) throws Exception {
		return named(delegate.processInput(ctx, input));
	}

	@Override
	public NDList batchProcessInput(TranslatorContext ctx, List<String> inputs) throws Exception {
		return named(delegate.batchProcessInput(ctx, inputs));
	}

	@Override
	public O processOutput(TranslatorContext ctx, NDList list) throws Exception {
		return delegate.processOutput(ctx, list);
	}

	@Override
	public List<O> batchProcessOutput(TranslatorContext ctx, NDList list) throws Exception {
		return delegate.batchProcessOutput(ctx, list);
	}

	@Override
	public Batchifier getBatchifier() {
		return delegate.getBatchifier();
	}
}
