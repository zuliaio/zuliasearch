package io.zulia.server;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;

import java.io.Reader;

public class HtmlStrippingWrapper extends AnalyzerWrapper {

	private final Analyzer baseAnalyzer;

	public HtmlStrippingWrapper(Analyzer baseAnalyzer) {
		super(Analyzer.PER_FIELD_REUSE_STRATEGY);
		this.baseAnalyzer = baseAnalyzer;
	}

	@Override
	protected Analyzer getWrappedAnalyzer(String fieldName) {
		return baseAnalyzer;
	}

	@Override
	protected Reader wrapReader(String fieldName, Reader reader) {
		return new HTMLStripCharFilter(reader);
	}

	@Override
	protected Reader wrapReaderForNormalization(String fieldName, Reader reader) {
		return new HTMLStripCharFilter(reader);
	}

}
