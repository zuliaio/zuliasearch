package io.zulia.server.analysis.highlight;

import io.zulia.server.analysis.ZuliaPerFieldAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.Scorer;

/**
 * Created by Matt Davis on 6/21/16.
 *
 * @author mdavis
 */
public class ZuliaHighlighter extends Highlighter {
	private final String highlightField;
	private final String storedFieldName;
	private final int numberOfFragments;
	private final ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer;

	public ZuliaHighlighter(Formatter formatter, Scorer fragmentScorer, String highlightField, String storedFieldName, int numberOfFragments,
			ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer) {
		super(formatter, fragmentScorer);
		this.numberOfFragments = numberOfFragments;
		this.storedFieldName = storedFieldName;
		this.highlightField = highlightField;
		this.zuliaPerFieldAnalyzer = zuliaPerFieldAnalyzer;
	}

	public String getStoredFieldName() {
		return storedFieldName;
	}

	public int getNumberOfFragments() {
		return numberOfFragments;
	}

	public TokenStream getTokenStream(String content) {
		return zuliaPerFieldAnalyzer.tokenStream(highlightField, content);
	}
}
