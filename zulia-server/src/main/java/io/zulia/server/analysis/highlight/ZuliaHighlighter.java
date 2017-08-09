package io.zulia.server.analysis.highlight;

import io.zulia.message.ZuliaQuery.HighlightRequest;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.Scorer;

/**
 * Created by Matt Davis on 6/21/16.
 * @author mdavis
 */
public class ZuliaHighlighter extends Highlighter {
	private final HighlightRequest highlightRequest;

	public ZuliaHighlighter(Formatter formatter, Scorer fragmentScorer, HighlightRequest highlightRequest) {
		super(formatter, fragmentScorer);
		this.highlightRequest = highlightRequest;
	}

	public HighlightRequest getHighlight() {
		return highlightRequest;
	}
}
