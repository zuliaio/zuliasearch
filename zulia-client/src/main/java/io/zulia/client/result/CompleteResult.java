package io.zulia.client.result;

import io.zulia.message.ZuliaQuery;
import io.zulia.util.ResultHelper;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/***
 * Wrapper around ScoredResult to make it more convenient to use
 */
public class CompleteResult {

	private Document document;
	private Document metadata;

	private final ZuliaQuery.ScoredResult scoredResult;

	public CompleteResult(ZuliaQuery.ScoredResult scoredResult) {
		this.scoredResult = scoredResult;
	}

	public String getUniqueId() {
		return this.scoredResult.getUniqueId();
	}

	public float getScore() {
		return this.scoredResult.getScore();
	}

	public int getLuceneShardId() {
		return this.scoredResult.getLuceneShardId();
	}

	public String getIndexName() {
		return this.scoredResult.getIndexName();
	}

	public int getShard() {
		return this.scoredResult.getShard();
	}

	public int getResultIndex() {
		return this.scoredResult.getResultIndex();
	}

	public ZuliaQuery.SortValues getSortValues() {
		return this.scoredResult.getSortValues();
	}

	public long getTimestamp() {
		return this.scoredResult.getTimestamp();
	}

	public Document getDocument() {
		if (document == null) {
			document = ResultHelper.getDocumentFromScoredResult(scoredResult);
		}
		return document;
	}

	public List<ZuliaQuery.HighlightResult> getHighlightResultList() {
		return this.scoredResult.getHighlightResultList();
	}

	public List<String> getHighlightsForField(String field) {
		List<String> fragments = new ArrayList<>();
		for (ZuliaQuery.HighlightResult highlightResult : this.scoredResult.getHighlightResultList()) {
			if (field.equals(highlightResult.getField())) {
				fragments.addAll(highlightResult.getFragmentsList());
			}
		}
		return fragments;
	}

	public List<ZuliaQuery.AnalysisResult> getAnalysisResultList() {
		return this.scoredResult.getAnalysisResultList();
	}

	public Document getMetadata() {
		if (metadata == null) {
			metadata = ZuliaUtil.byteStringToMongoDocument(scoredResult.getResultDocument().getMetadata());
		}
		return metadata;
	}
}
