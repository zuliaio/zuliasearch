package io.zulia.rest.dto;

import org.bson.Document;

import java.util.List;

public class ScoredResultDTO {
	private String id;
	private Float score;
	private String indexName;
	private Document document;
	private List<HighlightDTO> highlights;
	private List<AnalysisResultDTO> analysis;

	public ScoredResultDTO() {
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Float getScore() {
		return score;
	}

	public void setScore(Float score) {
		this.score = score;
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	public Document getDocument() {
		return document;
	}

	public void setDocument(Document document) {
		this.document = document;
	}

	public List<HighlightDTO> getHighlights() {
		return highlights;
	}

	public void setHighlights(List<HighlightDTO> highlights) {
		this.highlights = highlights;
	}

	public List<AnalysisResultDTO> getAnalysis() {
		return analysis;
	}

	public void setAnalysis(List<AnalysisResultDTO> analysis) {
		this.analysis = analysis;
	}

	@Override
	public String toString() {
		return "ScoredResultDTO{" + "id='" + id + '\'' + ", score=" + score + ", indexName='" + indexName + '\'' + ", document=" + document + ", highlights="
				+ highlights + ", analysis=" + analysis + '}';
	}
}
