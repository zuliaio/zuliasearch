package io.zulia.rest.dto;

import java.util.List;

public class SearchResultsDTO {
	private long totalHits;
	private String cursor;
	private List<AnalysisDTO> analysis;
	private List<ScoredResultDTO> results;
	private List<FacetsDTO> facets;
	private List<DistinctCountDTO> distinctCounts;

	public SearchResultsDTO() {
	}

	public long getTotalHits() {
		return totalHits;
	}

	public void setTotalHits(long totalHits) {
		this.totalHits = totalHits;
	}

	public String getCursor() {
		return cursor;
	}

	public void setCursor(String cursor) {
		this.cursor = cursor;
	}

	public List<AnalysisDTO> getAnalysis() {
		return analysis;
	}

	public void setAnalysis(List<AnalysisDTO> analysis) {
		this.analysis = analysis;
	}

	public List<ScoredResultDTO> getResults() {
		return results;
	}

	public void setResults(List<ScoredResultDTO> results) {
		this.results = results;
	}

	public List<FacetsDTO> getFacets() {
		return facets;
	}

	public void setFacets(List<FacetsDTO> facets) {
		this.facets = facets;
	}

	public List<DistinctCountDTO> getDistinctCounts() {
		return distinctCounts;
	}

	public void setDistinctCounts(List<DistinctCountDTO> distinctCounts) {
		this.distinctCounts = distinctCounts;
	}

	@Override
	public String toString() {
		return "SearchResultsDTO{" + "totalHits=" + totalHits + ", cursor='" + cursor + '\'' + ", analysis=" + analysis + ", results=" + results + ", facets="
				+ facets + ", distinctCounts=" + distinctCounts + '}';
	}
}
