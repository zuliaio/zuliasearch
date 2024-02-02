package io.zulia.rest.dto;

import io.zulia.message.ZuliaQuery;

import java.util.List;

public record AnalysisDTO(String field, List<TermDTO> terms) {

	public static AnalysisDTO fromAnalysisResult(ZuliaQuery.AnalysisResult analysisResult) {
		return new AnalysisDTO(analysisResult.getAnalysisRequest().getField(), analysisResult.getTermsList().stream().map(TermDTO::fromTerm).toList());

	}
}
