package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery.AnalysisRequest;

public class SummaryAnalysis implements AnalysisBuilder {
	private final AnalysisRequest.Builder analysisBuilder;

	public SummaryAnalysis(String field) {
		analysisBuilder = AnalysisRequest.newBuilder().setField(field).setSummaryTerms(true);
	}

	public SummaryAnalysis setTopN(int topN) {
		analysisBuilder.setTopN(topN);
		return this;
	}

	@Override
	public AnalysisRequest getAnalysis() {
		return analysisBuilder.build();
	}
}
