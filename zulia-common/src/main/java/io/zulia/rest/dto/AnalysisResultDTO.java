package io.zulia.rest.dto;

import org.bson.Document;

import java.util.List;

public record AnalysisResultDTO(Document analysisRequest, List<String> tokens, List<TermDTO> terms) {
}