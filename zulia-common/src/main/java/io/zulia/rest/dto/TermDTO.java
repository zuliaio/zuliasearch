package io.zulia.rest.dto;

import io.zulia.message.ZuliaBase;

public record TermDTO(String term, long docFreq, long termFreq, double score) {

	public static TermDTO fromTerm(ZuliaBase.Term term) {
		return new TermDTO(term.getValue(), term.getDocFreq(), term.getTermFreq(), term.getScore());
	}

}
