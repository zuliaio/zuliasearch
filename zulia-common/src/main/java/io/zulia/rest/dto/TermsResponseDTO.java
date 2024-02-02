package io.zulia.rest.dto;

import java.util.List;

public class TermsResponseDTO {

	private String index;
	private String field;
	private List<TermDTO> terms;

	public TermsResponseDTO() {
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public List<TermDTO> getTerms() {
		return terms;
	}

	public void setTerms(List<TermDTO> terms) {
		this.terms = terms;
	}

	@Override
	public String toString() {
		return "TermsResponseDTO{" + "index='" + index + '\'' + ", field='" + field + '\'' + ", terms=" + terms + '}';
	}
}
