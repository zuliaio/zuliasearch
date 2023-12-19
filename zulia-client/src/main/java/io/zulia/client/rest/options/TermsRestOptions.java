package io.zulia.client.rest.options;

import io.zulia.ZuliaRESTConstants;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TermsRestOptions {

	public TermsRestOptions() {

	}

	private Integer amount;
	private Integer minDocFreq;
	private Integer minTermFreq;

	private String startTerm;

	private String endTerm;

	private String termFilter;

	private String termMatch;

	private List<String> includeTerm;

	public Integer getAmount() {
		return amount;
	}

	public TermsRestOptions setAmount(Integer amount) {
		this.amount = amount;
		return this;
	}

	public Integer getMinDocFreq() {
		return minDocFreq;
	}

	public TermsRestOptions setMinDocFreq(Integer minDocFreq) {
		this.minDocFreq = minDocFreq;
		return this;
	}

	public Integer getMinTermFreq() {
		return minTermFreq;
	}

	public TermsRestOptions setMinTermFreq(Integer minTermFreq) {
		this.minTermFreq = minTermFreq;
		return this;
	}

	public String getStartTerm() {
		return startTerm;
	}

	public TermsRestOptions setStartTerm(String startTerm) {
		this.startTerm = startTerm;
		return this;
	}

	public String getEndTerm() {
		return endTerm;
	}

	public TermsRestOptions setEndTerm(String endTerm) {
		this.endTerm = endTerm;
		return this;
	}

	public String getTermFilter() {
		return termFilter;
	}

	public TermsRestOptions setTermFilter(String termFilter) {
		this.termFilter = termFilter;
		return this;
	}

	public String getTermMatch() {
		return termMatch;
	}

	public TermsRestOptions setTermMatch(String termMatch) {
		this.termMatch = termMatch;
		return this;
	}

	public List<String> getIncludeTerm() {
		return includeTerm;
	}

	public TermsRestOptions setIncludeTerm(List<String> includeTerm) {
		this.includeTerm = includeTerm;
		return this;
	}

	@Override
	public String toString() {
		return "TermsOptions{" + "amount=" + amount + ", minDocFreq=" + minDocFreq + ", minTermFreq=" + minTermFreq + ", startTerm='" + startTerm + '\''
				+ ", endTerm='" + endTerm + '\'' + ", termFilter='" + termFilter + '\'' + ", termMatch='" + termMatch + '\'' + ", includeTerm=" + includeTerm
				+ '}';
	}

	public Map<String, Object> getParameters() {
		Map<String, Object> parameters = new HashMap<>();
		if (amount != null) {
			parameters.put(ZuliaRESTConstants.AMOUNT, amount);
		}
		if (minDocFreq != null) {
			parameters.put(ZuliaRESTConstants.MIN_DOC_FREQ, minDocFreq);
		}
		if (minTermFreq != null) {
			parameters.put(ZuliaRESTConstants.MIN_TERM_FREQ, minTermFreq);
		}
		if (startTerm != null) {
			parameters.put(ZuliaRESTConstants.START_TERM, startTerm);
		}
		if (endTerm != null) {
			parameters.put(ZuliaRESTConstants.END_TERM, endTerm);
		}
		if (termFilter != null) {
			parameters.put(ZuliaRESTConstants.TERM_FILTER, termFilter);
		}
		if (termMatch != null) {
			parameters.put(ZuliaRESTConstants.TERM_MATCH, termMatch);
		}
		if (includeTerm != null && !includeTerm.isEmpty()) {
			parameters.put(ZuliaRESTConstants.INCLUDE_TERM, includeTerm);
		}

		return parameters;
	}

}


