package io.zulia.testing.config;

import io.zulia.message.ZuliaQuery.Query.QueryType;

import java.util.List;

public class QueryConfig {

	private String q;
	private List<String> qf;
	private int mm;
	private QueryType queryType = QueryType.SCORE_MUST;

	public QueryConfig() {

	}

	public String getQ() {
		return q;
	}

	public void setQ(String q) {
		this.q = q;
	}

	public List<String> getQf() {
		return qf;
	}

	public void setQf(List<String> qf) {
		this.qf = qf;
	}

	public int getMm() {
		return mm;
	}

	public void setMm(int mm) {
		this.mm = mm;
	}

	public QueryType getQueryType() {
		return queryType;
	}

	public void setQueryType(QueryType queryType) {
		this.queryType = queryType;
	}

	@Override
	public String toString() {
		return "QueryConfig{" + "q='" + q + '\'' + ", qf=" + qf + ", mm=" + mm + ", queryType=" + queryType + '}';
	}
}
