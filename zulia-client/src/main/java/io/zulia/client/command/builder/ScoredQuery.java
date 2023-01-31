package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.Query.QueryType;

public class ScoredQuery extends StandardQuery<ScoredQuery> {
	private boolean must;
	private String scoreFunction;

	public ScoredQuery(String query) {
		this(query, true);
	}

	@Override
	protected ScoredQuery getSelf() {
		return this;
	}

	/**
	 * @param query
	 * @param must  - if must is true then query will be required, otherwise it will be used to as an optional (should clause) to help scoring
	 */
	public ScoredQuery(String query, boolean must) {
		super(query);
		this.must = must;
	}

	public boolean isMust() {
		return must;
	}

	public ScoredQuery setMust(boolean must) {
		this.must = must;
		return this;
	}

	public String getScoreFunction() {
		return scoreFunction;
	}

	public ScoredQuery setScoreFunction(String scoreFunction) {
		this.scoreFunction = scoreFunction;
		return this;
	}

	@Override
	protected void completeQuery(ZuliaQuery.Query.Builder queryBuilder) {
		queryBuilder.setQueryType(must ? QueryType.SCORE_MUST : QueryType.SCORE_SHOULD);
		if (scoreFunction != null) {
			queryBuilder.setScoreFunction(scoreFunction);
		}
	}
}
