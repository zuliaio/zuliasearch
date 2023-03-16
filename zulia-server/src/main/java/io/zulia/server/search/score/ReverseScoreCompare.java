package io.zulia.server.search.score;

import io.zulia.message.ZuliaQuery.ScoredResult;

import java.util.Comparator;

public class ReverseScoreCompare implements Comparator<ScoredResult> {

	@Override
	public int compare(ScoredResult o1, ScoredResult o2) {
		int compare = Double.compare(o1.getScore(), o2.getScore());
		if (compare == 0) {
			return Integer.compare(o1.getResultIndex(), o2.getResultIndex());
		}
		return compare;
	}

}


