package io.zulia.server.search.stat;

import org.apache.lucene.util.PriorityQueue;

public class TopStatsQueue<T extends Stats<T>> extends PriorityQueue<T> {

	public TopStatsQueue(int topN) {
		super(topN);
	}

	@Override
	protected boolean lessThan(T a, T b) {
		return a.compareTo(b) < 0;
	}

}