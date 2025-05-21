package io.zulia.server.index;

import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.MergePolicy;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class ZuliaConcurrentMergeScheduler extends ConcurrentMergeScheduler {

	private final AtomicInteger mergeCounter;


	public ZuliaConcurrentMergeScheduler() {
		mergeCounter = new AtomicInteger();
	}

	@Override
	protected void doMerge(MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
		long currentMerges = mergeCounter.incrementAndGet();

		try {
			super.doMerge(mergeSource, merge);
		}
		finally {
			currentMerges = mergeCounter.decrementAndGet();
		}
	}

	@Override
	protected synchronized boolean maybeStall(MergeSource mergeSource) {
		return true;
	}



	public boolean mergeSaturated() {
		return mergeCounter.get() > this.getMaxMergeCount();
	}
}