package io.zulia.util.pool;

import java.io.Serial;
import java.util.concurrent.ArrayBlockingQueue;

public class WaitingArrayBlockingQueue<T> extends ArrayBlockingQueue<T> {
	@Serial
	private static final long serialVersionUID = 1L;

	public WaitingArrayBlockingQueue(int maxQueued) {
		super(maxQueued);
	}

	@Override
	public boolean offer(T e) {
		try {
			put(e);
		}
		catch (InterruptedException e1) {
			throw new RuntimeException(e1);
		}
		return true;
	}

};