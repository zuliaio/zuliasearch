package io.zulia.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

public class ResourcePool<T> {

	private final BlockingQueue<T> pool;

	public ResourcePool(int size, Supplier<T> constructor) {
		pool = new LinkedBlockingQueue<>(size);
		for (int i = 0; i < size; i++) {
			pool.offer(constructor.get());
		}
	}

	public T acquire() throws InterruptedException {
		return pool.take();
	}

	public void release(T counter) {
		pool.offer(counter);
	}

	public void forEach(java.util.function.Consumer<T> action) {
		pool.stream().forEach(action);
	}

	public void forEachParallel(java.util.function.Consumer<T> action) {
		pool.stream().parallel().forEach(action);
	}

}
