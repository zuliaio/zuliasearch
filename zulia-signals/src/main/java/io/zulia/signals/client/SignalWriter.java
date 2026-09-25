package io.zulia.signals.client;

import io.zulia.signals.model.Signal;
import org.bson.Document;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * One virtual thread drains queued signals with synchronous stores, so a request thread never waits on the pool. The pool's
 * async submit blocks its caller at the pool's concurrency limit and, when the app shares the pool, competes with the app's own
 * searches for that limit, which is why the bound lives here. Shared by the onFailure copies of one client.
 */
final class SignalWriter implements AutoCloseable {

	static final int MAX_QUEUED = 10_000;

	private record Queued(Signal signal, Document document) {
	}

	private final BlockingQueue<Queued> queue = new LinkedBlockingQueue<>(MAX_QUEUED);
	private final BiConsumer<Signal, Document> store;
	private final AtomicBoolean started = new AtomicBoolean();
	private volatile boolean closed;
	private volatile Thread thread;

	SignalWriter(BiConsumer<Signal, Document> store) {
		this.store = store;
	}

	/** False when the queue is full, the caller decides what a drop means. */
	boolean enqueue(Signal signal, Document document) {
		if (closed) {
			return false;
		}
		if (started.compareAndSet(false, true)) {
			thread = Thread.ofVirtual().name("signals-writer").start(this::drain);
		}
		return queue.offer(new Queued(signal, document));
	}

	int queued() {
		return queue.size();
	}

	boolean isClosed() {
		return closed;
	}

	private void drain() {
		while (!closed || !queue.isEmpty()) {
			try {
				Queued next = queue.poll(200, TimeUnit.MILLISECONDS);
				if (next != null) {
					store.accept(next.signal(), next.document());
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
	}

	/** Stops taking signals and waits for the queue to drain, up to the wait. */
	void close(Duration wait) {
		closed = true;
		Thread writer = thread;
		if (writer != null) {
			try {
				writer.join(wait);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	@Override
	public void close() {
		close(Duration.ofSeconds(5));
	}
}
