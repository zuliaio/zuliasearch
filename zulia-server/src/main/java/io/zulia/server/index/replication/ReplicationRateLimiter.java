package io.zulia.server.index.replication;

public class ReplicationRateLimiter {

	private final long bytesPerSecond;
	private long availableBytes;
	private long lastRefillNanos;

	public ReplicationRateLimiter(long bytesPerSecond) {
		this.bytesPerSecond = bytesPerSecond;
		this.availableBytes = Math.max(0, bytesPerSecond);
		this.lastRefillNanos = System.nanoTime();
	}

	public synchronized void acquire(int bytes) throws InterruptedException {
		if (bytesPerSecond <= 0 || bytes <= 0) {
			return;
		}
		while (true) {
			refill();
			if (availableBytes >= bytes) {
				availableBytes -= bytes;
				return;
			}
			long needed = bytes - availableBytes;
			long waitMs = Math.max(1, (needed * 1000L) / bytesPerSecond);
			wait(waitMs);
		}
	}

	private void refill() {
		long now = System.nanoTime();
		long elapsedNanos = now - lastRefillNanos;
		if (elapsedNanos <= 0) {
			return;
		}
		long add = (elapsedNanos * bytesPerSecond) / 1_000_000_000L;
		if (add > 0) {
			availableBytes = Math.min(bytesPerSecond, availableBytes + add);
			lastRefillNanos = now;
		}
	}
}
