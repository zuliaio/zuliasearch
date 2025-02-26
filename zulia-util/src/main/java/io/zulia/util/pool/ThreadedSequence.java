package io.zulia.util.pool;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;

/**
 * Allows threaded computation of an input where you want to write the output in the same order as the input
 * Create a class defining doWork and outputBatch and then call processThreaded(iterable)
 *
 * @param <I> - Input class type
 * @param <O> - Output class type
 */
public abstract class ThreadedSequence<I, O> {

	public interface ProgressTracker {
		void updateProgress(int totalProcessed);
	}

	private final int batchSize;
	private final TaskExecutor taskExecutor;
	private int totalProcessed;
	private ProgressTracker progressTracker;

	/**
	 * @param threads - number of threads to use
	 */
	public ThreadedSequence(int threads) {
		this(threads, threads * 16);
	}

	/**
	 * @param threads   - number of threads to use
	 * @param batchSize - the number of items to process in memory before writing
	 */
	public ThreadedSequence(int threads, int batchSize) {
		if (batchSize < threads) {
			throw new IllegalArgumentException("Batch size should be a greater or equal to threads");
		}

		this.batchSize = batchSize;
		taskExecutor = WorkPool.nativePool(threads);
	}
	
	public ThreadedSequence(TaskExecutor taskExecutor, int batchSize) {
		this.batchSize = batchSize;
		this.taskExecutor = taskExecutor;
	}

	public void setProgressTracker(ProgressTracker progressTracker) {
		this.progressTracker = progressTracker;
	}

	public int getTotalProcessed() {
		return totalProcessed;
	}

	/**
	 * Performs the work.  This method is called from many threads
	 *
	 * @param i - One input
	 * @return
	 * @throws Exception
	 */
	public abstract O doWork(I i) throws Exception;

	/**
	 * Outputs an in order batch.  This method is called from a single thread
	 *
	 * @param batchOut - the batch for output
	 * @throws Exception
	 */
	public abstract void outputBatch(List<O> batchOut) throws Exception;

	/**
	 * Takes a list of input and executes them in parallel.  Calls outputBatch with a list of complete outputs from a single thread when done and increments progress by the batch size
	 *
	 * @param batch - list of input to be handled as a batch by many threads
	 * @throws Exception
	 */
	protected void handleBatch(List<I> batch) throws Exception {
		List<ListenableFuture<O>> futures = new ArrayList<>(batch.size());
		for (I in : batch) {
			futures.add(taskExecutor.executeAsync(() -> doWork(in)));
		}
		List<O> outputs = new ArrayList<>(batch.size());
		for (ListenableFuture<O> future : futures) {
			outputs.add(future.get());
		}

		synchronized (this) {
			outputBatch(outputs);
			totalProcessed += outputs.size();
			updateProgress(totalProcessed);
		}
	}

	public void updateProgress(int totalProcessed) {
		if (progressTracker != null) {
			progressTracker.updateProgress(totalProcessed);
		}
	}

	/**
	 * Processes an iterable source of content in threaded notion by batching the content into slices handled by
	 * multiple threads.  Each batch is processed in parallel and then outputted in ordered batch by batch, persevering order of the input
	 *
	 * @param input - iterable input
	 * @throws Exception -
	 */
	public void processThreaded(Iterable<I> input) throws Exception {

		List<I> nextBatch = new ArrayList<>(batchSize);

		for (I i : input) {
			nextBatch.add(i);

			if (nextBatch.size() == batchSize) {
				handleBatch(nextBatch);
				nextBatch.clear();
			}

		}

		if (!nextBatch.isEmpty()) {
			handleBatch(nextBatch);
		}

		taskExecutor.close();

	}

}