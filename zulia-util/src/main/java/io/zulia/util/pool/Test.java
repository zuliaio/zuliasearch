package io.zulia.util.pool;

public class Test {

	public static void main(String[] args) {

		int mb = 1024 * 1024;
		try (TaskExecutor workPool = WorkPool.virtualPool(10000, 1000)) {
			for (int i = 0; i < 1000000; i++) {

				int finalI = i;
				//System.out.println("Queuing " + finalI);
				workPool.executeAsync(() -> {
					//System.out.println("Created " + finalI);
					Thread.sleep(200);
					//System.out.println("Finished " + finalI);
					return null;
				});
			}
		}
	}
}
