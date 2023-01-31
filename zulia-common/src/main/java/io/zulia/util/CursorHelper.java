package io.zulia.util;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static io.zulia.message.ZuliaQuery.LastIndexResult;
import static io.zulia.message.ZuliaQuery.LastResult;
import static io.zulia.message.ZuliaQuery.ScoredResult;

/**
 * Created by Payam Meyer on 8/7/17.
 *
 * @author pmeyer
 */
public class CursorHelper {

	public static String getStaticIndexCursor(LastResult lastResult) {
		return new String(Base64.getEncoder().encode(lastResult.toByteArray()), StandardCharsets.UTF_8);
	}

	public static String getUniqueSortedCursor(LastResult lastResult) {

		LastResult.Builder lastResultBuilder = LastResult.newBuilder();
		for (LastIndexResult lastIndexResult : lastResult.getLastIndexResultList()) {
			LastIndexResult.Builder lastIndexResultBuilder = LastIndexResult.newBuilder();
			lastIndexResultBuilder.setIndexName(lastIndexResult.getIndexName());
			for (ScoredResult scoredResult : lastIndexResult.getLastForShardList()) {
				ScoredResult.Builder scoredResultBuilder = ScoredResult.newBuilder(scoredResult).clearScore().clearResultDocument();
				lastIndexResultBuilder.addLastForShard(scoredResultBuilder);
			}
			lastResultBuilder.addLastIndexResult(lastIndexResultBuilder);
		}

		return new String(Base64.getEncoder().encode(lastResultBuilder.build().toByteArray()), StandardCharsets.UTF_8);
	}

	public static LastResult getLastResultFromCursor(String cursor) {
		try {
			return LastResult.parseFrom(Base64.getDecoder().decode(cursor.getBytes(StandardCharsets.UTF_8)));
		}
		catch (Exception e) {
			throw new RuntimeException("Invalid cursor");
		}
	}
}

