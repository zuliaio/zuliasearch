package io.zulia.util;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static io.zulia.message.ZuliaQuery.LastIndexResult;
import static io.zulia.message.ZuliaQuery.LastResult;
import static io.zulia.message.ZuliaQuery.ScoredResult;

/**
 * Serializes a query's {@link LastResult} to a URL-safe string cursor for deep paging and restores it on the next page.
 * The cursor variant to use depends on how the query is sorted. Use {@link #getUniqueSortedCursor(LastResult)} when the
 * query sorts on a unique value or unique value combination. Use {@link #getStaticIndexCursor(LastResult)} when paging
 * in relevance (score) order against an index that is not changing.
 * <p>
 * Created by Payam Meyer on 8/7/17.
 *
 * @author pmeyer
 */
public class CursorHelper {

	/**
	 * Serializes the full {@link LastResult}, including the per-shard scores needed to resume a relevance (score)
	 * sorted query. Scores and Lucene doc ids are only stable while the index is unchanged, so this cursor is only
	 * reliable against a static index, as the name implies. Writes between pages can skip or repeat documents.
	 */
	public static String getStaticIndexCursor(LastResult lastResult) {
		return new String(Base64.getUrlEncoder().encode(lastResult.toByteArray()), StandardCharsets.UTF_8);
	}

	/**
	 * Serializes a minimal cursor keeping only the shard, Lucene doc id, and sort values for each shard. Only valid
	 * when the query sorts on a unique value or unique value combination (i.e. id or title,id). The sort values alone
	 * define the resume position, so this cursor stays correct while the index changes. Do not use this for relevance
	 * (score) sorted queries. The score is cleared here, so the server would resume score paging after 0.0, skipping
	 * every remaining document and failing the query.
	 */
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

		return new String(Base64.getUrlEncoder().encode(lastResultBuilder.build().toByteArray()), StandardCharsets.UTF_8);
	}

	/**
	 * Decodes a cursor produced by either method above back into a {@link LastResult} to set on the next page's query.
	 */
	public static LastResult getLastResultFromCursor(String cursor) {
		try {
			return LastResult.parseFrom(Base64.getUrlDecoder().decode(cursor.getBytes(StandardCharsets.UTF_8)));
		}
		catch (Exception e) {
			throw new RuntimeException("Invalid cursor");
		}
	}
}

