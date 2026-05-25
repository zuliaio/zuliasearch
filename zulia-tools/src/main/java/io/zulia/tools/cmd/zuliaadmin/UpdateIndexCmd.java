package io.zulia.tools.cmd.zuliaadmin;

import io.zulia.client.command.UpdateIndex;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.UpdateIndexResult;
import io.zulia.cmd.common.ZuliaCommonCmd;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.tools.cmd.ZuliaAdmin;
import io.zulia.tools.cmd.common.MultipleIndexArgs;
import picocli.CommandLine;

import java.util.Set;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "updateIndex", description = "Updates settings for index(es) specified by --index.  Only the options given are changed; everything else is left as-is.")
public class UpdateIndexCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@CommandLine.Mixin
	private MultipleIndexArgs multipleIndexArgs;

	@CommandLine.Option(names = "--numberOfReplicas", description = "Number of replicas maintained for each shard (default 0)")
	private Integer numberOfReplicas;

	@CommandLine.Option(names = "--description", description = "An optional description of what the index contains. Useful for documentation, UI display, and LLM/MCP integration")
	private String description;

	@CommandLine.Option(names = "--indexWeight", description = "Relative weight of the index used for cluster shard distribution and node load balancing (default 1, must be positive)")
	private Integer indexWeight;

	@CommandLine.Option(names = "--requestFactor", description = "Used in calculation of request size for a shard (default 2.0)")
	private Double requestFactor;

	@CommandLine.Option(names = "--minShardRequest", description = "Added to the calculated request for a shard (default 2)")
	private Integer minShardRequest;

	@CommandLine.Option(names = "--shardTolerance", description = "Difference in scores between shards tolerated before requesting full results from the shard (default 0.05)")
	private Double shardTolerance;

	@CommandLine.Option(names = "--shardQueryCacheSize", description = "Number of queries cached at the shard level")
	private Integer shardQueryCacheSize;

	@CommandLine.Option(names = "--shardQueryCacheMaxAmount", description = "Queries with more than this amount of documents returned are not cached")
	private Integer shardQueryCacheMaxAmount;

	@CommandLine.Option(names = "--idleTimeWithoutCommit", description = "Time without indexing before commit is forced in seconds (0 disables) (default 30)")
	private Integer idleTimeWithoutCommit;

	@CommandLine.Option(names = "--shardCommitInterval", description = "Indexes or deletes to shard before a commit is forced (default 3200)")
	private Integer shardCommitInterval;

	@CommandLine.Option(names = "--ramBufferMB", description = "RAM buffer size in MB used by each shard's Lucene IndexWriter before flushing to disk (default 128)")
	private Integer ramBufferMB;

	@CommandLine.Option(names = "--maxMergeThreads", description = "Maximum number of concurrent Lucene segment merge threads per shard (default 0 for auto-detect based on CPU cores)")
	private Integer maxMergeThreads;

	@CommandLine.Option(names = "--maxMergePending", description = "Maximum number of pending merges allowed above the merge thread count before indexing is stalled (default 0 for 5)")
	private Integer maxMergePending;

	@CommandLine.Option(names = "--indexingThrottle", description = "Number of concurrent indexing permits when merges are saturated (default 0 for 1)")
	private Integer indexingThrottle;

	@CommandLine.Option(names = "--defaultConcurrency", description = "Number of virtual threads used for parallel Lucene segment search and aggregation within each shard (default 1). Can be overridden per query")
	private Integer defaultConcurrency;

	@CommandLine.Option(names = "--disableCompression", arity = "1", paramLabel = "true|false", description = "When true, disables compression of stored documents (default false)")
	private Boolean disableCompression;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		Set<String> indexes = multipleIndexArgs.resolveIndexes(zuliaWorkPool);

		for (String index : indexes) {
			UpdateIndex updateIndex = buildUpdate(index);
			UpdateIndexResult result = zuliaWorkPool.updateIndex(updateIndex);

			IndexSettings fullIndexSettings = result.getFullIndexSettings();
			if (result.isChanged()) {
				ZuliaCommonCmd.printBlue("Updated index <" + index + "> (numberOfReplicas=" + fullIndexSettings.getNumberOfReplicas() + ")");
			}
			else {
				ZuliaCommonCmd.printMagenta("No changes for index <" + index + ">; settings already match the requested values");
			}
		}

		return CommandLine.ExitCode.OK;
	}

	private UpdateIndex buildUpdate(String index) {
		UpdateIndex updateIndex = new UpdateIndex(index);

		if (numberOfReplicas != null) {
			updateIndex.setNumberOfReplicas(numberOfReplicas);
		}
		if (description != null) {
			updateIndex.setDescription(description);
		}
		if (indexWeight != null) {
			updateIndex.setIndexWeight(indexWeight);
		}
		if (requestFactor != null) {
			updateIndex.setRequestFactor(requestFactor);
		}
		if (minShardRequest != null) {
			updateIndex.setMinShardRequest(minShardRequest);
		}
		if (shardTolerance != null) {
			updateIndex.setShardTolerance(shardTolerance);
		}
		if (shardQueryCacheSize != null) {
			updateIndex.setShardQueryCacheSize(shardQueryCacheSize);
		}
		if (shardQueryCacheMaxAmount != null) {
			updateIndex.setShardQueryCacheMaxAmount(shardQueryCacheMaxAmount);
		}
		if (idleTimeWithoutCommit != null) {
			updateIndex.setIdleTimeWithoutCommit(idleTimeWithoutCommit);
		}
		if (shardCommitInterval != null) {
			updateIndex.setShardCommitInterval(shardCommitInterval);
		}
		if (ramBufferMB != null) {
			updateIndex.setRamBufferMB(ramBufferMB);
		}
		if (maxMergeThreads != null) {
			updateIndex.setMaxMergeThreads(maxMergeThreads);
		}
		if (maxMergePending != null) {
			updateIndex.setMaxMergePending(maxMergePending);
		}
		if (indexingThrottle != null) {
			updateIndex.setIndexingThrottle(indexingThrottle);
		}
		if (defaultConcurrency != null) {
			updateIndex.setDefaultConcurrency(defaultConcurrency);
		}
		if (disableCompression != null) {
			updateIndex.setDisableCompression(disableCompression);
		}

		return updateIndex;
	}
}
