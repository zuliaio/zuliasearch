package io.zulia.tools.cmd.zuliaadmin;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.GetStatsResult;
import io.zulia.cmd.common.ZuliaCommonCmd;
import io.zulia.message.ZuliaBase.CacheStats;
import io.zulia.message.ZuliaBase.IndexStats;
import io.zulia.message.ZuliaBase.NodeStats;
import io.zulia.message.ZuliaBase.ShardCacheStats;
import io.zulia.tools.cmd.ZuliaAdmin;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "stats", description = "Display node statistics including JVM memory, disk space, and cache stats")
public class StatsCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		GetStatsResult result = zuliaWorkPool.getStats();
		NodeStats nodeStats = result.getNodeStats();

		System.out.println();
		ZuliaCommonCmd.printMagenta("Zulia Version: " + nodeStats.getZuliaVersion());
		System.out.println();

		ZuliaCommonCmd.printMagenta(String.format("%30s", "JVM Memory (MB)"));
		System.out.printf("%14s | %14s | %14s | %14s%n", "Used", "Free", "Total", "Max");
		System.out.printf("%14d | %14d | %14d | %14d%n", nodeStats.getJvmUsedMemoryMB(), nodeStats.getJvmFreeMemoryMB(), nodeStats.getJvmTotalMemoryMB(),
				nodeStats.getJvmMaxMemoryMB());
		System.out.println();

		ZuliaCommonCmd.printMagenta(String.format("%30s", "Data Dir Space (GB)"));
		System.out.printf("%14s | %14s | %14s%n", "Used", "Free", "Total");
		System.out.printf("%14.2f | %14.2f | %14.2f%n", nodeStats.getUsedDataDirSpaceGB(), nodeStats.getFreeDataDirSpaceGB(),
				nodeStats.getTotalDataDirSpaceGB());

		if (!nodeStats.getIndexStatList().isEmpty()) {
			System.out.println();
			ZuliaCommonCmd.printMagenta(String.format("%30s", "Index Cache Stats"));
			System.out.printf("%20s | %7s | %7s | %14s | %14s | %10s | %10s%n", "Index", "Shard", "Primary", "Pinned Size", "General Size", "Pinned Hits",
					"General Hits");

			for (IndexStats indexStats : nodeStats.getIndexStatList()) {
				int shard = 0;
				for (ShardCacheStats shardCacheStats : indexStats.getShardCacheStatList()) {
					CacheStats pinned = shardCacheStats.getPinnedCache();
					CacheStats general = shardCacheStats.getGeneralCache();
					System.out.printf("%20s | %7d | %7s | %14d | %14d | %10d | %10d%n", indexStats.getIndexName(), shard, shardCacheStats.getPrimary(),
							pinned.getEstimatedSize(), general.getEstimatedSize(), pinned.getHitCount(), general.getHitCount());
					shard++;
				}
			}
		}

		System.out.println();

		return CommandLine.ExitCode.OK;
	}
}
