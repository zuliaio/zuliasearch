package io.zulia.server.cmd.zuliaadmin;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.GetIndexesResult;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaIndex;
import io.zulia.server.cmd.ZuliaAdmin;
import io.zulia.server.cmd.ZuliaCommonCmd;
import picocli.CommandLine;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "displayIndexes", description = "Display the indexes")
public class DisplayIndexesCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@CommandLine.Option(names = "--detailed", description = "Shows details about the index")
	public boolean detailed;

	@Override
	public Integer call() throws Exception {
		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		GetIndexesResult indexes = zuliaWorkPool.getIndexes();

		if (detailed) {
			HashMap<String, String> indexLocation = new HashMap<>();
			List<ZuliaIndex.IndexShardMapping> indexShardMappings = zuliaWorkPool.getNodes().getIndexShardMappings();
			for (ZuliaIndex.IndexShardMapping indexShardMapping : indexShardMappings) {

				StringBuilder sb = new StringBuilder();
				for (ZuliaIndex.ShardMapping shardMapping : indexShardMapping.getShardMappingList()) {
					if (sb.length() != 0) {
						sb.append(",");
					}
					int shardNumber = shardMapping.getShardNumber();
					appendNode(sb, shardNumber, true, shardMapping.getPrimaryNode());
					for (ZuliaBase.Node rn : shardMapping.getReplicaNodeList()) {
						appendNode(sb, shardNumber, false, rn);
					}
				}
				indexLocation.put(indexShardMapping.getIndexName(), sb.toString());
			}

			List<String> indexNames = indexes.getIndexNames();

			ZuliaCommonCmd.printMagenta(String.format("%40s | %40s", "Index Name", "Location"));
			for (String indexName : indexNames) {
				System.out.printf("%40s | %40s\n", indexName, indexLocation.get(indexName));
			}
		}
		else {
			List<String> indexNames = indexes.getIndexNames();

			for (String indexName : indexNames) {
				System.out.println(indexName);
			}
		}

		return CommandLine.ExitCode.OK;
	}

	private void appendNode(StringBuilder sb, int shardNumber, boolean primary, ZuliaBase.Node node) {
		String nodeKey = node.getServerAddress() + ":" + node.getServicePort();
		sb.append(primary ? "p" : "r");
		sb.append(shardNumber);
		sb.append("-");
		sb.append(nodeKey);
	}
}
