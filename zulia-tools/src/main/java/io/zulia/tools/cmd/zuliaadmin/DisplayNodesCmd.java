package io.zulia.tools.cmd.zuliaadmin;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.GetNodesResult;
import io.zulia.cmd.common.ZuliaCommonCmd;
import io.zulia.message.ZuliaBase;
import io.zulia.tools.cmd.ZuliaAdmin;
import picocli.CommandLine;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "displayNodes", description = "Display the nodes in the cluster")
public class DisplayNodesCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@CommandLine.Option(names = { "-a", "--activeOnly" }, description = "Show only active nodes")
	private boolean activeOnly;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		GetNodesResult nodes = activeOnly ? zuliaWorkPool.getActiveNodes() : zuliaWorkPool.getNodes();

		ZuliaCommonCmd.printMagenta(String.format("%25s | %15s | %15s | %25s | %30s", "Server", "Service Port", "REST Port", "Heart Beat", "Version"));

		for (ZuliaBase.Node node : nodes.getNodes()) {
			long heartbeat = node.getHeartbeat();

			String heartbeatStr = "-";
			if (heartbeat != 0) {
				heartbeatStr = LocalDateTime.ofInstant(Instant.ofEpochMilli(heartbeat), ZoneId.systemDefault()).toString();
			}

			System.out.printf("%25s | %15s | %15s | %25s | %30s", node.getServerAddress(), node.getServicePort(), node.getRestPort(), heartbeatStr,
					node.getVersion());
			System.out.println();
		}

		return CommandLine.ExitCode.OK;
	}
}
