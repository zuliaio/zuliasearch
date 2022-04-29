package io.zulia.server.cmd.zuliaadmin;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.GetNodesResult;
import io.zulia.message.ZuliaBase;
import io.zulia.server.cmd.ZuliaAdmin;
import picocli.CommandLine;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "displayNodes", description = "Display the nodes in the cluster")
public class DisplayNodesCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;


	@CommandLine.Option(names = {"-a", "--activeOnly"},
			description = "Show only active nodes")
	private boolean activeOnly;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		System.out.printf("%25s | %15s | %15s | %25s | %30s\n", "Server", "Service Port", "REST Port", "Heart Beat", "Version");

		GetNodesResult nodes = activeOnly ? zuliaWorkPool.getActiveNodes() : zuliaWorkPool.getNodes();
		for (ZuliaBase.Node node : nodes.getNodes()) {
			long heartbeat = node.getHeartbeat();

			String heartbeatStr = "-";
			if (heartbeat != 0) {
				heartbeatStr = LocalDateTime.ofInstant(Instant.ofEpochMilli(heartbeat), ZoneId.systemDefault()).toString();
			}

			System.out.printf("%25s | %15s | %15s | %25s | %30s\n", node.getServerAddress(), node.getServicePort(), node.getRestPort(), heartbeatStr,
					node.getVersion());
		}

		return CommandLine.ExitCode.OK;
	}
}
