package io.zulia.server.cmd.zuliad;

import io.zulia.server.cmd.ZuliaD;
import io.zulia.server.config.NodeService;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "listNodes", description = "List current zulia nodes")
public class ListNodesCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaD zuliadCmd;

	@Override
	public Integer call() throws Exception {

		ZuliaDConfig zuliaDConfig = new ZuliaDConfig(zuliadCmd.getConfigPath());
		NodeService nodeService = zuliaDConfig.getNodeService();
		ZuliaDConfig.displayNodes(nodeService, "Registered Nodes:");
		return 0;

	}

}
