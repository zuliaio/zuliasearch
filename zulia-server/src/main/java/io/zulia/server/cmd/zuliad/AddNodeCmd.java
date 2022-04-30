package io.zulia.server.cmd.zuliad;

import io.zulia.message.ZuliaBase;
import io.zulia.server.cmd.ZuliaD;
import io.zulia.server.config.NodeService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.util.ZuliaVersion;
import picocli.CommandLine;

import java.util.concurrent.Callable;
import java.util.logging.Logger;

@CommandLine.Command(name = "addNode", description = "Add node to the cluster based on the zulia config given")
public class AddNodeCmd implements Callable<Integer> {
	public static final Logger LOG = Logger.getLogger(RemoveNodeCmd.class.getSimpleName());
	@CommandLine.ParentCommand
	private ZuliaD zuliadCmd;

	@Override
	public Integer call() throws Exception {

		ZuliaDConfig zuliaDConfig = new ZuliaDConfig(zuliadCmd.getConfigPath());

		NodeService nodeService = zuliaDConfig.getNodeService();
		ZuliaConfig zuliaConfig = zuliaDConfig.getZuliaConfig();

		ZuliaBase.Node node = ZuliaBase.Node.newBuilder().setServerAddress(zuliaConfig.getServerAddress()).setServicePort(zuliaConfig.getServicePort())
				.setRestPort(zuliaConfig.getRestPort()).setVersion(ZuliaVersion.getVersion()).build();

		LOG.info("Adding node: " + ZuliaDConfig.formatNode(node));

		nodeService.addNode(node);

		ZuliaDConfig.displayNodes(nodeService, "Registered Nodes:");

		return 0;

	}

}
