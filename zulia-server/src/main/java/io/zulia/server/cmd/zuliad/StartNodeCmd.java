package io.zulia.server.cmd.zuliad;

import io.zulia.message.ZuliaBase;
import io.zulia.server.cmd.ZuliaCommonCmd;
import io.zulia.server.cmd.ZuliaD;
import io.zulia.server.config.NodeService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.node.ZuliaNode;
import io.zulia.server.util.ZuliaNodeProvider;
import io.zulia.util.ZuliaVersion;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.Collection;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "start", description = "Start Zulia Node")
public class StartNodeCmd implements Callable<Integer> {
	public static final Logger LOG = LoggerFactory.getLogger(StartNodeCmd.class);

	@CommandLine.ParentCommand
	private ZuliaD zuliadCmd;

	@Override
	public Integer call() throws Exception {

		ZuliaDConfig.setLuceneStatic();

		ZuliaDConfig zuliaDConfig = new ZuliaDConfig(zuliadCmd.getConfigPath());
		NodeService nodeService = zuliaDConfig.getNodeService();
		ZuliaConfig zuliaConfig = zuliaDConfig.getZuliaConfig();

		Collection<ZuliaBase.Node> nodes = nodeService.getNodes();

		if (zuliaConfig.isCluster()) {
			if (nodes.isEmpty()) {
				LOG.error("No nodes added to the cluster");
				return 3;
			}
		}
		else {
			LOG.error("Running in single node mode");
		}

		ZuliaDConfig.displayNodes(nodeService, "Registered nodes:");

		String zuliaArt = """
				 ______     _ _
				|___  /    | (_)
				   / /_   _| |_  __ _
				  / /| | | | | |/ _` |
				 / /_| |_| | | | (_| |
				/_____\\__,_|_|_|\\__,_|""";

		ZuliaCommonCmd.printOrange(zuliaArt);
		System.out.println("  Zulia (" + ZuliaVersion.getVersion() + ") based on Lucene " + Version.LATEST);
		System.out.println();

		ZuliaNode zuliaNode = new ZuliaNode(zuliaConfig, nodeService);

		ZuliaNodeProvider.setZuliaNode(zuliaNode);
		zuliaNode.start();

		return 0;

	}

}
