package io.zulia.server.cmd.zuliad;

import io.zulia.server.cmd.ZuliaD;
import io.zulia.server.config.NodeService;
import picocli.CommandLine;

import java.util.concurrent.Callable;
import java.util.logging.Logger;

@CommandLine.Command(name = "removeNode", description = "Remove the node given by the command line params")
public class RemoveNodeCmd implements Callable<Integer> {

    public static final Logger LOG = Logger.getLogger(RemoveNodeCmd.class.getSimpleName());

    @CommandLine.ParentCommand
    private ZuliaD zuliadCmd;

    @CommandLine.Option(names = {"--server", "--address"}, description = "Server to remove from the cluster", required = true)
    private String server;

    @CommandLine.Option(names = "--servicePort", description = "Service port of server to remove from the cluster", required = true)
    private int servicePort;

    @Override
    public Integer call() throws Exception {

        ZuliaDConfig zuliaDConfig = new ZuliaDConfig(zuliadCmd.getConfigPath());
        NodeService nodeService = zuliaDConfig.getNodeService();
        LOG.info("Removing node: " + server + ":" + servicePort);
        nodeService.removeNode(server, servicePort);

        ZuliaDConfig.displayNodes(nodeService, "Registered Nodes:");

        return 0;

    }

}
