package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.IndexRouting;
import io.zulia.server.exceptions.ShardOfflineException;
import io.zulia.server.index.MasterSlaveSelector;
import io.zulia.server.index.ZuliaIndex;

import java.util.*;
import java.util.concurrent.ExecutorService;

public abstract class MasterSlaveNodeRequestFederator<I, O> extends NodeRequestFederator<I, O> {

    private Map<Node, List<IndexRouting>> nodeToRouting = new HashMap<>();

    public MasterSlaveNodeRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ZuliaBase.MasterSlaveSettings masterSlaveSettings,
                                           ZuliaIndex index, ExecutorService pool) throws ShardOfflineException {
        this(thisNode, otherNodesActive, masterSlaveSettings, Collections.singletonList(index), pool);
    }

    public MasterSlaveNodeRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ZuliaBase.MasterSlaveSettings masterSlaveSettings,
                                           Collection<ZuliaIndex> indexes, ExecutorService pool) throws ShardOfflineException {
        super(thisNode, otherNodesActive, pool);

        List<Node> nodesAvailable = new ArrayList<>();
        nodesAvailable.add(thisNode);
        nodesAvailable.addAll(otherNodesActive);

        for (ZuliaIndex index : indexes) {
            io.zulia.message.ZuliaIndex.IndexShardMapping indexShardMapping = index.getIndexShardMapping();
            MasterSlaveSelector masterSlaveSelector = new MasterSlaveSelector(masterSlaveSettings, nodesAvailable, indexShardMapping);

            Map<Node, IndexRouting.Builder> nodesForIndex = masterSlaveSelector.getNodesForIndex();

            for (Node node : nodesForIndex.keySet()) {
                IndexRouting indexRouting = nodesForIndex.get(node).setIndex(index.getIndexName()).build();
                nodeToRouting.computeIfAbsent(node, (k) -> new ArrayList<>()).add(indexRouting);
                nodes.add(node);
            }

        }
    }

    public List<IndexRouting> getIndexRouting(Node node) {
        return nodeToRouting.get(node);
    }
}
