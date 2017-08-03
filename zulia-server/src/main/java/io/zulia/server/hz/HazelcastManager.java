package io.zulia.server.hz;

import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import io.zulia.server.config.NodeService;
import io.zulia.server.index.ZuliaIndexManager;

public class HazelcastManager implements MembershipListener, LifecycleListener {

	private final ZuliaIndexManager zuliaIndexManager;
	private final NodeService nodeService;

	public HazelcastManager(ZuliaIndexManager zuliaIndexManager, NodeService nodeService) {

		this.zuliaIndexManager = zuliaIndexManager;
		this.nodeService = nodeService;
	}

	@Override
	public void stateChanged(LifecycleEvent event) {

	}

	@Override
	public void memberAdded(MembershipEvent membershipEvent) {


		//member to node via member.getAddress().getPort() and member.getAddress().getHost()
		//nodeService to get the full node info

		//zuliaIndexManager.handleNodeAdded(currentsNodes,nodeAdded,thisIsMaster);
	}

	@Override
	public void memberRemoved(MembershipEvent membershipEvent) {

		//member to node via member.getAddress().getPort() and member.getAddress().getHost()
		//nodeService to get the full node info

		//zuliaIndexManager.handleNodeRemoved(currentsNodes,nodeRemoved,thisIsMaster);
	}

	@Override
	public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {

	}

	public void shutdown() {

	}
}
