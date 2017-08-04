package io.zulia.server.hz;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.spi.properties.GroupProperty;
import io.zulia.message.ZuliaBase;
import io.zulia.server.config.NodeService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.log.LogUtil;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

public class HazelcastManager implements MembershipListener, LifecycleListener {

	private static final Logger LOG = Logger.getLogger(HazelcastManager.class.getName());

	private final ZuliaIndexManager zuliaIndexManager;
	private final NodeService nodeService;
	private HazelcastInstance hazelcastInstance;
	private Member self;

	public HazelcastManager(ZuliaIndexManager zuliaIndexManager, NodeService nodeService, ZuliaConfig zuliaConfig) {
		this.zuliaIndexManager = zuliaIndexManager;
		this.nodeService = nodeService;

		init(nodeService.getNodes(), zuliaConfig);

	}

	private void init(Collection<ZuliaBase.Node> nodes, ZuliaConfig zuliaConfig) {

		int hazelcastPort = zuliaConfig.getHazelcastPort();

		Config cfg = new Config();
		// disable Hazelcast shutdown hook to allow Zulia to handle
		cfg.setProperty(GroupProperty.SHUTDOWNHOOK_ENABLED.getName(), "false");

		cfg.getGroupConfig().setName(zuliaConfig.getClusterName());
		cfg.getGroupConfig().setPassword(zuliaConfig.getClusterName());
		cfg.getNetworkConfig().setPortAutoIncrement(false);
		cfg.getNetworkConfig().setPort(hazelcastPort);
		cfg.setInstanceName("" + hazelcastPort);

		cfg.getManagementCenterConfig().setEnabled(false);

		NetworkConfig network = cfg.getNetworkConfig();
		JoinConfig joinConfig = network.getJoin();

		joinConfig.getMulticastConfig().setEnabled(false);
		joinConfig.getTcpIpConfig().setEnabled(true);
		for (ZuliaBase.Node node : nodes) {
			joinConfig.getTcpIpConfig().addMember(node.getServerAddress() + ":" + node.getHazelcastPort());
		}

		hazelcastInstance = Hazelcast.newHazelcastInstance(cfg);
		LogUtil.init();

		self = hazelcastInstance.getCluster().getLocalMember();

		hazelcastInstance.getCluster().addMembershipListener(this);
		hazelcastInstance.getLifecycleService().addLifecycleListener(this);

		LOG.info("Initialized hazelcast");
		Set<Member> members = hazelcastInstance.getCluster().getMembers();

		zuliaIndexManager.loadIndexes();

		LOG.info("Current cluster members: <" + members + ">");
		zuliaIndexManager.openConnections(nodes);

	}

	@Override
	public void stateChanged(LifecycleEvent event) {
		LOG.info("Hazelcast has a new state <" + event.getState().name() + ">");
	}

	@Override
	public void memberAdded(MembershipEvent membershipEvent) {

		Member memberAdded = membershipEvent.getMember();

		Set<Member> members = membershipEvent.getCluster().getMembers();
		LOG.info("Added member: <" + membershipEvent.getMember() + "> Current members: <" + members + ">");

		ZuliaBase.Node nodeAdded = nodeService.getNode(memberAdded.getAddress().getHost(), memberAdded.getAddress().getPort());

		Collection<ZuliaBase.Node> currentOnlineNodes = new HashSet<>();
		for (Member member : members) {
			currentOnlineNodes.add(nodeService.getNode(member.getAddress().getHost(), member.getAddress().getPort()));
		}

		Member firstMember = members.iterator().next();
		boolean master = self.equals(firstMember);

		zuliaIndexManager.handleNodeAdded(currentOnlineNodes, nodeAdded, master);

	}

	@Override
	public void memberRemoved(MembershipEvent membershipEvent) {

		Member memberRemoved = membershipEvent.getMember();

		Set<Member> members = membershipEvent.getCluster().getMembers();
		LOG.info("Lost member: <" + memberRemoved + "> Current members: <" + members + ">");

		ZuliaBase.Node nodeRemoved = nodeService.getNode(memberRemoved.getAddress().getHost(), memberRemoved.getAddress().getPort());

		Collection<ZuliaBase.Node> currentOnlineNodes = new HashSet<>();
		for (Member member : members) {
			currentOnlineNodes.add(nodeService.getNode(member.getAddress().getHost(), member.getAddress().getPort()));
		}

		Member firstMember = membershipEvent.getCluster().getMembers().iterator().next();
		boolean master = self.equals(firstMember);

		zuliaIndexManager.handleNodeRemoved(currentOnlineNodes, nodeRemoved, master);

	}

	@Override
	public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
		LOG.info("Hazelcast member <" + memberAttributeEvent.getMember().getAddress().getHost() + ":" + memberAttributeEvent.getMember().getAddress().getPort()
				+ "> has changed attributes.");
	}

	public void shutdown() {
		LOG.info("Received signal to shutdown hazelcast.");
		hazelcastInstance.getLifecycleService().terminate();
	}
}
