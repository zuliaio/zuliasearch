package io.zulia.server.health;

import io.micronaut.context.condition.Condition;
import io.micronaut.context.condition.ConditionContext;
import io.zulia.server.node.ZuliaNode;

public class ClusterModeCondition implements Condition {
	@Override
	public boolean matches(ConditionContext context) {
		return context.findBean(ZuliaNode.class).map(node -> node.getZuliaConfig().isCluster()).orElse(false);
	}
}
