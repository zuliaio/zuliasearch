package io.zulia.server.health;

import io.micronaut.context.condition.Condition;
import io.micronaut.context.condition.ConditionContext;
import io.zulia.server.util.ZuliaNodeProvider;

public class ClusterModeCondition implements Condition {
	@Override
	public boolean matches(ConditionContext context) {
		return ZuliaNodeProvider.getZuliaNode().getZuliaConfig().isCluster();
	}
}