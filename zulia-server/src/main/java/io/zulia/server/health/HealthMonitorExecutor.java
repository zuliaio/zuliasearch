package io.zulia.server.health;

import io.micronaut.context.annotation.Context;
import io.micronaut.management.health.aggregator.HealthAggregator;
import io.micronaut.management.health.indicator.HealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import io.zulia.server.util.ZuliaNodeProvider;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jspecify.annotations.NullMarked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NullMarked
@Singleton
@Context
public class HealthMonitorExecutor {
	private static final Logger LOG = LoggerFactory.getLogger(HealthMonitorExecutor.class);

	@Inject
	HealthAggregator<HealthResult> healthAggregator;
	@Inject
	HealthIndicator[] healthIndicators;

	@PostConstruct
	public void init() {
		HealthMonitorMongoWriter writer = new HealthMonitorMongoWriter(this.healthAggregator, this.healthIndicators);

		if (ZuliaNodeProvider.getZuliaNode().getZuliaConfig().getHealth().getWriteToMongo()) {
			LOG.info("Zulia health monitor starting");
			writer.startAsync();
		}
	}
}
