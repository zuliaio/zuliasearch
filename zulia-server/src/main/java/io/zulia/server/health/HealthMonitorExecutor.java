package io.zulia.server.health;

import io.micronaut.context.annotation.Context;
import io.micronaut.management.health.aggregator.HealthAggregator;
import io.micronaut.management.health.indicator.HealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.node.ZuliaNode;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@NullMarked
@Singleton
@Context
public class HealthMonitorExecutor {
	private static final Logger LOG = LoggerFactory.getLogger(HealthMonitorExecutor.class);
	private @Nullable HealthMonitorMongoWriter writer = null;

	private final HealthAggregator<HealthResult> healthAggregator;
	private final HealthIndicator[] healthIndicators;
	private final ZuliaConfig zuliaConfig;

	public HealthMonitorExecutor(ZuliaNode zuliaNode, HealthAggregator<HealthResult> healthAggregator, HealthIndicator[] healthIndicators) {
		this.zuliaConfig = zuliaNode.getZuliaConfig();
		this.healthAggregator = healthAggregator;
		this.healthIndicators = healthIndicators;
	}

	@PostConstruct
	public void init() {
		if (zuliaConfig.getHealth().getWriteToMongo()) {
			writer = new HealthMonitorMongoWriter(zuliaConfig, this.healthAggregator, this.healthIndicators);
			LOG.info("Zulia health monitor starting");
			writer.startAsync();
		}
	}

	@PreDestroy
	public void shutdown() throws TimeoutException {
		if (writer != null && writer.isRunning()) {
			LOG.info("Zulia health monitor stopping");
			writer.stopAsync();
			writer.awaitTerminated(10, TimeUnit.SECONDS);
		}
	}
}
