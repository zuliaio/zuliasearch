package io.zulia.server.health;

import io.micronaut.context.annotation.Context;
import io.micronaut.management.health.aggregator.HealthAggregator;
import io.micronaut.management.health.indicator.HealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import io.zulia.server.util.ZuliaNodeProvider;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
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

	@Inject
	private HealthAggregator<HealthResult> healthAggregator;
	@Inject
	private HealthIndicator[] healthIndicators;

	@PostConstruct
	public void init() {
		if (ZuliaNodeProvider.getZuliaNode().getZuliaConfig().getHealth().getWriteToMongo()) {
			writer = new HealthMonitorMongoWriter(this.healthAggregator, this.healthIndicators);
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
