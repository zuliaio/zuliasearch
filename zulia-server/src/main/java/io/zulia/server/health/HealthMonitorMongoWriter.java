package io.zulia.server.health;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import io.micronaut.health.HealthStatus;
import io.micronaut.management.endpoint.health.HealthLevelOfDetail;
import io.micronaut.management.health.aggregator.HealthAggregator;
import io.micronaut.management.health.indicator.HealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.util.MongoProvider;
import io.zulia.server.util.ServerNameHelper;
import io.zulia.server.util.ZuliaNodeProvider;
import org.jspecify.annotations.NullMarked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.net.SocketException;
import java.util.concurrent.TimeUnit;

@NullMarked
public class HealthMonitorMongoWriter extends AbstractScheduledService {
	private static final Logger LOG = LoggerFactory.getLogger(HealthMonitorMongoWriter.class);
	private final HealthAggregator<HealthResult> healthAggregator;
	private final HealthIndicator[] healthIndicators;
	private final ZuliaConfig zuliaConfig;
	private final String db;
	private final String collection;
	private final Long ttl;

	public HealthMonitorMongoWriter(HealthAggregator<HealthResult> healthAggregator, HealthIndicator[] healthIndicators) {
		this.healthAggregator = healthAggregator;
		this.healthIndicators = healthIndicators;
		this.zuliaConfig = ZuliaNodeProvider.getZuliaNode().getZuliaConfig();
		this.db = this.zuliaConfig.getHealth().getDb();
		this.collection = this.zuliaConfig.getHealth().getCollection();
		this.ttl = this.zuliaConfig.getHealth().getTtlDays();
	}

	@Override
	protected void runOneIteration() throws SocketException {
		var aggregatedResultsPublisher = Mono.from(healthAggregator.aggregate(healthIndicators, HealthLevelOfDetail.STATUS_DESCRIPTION_DETAILS));
		var healthResult = aggregatedResultsPublisher.block();

		if (healthResult != null) {
			MongoCollection<HealthResultDTO> healthCollection = MongoProvider.getMongoClient().getDatabase(db).getCollection(collection, HealthResultDTO.class);
			var indexOptions = new IndexOptions().background(true).expireAfter(ttl, TimeUnit.DAYS);
			healthCollection.createIndex(Indexes.descending("timestamp"), indexOptions);

			String serverAddress = zuliaConfig.getServerAddress() + ":" + this.zuliaConfig.getServicePort();
			if (zuliaConfig.getServerAddress() == null || zuliaConfig.getServerAddress().isEmpty()) {
				serverAddress = ServerNameHelper.getLocalServer();
			}
			if (serverAddress == null || serverAddress.isEmpty()) {
				serverAddress = "UNKNOWN";
			}

			var healthDto = HealthResultDTO.fromMicronautHealthResult(healthResult);

			if (healthDto != null) {
				healthDto.setHost(serverAddress);
				var insertResult = healthCollection.insertOne(healthDto);
				if (!insertResult.wasAcknowledged()) {
					LOG.warn("Unable to insert health result into {}.{}", db, collection);
				}
			}
			else {
				LOG.warn("Health DTO is null");
			}

			if (healthResult.getStatus().getName().equals(HealthStatus.UP.getName())) {
				LOG.debug("{}: {}", healthResult.getName(), healthResult.getStatus());
			}
			else {
				LOG.warn("{}: {}", healthResult.getName(), healthResult.getStatus());
			}
		}
		else {
			LOG.warn("Health result is null");
		}
	}

	@Override
	protected Scheduler scheduler() {
		return Scheduler.newFixedRateSchedule(10, this.zuliaConfig.getHealth().getWriteIntervalSeconds(), TimeUnit.SECONDS);
	}
}
