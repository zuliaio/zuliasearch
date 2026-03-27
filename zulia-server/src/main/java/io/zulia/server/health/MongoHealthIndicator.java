package io.zulia.server.health;

import com.mongodb.client.MongoClient;
import io.micronaut.context.annotation.Requires;
import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.indicator.AbstractHealthIndicator;
import io.zulia.server.util.MongoProvider;
import jakarta.inject.Singleton;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Singleton
@Requires(condition = ClusterModeCondition.class)
@NullMarked
public class MongoHealthIndicator extends AbstractHealthIndicator<Map<String, Object>> {

	@Override
	protected String getName() {
		// The name that will appear in the /health endpoint response
		return "mongoStatus";
	}

	@Override
	protected Map<String, Object> getHealthInformation() {
		try {
			MongoClient client = MongoProvider.getMongoClient();
			Document pingResult = client.getDatabase("admin").withTimeout(10, TimeUnit.SECONDS).runCommand(new Document("ping", 1));
			if (pingResult.containsKey("ok") && pingResult.getDouble("ok") == 1.0) {
				healthStatus = HealthStatus.UP.describe("Able to connect to Mongo instance");
				return Collections.singletonMap("statusDetails", "Able to connect to Mongo instance");
			}
			else {
				healthStatus = HealthStatus.DOWN.describe("Mongo ping invalid");
				return Collections.singletonMap("error", "Mongo ping invalid");
			}
		}
		catch (Exception e) {
			healthStatus = HealthStatus.DOWN.describe("Mongo ping threw exception");
			return Collections.singletonMap("error", "Mongo ping threw exception");
		}
	}
}