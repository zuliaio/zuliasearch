package io.zulia.server.health;

import com.mongodb.client.MongoClient;
import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.indicator.AbstractHealthIndicator;
import io.zulia.server.util.MongoProvider;
import jakarta.inject.Singleton;
import org.jspecify.annotations.NullMarked;

import java.util.Collections;
import java.util.Map;

@Singleton
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
			if(client.listDatabases().first() != null){
				healthStatus = HealthStatus.UP.describe("Able to connect to Mongo instance");
				return Collections.singletonMap("statusDetails", "Able to connect to Mongo instance");
			}
			else {
				healthStatus = HealthStatus.DOWN.describe("Unable to contact Mongo instance");
				return Collections.singletonMap("error", "Mongo is down");
			}
		}
		catch (Exception e) {
			healthStatus = HealthStatus.DOWN.describe(e.getMessage());
			return Collections.singletonMap("error", "Mongo is down");
		}
	}
}