package io.zulia.server.health;

import io.micronaut.management.health.indicator.HealthResult;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@NullMarked
public class HealthResultDTO {
	private static final Logger LOG = LoggerFactory.getLogger(HealthResultDTO.class);
	@Nullable Map<String, HealthResultDetailsDTO> details;
	private String name = "unknown";
	private String status = "UNKNOWN";
	private String host = "UNKNOWN";
	private Instant timestamp = Instant.now();

	public static @Nullable HealthResultDTO fromMicronautHealthResult(HealthResult healthResult) {
		HealthResultDTO result = new HealthResultDTO();
		result.name = healthResult.getName();
		result.status = healthResult.getStatus().getName();
		result.details = new HashMap<>();
		try {
			@SuppressWarnings("unchecked") var healthResultDetailsMap = (Map<String, Object>) healthResult.getDetails();
			for (var entry : healthResultDetailsMap.entrySet()) {
				var detailsResult = (HealthResult) entry.getValue();
				var entryDetails = HealthResultDetailsDTO.fromMicronautHealthResultDetails(detailsResult);
				if (entryDetails != null) {
					result.details.put(entry.getKey(), entryDetails);
					if (result.status.equalsIgnoreCase("UP") && !entryDetails.getStatus().equalsIgnoreCase("UP")) {
						result.status = entryDetails.getStatus();
					}
				}
			}
		}
		catch (ClassCastException e) {
			LOG.error(e.getMessage());
			return null;
		}
		return result;
	}

	public Instant getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Instant timestamp) {
		this.timestamp = timestamp;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public @Nullable Map<String, HealthResultDetailsDTO> getDetails() {
		return details;
	}

	public void setDetails(Map<String, HealthResultDetailsDTO> details) {
		this.details = details;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	@Override
	public String toString() {
		return "IToolsHealthResult{" + "name='" + name + '\'' + ", status='" + status + '\'' + ", host='" + host + '\'' + ", details=" + details
				+ ", timestamp=" + timestamp + '}';
	}
}
