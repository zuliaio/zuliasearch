package io.zulia.server.health;

import io.micronaut.management.health.indicator.HealthResult;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

@NullMarked
public class HealthResultDetailsDTO {
	private static final Logger LOG = LoggerFactory.getLogger(HealthResultDetailsDTO.class);
	private String name = "unknown";
	private String status = "UNKNOWN";
	private @Nullable Map<String, String> details;

	public static @Nullable HealthResultDetailsDTO fromMicronautHealthResultDetails(HealthResult detailsResult) {
		HealthResultDetailsDTO result = new HealthResultDetailsDTO();
		result.setName(detailsResult.getName());
		result.setStatus(detailsResult.getStatus().getName());
		try {
			@SuppressWarnings("unchecked") Map<String, Object> detailsResultMap = (Map<String, Object>) detailsResult.getDetails();
			if (detailsResultMap != null) {
				var stringDetailsMap = new HashMap<String, String>();
				for (var detailsEntry : detailsResultMap.entrySet()) {
					stringDetailsMap.put(detailsEntry.getKey(), detailsEntry.getValue().toString());
				}
				result.setDetails(stringDetailsMap);
			}
		}
		catch (ClassCastException e) {
			LOG.error(e.getMessage());
			return null;
		}
		return result;
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

	public @Nullable Map<String, String> getDetails() {
		return details;
	}

	public void setDetails(Map<String, String> details) {
		this.details = details;
	}

	@Override
	public String toString() {
		return "HealthResultDetails{" + "name='" + name + '\'' + ", status='" + status + '\'' + ", details=" + details + '}';
	}
}
