package io.zulia.server.health;

import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.indicator.AbstractHealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import io.zulia.server.node.ZuliaNode;
import jakarta.inject.Singleton;
import org.jspecify.annotations.NullMarked;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.text.DecimalFormat;
import java.util.LinkedHashMap;
import java.util.Map;

@Singleton
@NullMarked
public class MemoryHealthIndicator extends AbstractHealthIndicator<Map<String, Object>> {
	private final static String percentFormat = "%.2f%%";

	private final Double memoryThreshold;

	public MemoryHealthIndicator(ZuliaNode zuliaNode) {
		this.memoryThreshold = zuliaNode.getZuliaConfig().getHealth().getMemoryThresholdPercent();
	}

	private static String readableMemorySize(long size) {
		if (size <= 0)
			return "0 B";
		// Use the binary units (KiB, MiB, etc.) where 1024 is the factor
		final String[] units = new String[] { "B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB" };
		int digitGroups = (int) (Math.log10(size) / Math.log10(1024));

		// For large numbers, ensure the index is within the bounds of the units array
		if (digitGroups >= units.length) {
			digitGroups = units.length - 1;
		}

		// Format the number to one decimal place with grouping separators
		return new DecimalFormat("#,##0.#").format(size / Math.pow(1024, digitGroups)) + " " + units[digitGroups];
	}

	@Override
	public Map<String, Object> getHealthInformation() {
		MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
		long memUsed = memoryBean.getHeapMemoryUsage().getUsed();
		long memMax = memoryBean.getHeapMemoryUsage().getMax();
		long memCommitted = memoryBean.getHeapMemoryUsage().getCommitted();
		Map<String, Object> detail = new LinkedHashMap<>(5);

		if (100.0 * memUsed / memMax < memoryThreshold) {
			healthStatus = HealthStatus.UP.describe("Memory usage within range");
		}
		else {
			healthStatus = new HealthStatus("DEGRADED", "Memory usage exceeded threshold values", true, 500);
		}

		detail.put("memoryUsed", readableMemorySize(memUsed));
		detail.put("memoryMax", readableMemorySize(memMax));
		detail.put("memoryCommitted", readableMemorySize(memCommitted));
		detail.put("memoryPercent", String.format(percentFormat, 100.0d * (double) memUsed / (double) memMax));
		detail.put("thresholdPercent", String.format(percentFormat, memoryThreshold));
		return detail;
	}

	@Override
	public String getName() {
		return "memoryUsage";
	}

	@Override
	public HealthResult getHealthResult() {
		return super.getHealthResult();
	}
}
