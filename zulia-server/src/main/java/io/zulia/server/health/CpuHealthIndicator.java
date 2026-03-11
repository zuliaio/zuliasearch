package io.zulia.server.health;

import com.sun.management.OperatingSystemMXBean;
import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.indicator.AbstractHealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import io.zulia.server.node.ZuliaNode;
import jakarta.inject.Singleton;
import org.jspecify.annotations.NullMarked;

import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.Map;

@Singleton
@NullMarked
public class CpuHealthIndicator extends AbstractHealthIndicator<Map<String, Object>> {
	private final static String percentFormat = "%.2f%%";
	private final Double systemThresholdCpu;
	private final Double jvmThresholdCpu;

	public CpuHealthIndicator(ZuliaNode zuliaNode) {
		this.systemThresholdCpu = zuliaNode.getZuliaConfig().getHealth().getSystemCpuThresholdPercent();
		this.jvmThresholdCpu = zuliaNode.getZuliaConfig().getHealth().getJvmCpuThresholdPercent();
	}

	@Override
	protected Map<String, Object> getHealthInformation() {
		OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
		Map<String, Object> detail = new LinkedHashMap<>(5);

		var systemCpu = osBean.getCpuLoad() * 100.0;
		var jvmCpu = osBean.getProcessCpuLoad() * 100.0;

		if (systemCpu < systemThresholdCpu && jvmCpu < jvmThresholdCpu) {
			healthStatus = HealthStatus.UP.describe("CPU usage within range");
		} else {
			healthStatus = new HealthStatus("DEGRADED", "CPU usage exceeded threshold values", true, 500);
		}
		detail.put("system", String.format(percentFormat, systemCpu));
		detail.put("systemAlertThreshold", String.format(percentFormat, systemThresholdCpu));
		detail.put("jvm", String.format(percentFormat, jvmCpu));
		detail.put("jvmAlertThreshold", String.format(percentFormat, jvmThresholdCpu));
		detail.put("numProcessors", osBean.getAvailableProcessors());
		return detail;
	}

	@Override
	protected String getName() {
		return "cpuUsage";
	}

	@Override
	public HealthResult getHealthResult() {
		return super.getHealthResult();
	}
}
