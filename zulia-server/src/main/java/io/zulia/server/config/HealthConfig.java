package io.zulia.server.config;

import org.jspecify.annotations.NullMarked;

@NullMarked
public class HealthConfig {
	private Double memoryThresholdPercent = 90.0;
	private Boolean writeToMongo = false;
	private String db = "zulia";
	private String collection = "health";
	private Long writeIntervalSeconds = 60L;
	private Long ttlDays = 30L;
	private Double systemCpuThresholdPercent = 90.0;
	private Double jvmCpuThresholdPercent = 80.0;

	public Double getMemoryThresholdPercent() {
		return memoryThresholdPercent;
	}

	public void setMemoryThresholdPercent(Double memoryThresholdPercent) {
		this.memoryThresholdPercent = memoryThresholdPercent;
	}

	public Boolean getWriteToMongo() {
		return writeToMongo;
	}

	public void setWriteToMongo(Boolean writeToMongo) {
		this.writeToMongo = writeToMongo;
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public String getCollection() {
		return collection;
	}

	public void setCollection(String collection) {
		this.collection = collection;
	}

	public Long getWriteIntervalSeconds() {
		return writeIntervalSeconds;
	}

	public void setWriteIntervalSeconds(Long writeIntervalSeconds) {
		this.writeIntervalSeconds = writeIntervalSeconds;
	}

	public Long getTtlDays() {
		return ttlDays;
	}

	public void setTtlDays(Long ttlDays) {
		this.ttlDays = ttlDays;
	}

	public Double getSystemCpuThresholdPercent() {
		return systemCpuThresholdPercent;
	}

	public void setSystemCpuThresholdPercent(Double systemCpuThresholdPercent) {
		this.systemCpuThresholdPercent = systemCpuThresholdPercent;
	}

	public Double getJvmCpuThresholdPercent() {
		return jvmCpuThresholdPercent;
	}

	public void setJvmCpuThresholdPercent(Double jvmCpuThresholdPercent) {
		this.jvmCpuThresholdPercent = jvmCpuThresholdPercent;
	}

	@Override
	public String toString() {
		return "HealthConfig{" + "memoryThresholdPercent=" + memoryThresholdPercent + ", writeToMongo=" + writeToMongo + ", db='" + db + '\'' + ", collection='"
				+ collection + '\'' + ", writeIntervalSeconds=" + writeIntervalSeconds + ", ttlDays=" + ttlDays + ", systemCpuThresholdPercent="
				+ systemCpuThresholdPercent + ", jvmCpuThresholdPercent=" + jvmCpuThresholdPercent + '}';
	}
}
