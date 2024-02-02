package io.zulia.rest.dto;

public class StatsDTO {

	private long jvmUsedMemoryMB;
	private long jvmFreeMemoryMB;
	private long jvmTotalMemoryMB;
	private long jvmMaxMemoryMB;
	private double freeDataDirSpaceGB;
	private double totalDataDirSpaceGB;
	private double usedDataDirSpaceGB;
	private String zuliaVersion;

	public StatsDTO() {
	}

	public long getJvmUsedMemoryMB() {
		return jvmUsedMemoryMB;
	}

	public void setJvmUsedMemoryMB(long jvmUsedMemoryMB) {
		this.jvmUsedMemoryMB = jvmUsedMemoryMB;
	}

	public long getJvmFreeMemoryMB() {
		return jvmFreeMemoryMB;
	}

	public void setJvmFreeMemoryMB(long jvmFreeMemoryMB) {
		this.jvmFreeMemoryMB = jvmFreeMemoryMB;
	}

	public long getJvmTotalMemoryMB() {
		return jvmTotalMemoryMB;
	}

	public void setJvmTotalMemoryMB(long jvmTotalMemoryMB) {
		this.jvmTotalMemoryMB = jvmTotalMemoryMB;
	}

	public long getJvmMaxMemoryMB() {
		return jvmMaxMemoryMB;
	}

	public void setJvmMaxMemoryMB(long jvmMaxMemoryMB) {
		this.jvmMaxMemoryMB = jvmMaxMemoryMB;
	}

	public double getFreeDataDirSpaceGB() {
		return freeDataDirSpaceGB;
	}

	public void setFreeDataDirSpaceGB(double freeDataDirSpaceGB) {
		this.freeDataDirSpaceGB = freeDataDirSpaceGB;
	}

	public double getTotalDataDirSpaceGB() {
		return totalDataDirSpaceGB;
	}

	public void setTotalDataDirSpaceGB(double totalDataDirSpaceGB) {
		this.totalDataDirSpaceGB = totalDataDirSpaceGB;
	}

	public double getUsedDataDirSpaceGB() {
		return usedDataDirSpaceGB;
	}

	public void setUsedDataDirSpaceGB(double usedDataDirSpaceGB) {
		this.usedDataDirSpaceGB = usedDataDirSpaceGB;
	}

	public String getZuliaVersion() {
		return zuliaVersion;
	}

	public void setZuliaVersion(String zuliaVersion) {
		this.zuliaVersion = zuliaVersion;
	}

	@Override
	public String toString() {
		return "StatsDTO{" + "jvmUsedMemoryMB=" + jvmUsedMemoryMB + ", jvmFreeMemoryMB=" + jvmFreeMemoryMB + ", jvmTotalMemoryMB=" + jvmTotalMemoryMB
				+ ", jvmMaxMemoryMB=" + jvmMaxMemoryMB + ", freeDataDirSpaceGB=" + freeDataDirSpaceGB + ", totalDataDirSpaceGB=" + totalDataDirSpaceGB
				+ ", usedDataDirSpaceGB=" + usedDataDirSpaceGB + ", zuliaVersion='" + zuliaVersion + '\'' + '}';
	}
}
