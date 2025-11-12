package io.zulia.ai.features.stat;

import java.io.Serializable;

public class FeatureStat implements Serializable {

	private double min;
	private double max;
	private double avg;
	private double p05;
	private double p10;
	private double p25;
	private double p50;
	private double p75;
	private double p90;
	private double p95;

	public FeatureStat() {
	}

	public double getMin() {
		return min;
	}

	public void setMin(double min) {
		this.min = min;
	}

	public double getMax() {
		return max;
	}

	public void setMax(double max) {
		this.max = max;
	}

	public double getAvg() {
		return avg;
	}

	public void setAvg(double avg) {
		this.avg = avg;
	}

	public double getP05() {
		return p05;
	}

	public void setP05(double p05) {
		this.p05 = p05;
	}

	public double getP10() {
		return p10;
	}

	public void setP10(double p10) {
		this.p10 = p10;
	}

	public double getP25() {
		return p25;
	}

	public void setP25(double p25) {
		this.p25 = p25;
	}

	public double getP50() {
		return p50;
	}

	public void setP50(double p50) {
		this.p50 = p50;
	}

	public double getP75() {
		return p75;
	}

	public void setP75(double p75) {
		this.p75 = p75;
	}

	public double getP90() {
		return p90;
	}

	public void setP90(double p90) {
		this.p90 = p90;
	}

	public double getP95() {
		return p95;
	}

	public void setP95(double p95) {
		this.p95 = p95;
	}
}
