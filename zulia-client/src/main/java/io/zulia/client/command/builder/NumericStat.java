package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery.StatRequest;

import java.util.List;

public class NumericStat implements StatBuilder {

	private final StatRequest.Builder statRequestBuilder;

	public NumericStat(String numericField) {
		statRequestBuilder = StatRequest.newBuilder().setNumericField(numericField);
	}

	public NumericStat setPercentilePrecision(Double precision) {
		if (precision > 0.0) {
			statRequestBuilder.setPrecision(precision);
		}
		else {
			throw new IllegalArgumentException("Percentile precision must be > 0.0");
		}
		return this;
	}

	public NumericStat setPercentiles(List<Double> percentilePoints) {
		// Percentiles are a fraction
		if (percentilePoints.stream().allMatch(d -> (0.0 <= d && d <= 1.0))) {
			statRequestBuilder.addAllPercentiles(percentilePoints);
		}
		else {
			throw new IllegalArgumentException("Percentiles must be in the range [0.0, 1.0]");
		}
		return this;
	}

	@Override
	public StatRequest getStatRequest() {
		return statRequestBuilder.build();
	}
}
