package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery.StatRequest;

import java.util.List;
import java.util.logging.Logger;

public class NumericStat implements StatBuilder {

	private final StatRequest.Builder statRequestBuilder;
	private static final Logger LOG = Logger.getLogger(NumericStat.class.getName());

	public NumericStat(String numericField) {
		statRequestBuilder = StatRequest.newBuilder().setNumericField(numericField);
	}

	public NumericStat setPercentiles(List<Double> percentilePoints, Double precision) {
		if (precision > 0.0) {
			statRequestBuilder.setPrecision(precision);

			// Percentiles are a fraction
			if (percentilePoints.stream().allMatch(d -> (0.0 <= d && d <= 1.0))) {
				statRequestBuilder.addAllPercentiles(percentilePoints);
			}
			else {
				LOG.severe("Percentiles must be between 0.0 and 1.0. Request not added");
				statRequestBuilder.clearPrecision();
			}
		}
		else {
			//TODO(Ian): How to handle this?
			LOG.severe("Precision value must be greater than 0.0 to request data percentiles. Request not added");
		}
		return this;
	}

	@Override
	public StatRequest getStatRequest() {
		return statRequestBuilder.build();
	}
}
