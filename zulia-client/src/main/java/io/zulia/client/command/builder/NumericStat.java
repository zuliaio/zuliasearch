package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery.StatRequest;

public class NumericStat implements StatBuilder {

	private final StatRequest.Builder statRequestBuilder;

	public NumericStat(String numericField) {
		statRequestBuilder = StatRequest.newBuilder().setNumericField(numericField);
	}

	@Override
	public StatRequest getStatRequest() {
		return statRequestBuilder.build();
	}
}
