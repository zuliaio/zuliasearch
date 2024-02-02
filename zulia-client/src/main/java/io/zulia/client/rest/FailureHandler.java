package io.zulia.client.rest;

import kong.unirest.core.HttpResponse;
import kong.unirest.core.UnirestParsingException;

import java.util.Optional;
import java.util.function.Consumer;

public class FailureHandler<T> implements Consumer<HttpResponse<T>> {

	@Override
	public void accept(HttpResponse<T> tHttpResponse) {
		Optional<UnirestParsingException> parsingError = tHttpResponse.getParsingError();
		if (parsingError.isPresent()) {
			UnirestParsingException unirestParsingException = parsingError.get();
			throw new RuntimeException("Failed to parse message: " + unirestParsingException.getMessage());
		}
		else {
			throw new RuntimeException(tHttpResponse.getStatus() + "");
		}
	}

}