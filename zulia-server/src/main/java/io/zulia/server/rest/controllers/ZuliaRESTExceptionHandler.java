package io.zulia.server.rest.controllers;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.hateoas.JsonError;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import io.zulia.server.exceptions.NotFoundException;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Produces
@Singleton
@Requires(classes = { Throwable.class, ExceptionHandler.class })
public class ZuliaRESTExceptionHandler implements ExceptionHandler<Throwable, HttpResponse<?>> {
	private static final Logger LOG = LoggerFactory.getLogger(Throwable.class);

	@Override
	public HttpResponse<?> handle(HttpRequest request, Throwable throwable) {

		if (throwable instanceof NotFoundException) {
			return HttpResponse.notFound(new JsonError(throwable.getMessage()));
		}
		else if (throwable instanceof IllegalArgumentException) {
			return HttpResponse.badRequest(new JsonError(throwable.getMessage()));
		}

		LOG.error(throwable.getClass().getSimpleName() + ": ", throwable.getMessage(), throwable);
		return HttpResponse.serverError(new JsonError(throwable.getMessage()));
	}

}