package io.zulia.server.rest.controllers;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.reactivex.Single;

import javax.validation.constraints.NotBlank;

/**
 * @author Graeme Rocher
 * @since 1.0
 */
@Controller("/")
public class HelloController {

	@Get("/hello/{name}")
	public Single<String> hello(@NotBlank String name) {
		return Single.just("Hello " + name + "!");
	}
}