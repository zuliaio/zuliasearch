package io.zulia.ui.rest;

import io.micronaut.openapi.annotation.OpenAPIInclude;
import io.micronaut.openapi.annotation.OpenAPISecurity;
import io.micronaut.runtime.Micronaut;

@OpenAPISecurity
@OpenAPIInclude(classes = { io.micronaut.security.endpoints.OauthController.class })
public class ZuliaUIREST {
	public static void main(String[] args) {
		Micronaut.run(ZuliaUIREST.class, args);
	}
}
