package io.zulia.ui.rest;

import io.micronaut.openapi.annotation.OpenAPIInclude;
import io.micronaut.openapi.annotation.OpenAPISecurity;
import io.micronaut.runtime.Micronaut;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;

@OpenAPIDefinition(info = @Info(title = "zulia-ui-rest-service", version = "1.0.0"))
@OpenAPISecurity
@OpenAPIInclude(classes = { io.micronaut.security.endpoints.OauthController.class })
public class ZuliaUIREST {
	public static void main(String[] args) {
		Micronaut.run(ZuliaUIREST.class, args);
	}
}
