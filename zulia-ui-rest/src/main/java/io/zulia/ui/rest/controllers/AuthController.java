package io.zulia.ui.rest.controllers;

import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEvent;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.hateoas.JsonError;
import io.micronaut.http.server.util.HttpHostResolver;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.authentication.Authenticator;
import io.micronaut.security.endpoints.LoginController;
import io.micronaut.security.handlers.LoginHandler;
import io.micronaut.security.handlers.LogoutHandler;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.security.token.claims.ClaimsGenerator;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@Replaces(LoginController.class)
@Requires(beans = LoginHandler.class)
@Requires(beans = Authenticator.class)
@Requires(beans = LogoutHandler.class)
@Controller("auth")
@Secured(SecurityRule.IS_ANONYMOUS)
@ApiResponses({ @ApiResponse(responseCode = "400", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "401", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "500", content = { @Content(schema = @Schema(implementation = JsonError.class)) }) })
public class AuthController {

	@Inject
	protected ClaimsGenerator claimsGenerator;

	protected final LoginHandler<HttpRequest<?>, MutableHttpResponse<?>> loginHandler;
	protected final LogoutHandler<HttpRequest<?>, MutableHttpResponse<?>> logoutHandler;
	protected final HttpHostResolver httpHostResolver;
	protected final ApplicationEventPublisher<ApplicationEvent> eventPublisher;
	private static final Logger LOG = LoggerFactory.getLogger(AuthController.class);

	public AuthController(LoginHandler<HttpRequest<?>, MutableHttpResponse<?>> loginHandler,
			LogoutHandler<HttpRequest<?>, MutableHttpResponse<?>> logoutHandler, HttpHostResolver httpHostResolver,
			ApplicationEventPublisher<ApplicationEvent> eventPublisher) {
		this.loginHandler = loginHandler;
		this.logoutHandler = logoutHandler;
		this.httpHostResolver = httpHostResolver;
		this.eventPublisher = eventPublisher;
	}
}
