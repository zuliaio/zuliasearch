package io.zulia.ui.rest;

import io.micronaut.context.ApplicationContext;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.endpoints.TokenRefreshRequest;
import io.micronaut.security.token.generator.RefreshTokenGenerator;
import io.micronaut.security.token.render.BearerAccessRefreshToken;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static io.micronaut.http.HttpStatus.BAD_REQUEST;
import static io.zulia.ui.rest.TestConstants.USERNAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RefreshTokenNotFoundTest {

	EmbeddedServer embeddedServer = ApplicationContext.run(EmbeddedServer.class, Collections.emptyMap());
	HttpClient client = embeddedServer.getApplicationContext().createBean(HttpClient.class, embeddedServer.getURL());
	RefreshTokenGenerator refreshTokenGenerator = embeddedServer.getApplicationContext().getBean(RefreshTokenGenerator.class);

	@Test
	void accessingSecuredURLWithoutAuthenticatingReturnsUnauthorized() {
		Authentication user = Authentication.build(USERNAME);

		String refreshToken = refreshTokenGenerator.createKey(user);
		Optional<String> refreshTokenOptional = refreshTokenGenerator.generate(user, refreshToken);
		assertTrue(refreshTokenOptional.isPresent());

		String signedRefreshToken = refreshTokenOptional.get();
		Argument<BearerAccessRefreshToken> bodyArgument = Argument.of(BearerAccessRefreshToken.class);
		Argument<Map> errorArgument = Argument.of(Map.class);
		HttpRequest<?> req = HttpRequest.POST("/zuliauirest/oauth/access_token",
				new TokenRefreshRequest(TokenRefreshRequest.GRANT_TYPE_REFRESH_TOKEN, signedRefreshToken));

		HttpClientResponseException e = assertThrows(HttpClientResponseException.class, () -> {
			client.toBlocking().exchange(req, bodyArgument, errorArgument);
		});
		assertEquals(BAD_REQUEST, e.getStatus());

		Optional<Map> mapOptional = e.getResponse().getBody(Map.class);
		assertTrue(mapOptional.isPresent());

		Map m = mapOptional.get();
		assertEquals("invalid_grant", m.get("error"));
		assertEquals("refresh token not found", m.get("error_description"));
	}
}