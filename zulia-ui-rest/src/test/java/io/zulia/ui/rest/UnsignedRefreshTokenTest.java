package io.zulia.ui.rest;

import io.micronaut.context.ApplicationContext;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.security.endpoints.TokenRefreshRequest;
import io.micronaut.security.token.render.BearerAccessRefreshToken;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static io.micronaut.http.HttpStatus.BAD_REQUEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnsignedRefreshTokenTest {

	EmbeddedServer embeddedServer = ApplicationContext.run(EmbeddedServer.class, Collections.emptyMap());
	HttpClient client = embeddedServer.getApplicationContext().createBean(HttpClient.class, embeddedServer.getURL());

	@Test
	void accessingSecuredURLWithoutAuthenticatingReturnsUnauthorized() {

		String unsignedRefreshedToken = "foo";

		var bodyArgument = Argument.of(BearerAccessRefreshToken.class);
		Argument<Map> errorArgument = Argument.of(Map.class);

		HttpClientResponseException e = assertThrows(HttpClientResponseException.class, () -> {
			client.toBlocking().exchange(
					HttpRequest.POST("/oauth/access_token", new TokenRefreshRequest(TokenRefreshRequest.GRANT_TYPE_REFRESH_TOKEN, unsignedRefreshedToken)),
					bodyArgument, errorArgument);
		});
		assertEquals(BAD_REQUEST, e.getStatus());

		Optional<Map> mapOptional = e.getResponse().getBody(Map.class);
		assertTrue(mapOptional.isPresent());

		Map m = mapOptional.get();
		assertEquals("invalid_grant", m.get("error"));
		assertEquals("Refresh token is invalid", m.get("error_description"));
	}

}
