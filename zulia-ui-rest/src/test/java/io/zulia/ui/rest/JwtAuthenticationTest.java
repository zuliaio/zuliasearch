package io.zulia.ui.rest;

import com.nimbusds.jwt.JWTParser;
import com.nimbusds.jwt.SignedJWT;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.security.token.render.BearerAccessRefreshToken;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.text.ParseException;

import static io.micronaut.http.HttpStatus.OK;
import static io.micronaut.http.HttpStatus.UNAUTHORIZED;
import static io.micronaut.http.MediaType.TEXT_PLAIN;
import static io.zulia.ui.rest.TestConstants.PASSWORD;
import static io.zulia.ui.rest.TestConstants.USERNAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
public class JwtAuthenticationTest {

	@Inject
	@Client("/")
	HttpClient client;

	@Test
	void accessingASecuredUrlWithoutAuthenticatingReturnsUnauthorized() {
		HttpClientResponseException e = assertThrows(HttpClientResponseException.class, () -> {
			client.toBlocking().exchange(HttpRequest.GET("/").accept(TEXT_PLAIN));
		});

		assertEquals(UNAUTHORIZED, e.getStatus());
	}

	@Test
	void uponSuccessfulAuthenticationAJsonWebTokenIsIssuedToTheUser() throws ParseException {
		UsernamePasswordCredentials creds = new UsernamePasswordCredentials(USERNAME, PASSWORD);
		HttpRequest<?> request = HttpRequest.POST("/login", creds);
		HttpResponse<BearerAccessRefreshToken> rsp = client.toBlocking().exchange(request, BearerAccessRefreshToken.class);
		assertEquals(OK, rsp.getStatus());

		BearerAccessRefreshToken bearerAccessRefreshToken = rsp.body();
		assertEquals(USERNAME, bearerAccessRefreshToken.getUsername());
		assertNotNull(bearerAccessRefreshToken.getAccessToken());
		assertInstanceOf(SignedJWT.class, JWTParser.parse(bearerAccessRefreshToken.getAccessToken()));

		String accessToken = bearerAccessRefreshToken.getAccessToken();
		HttpRequest<?> requestWithAuthorization = HttpRequest.GET("/").accept(TEXT_PLAIN).bearerAuth(accessToken);
		HttpResponse<String> response = client.toBlocking().exchange(requestWithAuthorization, String.class);

		assertEquals(OK, rsp.getStatus());
		assertEquals(USERNAME, response.body());
	}

}
