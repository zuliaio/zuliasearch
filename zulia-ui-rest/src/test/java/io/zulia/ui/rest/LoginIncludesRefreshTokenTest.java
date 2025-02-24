package io.zulia.ui.rest;

import com.nimbusds.jwt.JWTParser;
import com.nimbusds.jwt.SignedJWT;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.security.token.render.BearerAccessRefreshToken;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.text.ParseException;

import static io.zulia.ui.rest.TestConstants.PASSWORD;
import static io.zulia.ui.rest.TestConstants.USERNAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@MicronautTest
public class LoginIncludesRefreshTokenTest {

	@Inject
	@Client("/")
	HttpClient client;

	@Test
	void uponSuccessfulAuthenticationUserGetsAccessTokenAndRefreshToken() throws ParseException {
		UsernamePasswordCredentials creds = new UsernamePasswordCredentials(USERNAME, PASSWORD);
		HttpRequest<?> request = HttpRequest.POST("/login", creds);
		BearerAccessRefreshToken rsp = client.toBlocking().retrieve(request, BearerAccessRefreshToken.class);

		assertEquals(USERNAME, rsp.getUsername());
		assertNotNull(rsp.getAccessToken());
		assertNotNull(rsp.getRefreshToken());

		assertInstanceOf(SignedJWT.class, JWTParser.parse(rsp.getAccessToken()));
	}

}
