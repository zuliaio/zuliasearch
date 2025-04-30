package io.zulia.ui.rest;

import com.nimbusds.jwt.JWTParser;
import com.nimbusds.jwt.SignedJWT;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.security.token.render.BearerAccessRefreshToken;
import io.zulia.ui.rest.beans.UserEntity;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.util.Collections;

import static io.zulia.ui.rest.TestConstants.PASSWORD;
import static io.zulia.ui.rest.TestConstants.USERNAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class LoginIncludesRefreshTokenTest {

	EmbeddedServer embeddedServer = ApplicationContext.run(EmbeddedServer.class, Collections.emptyMap());
	HttpClient client = embeddedServer.getApplicationContext().createBean(HttpClient.class, embeddedServer.getURL());

	@Test
	void uponSuccessfulAuthenticationUserGetsAccessTokenAndRefreshToken() throws ParseException {
		{
			// create user
			UserEntity user = new UserEntity();
			user.setUsername("zulia-test");
			user.setPassword("password");
			HttpRequest<?> request = HttpRequest.POST("/zuliauirest/create-user", user); // <4>
			HttpResponse<BearerAccessRefreshToken> rsp = client.toBlocking().exchange(request, BearerAccessRefreshToken.class);
		}

		UsernamePasswordCredentials creds = new UsernamePasswordCredentials(USERNAME, PASSWORD);
		HttpRequest<?> request = HttpRequest.POST("/zuliauirest/login", creds);
		BearerAccessRefreshToken rsp = client.toBlocking().retrieve(request, BearerAccessRefreshToken.class);

		assertEquals(USERNAME, rsp.getUsername());
		assertNotNull(rsp.getAccessToken());
		assertNotNull(rsp.getRefreshToken());

		assertInstanceOf(SignedJWT.class, JWTParser.parse(rsp.getAccessToken()));
	}

}
