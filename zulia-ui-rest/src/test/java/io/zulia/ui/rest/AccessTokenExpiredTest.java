package io.zulia.ui.rest;

import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.security.endpoints.TokenRefreshRequest;
import io.micronaut.security.token.render.AccessRefreshToken;
import io.micronaut.security.token.render.BearerAccessRefreshToken;
import io.zulia.ui.rest.persistence.RefreshTokenRepository;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static io.micronaut.http.HttpStatus.OK;
import static io.micronaut.http.HttpStatus.UNAUTHORIZED;
import static io.micronaut.http.MediaType.TEXT_PLAIN;
import static io.zulia.ui.rest.TestConstants.PASSWORD;
import static io.zulia.ui.rest.TestConstants.USERNAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AccessTokenExpiredTest {

	EmbeddedServer embeddedServer = ApplicationContext.run(EmbeddedServer.class, Collections.emptyMap());
	HttpClient client = embeddedServer.getApplicationContext().createBean(HttpClient.class, embeddedServer.getURL());
	RefreshTokenRepository refreshTokenRepository = embeddedServer.getApplicationContext().getBean(RefreshTokenRepository.class);

	@Test
	public void verifyJWTAccessTokenRefreshWorks() throws InterruptedException {

		{
			// Login to obtain an access token
			UsernamePasswordCredentials creds = new UsernamePasswordCredentials(USERNAME, PASSWORD);
			HttpRequest<?> request = HttpRequest.POST("/zuliauirest/login", creds);
			BearerAccessRefreshToken rsp = client.toBlocking().retrieve(request, BearerAccessRefreshToken.class);

			String accessToken = rsp.getAccessToken();

			// test that token can access protected resource
			HttpRequest<?> requestWithAuthorization = HttpRequest.GET("/zuliauirest/").accept(TEXT_PLAIN).bearerAuth(accessToken);
			HttpResponse<String> response = client.toBlocking().exchange(requestWithAuthorization, String.class);
			assertEquals(OK, response.getStatus());

			System.out.println("Expires in: " + rsp.getExpiresIn());

			// Wait for expiration period (30 seconds)
			Thread.sleep(31_000);

			// Attempt to access a protected resource with the expired token
			HttpClientResponseException e = assertThrows(HttpClientResponseException.class, () -> {
				client.toBlocking().exchange(HttpRequest.GET("/zuliauirest/").accept(TEXT_PLAIN));
			});
			assertEquals(UNAUTHORIZED, e.getStatus());
		}

		{
			// Login to obtain an access token
			UsernamePasswordCredentials creds = new UsernamePasswordCredentials(USERNAME, PASSWORD);
			HttpRequest<?> request = HttpRequest.POST("/zuliauirest/login", creds);
			BearerAccessRefreshToken rsp = client.toBlocking().retrieve(request, BearerAccessRefreshToken.class);

			String accessToken = rsp.getAccessToken();
			System.out.println("Access token: " + accessToken);
			// test that token can access protected resource
			HttpRequest<?> requestWithAuthorization = HttpRequest.GET("/zuliauirest/").accept(TEXT_PLAIN).bearerAuth(accessToken);
			HttpResponse<String> response = client.toBlocking().exchange(requestWithAuthorization, String.class);
			assertEquals(OK, response.getStatus());

			System.out.println("Expires in: " + rsp.getExpiresIn() + " and waiting for 10");

			Thread.sleep(10_000);

			// attempt to refresh the expired token
			AccessRefreshToken refreshResponse = client.toBlocking().retrieve(HttpRequest.POST("/zuliauirest/oauth/access_token",
					new TokenRefreshRequest(TokenRefreshRequest.GRANT_TYPE_REFRESH_TOKEN, rsp.getRefreshToken())), AccessRefreshToken.class);

			assertNotNull(refreshResponse.getAccessToken());
			assertNotEquals(rsp.getAccessToken(), refreshResponse.getAccessToken());

			System.out.println("Refreshed access token: " + refreshResponse.getAccessToken());
			// test that refreshed token can access protected resource
			HttpRequest<?> requestWithAuthorization2 = HttpRequest.GET("/zuliauirest/").accept(TEXT_PLAIN).bearerAuth(refreshResponse.getAccessToken());
			HttpResponse<String> response2 = client.toBlocking().exchange(requestWithAuthorization2, String.class);
			assertEquals(OK, response2.getStatus());

			// test that refreshed token can access protected resource
			HttpRequest<?> requestWithAuthorization3 = HttpRequest.GET("/zuliauirest/").accept(TEXT_PLAIN).bearerAuth(accessToken);
			HttpResponse<String> response3 = client.toBlocking().exchange(requestWithAuthorization3, String.class);
			assertEquals(OK, response3.getStatus());
		}

		refreshTokenRepository.deleteAll();

	}

}
