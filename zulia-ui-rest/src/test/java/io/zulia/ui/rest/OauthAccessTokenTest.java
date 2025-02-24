package io.zulia.ui.rest;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.security.endpoints.TokenRefreshRequest;
import io.micronaut.security.token.render.AccessRefreshToken;
import io.micronaut.security.token.render.BearerAccessRefreshToken;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.zulia.ui.rest.persistence.RefreshTokenRepository;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static io.zulia.ui.rest.TestConstants.PASSWORD;
import static io.zulia.ui.rest.TestConstants.USERNAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@MicronautTest(rollback = false)
public class OauthAccessTokenTest {

	@Inject
	@Client("/")
	HttpClient client;

	@Inject
	RefreshTokenRepository refreshTokenRepository;

	@Test
	public void verifyJWTAccessTokenRefreshWorks() throws InterruptedException {

		UsernamePasswordCredentials creds = new UsernamePasswordCredentials(USERNAME, PASSWORD);
		HttpRequest<?> request = HttpRequest.POST("/login", creds);

		var oldTokenCount = refreshTokenRepository.countDocs();
		BearerAccessRefreshToken rsp = client.toBlocking().retrieve(request, BearerAccessRefreshToken.class);
		Thread.sleep(3_000);
		assertEquals(oldTokenCount + 1, refreshTokenRepository.countDocs());

		assertNotNull(rsp.getAccessToken());
		assertNotNull(rsp.getRefreshToken());

		Thread.sleep(1_000); // sleep for one second to give time for the issued at `iat` Claim to change
		AccessRefreshToken refreshResponse = client.toBlocking()
				.retrieve(HttpRequest.POST("/oauth/access_token", new TokenRefreshRequest(TokenRefreshRequest.GRANT_TYPE_REFRESH_TOKEN, rsp.getRefreshToken())),
						AccessRefreshToken.class);

		assertNotNull(refreshResponse.getAccessToken());
		assertNotEquals(rsp.getAccessToken(), refreshResponse.getAccessToken());

		refreshTokenRepository.deleteAllByUsername(USERNAME);
	}

}
