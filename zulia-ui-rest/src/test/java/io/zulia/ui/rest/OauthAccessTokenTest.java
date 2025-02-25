package io.zulia.ui.rest;

import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.security.endpoints.TokenRefreshRequest;
import io.micronaut.security.token.render.AccessRefreshToken;
import io.micronaut.security.token.render.BearerAccessRefreshToken;
import io.zulia.ui.rest.persistence.RefreshTokenRepository;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static io.zulia.ui.rest.TestConstants.PASSWORD;
import static io.zulia.ui.rest.TestConstants.USERNAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class OauthAccessTokenTest {

	EmbeddedServer embeddedServer = ApplicationContext.run(EmbeddedServer.class, Collections.emptyMap());
	HttpClient client = embeddedServer.getApplicationContext().createBean(HttpClient.class, embeddedServer.getURL());
	RefreshTokenRepository refreshTokenRepository = embeddedServer.getApplicationContext().getBean(RefreshTokenRepository.class);

	@Test
	void verifyJWTAccessTokenRefreshWorks() throws InterruptedException {

		UsernamePasswordCredentials creds = new UsernamePasswordCredentials(USERNAME, PASSWORD);
		HttpRequest<?> request = HttpRequest.POST("/login", creds);

		long oldTokenCount = refreshTokenRepository.count();
		BearerAccessRefreshToken rsp = client.toBlocking().retrieve(request, BearerAccessRefreshToken.class);
		Thread.sleep(3_000);
		assertEquals(oldTokenCount + 1, refreshTokenRepository.count());

		assertNotNull(rsp.getAccessToken());
		assertNotNull(rsp.getRefreshToken());

		Thread.sleep(1_000); // sleep for one second to give time for the issued at `iat` Claim to change
		AccessRefreshToken refreshResponse = client.toBlocking()
				.retrieve(HttpRequest.POST("/oauth/access_token", new TokenRefreshRequest(TokenRefreshRequest.GRANT_TYPE_REFRESH_TOKEN, rsp.getRefreshToken())),
						AccessRefreshToken.class); // <1>

		assertNotNull(refreshResponse.getAccessToken());
		assertNotEquals(rsp.getAccessToken(), refreshResponse.getAccessToken()); // <2>

		refreshTokenRepository.deleteAll();
	}

}
