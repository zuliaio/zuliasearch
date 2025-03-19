/*
 * Copyright 2017-2024 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zulia.ui.rest;

import com.nimbusds.jwt.JWTParser;
import com.nimbusds.jwt.SignedJWT;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.security.token.render.BearerAccessRefreshToken;
import io.zulia.ui.rest.beans.UserEntity;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.util.Collections;

import static io.micronaut.http.HttpStatus.OK;
import static io.micronaut.http.HttpStatus.UNAUTHORIZED;
import static io.micronaut.http.MediaType.TEXT_PLAIN;
import static io.zulia.ui.rest.TestConstants.PASSWORD;
import static io.zulia.ui.rest.TestConstants.USERNAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JwtAuthenticationTest {

	EmbeddedServer embeddedServer = ApplicationContext.run(EmbeddedServer.class, Collections.emptyMap());
	HttpClient client = embeddedServer.getApplicationContext().createBean(HttpClient.class, embeddedServer.getURL());

	@Test
	void accessingASecuredUrlWithoutAuthenticatingReturnsUnauthorized() {
		HttpClientResponseException e = assertThrows(HttpClientResponseException.class, () -> {
			client.toBlocking().exchange(HttpRequest.GET("/zuliauirest/").accept(TEXT_PLAIN)); // <3>
		});

		assertEquals(UNAUTHORIZED, e.getStatus()); // <3>
	}

	@Test
	void uponSuccessfulAuthenticationAJsonWebTokenIsIssuedToTheUser() throws ParseException {
		{
			// create user
			UserEntity user = new UserEntity();
			user.setUsername("zulia-test");
			user.setPassword("password");
			HttpRequest<?> request = HttpRequest.POST("/zuliauirest/create-user", user); // <4>
			HttpResponse<BearerAccessRefreshToken> rsp = client.toBlocking().exchange(request, BearerAccessRefreshToken.class);
		}

		UsernamePasswordCredentials creds = new UsernamePasswordCredentials(USERNAME, PASSWORD);
		HttpRequest<?> request = HttpRequest.POST("/zuliauirest/login", creds); // <4>
		HttpResponse<BearerAccessRefreshToken> rsp = client.toBlocking().exchange(request, BearerAccessRefreshToken.class); // <5>
		assertEquals(OK, rsp.getStatus());

		BearerAccessRefreshToken bearerAccessRefreshToken = rsp.body();
		assertEquals(USERNAME, bearerAccessRefreshToken.getUsername());
		assertNotNull(bearerAccessRefreshToken.getAccessToken());
		assertInstanceOf(SignedJWT.class, JWTParser.parse(bearerAccessRefreshToken.getAccessToken()));

		String accessToken = bearerAccessRefreshToken.getAccessToken();
		HttpRequest<?> requestWithAuthorization = HttpRequest.GET("/zuliauirest/").accept(TEXT_PLAIN).bearerAuth(accessToken); // <6>
		HttpResponse<String> response = client.toBlocking().exchange(requestWithAuthorization, String.class);

		assertEquals(OK, rsp.getStatus());
		assertEquals(USERNAME, response.body()); // <7>
	}
}
