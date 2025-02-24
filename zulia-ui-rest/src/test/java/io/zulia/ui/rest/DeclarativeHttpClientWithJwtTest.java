package io.zulia.ui.rest;

import com.nimbusds.jwt.JWTParser;
import com.nimbusds.jwt.SignedJWT;
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
public class DeclarativeHttpClientWithJwtTest {

	@Inject
	AppClient appClient;

	@Test
	void verifyJwtAuthenticationWorksWithDeclarativeClient() throws ParseException {
		UsernamePasswordCredentials creds = new UsernamePasswordCredentials(USERNAME, PASSWORD);
		BearerAccessRefreshToken loginRsp = appClient.login(creds);

		assertNotNull(loginRsp);
		assertNotNull(loginRsp.getAccessToken());
		assertInstanceOf(SignedJWT.class, JWTParser.parse(loginRsp.getAccessToken()));

		String msg = appClient.home("Bearer " + loginRsp.getAccessToken());
		assertEquals(USERNAME, msg);
	}

}
