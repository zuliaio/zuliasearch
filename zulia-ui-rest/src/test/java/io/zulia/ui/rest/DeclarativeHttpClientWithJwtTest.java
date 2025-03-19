package io.zulia.ui.rest;

import com.nimbusds.jwt.JWTParser;
import com.nimbusds.jwt.SignedJWT;
import io.micronaut.context.ApplicationContext;
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

public class DeclarativeHttpClientWithJwtTest {

	EmbeddedServer embeddedServer = ApplicationContext.run(EmbeddedServer.class, Collections.emptyMap());
	AppClient appClient = embeddedServer.getApplicationContext().createBean(AppClient.class, embeddedServer.getURL());

	@Test
	void verifyJwtAuthenticationWorksWithDeclarativeClient() throws ParseException {

		// create user
		UserEntity user = new UserEntity();
		user.setUsername("zulia-test");
		user.setPassword("password");
		appClient.createUser(user);

		UsernamePasswordCredentials creds = new UsernamePasswordCredentials(USERNAME, PASSWORD);
		BearerAccessRefreshToken loginRsp = appClient.login(creds);

		assertNotNull(loginRsp);
		assertNotNull(loginRsp.getAccessToken());
		assertInstanceOf(SignedJWT.class, JWTParser.parse(loginRsp.getAccessToken()));

		String msg = appClient.home("Bearer " + loginRsp.getAccessToken());
		assertEquals(USERNAME, msg);
	}

}
