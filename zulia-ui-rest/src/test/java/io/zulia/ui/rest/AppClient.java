package io.zulia.ui.rest;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.security.token.render.BearerAccessRefreshToken;
import io.zulia.ui.rest.beans.UserEntity;

import static io.micronaut.http.HttpHeaders.AUTHORIZATION;
import static io.micronaut.http.MediaType.TEXT_PLAIN;

@Client("/zuliauirest")
public interface AppClient {

	@Post("/create-user")
	void createUser(@Body UserEntity user);

	@Post("/login")
	BearerAccessRefreshToken login(@Body UsernamePasswordCredentials credentials);

	@Consumes(TEXT_PLAIN)
	@Get
	String home(@Header(AUTHORIZATION) String authorization);

}
